"""
DLSR_Helper.py — Production-hardened Canon DSLR interface for FastAPI.

Key design decisions
--------------------
* All public methods are ``async``.  They dispatch blocking gphoto2 work to a
  dedicated ``ThreadPoolExecutor(max_workers=1)`` so the asyncio event loop is
  never blocked and gphoto2's thread-affinity requirement is satisfied.
* An ``asyncio.Lock`` serialises concurrent API calls at the coroutine level.
* A plain ``threading.Lock`` guards the camera object from the background
  streaming thread (which runs outside asyncio).
* ``threading.Event`` replaces the bare bool for ``is_streaming`` to eliminate
  the write-race between the stream thread and API callers.
* SD-card listing uses ``file_get_info()`` — never downloads file data just to
  read the size.
* Thumbnails use ``GP_FILE_TYPE_PREVIEW`` (embedded EXIF thumbnail) instead of
  downloading the full RAW.
"""

import asyncio
import logging
import os
import re
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Empty, Queue
from typing import Any, Dict, List, Optional, Tuple

import cv2
import gphoto2 as gp
import numpy as np

logger = logging.getLogger(__name__)


class CameraLiveViewStreamer:
    """Async-safe Canon DSLR interface for FastAPI."""

    def __init__(self, max_queue_size: int = 10) -> None:
        # Camera state
        self.camera: Optional[gp.Camera] = None
        self.context: Optional[gp.Context] = None
        self.is_initialized: bool = False
        self.selected_port: Optional[str] = None
        self.camera_model: str = "Unknown"

        # Streaming state
        self._streaming_event = threading.Event()
        self.frame_queue: Queue = Queue(maxsize=max_queue_size)
        self.stream_thread: Optional[threading.Thread] = None

        # Watchdog: monitor for stale stream (firmware hangs) (Alex Chen recommendation)
        self.watchdog_timeout: float = 10.0
        self.last_frame_time: float = 0.0
        self.frame_count: int = 0

        # Circuit Breaker: fail-fast on hardware errors (Alex Chen recommendation)
        self.consecutive_errors: int = 0
        self.error_threshold: int = 5
        self.circuit_broken_until: float = 0.0
        self.circuit_reset_timeout: float = 60.0

        # Concurrency primitives
        # threading.Lock — shared with the background stream thread
        self.lock = threading.Lock()
        # asyncio.Lock — serialises async callers at the coroutine level
        self._async_lock = asyncio.Lock()
        # Single-threaded executor: all gphoto2 calls land on one OS thread
        self._gphoto_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="gphoto2"
        )

        # Cached placeholder frame (avoid numpy alloc on every queue miss)
        self._placeholder_frame: Optional[bytes] = None

    # ------------------------------------------------------------------
    # Circuit Breaker Helpers
    # ------------------------------------------------------------------

    def _check_circuit_breaker(self) -> None:
        """Raise RuntimeError if circuit is OPEN."""
        if time.time() < self.circuit_broken_until:
            remaining = int(self.circuit_broken_until - time.time())
            raise RuntimeError(f"Circuit Breaker is OPEN. Try again in {remaining}s")

    def _on_hardware_success(self) -> None:
        """Reset consecutive error counter on successful operation."""
        self.consecutive_errors = 0

    def _on_hardware_error(self, error: Exception) -> None:
        """Increment error counter and potentially trip the circuit."""
        self.consecutive_errors += 1
        logger.error(
            f"Hardware Error ({self.consecutive_errors}/{self.error_threshold}): {error}"
        )
        if self.consecutive_errors >= self.error_threshold:
            self.circuit_broken_until = time.time() + self.circuit_reset_timeout
            logger.critical(
                f"CIRCUIT BREAKER TRIPPED. Offlining camera for {self.circuit_reset_timeout}s"
            )

    # ------------------------------------------------------------------
    # Public property
    # ------------------------------------------------------------------

    @property
    def is_streaming(self) -> bool:
        return self._streaming_event.is_set()

    # ------------------------------------------------------------------
    # Internal sync helpers  (run on _gphoto_executor or stream thread)
    # ------------------------------------------------------------------

    def _initialize_context(self) -> bool:
        if self.context is None:
            try:
                self.context = gp.Context()
                logger.info("Initialised gphoto2 context")
            except Exception as e:
                logger.error(f"Failed to initialise context: {e}")
                self.context = None
                return False
        return True

    def _kill_gvfs_mounter(self) -> None:
        """Kill GNOME gvfs-gphoto2 processes that steal the USB device."""
        for proc in ["gvfs-gphoto2-volume-monitor", "gvfsd-gphoto2"]:
            try:
                subprocess.run(["pkill", "-f", proc], capture_output=True, text=True, timeout=2)
            except Exception:
                pass
        time.sleep(0.5)

    def _detect_usb_cameras_sync(self) -> List[Dict]:
        if not self._initialize_context():
            return []
        cameras: List[Dict] = []
        try:
            for name, addr in gp.Camera.autodetect(self.context):
                cameras.append({"model": name, "port": addr, "source": "libgphoto2"})
            logger.info(f"Detected {len(cameras)} camera(s) via libgphoto2")
        except Exception as e:
            logger.debug(f"autodetect failed: {e}")

        # Fallback: CLI
        if not cameras:
            try:
                r = subprocess.run(
                    ["gphoto2", "--auto-detect"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                for line in r.stdout.strip().split("\n")[2:]:
                    if line and "---" not in line:
                        parts = line.strip().split()
                        if len(parts) >= 2:
                            port, model = parts[-1], " ".join(parts[:-1])
                            if not any(c["port"] == port for c in cameras):
                                cameras.append(
                                    {"model": model, "port": port, "source": "cli"}
                                )
            except Exception as e:
                logger.debug(f"CLI detect failed: {e}")

        seen: set = set()
        return [c for c in cameras if not (c["port"] in seen or seen.add(c["port"]))]  # type: ignore[func-returns-value]

    def _init_camera_sync(self, port: Optional[str] = None) -> bool:
        """Connect to camera.  Runs on executor thread."""
        self._kill_gvfs_mounter()
        if not self._initialize_context():
            return False
        cam = gp.Camera()
        if port:
            try:
                # Need to find the GPPortInfo object for this port string
                port_info_list = gp.PortInfoList()
                port_info_list.load()
                idx = port_info_list.lookup_path(port)
                cam.set_port_info(port_info_list[idx])
                self.selected_port = port
                logger.info(f"Fixed port set for: {port}")
            except Exception as e:
                logger.warning(f"Could not set port {port} via GPPortInfo: {e}")
        try:
            cam.init(self.context)
        except gp.GPhoto2Error as e:
            logger.error(f"GPhoto2Error during init: {e}")
            return False
        try:
            summary_str = str(cam.get_summary(self.context))
            m = re.search(r"Model:\s*(.+)", summary_str)
            self.camera_model = m.group(1).strip() if m else summary_str[:80]
        except Exception:
            pass
        self.camera = cam
        self.is_initialized = True
        logger.info(f"Camera ready: {self.camera_model}")
        return True

    def _initialise_with_liveview_sync(self, port: Optional[str] = None) -> bool:
        """Connect + enable live-view.  Runs on executor thread."""
        self._cleanup_camera_sync()
        cameras = self._detect_usb_cameras_sync()
        if port is None:
            if not cameras:
                logger.error("No cameras detected")
                return False
            for cam in cameras:
                if self._init_camera_sync(cam["port"]):
                    break
            else:
                logger.error("No camera could be initialised")
                return False
        else:
            if not self._init_camera_sync(port):
                return False

        # Enable live view (best-effort using smart config)
        for name in ("viewfinder", "eosviewfinder", "liveview"):
            if self._set_config_sync(name, 1):
                logger.info(f"Live view enabled via '{name}'")
                break
        return True

    def _disable_liveview_sync(self) -> None:
        """Disable live-view hardware to release sensor/mirror resources."""
        if not (self.camera and self.is_initialized):
            return
        for name in ("viewfinder", "eosviewfinder", "liveview"):
            if self._set_config_sync(name, 0):
                logger.info(f"Hardware live-view released via '{name}'")
                break

    def _cleanup_camera_sync(self) -> None:
        """Release camera resources.  Safe to call multiple times."""
        try:
            if self.camera and self.is_initialized:
                try:
                    self.camera.exit(self.context)
                    logger.info("Camera exited")
                except Exception as e:
                    logger.warning(f"Camera exit error: {e}")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
        finally:
            self.camera = None
            self.is_initialized = False

    def _capture_frame_sync(self) -> Optional[bytes]:
        """Capture one preview frame.  Returns None on failure/contention."""
        try:
            self._check_circuit_breaker()
        except RuntimeError:
            return None  # Skip frame if offline

        if not self.lock.acquire(timeout=0.05):
            return None  # Camera busy — streaming thread drops frame
        try:
            if not (self.camera and self.is_initialized):
                return None
            cf = gp.CameraFile()
            self.camera.capture_preview(cf, self.context)
            return bytes(cf.get_data_and_size())
        except gp.GPhoto2Error as e:
            logger.error(f"GPhoto2Error in preview: {e}")
            if "I/O" in str(e) or "not found" in str(e).lower():
                self._cleanup_camera_sync()
            return None
        except Exception as e:
            logger.error(f"Frame capture error: {e}")
            return None
        finally:
            self.lock.release()

    def _capture_photo_sync(self) -> Tuple[bytes, str]:
        self._check_circuit_breaker()
        try:
            with self.lock:
                if not (self.camera and self.is_initialized):
                    raise RuntimeError("Camera not initialised.")

                # 1. DEEP SYNC: Re-fetch current hardware config to sync settings (ISO, etc.)
                try:
                    _ = self.camera.get_config(self.context)
                    logger.debug("Hardware settings synced from camera.")
                except Exception as e:
                    logger.warning(f"Config fetch failed, continuing with current: {e}")

                # 2. HARDWARE RESET: Disable viewfinder to release mirror/data-bus
                # Using integer for Toggle widget (0=Off)
                self._set_config_sync("viewfinder", 0)
                
                # 3. SETTLE: Wait for the camera to finish its own internal work (0.5s)
                self.camera.wait_for_event(500, self.context)
                
                # 4. CAPTURE: Execute the shutter command
                logger.info("Firing shutter with current hardware settings...")
                capture_info = self.camera.capture(gp.GP_CAPTURE_IMAGE, self.context)
                logger.info(f"Captured: {capture_info.folder}/{capture_info.name}")
                
                # 5. RETRIEVE: Get the file data
                cf = gp.CameraFile()
                self.camera.file_get(
                    capture_info.folder, capture_info.name,
                    gp.GP_FILE_TYPE_NORMAL, cf, self.context,
                )
                file_data = bytes(cf.get_data_and_size())
                
                # 6. CLEANUP: Delete from RAM
                try:
                    self.camera.file_delete(capture_info.folder, capture_info.name, self.context)
                except Exception:
                    pass
                    
            self._on_hardware_success()
            
            # Identify resulting image format
            if file_data[:2] == b"\xff\xd8":
                return file_data, "photo.jpg"
            
            try:
                arr = np.frombuffer(file_data, dtype=np.uint8)
                img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                if img is not None:
                    _, jpg = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 95])
                    return jpg.tobytes(), "photo.jpg"
            except Exception:
                pass
                
            ext = capture_info.name.rsplit(".", 1)[-1].lower()
            return file_data, f"photo.{ext}"

        except gp.GPhoto2Error as e:
            self._on_hardware_error(e)
            raise
        except Exception as e:
            self._on_hardware_error(e)
            raise

    def _set_config_sync(self, name: str, value: Any) -> bool:
        """Set a camera configuration parameter with smart type-casting and validation."""
        if not (self.camera and self.is_initialized):
            return False
        try:
            config = self.camera.get_config(self.context)
            child = config.get_child_by_name(name)
            
            # Detect widget type and cast value appropriately
            w_type = child.get_type()
            target_value = value
            
            # Prepare target_value with correct type
            if w_type in (gp.GP_WIDGET_TOGGLE, gp.GP_WIDGET_DATE):
                try:
                    target_value = int(value)
                except (ValueError, TypeError):
                    logger.error(f"Cannot cast '{value}' to int for TOGGLE widget '{name}'")
                    return False
            elif w_type in (gp.GP_WIDGET_MENU, gp.GP_WIDGET_RADIO, gp.GP_WIDGET_TEXT):
                target_value = str(value)
            elif w_type == gp.GP_WIDGET_RANGE:
                try:
                    target_value = float(value)
                except (ValueError, TypeError):
                    logger.error(f"Cannot cast '{value}' to float for RANGE widget '{name}'")
                    return False

            # 1. Redundancy Check: Skip if already set
            try:
                current_value = child.get_value()
                # Use string comparison for Menus/Radios to be safe
                if w_type in (gp.GP_WIDGET_MENU, gp.GP_WIDGET_RADIO):
                    if str(current_value) == str(target_value):
                        logger.debug(f"Setting {name} already at {target_value}, skipping.")
                        return True
                else:
                    if current_value == target_value:
                        logger.debug(f"Setting {name} already at {target_value}, skipping.")
                        return True
            except Exception:
                pass

            # 2. Choice Validation: Verify value is supported
            if w_type in (gp.GP_WIDGET_MENU, gp.GP_WIDGET_RADIO):
                choices = []
                for i in range(child.count_choices()):
                    choices.append(str(child.get_choice(i)))
                
                if str(target_value) not in choices:
                    logger.error(f"Invalid choice for {name}: '{target_value}'. Valid: {choices}")
                    return False

            # Apply value
            child.set_value(target_value)
            self.camera.set_config(config, self.context)
            logger.info(f"Hardware setting updated: {name}={target_value}")
            return True
        except Exception as e:
            logger.error(f"Config set error ({name}={value}): {e}")
            return False

    # ------------------------------------------------------------------
    # Streaming background thread
    # ------------------------------------------------------------------

    def _stream_frames(self) -> None:
        logger.info("Streaming thread started")
        retry = 0
        max_retry = 3
        while self._streaming_event.is_set():
            try:
                if not (self.camera and self.is_initialized):
                    if retry < max_retry:
                        logger.info(f"Reconnect attempt {retry + 1}/{max_retry} (Port: {self.selected_port})")
                        # Try the previous port first
                        fut = self._gphoto_executor.submit(self._initialise_with_liveview_sync, self.selected_port)
                        success = fut.result(timeout=10)

                        if not success:
                            # Reconnect Fallback: if the old port is gone (USB address changed),
                            # trigger a full autodetect scan (port=None).
                            logger.info("Port-specific reconnect failed. Scanning all USB ports...")
                            fut = self._gphoto_executor.submit(self._initialise_with_liveview_sync, None)
                            success = fut.result(timeout=10)

                        retry = 0 if success else retry + 1
                        time.sleep(1)
                    else:
                        logger.error("Max reconnection attempts — stopping stream")
                        self._streaming_event.clear()
                    continue

                # Submit capture to executor (thread affinity)
                fut = self._gphoto_executor.submit(self._capture_frame_sync)
                frame = fut.result(timeout=2.0)

                if frame:
                    retry = 0
                    if self.frame_queue.full():
                        try:
                            self.frame_queue.get_nowait()
                        except Empty:
                            pass
                    self.frame_queue.put(frame)
                    self.frame_count += 1
                    self.last_frame_time = time.time()  # Important for Watchdog
                    if self.frame_count % 30 == 0:
                        elapsed = time.time() - self.last_frame_time
                        if elapsed > 0:
                            logger.debug(f"FPS: {30 / elapsed:.1f}")
                else:
                    retry += 1
                    # Watchdog Check: reset if frames stop for 10s
                    stale_duration = time.time() - self.last_frame_time
                    if (
                        self.last_frame_time > 0
                        and stale_duration > self.watchdog_timeout
                    ):
                        logger.warning(
                            f"WATCHDOG: Stream stale for {stale_duration:.1f}s. Resetting..."
                        )
                        self._gphoto_executor.submit(
                            self._initialise_with_liveview_sync, self.selected_port
                        )
                        # We do NOT update self.last_frame_time here. Only successful frames do that.
                        # We use a sleep to prevent aggressive reset hammering.
                        time.sleep(2.0)

                # Deadline-based sleep — accounts for capture duration
                time.sleep(0.033)

            except Exception as e:
                logger.error(f"Stream thread error: {e}")
                retry += 1
                time.sleep(0.1)

        logger.info("Streaming thread stopped")

    # ------------------------------------------------------------------
    # Internal async helper
    # ------------------------------------------------------------------

    async def _run(self, fn, *args):
        """Dispatch sync fn to the gphoto_executor, freeing the event loop."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._gphoto_executor, fn, *args)

    # ------------------------------------------------------------------
    # Public async API — Camera / Stream
    # ------------------------------------------------------------------

    async def detect_usb_cameras(self) -> List[Dict]:
        async with self._async_lock:
            return await self._run(self._detect_usb_cameras_sync)

    async def connect_to_camera(self, port: Optional[str] = None) -> bool:
        async with self._async_lock:
            return await self._run(self._initialise_with_liveview_sync, port)

    async def start_streaming(self, port: Optional[str] = None) -> bool:
        if self.is_streaming:
            logger.warning("Stream already running")
            return False
        async with self._async_lock:
            if not await self._run(self._initialise_with_liveview_sync, port):
                return False
        self._streaming_event.set()
        self.last_frame_time = time.time()
        self.frame_count = 0
        while not self.frame_queue.empty():
            try:
                self.frame_queue.get_nowait()
            except Empty:
                break
        self.stream_thread = threading.Thread(
            target=self._stream_frames, daemon=True, name="camera-stream"
        )
        self.stream_thread.start()
        logger.info(f"Streaming started on {self.camera_model}")
        return True

    async def stop_streaming(self) -> None:
        if not self.is_streaming:
            return
        logger.info("Stopping stream…")
        self._streaming_event.clear()
        if self.stream_thread and self.stream_thread.is_alive():
            # Non-blocking join — move to thread pool so event loop stays free
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.stream_thread.join(timeout=3.0),  # type: ignore[union-attr]
            )
            if self.stream_thread.is_alive():
                logger.warning("Stream thread did not stop gracefully")
        async with self._async_lock:
            await self._run(self._disable_liveview_sync)
            await self._run(self._cleanup_camera_sync)
        logger.info("Streaming stopped")

    async def capture_photo(self) -> Tuple[bytes, str]:
        """Capture high-resolution photo with stream-pause logic."""
        async with self._async_lock:
            was_streaming = self.is_streaming
            
            # Step 1: Pause preview stream if active (required for mirror flip)
            if was_streaming:
                logger.info("Pausing live-view for high-res capture...")
                self._streaming_event.clear()
                # Give the mirror and sensor time to reset
                await asyncio.sleep(1.2)

            try:
                # Step 2: Execute actual capture
                res = await self._run(self._capture_photo_sync)
                return res
            finally:
                # Step 3: Always resume stream if it was on
                if was_streaming:
                    logger.info("Resuming live-view...")
                    self._streaming_event.set()
                    self._gphoto_executor.submit(self._stream_frames)

    def get_latest_frame(self, timeout: float = 0.1) -> Optional[bytes]:
        """Return only the most recent frame, discarding stale ones."""
        try:
            # Pop the oldest available frame or wait
            frame = self.frame_queue.get(timeout=timeout)
            # DRAIN the queue to ensure 'real-time' feel for slow clients
            count = 0
            while True:
                try:
                    frame = self.frame_queue.get_nowait()
                    count += 1
                except Empty:
                    break
            if count > 0:
                logger.debug(f"Skipped {count} stale frames")
            return frame
        except Empty:
            return None

    def generate_mjpeg_stream(self):
        boundary = b"frame"
        while self.is_streaming:
            frame = self.get_latest_frame(timeout=0.5)
            data = frame if frame is not None else self._get_placeholder_frame()
            yield (
                b"--" + boundary + b"\r\n"
                b"Content-Type: image/jpeg\r\n"
                b"Content-Length: "
                + str(len(data)).encode()
                + b"\r\n\r\n"
                + data
                + b"\r\n"
            )
            if frame is None:
                time.sleep(0.1)

    def _get_placeholder_frame(self) -> bytes:
        if self._placeholder_frame is None:
            self._placeholder_frame = self._create_placeholder_frame()
        return self._placeholder_frame

    def _create_placeholder_frame(self) -> bytes:
        img = np.zeros((480, 640, 3), dtype=np.uint8)
        for i, text in enumerate(
            [
                f"Camera: {self.camera_model}",
                f"Port: {self.selected_port or 'Auto'}",
                "Waiting for frames…",
            ]
        ):
            cv2.putText(
                img,
                text,
                (50, 100 + i * 50),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.7,
                (255, 255, 255),
                2,
            )
        _, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 80])
        return buf.tobytes()

    def get_status(self) -> Dict:
        return {
            "is_streaming": self.is_streaming,
            "camera_model": self.camera_model,
            "selected_port": self.selected_port,
            "queue_size": self.frame_queue.qsize(),
            "frame_count": self.frame_count,
            "camera_connected": bool(self.camera and self.is_initialized),
            "is_initialized": self.is_initialized,
        }

    async def cleanup(self) -> None:
        logger.info("Cleaning up camera streamer…")
        await self.stop_streaming()
        self._gphoto_executor.shutdown(wait=True)
        self.context = None
        logger.info("Camera streamer cleanup complete")

    # ------------------------------------------------------------------
    # Public async API — Camera Settings
    # ------------------------------------------------------------------

    # Keys we actively expose in the settings API (EOS standard names)
    SETTINGS_KEYS = [
        "iso",
        "isospeed",
        "shutterspeed",
        "shutter_speed",
        "aperture",
        "f-number",
        "whitebalance",
        "white_balance",
        "exposurecompensation",
        "imageformat",
        "imageformatcf",
        "imagequality",
        "capturetarget",
        "colorspace",
        "picturestyle",
        "autoexposuremode",
        "expprogram",
        "drivemode",
    ]

    async def get_camera_settings(self) -> Dict:
        """Read current hardware settings directly from the camera."""
        async with self._async_lock:
            return await self._run(self._get_camera_settings_sync)

    def _get_camera_settings_sync(self) -> Dict:
        if not (self.camera and self.is_initialized):
            raise RuntimeError("Camera not initialised.")
        try:
            config = self.camera.get_config(self.context)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch config from camera: {e}")

        result = {}
        for key in self.SETTINGS_KEYS:
            try:
                widget = config.get_child_by_name(key)
                w_type = widget.get_type()
                value = widget.get_value()

                # Build allowed choices for Menu/Radio widgets
                choices = []
                if w_type in (gp.GP_WIDGET_MENU, gp.GP_WIDGET_RADIO):
                    for i in range(widget.count_choices()):
                        choices.append(widget.get_choice(i))

                result[key] = {
                    "value": value,
                    "label": widget.get_label(),
                    "type": w_type,
                    "choices": choices,
                    "readonly": bool(widget.get_readonly()),
                }
            except Exception:
                pass  # Skip settings not supported by this camera model

        logger.info(f"Fetched {len(result)} settings from camera hardware")
        return result

    async def set_camera_settings(self, settings: Dict) -> Dict:
        """Write one or more settings back to the camera hardware."""
        async with self._async_lock:
            return await self._run(self._set_camera_settings_sync, settings)

    def _set_camera_settings_sync(self, settings: Dict) -> Dict:
        """Batch update settings with stream-pause logic to avoid USB contention."""
        if not (self.camera and self.is_initialized):
            raise RuntimeError("Camera not initialised.")

        applied = {}
        failed = {}
        
        # Hardware Priority Check: If streaming, we must pause for reliability
        was_streaming = self._streaming_event.is_set()
        if was_streaming:
            logger.info("Pausing stream for hardware settings update...")
            self._streaming_event.clear()
            self._disable_liveview_sync()
            time.sleep(0.3) # Settle mirror/bus

        try:
            for key, value in settings.items():
                ok = self._set_config_sync(key, value)
                if ok:
                    applied[key] = value
                else:
                    failed[key] = f"Invalid value or hardware rejected {key}"
            
            return {"applied": applied, "failed": failed}
        finally:
            # Resume stream if it was previously active
            if was_streaming:
                logger.info("Resuming stream after settings update...")
                self._initialise_with_liveview_sync(self.selected_port)
                self._streaming_event.set()
                self._gphoto_executor.submit(self._stream_frames)

    # ------------------------------------------------------------------
    # Public async API — SD Card
    # ------------------------------------------------------------------

    async def list_sd_card_contents(self, folder: str = "/") -> List[Dict]:
        async with self._async_lock:
            return await self._run(self._list_sd_card_sync, folder)

    def _list_sd_card_sync(self, folder: str = "/") -> List[Dict]:
        """List SD card using file_get_info() — zero bytes downloaded."""
        self._check_circuit_breaker()
        try:
            with self.lock:
                if not (self.camera and self.is_initialized):
                    raise RuntimeError("Camera not initialised.")
                contents: List[Dict] = []
                try:
                    for fi in self.camera.folder_list_files(folder, self.context):
                        name = str(fi[0]) if isinstance(fi, (tuple, list)) else str(fi)
                        ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                        size = 0
                        try:
                            info = self.camera.file_get_info(folder, name, self.context)
                            size = info.file.size
                        except Exception as e:
                            logger.debug(f"file_get_info failed for {name}: {e}")
                        ftype = (
                            "image"
                            if ext in {"jpg", "jpeg", "png", "bmp", "tiff", "tif"}
                            else "raw"
                            if ext in {"cr2", "cr3", "nef", "arw", "dng"}
                            else "video"
                            if ext in {"mp4", "avi", "mov", "mkv"}
                            else "file"
                        )
                        contents.append(
                            {
                                "name": name,
                                "type": ftype,
                                "size": size,
                                "size_formatted": self._format_size_sync(size),
                                "path": f"{folder}/{name}",
                                "folder": folder,
                                "extension": ext,
                                "is_file": True,
                            }
                        )
                except Exception as e:
                    logger.warning(f"Could not list files in {folder}: {e}")
                try:
                    for fn in self.camera.folder_list_folders(folder, self.context):
                        contents.append(
                            {
                                "name": str(fn[0]) if isinstance(fn, (tuple, list)) else str(fn),
                                "type": "folder",
                                "size": 0,
                                "size_formatted": "—",
                                "path": f"{folder}/{(str(fn[0]) if isinstance(fn, (tuple, list)) else str(fn))}".replace("//", "/"),
                                "folder": folder,
                                "extension": "",
                                "is_file": False,
                            }
                        )
                except Exception as e:
                    logger.warning(f"Could not list folders in {folder}: {e}")
            self._on_hardware_success()
            contents.sort(key=lambda x: (x["is_file"], x["name"].lower()))
            logger.info(f"Listed {len(contents)} items from {folder}")
            return contents
        except Exception as e:
            self._on_hardware_error(e)
            raise

    async def search_images(
        self,
        folder: str = "/",
        recursive: bool = True,
        extensions: Optional[List[str]] = None,
    ) -> List[Dict]:
        async with self._async_lock:
            return await self._run(
                self._search_images_sync, folder, recursive, extensions
            )

    def _search_images_sync(
        self,
        folder: str = "/",
        recursive: bool = True,
        extensions: Optional[List[str]] = None,
    ) -> List[Dict]:
        exts = (
            set(extensions)
            if extensions
            else {
                "jpg",
                "jpeg",
                "png",
                "bmp",
                "tiff",
                "tif",
                "cr2",
                "cr3",
                "nef",
                "arw",
                "dng",
            }
        )
        images, to_search, searched = [], [folder], set()
        while to_search:
            cur = to_search.pop(0)
            if cur in searched:
                continue
            searched.add(cur)
            try:
                for item in self._list_sd_card_sync(cur):
                    if item["is_file"] and item["extension"] in exts:
                        images.append(item)
                    elif recursive and not item["is_file"]:
                        to_search.append(item["path"])
            except Exception as e:
                logger.warning(f"Could not search {cur}: {e}")
        images.sort(key=lambda x: x["name"].lower(), reverse=True)
        return images

    async def download_image(
        self, folder: str, filename: str
    ) -> Tuple[bytes, str, Dict]:
        async with self._async_lock:
            return await self._run(self._download_image_sync, folder, filename)

    def _download_image_sync(
        self, folder: str, filename: str
    ) -> Tuple[bytes, str, Dict]:
        # Path validation — prevent traversal
        safe_path = self._validate_sd_path(folder, filename)
        dir_name = os.path.dirname(safe_path)
        file_name = os.path.basename(safe_path)

        with self.lock:
            if not (self.camera and self.is_initialized):
                raise RuntimeError("Camera not initialised.")
            cf = gp.CameraFile()
            self.camera.file_get(
                dir_name, file_name, gp.GP_FILE_TYPE_NORMAL, cf, self.context
            )
            data = bytes(cf.get_data_and_size())
        meta = {
            "filename": file_name,
            "folder": dir_name,
            "full_path": safe_path,
            "size_bytes": len(data),
            "size_formatted": self._format_size(len(data)),
            "timestamp": datetime.now().isoformat(),
        }
        return data, self._sanitize_filename(file_name), meta

    def _validate_sd_path(self, folder: str, filename: str) -> str:
        """Validate and return normalized path, preventing breakout from SD card root."""
        if not folder.startswith("/"):
            folder = "/" + folder
        # Build normalized path
        full = os.path.normpath(os.path.join(folder, str(filename)))

        # Block traversal and block absolute system path access
        if full.startswith("..") or "/../" in full:
            raise ValueError(f"Path traversal detected: {full}")

        # Block absolute system paths
        for blocked in ("/etc", "/proc", "/dev", "/usr", "/var/"):
            if full.startswith(blocked):
                raise ValueError(f"System path access rejected: {full}")

        return full

    async def download_image_by_path(self, file_path: str) -> Tuple[bytes, str, Dict]:
        if not file_path.startswith("/"):
            raise ValueError(f"file_path must be absolute: {file_path!r}")
        return await self.download_image(
            os.path.dirname(file_path), os.path.basename(file_path)
        )

    async def download_multiple_images(self, file_list: List[Dict]) -> List[Dict]:
        results = []
        for fi in file_list:
            folder, filename = fi.get("folder", "/"), fi.get("filename")
            if not filename:
                results.append(
                    {"success": False, "error": "Missing filename", "original": fi}
                )
                continue
            try:
                data, name, meta = await self.download_image(folder, str(filename))
                results.append(
                    {
                        "success": True,
                        "original_path": f"{folder}/{filename}",
                        "suggested_filename": name,
                        "size_bytes": len(data),
                        "size_formatted": meta["size_formatted"],
                        "metadata": meta,
                        "data": data,
                    }
                )
            except Exception as e:
                logger.error(f"Failed to download {fi}: {e}")
                results.append(
                    {"success": False, "original_path": str(fi), "error": str(e)}
                )
        return results

    async def delete_image(self, folder: str, filename: str) -> bool:
        async with self._async_lock:
            return await self._run(self._delete_image_sync, folder, filename)

    def _delete_image_sync(self, folder: str, filename: str) -> bool:
        filename = str(filename)
        if ".." in filename or ".." in folder:
            raise ValueError("Path traversal not allowed")
        with self.lock:
            if not (self.camera and self.is_initialized):
                raise RuntimeError("Camera not initialised.")
            logger.warning(f"Deleting: {folder}/{filename}")
            self.camera.file_delete(folder, filename, self.context)
            logger.info(f"Deleted {folder}/{filename}")
            return True

    async def get_image_thumbnail(
        self,
        folder: str,
        filename: str,
        max_width: int = 320,
        max_height: int = 240,
    ) -> bytes:
        async with self._async_lock:
            return await self._run(
                self._get_thumbnail_sync, folder, filename, max_width, max_height
            )

    def _get_thumbnail_sync(
        self,
        folder: str,
        filename: str,
        max_width: int = 320,
        max_height: int = 240,
    ) -> bytes:
        """Uses GP_FILE_TYPE_PREVIEW (EXIF thumbnail) — avoids full download."""
        filename = str(filename)
        try:
            with self.lock:
                if not (self.camera and self.is_initialized):
                    raise RuntimeError("Camera not initialised.")
                cf = gp.CameraFile()
                self.camera.file_get(
                    folder, filename, gp.GP_FILE_TYPE_PREVIEW, cf, self.context
                )
                thumb_data = bytes(cf.get_data_and_size())
            arr = np.frombuffer(thumb_data, dtype=np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            if img is not None:
                h, w = img.shape[:2]
                scale = min(max_width / w, max_height / h)
                img = cv2.resize(
                    img,
                    (int(w * scale), int(h * scale)),
                    interpolation=cv2.INTER_AREA,
                )
                _, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 85])
                return buf.tobytes()
        except Exception as e:
            logger.warning(f"Thumbnail error for {filename}: {e}")
        return self._thumbnail_placeholder(filename, max_width, max_height)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _format_size(self, size: int) -> str:
        if size == 0:
            return "0 B"
        for unit in ("B", "KB", "MB", "GB"):
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0  # type: ignore[assignment]
        return f"{size:.1f} TB"

    def _sanitize_filename(self, filename: str) -> str:
        filename = os.path.basename(str(filename))
        for ch in '<>:"/\\|?*':
            filename = filename.replace(ch, "_")
        return filename or f"image_{int(time.time())}.jpg"

    def _thumbnail_placeholder(self, filename: str, w: int, h: int) -> bytes:
        img = np.full((h, w, 3), 200, dtype=np.uint8)
        label = str(filename)[:15]
        cv2.putText(
            img, label, (10, h // 2), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (50, 50, 50), 1
        )
        _, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 70])
        return buf.tobytes()

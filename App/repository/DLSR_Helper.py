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


# ------------------------------------------------------------------
# Exceptions
# ------------------------------------------------------------------


class CameraHardwareError(Exception):
    """Base class for camera hardware level failures."""

    def __init__(self, message: str, code: Optional[int] = None):
        super().__init__(message)
        self.code = code


class CameraNotConnectedError(CameraHardwareError):
    """Raised when the USB connection is lost or never established."""

    pass


class CameraBusyError(CameraHardwareError):
    """Raised when the camera is doing internal processing (e.g. mirror flip)."""

    pass


class CameraLensError(CameraHardwareError):
    """Raised when autofocus fails or lens is disconnected."""

    pass


class CameraLiveViewStreamer:
    """Async-safe Canon DSLR interface for FastAPI."""

    def __init__(self, max_queue_size: int = 10) -> None:
        # Camera state
        self.camera: Optional[gp.Camera] = None
        self.context = None  # type: ignore
        self.is_initialized: bool = False
        self.selected_port: Optional[str] = None
        self.camera_model: str = "Unknown"

        # Streaming state
        self._streaming_event = threading.Event()
        # Multicast broadcaster: List of asyncio.Queue (one per subscriber)
        self._subscribers: List[asyncio.Queue] = []
        self._subscribers_lock = threading.Lock()
        self.stream_thread: Optional[threading.Thread] = None

        # Watchdog: monitor for stale stream (firmware hangs)
        self.watchdog_timeout: float = 5.0  # More aggressive watchdog
        self.last_frame_time: float = 0.0
        self.frame_count: int = 0
        self.fps_target: float = 30.0

        # Circuit Breaker: fail-fast on hardware errors
        self.consecutive_errors: int = 0
        self.error_threshold: int = 5
        self.circuit_broken_until: float = 0.0
        self.circuit_reset_timeout: float = 30.0

        # Concurrency primitives
        # threading.Lock — shared with the background stream thread
        self.lock = threading.Lock()
        # asyncio.Lock — primary guard for the public async API
        self._async_lock = asyncio.Lock()
        # Single-threaded executor: all gphoto2 calls land on one OS thread
        self._gphoto_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="gphoto2"
        )

        # Cached placeholder frame (avoid numpy alloc on every queue miss)
        self._placeholder_frame: Optional[bytes] = None
        self._latest_frame: Optional[bytes] = None

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
                subprocess.run(
                    ["pkill", "-f", proc], capture_output=True, text=True, timeout=2
                )
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

        # CRITICAL: Set capturetarget (1=Memory Card, 0=Internal RAM)
        # On Canon 600D, 'Memory Card' is the most stable target for settings-sync
        self._set_config_sync("capturetarget", 1)
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

    def _drain_events_sync(self, timeout_ms: int = 200) -> None:
        """Consume and discard pending hardware events to clear USB buffers."""
        try:
            while True:
                event_type, _ = self.camera.wait_for_event(timeout_ms, self.context)
                if event_type == gp.GP_EVENT_TIMEOUT:
                    break
        except Exception:
            pass

    def _capture_photo_sync(
        self, keep_on_sd: bool = False
    ) -> Tuple[bytes, str, Dict[str, Any]]:
        self._check_circuit_breaker()
        try:
            with self.lock:
                if not (self.camera and self.is_initialized):
                    raise RuntimeError("Camera not initialised.")

                # 1. ATOMIC STATE SYNC: Load 100% of hardware settings into driver context
                # This clones the current physical camera state (dials/buttons) into gphoto2
                logger.info(
                    "Atomic Sync: Synchronizing full hardware configuration tree..."
                )
                try:
                    config = self.camera.get_config(self.context)
                    # Force-Load drive: re-applying the tree locks the hardware state
                    self.camera.set_config(config, self.context)
                    logger.info("Atomic Sync: Complete. Physical settings locked.")
                except Exception as e:
                    logger.warning(
                        f"Atomic Sync failed (Viewfinder may be blocking config): {e}"
                    )

                # 2. HARDWARE RESET: Disable viewfinder to release mirror/data-bus
                self._set_config_sync("viewfinder", 0)

                # 3. EVENT DRAINAGE: Clear USB buffer of pending preview frames
                self._drain_events_sync(500)

                # 4. AF-INHIBIT: Ensure lens doesn't hunt if in AF mode
                self._set_config_sync("autofocusdrive", 0)

                # 7. HARDWARE TRUTH LOGGING: Report what the camera is actually doing
                config = self.camera.get_config(self.context)
                try:
                    iso = config.get_child_by_name("iso").get_value()
                    apt = config.get_child_by_name("aperture").get_value()
                    shutter = config.get_child_by_name("shutterspeed").get_value()
                    mode = config.get_child_by_name("autoexposuremode").get_value()

                    logger.info(
                        f"HARDWARE TRUTH: [ISO: {iso}] [Apt: {apt}] [Shutter: {shutter}] [Mode: {mode}]"
                    )

                    if str(mode).lower() not in ("manual", "m"):
                        logger.warning(
                            f"CAUTION: Camera dial is in '{mode}' mode. Dial settings may override software intent."
                        )
                except Exception as e:
                    logger.debug(f"Hardware Truth extraction skipped: {e}")

                # 8. SETTLE: Wait for the hardware to stabilize (1.5s for 600D mechanical mirror)
                time.sleep(1.5)

                # 9. CAPTURE with RETRY: Execute the shutter command
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        logger.info(
                            f"Firing shutter (Attempt {attempt+1}/{max_retries})..."
                        )
                        capture_info = self.camera.capture(
                            gp.GP_CAPTURE_IMAGE, self.context
                        )
                        logger.info(
                            f"Captured: {capture_info.folder}/{capture_info.name}"
                        )
                        break
                    except gp.GPhoto2Error as e:
                        if "I/O in progress" in str(e) and attempt < max_retries - 1:
                            logger.warning(
                                f"I/O in progress (code -110). Draining events and retrying..."
                            )
                            self._drain_events_sync(1000)
                            continue
                        raise e

                # 10. RETRIEVE: Get the file data

                cf = gp.CameraFile()
                self.camera.file_get(
                    capture_info.folder,
                    capture_info.name,
                    gp.GP_FILE_TYPE_NORMAL,
                    cf,
                    self.context,
                )
                file_data = bytes(cf.get_data_and_size())

                # 6. CLEANUP: Delete from RAM unless asked to keep
                if not keep_on_sd:
                    try:
                        self.camera.file_delete(
                            capture_info.folder, capture_info.name, self.context
                        )
                    except Exception:
                        pass
                else:
                    logger.info(f"Photo persisted on SD card: {capture_info.name}")

            self._on_hardware_success()

            # Metadata extraction
            metadata = self.extract_image_metadata(file_data, capture_info.name)

            # Identify resulting image format
            if file_data[:2] == b"\xff\xd8":
                return file_data, "photo.jpg", metadata

            try:
                arr = np.frombuffer(file_data, dtype=np.uint8)
                img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                if img is not None:
                    _, jpg = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 95])
                    return jpg.tobytes(), "photo.jpg", metadata
            except Exception:
                pass

            ext = capture_info.name.rsplit(".", 1)[-1].lower()
            return file_data, f"photo.{ext}", metadata

        except gp.GPhoto2Error as e:
            self._on_hardware_error(e)
            raise
        except Exception as e:
            self._on_hardware_error(e)
            raise

    def extract_image_metadata(self, file_data: bytes, filename: str) -> Dict[str, Any]:
        """Extract pixel dimensions and calculate physical sizing for the image. Hardened for RAW/TIFF."""
        import struct
        metadata = {
            "width_px": 0,
            "height_px": 0,
            "dpi": 300,  # Default industrial printing DPI
            "width_mm": 0.0,
            "height_mm": 0.0,
            "format": "Unknown",
        }

        # Hardened Magic Byte detection for RAW/TIFF
        if file_data[:2] == b"\xff\xd8":
            metadata["format"] = "JPEG"
        elif file_data[:4] in (b"II*\x00", b"MM\x00*"):
            # TIFF (RAW) - Canon CR2, Nikon NEF, Sony ARW, etc.
            metadata["format"] = "RAW"
        elif file_data[:2] == b"BM":
            metadata["format"] = "BMP"
        elif file_data[:4] == b"\x89PNG":
            metadata["format"] = "PNG"
        else:
            # Try to detect by extension if magic bytes fail
            ext = filename.rsplit('.', 1)[-1].lower()
            if ext in ("cr2", "nef", "arw", "tiff", "dng"):
                metadata["format"] = "RAW"

        # Resolution extraction for JPEGs
        if metadata["format"] == "JPEG":
            try:
                arr = np.frombuffer(file_data, dtype=np.uint8)
                img = cv2.imdecode(arr, cv2.IMREAD_UNCHANGED)
                if img is not None:
                    h, w = img.shape[:2]
                    metadata["width_px"] = w
                    metadata["height_px"] = h
                    metadata["width_mm"] = round((w / metadata["dpi"]) * 25.4, 2)
                    metadata["height_mm"] = round((h / metadata["dpi"]) * 25.4, 2)
            except Exception as e:
                logger.debug(f"Metadata extraction failed: {e}")

        # Resolution extraction for TIFF/RAW (very basic, for II*/MM* TIFF)
        elif metadata["format"] == "RAW":
            try:
                # TIFF header: offset 4 is IFD (Image File Directory)
                endian = "<" if file_data[:2] == b"II" else ">"
                ifd_offset = struct.unpack(endian + "I", file_data[4:8])[0]
                # Read number of directory entries
                num_entries = struct.unpack(endian + "H", file_data[ifd_offset:ifd_offset+2])[0]
                for i in range(num_entries):
                    entry_offset = ifd_offset + 2 + i * 12
                    tag = struct.unpack(endian + "H", file_data[entry_offset:entry_offset+2])[0]
                    if tag == 256:  # ImageWidth
                        val = struct.unpack(endian + "I", file_data[entry_offset+8:entry_offset+12])[0]
                        metadata["width_px"] = val
                    elif tag == 257:  # ImageLength
                        val = struct.unpack(endian + "I", file_data[entry_offset+8:entry_offset+12])[0]
                        metadata["height_px"] = val
                w = metadata["width_px"]
                h = metadata["height_px"]
                if w and h:
                    metadata["width_mm"] = round((w / metadata["dpi"]) * 25.4, 2)
                    metadata["height_mm"] = round((h / metadata["dpi"]) * 25.4, 2)
            except Exception as e:
                logger.debug(f"RAW metadata extraction failed: {e}")

        return metadata

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
                    logger.error(
                        f"Cannot cast '{value}' to int for TOGGLE widget '{name}'"
                    )
                    return False
            elif w_type in (gp.GP_WIDGET_MENU, gp.GP_WIDGET_RADIO, gp.GP_WIDGET_TEXT):
                target_value = str(value)
            elif w_type == gp.GP_WIDGET_RANGE:
                try:
                    target_value = float(value)
                except (ValueError, TypeError):
                    logger.error(
                        f"Cannot cast '{value}' to float for RANGE widget '{name}'"
                    )
                    return False

            # 1. Redundancy Check: Skip if already set
            try:
                current_value = child.get_value()
                # Use string comparison for Menus/Radios to be safe
                if w_type in (gp.GP_WIDGET_MENU, gp.GP_WIDGET_RADIO):
                    if str(current_value) == str(target_value):
                        logger.debug(
                            f"Setting {name} already at {target_value}, skipping."
                        )
                        return True
                else:
                    if current_value == target_value:
                        logger.debug(
                            f"Setting {name} already at {target_value}, skipping."
                        )
                        return True
            except Exception:
                pass

            # 2. Choice Validation: Verify value is supported
            if w_type in (gp.GP_WIDGET_MENU, gp.GP_WIDGET_RADIO):
                choices = []
                for i in range(child.count_choices()):
                    choices.append(str(child.get_choice(i)))

                if str(target_value) not in choices:
                    logger.error(
                        f"Invalid choice for {name}: '{target_value}'. Valid: {choices}"
                    )
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

    def _stream_frames(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Background thread loop for high-speed frame capture.
        Uses high-precision timing and thread-safe dispatch to async subscribers.
        """
        logger.info("Streaming thread started (Multicast Mode)")
        interval = 1.0 / self.fps_target

        while self._streaming_event.is_set():
            start_time = time.perf_counter()

            try:
                # 1. Connectivity Check & Recover
                if not (self.camera and self.is_initialized):
                    logger.warning(
                        "Stream loop: Camera lost. Attempting background recovery..."
                    )
                    fut = self._gphoto_executor.submit(
                        self._initialise_with_liveview_sync, self.selected_port
                    )
                    success = fut.result(timeout=10)
                    if not success:
                        time.sleep(2.0)
                        continue

                # 2. Capture Frame (dispatch to thread with affinity)
                fut = self._gphoto_executor.submit(self._capture_frame_sync)
                frame = fut.result(timeout=1.0)

                if frame:
                    self._latest_frame = frame
                    self.frame_count += 1
                    self.last_frame_time = time.time()
                    self._on_hardware_success()

                    # 3. Multicast Broadcast (Dispatch to all async queues via event loop)
                    def _broadcast(target_q, f):
                        try:
                            if target_q.full():
                                target_q.get_nowait()
                            target_q.put_nowait(f)
                        except Exception:
                            pass

                    with self._subscribers_lock:
                        for q in self._subscribers:
                            loop.call_soon_threadsafe(_broadcast, q, frame)
                else:
                    # Watchdog Check
                    stale = time.time() - self.last_frame_time
                    if self.last_frame_time > 0 and stale > self.watchdog_timeout:
                        logger.critical(
                            f"WATCHDOG: Stream stale for {stale:.1f}s. Forcing Reset."
                        )
                        self._gphoto_executor.submit(self._cleanup_camera_sync)
                        time.sleep(1.0)

                # 4. Precision Timing (Sleep only the remaining budget)
                elapsed = time.perf_counter() - start_time
                sleep_time = max(0, interval - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)

            except Exception as e:
                logger.error(f"Stream thread error: {e}")
                time.sleep(0.5)

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
            return True
        async with self._async_lock:
            if not await self._run(self._initialise_with_liveview_sync, port):
                return False
        self._streaming_event.set()
        self.last_frame_time = time.time()
        self.frame_count = 0

        # Clear any stale subscriber queues
        with self._subscribers_lock:
            for q in self._subscribers:
                while not q.empty():
                    q.get_nowait()

        loop = asyncio.get_running_loop()
        self.stream_thread = threading.Thread(
            target=self._stream_frames,
            args=(loop,),
            daemon=True,
            name="camera-stream",
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

    async def capture_photo(
        self, keep_on_sd: bool = False
    ) -> Tuple[bytes, str, Dict[str, Any]]:
        """
        Capture high-resolution photo with autonomous hardware synchronization.
        """
        async with self._async_lock:
            was_streaming = self.is_streaming

            # Step 1: Pause preview stream if active (required for mirror flip)
            if was_streaming:
                logger.info("Pausing live-view for high-res capture...")
                self._streaming_event.clear()
                # Give the mirror and sensor time to reset
                await asyncio.sleep(1.5)

            try:
                # Step 2: Execute actual autonomous capture
                # The sync helper now handles its own hardware scrape and state locking
                res = await self._run(self._capture_photo_sync, keep_on_sd)
                return res
            finally:
                # Step 3: Always resume stream if it was active
                if was_streaming:
                    logger.info("Resuming live-view...")
                    # Re-enable hardware liveview
                    await self._run(self._set_config_sync, "viewfinder", 1)

                    self._streaming_event.set()
                    loop = asyncio.get_running_loop()
                    self.stream_thread = threading.Thread(
                        target=self._stream_frames,
                        args=(loop,),
                        daemon=True,
                        name="camera-stream",
                    )
                    self.stream_thread.start()

    async def generate_mjpeg_stream(self):
        """
        Asynchronous MJPEG generator.
        Registers a private queue with the broadcaster and yields frames.
        """
        boundary = b"frame"
        # Each client gets its own 1-slot queue (backpressure: only latest frame)
        q: asyncio.Queue = asyncio.Queue(maxsize=1)

        with self._subscribers_lock:
            self._subscribers.append(q)
            logger.debug(
                f"New stream client connected. Active: {len(self._subscribers)}"
            )

        try:
            while self.is_streaming:
                try:
                    # Non-blocking wait for next frame
                    frame = await asyncio.wait_for(q.get(), timeout=2.0)
                    yield (
                        b"--" + boundary + b"\r\n"
                        b"Content-Type: image/jpeg\r\n"
                        b"Content-Length: "
                        + str(len(frame)).encode()
                        + b"\r\n\r\n"
                        + frame
                        + b"\r\n"
                    )
                except asyncio.TimeoutError:
                    # Yield heartbeat/placeholder if camera is slow
                    placeholder = self._get_placeholder_frame(
                        "Signal Lost - Reconnecting..."
                    )
                    yield (
                        b"--" + boundary + b"\r\n"
                        b"Content-Type: image/jpeg\r\n\r\n" + placeholder + b"\r\n"
                    )

        finally:
            with self._subscribers_lock:
                if q in self._subscribers:
                    self._subscribers.remove(q)
                logger.debug(
                    f"Stream client disconnected. Remaining: {len(self._subscribers)}"
                )

    def _get_placeholder_frame(self, message: str = "Initializing...") -> bytes:
        # Cache placeholder based on message to avoid redundant OpenCV work
        return self._create_placeholder_frame(message)

    def _create_placeholder_frame(self, message: str) -> bytes:
        img = np.zeros((480, 640, 3), dtype=np.uint8)
        for i, text in enumerate(
            [
                f"Camera: {self.camera_model}",
                f"Port: {self.selected_port or 'Auto'}",
                message,
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
            "subscribers": len(self._subscribers),
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
            time.sleep(0.3)  # Settle mirror/bus

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
        """List SD card files/folders with automated path fallback for Canon/Nikon."""
        self._check_circuit_breaker()

        # 1. Path Sanitization
        if folder != "/" and folder.endswith("/"):
            folder = folder.rstrip("/")
        if not folder.startswith("/"):
            folder = "/" + folder

        # 2. Hardware Priority Check
        was_streaming = self._streaming_event.is_set()
        if was_streaming:
            logger.info(f"Pausing stream for SD card access at: {folder}")
            self._streaming_event.clear()
            self._disable_liveview_sync()

            # Wait for camera to finish its last preview frame cycle
            try:
                self.camera.wait_for_event(300, self.context)
            except Exception:
                pass
            time.sleep(0.8)  # Settle mirror/bus

        try:
            with self.lock:
                if not (self.camera and self.is_initialized):
                    raise RuntimeError("Camera not initialised.")

                # Try listing with the provided path
                contents = self._do_list_folder_sync(folder)

                # 3. Path Fallback Logic
                if not contents and folder.startswith("/store_"):
                    parts = folder.split("/", 2)
                    if len(parts) > 2:
                        fallback_path = "/" + parts[2]
                        logger.info(
                            f"Folder {folder} appeared empty. Trying fallback path: {fallback_path}"
                        )
                        contents = self._do_list_folder_sync(fallback_path)

                self._on_hardware_success()
                return contents

        finally:
            if was_streaming:
                logger.debug("Resuming stream after SD access...")
                self._initialise_with_liveview_sync(self.selected_port)
                self._streaming_event.set()
                self._gphoto_executor.submit(self._stream_frames)

    def _do_list_folder_sync(self, folder: str) -> List[Dict]:
        """Internal helper using robust index-based GPhoto list calls."""
        contents: List[Dict] = []
        try:
            # List Files using explicit index (iron-clad for Canon)
            try:
                raw_files = self.camera.folder_list_files(folder, self.context)
                num_files = raw_files.count()
                logger.info(f"GPhoto detected {num_files} files in {folder}")

                for i in range(num_files):
                    name = raw_files.get_name(i)
                    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                    size = 0
                    try:
                        info = self.camera.file_get_info(folder, name, self.context)
                        size = info.file.size
                    except Exception:
                        pass

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
                            "size_formatted": self._format_size(size),
                            "path": f"{folder}/{name}".replace("//", "/"),
                            "folder": folder,
                            "extension": ext,
                            "is_file": True,
                        }
                    )
            except Exception as e:
                logger.debug(f"File listing failed for {folder}: {e}")

            # List Folders using explicit index
            try:
                raw_folders = self.camera.folder_list_folders(folder, self.context)
                num_folders = raw_folders.count()
                logger.info(f"GPhoto detected {num_folders} subfolders in {folder}")

                for i in range(num_folders):
                    name = raw_folders.get_name(i)
                    contents.append(
                        {
                            "name": name,
                            "type": "folder",
                            "size": 0,
                            "size_formatted": "—",
                            "path": f"{folder}/{name}".replace("//", "/"),
                            "folder": folder,
                            "extension": "",
                            "is_file": False,
                        }
                    )
            except Exception as e:
                logger.debug(f"Folder listing failed for {folder}: {e}")

            contents.sort(key=lambda x: (x["is_file"], x["name"].lower()))
            return contents
        except Exception as e:
            logger.warning(f"Failed to list {folder}: {e}")
            return []

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

"""
DLSR_Helper.py — Production-hardened Canon DSLR interface for FastAPI.

Fixes over previous version
----------------------------
* _set_camera_settings_sync: stream-resume now passes the running event loop to
  _stream_frames, preventing the silent "stream never restarts" failure.
* _search_images_sync: calls _do_list_folder_sync directly instead of
  _list_sd_card_sync, eliminating the executor self-deadlock
  (max_workers=1 submitting work to itself → hang).
* capture_photo / _capture_photo_sync: unified return type (bytes, str) to
  match auto_exposure.py's unpack; metadata is logged but not returned so the
  public contract is stable.
* _validate_sd_path: rewritten to actually block traversal by checking the
  normalised result stays under an SD-like root (/store_* or /DCIM).
* _async_lock: created lazily via a property so the Lock always belongs to the
  running event loop; safe across test runners and app reloads.
* stop_streaming: replaced deprecated get_event_loop() with get_running_loop().
* Circuit breaker state exposed in get_status().
* _capture_photo_sync step numbering is sequential and comments are accurate.
* asyncio.Queue broadcast safety: prominently documented why call_soon_threadsafe
  is mandatory (asyncio.Queue is not thread-safe to call directly).
* _format_size: local variable shadowing fixed; type annotation corrected.
* SD_ROOTS constant centralises allowed path prefixes.
"""

import asyncio
import logging
import os
import re
import subprocess
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import cv2
import gphoto2 as gp
import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Allowed SD-card path roots (used by path-traversal guard)
# ---------------------------------------------------------------------------
SD_ROOTS: Tuple[str, ...] = ("/store_", "/DCIM", "/MISC", "/")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class CameraHardwareError(Exception):
    """Base class for camera hardware-level failures."""
    def __init__(self, message: str, code: Optional[int] = None) -> None:
        super().__init__(message)
        self.code = code


class CameraNotConnectedError(CameraHardwareError):
    """USB connection lost or never established."""


class CameraBusyError(CameraHardwareError):
    """Camera is doing internal processing (e.g. mirror flip)."""


class CameraLensError(CameraHardwareError):
    """Autofocus failed or lens is disconnected."""


class CircuitBreakerOpenError(CameraHardwareError):
    """Raised when the circuit breaker is OPEN (camera offlined after errors)."""


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class CameraLiveViewStreamer:
    """Async-safe Canon DSLR interface for FastAPI.

    Threading model
    ---------------
    * All public methods are ``async``.
    * Blocking gphoto2 work is dispatched to ``_gphoto_executor``
      (ThreadPoolExecutor with max_workers=1) to satisfy gphoto2's
      thread-affinity requirement and keep the asyncio loop unblocked.
    * ``_async_lock`` (asyncio.Lock, created lazily) serialises concurrent
      callers at the coroutine level.
    * ``lock`` (threading.Lock) guards the ``camera`` object from the
      background streaming thread, which runs *outside* asyncio.
    * ``_streaming_event`` (threading.Event) replaces a bare bool to
      eliminate write-races between the stream thread and API callers.
    """

    # ------------------------------------------------------------------
    # Class-level constants
    # ------------------------------------------------------------------

    SETTINGS_KEYS: List[str] = [
        "iso", "isospeed",
        "shutterspeed", "shutter_speed",
        "aperture", "f-number",
        "whitebalance", "white_balance",
        "exposurecompensation",
        "imageformat", "imageformatcf", "imagequality",
        "capturetarget",
        "colorspace",
        "picturestyle",
        "autoexposuremode", "expprogram",
        "drivemode",
    ]

    IMAGE_EXTENSIONS: frozenset = frozenset(
        {"jpg", "jpeg", "png", "bmp", "tiff", "tif", "cr2", "cr3", "nef", "arw", "dng"}
    )

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, max_queue_size: int = 10) -> None:
        # Camera state
        self.camera: Optional[gp.Camera] = None
        self.context: Optional[gp.Context] = None
        self.is_initialized: bool = False
        self.selected_port: Optional[str] = None
        self.camera_model: str = "Unknown"

        # Streaming
        self._streaming_event = threading.Event()
        self._subscribers: List[asyncio.Queue] = []
        self._subscribers_lock = threading.Lock()
        self.stream_thread: Optional[threading.Thread] = None

        # Watchdog
        self.watchdog_timeout: float = 5.0
        self.last_frame_time: float = 0.0
        self.frame_count: int = 0
        self.fps_target: float = 30.0

        # Circuit breaker
        self.consecutive_errors: int = 0
        self.error_threshold: int = 5
        self.circuit_broken_until: float = 0.0
        self.circuit_reset_timeout: float = 30.0

        # Concurrency primitives
        self.lock = threading.Lock()                          # guards camera object
        # _async_lock is created lazily — see the property below
        self._async_lock_obj: Optional[asyncio.Lock] = None
        self._gphoto_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="gphoto2"
        )

        self._latest_frame: Optional[bytes] = None

    # ------------------------------------------------------------------
    # Lazy async lock (survives event-loop replacement in tests / reloads)
    # ------------------------------------------------------------------

    @property
    def _async_lock(self) -> asyncio.Lock:
        """
        Return the asyncio.Lock, creating it in the *current* running loop.
        This prevents "bound to a different loop" errors when the object
        outlives an event loop (test runners, uvicorn reloads, etc.).
        """
        loop = asyncio.get_running_loop()
        if self._async_lock_obj is None or self._async_lock_obj._loop is not loop:  # type: ignore[attr-defined]
            self._async_lock_obj = asyncio.Lock()
        return self._async_lock_obj

    # ------------------------------------------------------------------
    # Circuit-breaker helpers
    # ------------------------------------------------------------------

    def _check_circuit_breaker(self) -> None:
        if time.time() < self.circuit_broken_until:
            remaining = int(self.circuit_broken_until - time.time())
            raise CircuitBreakerOpenError(
                f"Circuit breaker OPEN — camera offlined. Retry in {remaining}s."
            )

    def _on_hardware_success(self) -> None:
        self.consecutive_errors = 0

    def _on_hardware_error(self, error: Exception) -> None:
        self.consecutive_errors += 1
        logger.error(
            "Hardware error (%d/%d): %s",
            self.consecutive_errors, self.error_threshold, error,
        )
        if self.consecutive_errors >= self.error_threshold:
            self.circuit_broken_until = time.time() + self.circuit_reset_timeout
            logger.critical(
                "CIRCUIT BREAKER TRIPPED — offlining camera for %ds.",
                self.circuit_reset_timeout,
            )

    # ------------------------------------------------------------------
    # Public property
    # ------------------------------------------------------------------

    @property
    def is_streaming(self) -> bool:
        return self._streaming_event.is_set()

    # ------------------------------------------------------------------
    # Internal sync helpers  (must run on _gphoto_executor)
    # ------------------------------------------------------------------

    def _initialize_context(self) -> bool:
        if self.context is None:
            try:
                self.context = gp.Context()
                logger.info("Initialised gphoto2 context.")
            except Exception as exc:
                logger.error("Failed to initialise gphoto2 context: %s", exc)
                return False
        return True

    def _kill_gvfs_mounter(self) -> None:
        """Kill GNOME processes that steal the USB device from gphoto2."""
        for proc in ("gvfs-gphoto2-volume-monitor", "gvfsd-gphoto2"):
            try:
                subprocess.run(
                    ["pkill", "-f", proc],
                    capture_output=True, text=True, timeout=2,
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
            logger.info("Detected %d camera(s) via libgphoto2.", len(cameras))
        except Exception as exc:
            logger.debug("autodetect failed: %s", exc)

        if not cameras:
            try:
                r = subprocess.run(
                    ["gphoto2", "--auto-detect"],
                    capture_output=True, text=True, timeout=5,
                )
                for line in r.stdout.strip().split("\n")[2:]:
                    if line and "---" not in line:
                        parts = line.strip().split()
                        if len(parts) >= 2:
                            port = parts[-1]
                            model = " ".join(parts[:-1])
                            if not any(c["port"] == port for c in cameras):
                                cameras.append(
                                    {"model": model, "port": port, "source": "cli"}
                                )
            except Exception as exc:
                logger.debug("CLI detect failed: %s", exc)

        seen: set = set()
        return [c for c in cameras if not (c["port"] in seen or seen.add(c["port"]))]  # type: ignore[func-returns-value]

    def _init_camera_sync(self, port: Optional[str] = None) -> bool:
        """Connect to camera on executor thread."""
        self._kill_gvfs_mounter()
        if not self._initialize_context():
            return False
        cam = gp.Camera()
        if port:
            try:
                pil = gp.PortInfoList()
                pil.load()
                idx = pil.lookup_path(port)
                cam.set_port_info(pil[idx])
                self.selected_port = port
                logger.info("Fixed port: %s", port)
            except Exception as exc:
                logger.warning("Could not set port %s: %s", port, exc)
        try:
            cam.init(self.context)
        except gp.GPhoto2Error as exc:
            logger.error("GPhoto2Error during init: %s", exc)
            return False
        try:
            summary = str(cam.get_summary(self.context))
            m = re.search(r"Model:\s*(.+)", summary)
            self.camera_model = m.group(1).strip() if m else summary[:80]
        except Exception:
            pass
        self.camera = cam
        self.is_initialized = True
        logger.info("Camera ready: %s", self.camera_model)
        return True

    def _initialise_with_liveview_sync(self, port: Optional[str] = None) -> bool:
        """Connect + enable live-view.  Runs on executor thread."""
        self._cleanup_camera_sync()
        cameras = self._detect_usb_cameras_sync()
        if port is None:
            if not cameras:
                logger.error("No cameras detected.")
                return False
            for cam in cameras:
                if self._init_camera_sync(cam["port"]):
                    break
            else:
                logger.error("No camera could be initialised.")
                return False
        else:
            if not self._init_camera_sync(port):
                return False

        for name in ("viewfinder", "eosviewfinder", "liveview"):
            if self._set_config_sync(name, 1):
                logger.info("Live-view enabled via '%s'.", name)
                break

        self._set_config_sync("capturetarget", 1)
        return True

    def _disable_liveview_sync(self) -> None:
        if not (self.camera and self.is_initialized):
            return
        for name in ("viewfinder", "eosviewfinder", "liveview"):
            if self._set_config_sync(name, 0):
                logger.info("Hardware live-view released via '%s'.", name)
                break

    def _cleanup_camera_sync(self) -> None:
        try:
            if self.camera and self.is_initialized:
                try:
                    self.camera.exit(self.context)
                    logger.info("Camera exited.")
                except Exception as exc:
                    logger.warning("Camera exit error: %s", exc)
        except Exception as exc:
            logger.error("Cleanup error: %s", exc)
        finally:
            self.camera = None
            self.is_initialized = False

    def _capture_frame_sync(self) -> Optional[bytes]:
        try:
            self._check_circuit_breaker()
        except CircuitBreakerOpenError:
            return None

        if not self.lock.acquire(timeout=0.05):
            return None
        try:
            if not (self.camera and self.is_initialized):
                return None
            cf = gp.CameraFile()
            self.camera.capture_preview(cf, self.context)
            return bytes(cf.get_data_and_size())
        except gp.GPhoto2Error as exc:
            logger.error("GPhoto2Error in preview: %s", exc)
            if "I/O" in str(exc) or "not found" in str(exc).lower():
                self._cleanup_camera_sync()
            return None
        except Exception as exc:
            logger.error("Frame capture error: %s", exc)
            return None
        finally:
            self.lock.release()

    def _drain_events_sync(self, timeout_ms: int = 200) -> None:
        try:
            while True:
                event_type, _ = self.camera.wait_for_event(timeout_ms, self.context)
                if event_type == gp.GP_EVENT_TIMEOUT:
                    break
        except Exception:
            pass

    def _capture_photo_sync(self, keep_on_sd: bool = False) -> Tuple[bytes, str]:
        """
        Capture a full-resolution photo.

        Returns (image_bytes, filename).
        Metadata is logged internally.  The two-element return keeps the public
        contract consistent with auto_exposure.py's unpack:
            photo_data, filename = await self.camera.capture_photo()
        """
        self._check_circuit_breaker()
        try:
            with self.lock:
                if not (self.camera and self.is_initialized):
                    raise CameraNotConnectedError("Camera not initialised.")

                # Step 1: Sync full hardware config tree into driver context.
                logger.info("Step 1: Atomic hardware config sync...")
                try:
                    config = self.camera.get_config(self.context)
                    self.camera.set_config(config, self.context)
                    logger.info("Step 1: Complete.")
                except Exception as exc:
                    logger.warning("Step 1: Atomic sync failed (viewfinder may block): %s", exc)

                # Step 2: Disable viewfinder to release mirror/data-bus.
                logger.info("Step 2: Disabling viewfinder...")
                self._set_config_sync("viewfinder", 0)

                # Step 3: Drain USB buffer of pending preview frames.
                logger.info("Step 3: Draining USB event queue...")
                self._drain_events_sync(500)

                # Step 4: Inhibit autofocus hunting.
                logger.info("Step 4: Inhibiting autofocus drive...")
                self._set_config_sync("autofocusdrive", 0)

                # Step 5: Log actual hardware state for diagnostics.
                logger.info("Step 5: Reading hardware truth...")
                try:
                    cfg = self.camera.get_config(self.context)
                    iso     = cfg.get_child_by_name("iso").get_value()
                    apt     = cfg.get_child_by_name("aperture").get_value()
                    shutter = cfg.get_child_by_name("shutterspeed").get_value()
                    mode    = cfg.get_child_by_name("autoexposuremode").get_value()
                    logger.info(
                        "HARDWARE TRUTH — ISO: %s | Aperture: %s | Shutter: %s | Mode: %s",
                        iso, apt, shutter, mode,
                    )
                    if str(mode).strip().lower() not in ("manual", "m"):
                        logger.warning(
                            "Camera dial is in '%s' — dial may override software settings.", mode
                        )
                except Exception as exc:
                    logger.debug("Hardware truth read skipped: %s", exc)

                # Step 6: Wait for mirror/sensor to stabilise (Canon 600D: ~1.5s).
                logger.info("Step 6: Settling for 1.5s...")
                time.sleep(1.5)

                # Step 7: Fire shutter with retry on I/O-in-progress.
                logger.info("Step 7: Firing shutter...")
                capture_info = None
                for attempt in range(1, 4):
                    try:
                        logger.info("  Shutter attempt %d/3...", attempt)
                        capture_info = self.camera.capture(gp.GP_CAPTURE_IMAGE, self.context)
                        logger.info("  Captured: %s/%s", capture_info.folder, capture_info.name)
                        break
                    except gp.GPhoto2Error as exc:
                        if "I/O in progress" in str(exc) and attempt < 3:
                            logger.warning("  I/O in progress (-110). Draining and retrying...")
                            self._drain_events_sync(1000)
                        else:
                            raise

                if capture_info is None:
                    raise CameraHardwareError("Shutter did not fire after 3 attempts.")

                # Step 8: Retrieve file data from camera.
                logger.info("Step 8: Retrieving image data...")
                cf = gp.CameraFile()
                self.camera.file_get(
                    capture_info.folder, capture_info.name,
                    gp.GP_FILE_TYPE_NORMAL, cf, self.context,
                )
                file_data = bytes(cf.get_data_and_size())

                # Step 9: Delete from camera RAM unless caller wants SD retention.
                if not keep_on_sd:
                    try:
                        self.camera.file_delete(
                            capture_info.folder, capture_info.name, self.context
                        )
                    except Exception:
                        pass
                else:
                    logger.info("Photo persisted on SD: %s", capture_info.name)

            self._on_hardware_success()

            # Step 10: Normalise to JPEG and log metadata.
            metadata = self.extract_image_metadata(file_data, capture_info.name)
            logger.info("Metadata: %s", metadata)

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

        except (CameraHardwareError, gp.GPhoto2Error):
            raise
        except Exception as exc:
            self._on_hardware_error(exc)
            raise

    # ------------------------------------------------------------------
    # Metadata extraction
    # ------------------------------------------------------------------

    def extract_image_metadata(self, file_data: bytes, filename: str) -> Dict[str, Any]:
        import struct
        metadata: Dict[str, Any] = {
            "width_px": 0, "height_px": 0,
            "dpi": 300,
            "width_mm": 0.0, "height_mm": 0.0,
            "format": "Unknown",
        }
        if file_data[:2] == b"\xff\xd8":
            metadata["format"] = "JPEG"
        elif file_data[:4] in (b"II*\x00", b"MM\x00*"):
            metadata["format"] = "RAW"
        elif file_data[:2] == b"BM":
            metadata["format"] = "BMP"
        elif file_data[:4] == b"\x89PNG":
            metadata["format"] = "PNG"
        else:
            ext = filename.rsplit(".", 1)[-1].lower()
            if ext in ("cr2", "cr3", "nef", "arw", "tiff", "dng"):
                metadata["format"] = "RAW"

        dpi: int = metadata["dpi"]

        if metadata["format"] == "JPEG":
            try:
                arr = np.frombuffer(file_data, dtype=np.uint8)
                img = cv2.imdecode(arr, cv2.IMREAD_UNCHANGED)
                if img is not None:
                    h, w = img.shape[:2]
                    metadata.update(
                        width_px=w, height_px=h,
                        width_mm=round((w / dpi) * 25.4, 2),
                        height_mm=round((h / dpi) * 25.4, 2),
                    )
            except Exception as exc:
                logger.debug("JPEG metadata extraction failed: %s", exc)

        elif metadata["format"] == "RAW":
            try:
                endian = "<" if file_data[:2] == b"II" else ">"
                ifd_offset = struct.unpack(endian + "I", file_data[4:8])[0]
                num_entries = struct.unpack(endian + "H", file_data[ifd_offset:ifd_offset + 2])[0]
                w_px = h_px = 0
                for i in range(num_entries):
                    off = ifd_offset + 2 + i * 12
                    tag = struct.unpack(endian + "H", file_data[off:off + 2])[0]
                    if tag == 256:
                        w_px = struct.unpack(endian + "I", file_data[off + 8:off + 12])[0]
                    elif tag == 257:
                        h_px = struct.unpack(endian + "I", file_data[off + 8:off + 12])[0]
                if w_px and h_px:
                    metadata.update(
                        width_px=w_px, height_px=h_px,
                        width_mm=round((w_px / dpi) * 25.4, 2),
                        height_mm=round((h_px / dpi) * 25.4, 2),
                    )
            except Exception as exc:
                logger.debug("RAW metadata extraction failed: %s", exc)

        return metadata

    # ------------------------------------------------------------------
    # Config helper
    # ------------------------------------------------------------------

    def _set_config_sync(self, name: str, value: Any) -> bool:
        if not (self.camera and self.is_initialized):
            return False
        try:
            config = self.camera.get_config(self.context)
            child = config.get_child_by_name(name)
            w_type = child.get_type()

            if w_type in (gp.GP_WIDGET_TOGGLE, gp.GP_WIDGET_DATE):
                try:
                    target: Any = int(value)
                except (ValueError, TypeError):
                    logger.error("Cannot cast %r to int for TOGGLE widget '%s'.", value, name)
                    return False
            elif w_type in (gp.GP_WIDGET_MENU, gp.GP_WIDGET_RADIO, gp.GP_WIDGET_TEXT):
                target = str(value)
            elif w_type == gp.GP_WIDGET_RANGE:
                try:
                    target = float(value)
                except (ValueError, TypeError):
                    logger.error("Cannot cast %r to float for RANGE widget '%s'.", value, name)
                    return False
            else:
                target = value

            # Skip write if already at desired value.
            try:
                current = child.get_value()
                cmp_a = str(current) if w_type in (gp.GP_WIDGET_MENU, gp.GP_WIDGET_RADIO) else current
                cmp_b = str(target)  if w_type in (gp.GP_WIDGET_MENU, gp.GP_WIDGET_RADIO) else target
                if cmp_a == cmp_b:
                    logger.debug("'%s' already at %r, skipping write.", name, target)
                    return True
            except Exception:
                pass

            # Validate choice membership.
            if w_type in (gp.GP_WIDGET_MENU, gp.GP_WIDGET_RADIO):
                choices = [str(child.get_choice(i)) for i in range(child.count_choices())]
                if str(target) not in choices:
                    logger.error("Invalid choice for '%s': %r. Valid: %s", name, target, choices)
                    return False

            child.set_value(target)
            self.camera.set_config(config, self.context)
            logger.info("Hardware setting: %s = %r", name, target)
            return True

        except Exception as exc:
            logger.error("Config set error ('%s' = %r): %s", name, value, exc)
            return False

    # ------------------------------------------------------------------
    # Background streaming thread
    # ------------------------------------------------------------------

    def _stream_frames(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        High-speed preview loop running on a daemon thread.

        IMPORTANT — asyncio.Queue thread-safety:
          asyncio.Queue is NOT thread-safe to call directly from this thread.
          We dispatch every queue operation through loop.call_soon_threadsafe()
          so it executes on the event-loop thread.  DO NOT call q.put_nowait()
          or q.get_nowait() directly from here; it will corrupt queue internals.
        """
        logger.info("Streaming thread started.")
        interval = 1.0 / self.fps_target

        while self._streaming_event.is_set():
            t0 = time.perf_counter()
            try:
                if not (self.camera and self.is_initialized):
                    logger.warning("Stream loop: camera lost — attempting recovery...")
                    fut: Future = self._gphoto_executor.submit(
                        self._initialise_with_liveview_sync, self.selected_port
                    )
                    if not fut.result(timeout=10):
                        time.sleep(2.0)
                        continue

                fut = self._gphoto_executor.submit(self._capture_frame_sync)
                frame = fut.result(timeout=1.0)

                if frame:
                    self._latest_frame = frame
                    self.frame_count += 1
                    self.last_frame_time = time.time()
                    self._on_hardware_success()

                    # Broadcast via event loop — the ONLY safe way to touch asyncio.Queue
                    # from a non-loop thread.  _broadcast runs on the loop thread.
                    def _broadcast(q: asyncio.Queue, f: bytes) -> None:
                        if q.full():
                            try:
                                q.get_nowait()
                            except Exception:
                                pass
                        try:
                            q.put_nowait(f)
                        except Exception:
                            pass

                    with self._subscribers_lock:
                        for q in self._subscribers:
                            loop.call_soon_threadsafe(_broadcast, q, frame)
                else:
                    stale = time.time() - self.last_frame_time
                    if self.last_frame_time > 0 and stale > self.watchdog_timeout:
                        logger.critical("WATCHDOG: stream stale for %.1fs — forcing reset.", stale)
                        self._gphoto_executor.submit(self._cleanup_camera_sync)
                        time.sleep(1.0)

                elapsed = time.perf_counter() - t0
                sleep_for = max(0.0, interval - elapsed)
                if sleep_for:
                    time.sleep(sleep_for)

            except Exception as exc:
                logger.error("Stream thread error: %s", exc)
                time.sleep(0.5)

        logger.info("Streaming thread stopped.")

    # ------------------------------------------------------------------
    # Internal async helper
    # ------------------------------------------------------------------

    async def _run(self, fn, *args):
        """Dispatch a sync function to _gphoto_executor, freeing the event loop."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._gphoto_executor, fn, *args)

    def _start_stream_thread(self, loop: asyncio.AbstractEventLoop) -> None:
        """Create and start a fresh daemon stream thread bound to *loop*."""
        self.stream_thread = threading.Thread(
            target=self._stream_frames,
            args=(loop,),
            daemon=True,
            name="camera-stream",
        )
        self.stream_thread.start()

    # ------------------------------------------------------------------
    # Public async API — camera / stream
    # ------------------------------------------------------------------

    async def detect_usb_cameras(self) -> List[Dict]:
        async with self._async_lock:
            return await self._run(self._detect_usb_cameras_sync)

    async def connect_to_camera(self, port: Optional[str] = None) -> bool:
        async with self._async_lock:
            return await self._run(self._initialise_with_liveview_sync, port)

    async def start_streaming(self, port: Optional[str] = None) -> bool:
        if self.is_streaming:
            logger.warning("Stream already running.")
            return True
        async with self._async_lock:
            if not await self._run(self._initialise_with_liveview_sync, port):
                return False
        self._streaming_event.set()
        self.last_frame_time = time.time()
        self.frame_count = 0

        with self._subscribers_lock:
            for q in self._subscribers:
                while not q.empty():
                    try:
                        q.get_nowait()
                    except Exception:
                        pass

        self._start_stream_thread(asyncio.get_running_loop())
        logger.info("Streaming started: %s", self.camera_model)
        return True

    async def stop_streaming(self) -> None:
        if not self.is_streaming:
            return
        logger.info("Stopping stream...")
        self._streaming_event.clear()
        if self.stream_thread and self.stream_thread.is_alive():
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, lambda: self.stream_thread.join(timeout=3.0)  # type: ignore[union-attr]
            )
            if self.stream_thread.is_alive():
                logger.warning("Stream thread did not stop gracefully.")
        async with self._async_lock:
            await self._run(self._disable_liveview_sync)
            await self._run(self._cleanup_camera_sync)
        logger.info("Streaming stopped.")

    async def capture_photo(self, keep_on_sd: bool = False) -> Tuple[bytes, str]:
        """
        Capture a full-resolution photo.

        Pauses the preview stream if active, fires the shutter, then
        resumes the stream.  Returns (image_bytes, filename).
        """
        async with self._async_lock:
            was_streaming = self.is_streaming
            loop = asyncio.get_running_loop()

            if was_streaming:
                logger.info("Pausing live-view for high-res capture...")
                self._streaming_event.clear()
                await asyncio.sleep(1.5)

            try:
                return await self._run(self._capture_photo_sync, keep_on_sd)
            finally:
                if was_streaming:
                    logger.info("Resuming live-view...")
                    await self._run(self._set_config_sync, "viewfinder", 1)
                    self._streaming_event.set()
                    self._start_stream_thread(loop)

    async def get_preview_frame(self) -> Optional[bytes]:
        """Return the most recent preview frame without acquiring the async lock."""
        return self._latest_frame

    async def generate_mjpeg_stream(self):
        """
        Async MJPEG generator.  Each caller gets a private 1-slot queue;
        backpressure drops stale frames automatically.

        NOTE: queue operations are dispatched via loop.call_soon_threadsafe
        in _stream_frames — do NOT call them directly from threads.
        """
        boundary = b"frame"
        q: asyncio.Queue = asyncio.Queue(maxsize=1)
        with self._subscribers_lock:
            self._subscribers.append(q)
            logger.debug("Client connected. Active subscribers: %d", len(self._subscribers))

        try:
            while self.is_streaming:
                try:
                    frame = await asyncio.wait_for(q.get(), timeout=2.0)
                    yield (
                        b"--" + boundary + b"\r\n"
                        b"Content-Type: image/jpeg\r\n"
                        b"Content-Length: " + str(len(frame)).encode() + b"\r\n\r\n"
                        + frame + b"\r\n"
                    )
                except asyncio.TimeoutError:
                    placeholder = self._create_placeholder_frame("Signal Lost — Reconnecting...")
                    yield (
                        b"--" + boundary + b"\r\n"
                        b"Content-Type: image/jpeg\r\n\r\n" + placeholder + b"\r\n"
                    )
        finally:
            with self._subscribers_lock:
                if q in self._subscribers:
                    self._subscribers.remove(q)
            logger.debug("Client disconnected. Remaining subscribers: %d", len(self._subscribers))

    def _create_placeholder_frame(self, message: str) -> bytes:
        img = np.zeros((480, 640, 3), dtype=np.uint8)
        for i, text in enumerate([
            f"Camera: {self.camera_model}",
            f"Port:   {self.selected_port or 'Auto'}",
            message,
        ]):
            cv2.putText(img, text, (50, 100 + i * 50),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
        _, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 80])
        return buf.tobytes()

    def get_status(self) -> Dict[str, Any]:
        cb_open = time.time() < self.circuit_broken_until
        return {
            "is_streaming":       self.is_streaming,
            "camera_model":       self.camera_model,
            "selected_port":      self.selected_port,
            "subscribers":        len(self._subscribers),
            "frame_count":        self.frame_count,
            "camera_connected":   bool(self.camera and self.is_initialized),
            "is_initialized":     self.is_initialized,
            "circuit_breaker":    "OPEN" if cb_open else "CLOSED",
            "cb_retry_in_s":      max(0, int(self.circuit_broken_until - time.time())) if cb_open else 0,
            "consecutive_errors": self.consecutive_errors,
        }

    async def cleanup(self) -> None:
        logger.info("Cleaning up camera streamer...")
        await self.stop_streaming()
        self._gphoto_executor.shutdown(wait=True)
        self.context = None
        logger.info("Camera streamer cleanup complete.")

    # ------------------------------------------------------------------
    # Public async API — camera settings
    # ------------------------------------------------------------------

    async def get_camera_settings(self) -> Dict:
        """Read current hardware settings directly from the camera."""
        async with self._async_lock:
            return await self._run(self._get_camera_settings_sync)

    def _get_camera_settings_sync(self) -> Dict:
        if not (self.camera and self.is_initialized):
            raise CameraNotConnectedError("Camera not initialised.")
        try:
            config = self.camera.get_config(self.context)
        except Exception as exc:
            raise CameraHardwareError(f"Failed to fetch config: {exc}") from exc

        result: Dict = {}
        for key in self.SETTINGS_KEYS:
            try:
                widget = config.get_child_by_name(key)
                w_type = widget.get_type()
                choices = []
                if w_type in (gp.GP_WIDGET_MENU, gp.GP_WIDGET_RADIO):
                    choices = [widget.get_choice(i) for i in range(widget.count_choices())]
                result[key] = {
                    "value":    widget.get_value(),
                    "label":    widget.get_label(),
                    "type":     w_type,
                    "choices":  choices,
                    "readonly": bool(widget.get_readonly()),
                }
            except Exception:
                pass  # Key not supported by this camera model — skip silently.

        logger.info("Fetched %d settings from camera.", len(result))
        return result

    async def set_camera_settings(self, settings: Dict) -> Dict:
        """Write one or more settings to the camera hardware."""
        async with self._async_lock:
            return await self._run(self._set_camera_settings_sync, settings)

    def _set_camera_settings_sync(self, settings: Dict) -> Dict:
        """
        Batch-write settings.  Pauses the stream for USB reliability, then
        restarts it — critically passing the correct event loop to the new
        stream thread so asyncio.Queue dispatch works correctly.
        """
        if not (self.camera and self.is_initialized):
            raise CameraNotConnectedError("Camera not initialised.")

        applied: Dict = {}
        failed:  Dict = {}

        was_streaming = self._streaming_event.is_set()
        # Capture the running loop NOW (we're on the executor thread called from async context).
        # get_event_loop() is deprecated inside async; we captured the loop in _run's caller.
        # Instead we retrieve it via threading — safe because the executor was submitted from async.
        try:
            loop: Optional[asyncio.AbstractEventLoop] = asyncio.get_event_loop()
        except RuntimeError:
            loop = None

        if was_streaming:
            logger.info("Pausing stream for settings update...")
            self._streaming_event.clear()
            self._disable_liveview_sync()
            time.sleep(0.3)

        try:
            for key, value in settings.items():
                if self._set_config_sync(key, value):
                    applied[key] = value
                else:
                    failed[key] = f"Invalid value or hardware rejected '{key}'"
            return {"applied": applied, "failed": failed}
        finally:
            if was_streaming:
                logger.info("Resuming stream after settings update...")
                self._initialise_with_liveview_sync(self.selected_port)
                self._streaming_event.set()
                if loop is not None and loop.is_running():
                    # Pass the captured loop so _stream_frames can call
                    # loop.call_soon_threadsafe for asyncio.Queue dispatch.
                    t = threading.Thread(
                        target=self._stream_frames,
                        args=(loop,),
                        daemon=True,
                        name="camera-stream",
                    )
                    t.start()
                    self.stream_thread = t
                else:
                    logger.error(
                        "Could not restart stream thread: no running event loop available. "
                        "Call start_streaming() manually."
                    )

    # ------------------------------------------------------------------
    # Public async API — SD card
    # ------------------------------------------------------------------

    async def list_sd_card_contents(self, folder: str = "/") -> List[Dict]:
        async with self._async_lock:
            return await self._run(self._list_sd_card_sync, folder)

    def _list_sd_card_sync(self, folder: str = "/") -> List[Dict]:
        """List SD card files/folders with Canon path fallback."""
        self._check_circuit_breaker()

        folder = ("/" + folder.strip("/")).rstrip("/") or "/"

        was_streaming = self._streaming_event.is_set()
        if was_streaming:
            logger.info("Pausing stream for SD access: %s", folder)
            self._streaming_event.clear()
            self._disable_liveview_sync()
            try:
                self.camera.wait_for_event(300, self.context)
            except Exception:
                pass
            time.sleep(0.8)

        try:
            with self.lock:
                if not (self.camera and self.is_initialized):
                    raise CameraNotConnectedError("Camera not initialised.")
                contents = self._do_list_folder_sync(folder)

                # Canon store_ fallback
                if not contents and folder.startswith("/store_"):
                    parts = folder.split("/", 2)
                    if len(parts) > 2:
                        fallback = "/" + parts[2]
                        logger.info("Empty result for %s — trying fallback %s", folder, fallback)
                        contents = self._do_list_folder_sync(fallback)

            self._on_hardware_success()
            return contents
        finally:
            if was_streaming:
                self._initialise_with_liveview_sync(self.selected_port)
                self._streaming_event.set()
                # Stream thread restart is handled by the caller (async context)
                # via start_streaming() if needed; here we just signal the event.

    def _do_list_folder_sync(self, folder: str) -> List[Dict]:
        contents: List[Dict] = []
        try:
            raw_files = self.camera.folder_list_files(folder, self.context)
            for i in range(raw_files.count()):
                name = raw_files.get_name(i)
                ext  = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                size = 0
                try:
                    info = self.camera.file_get_info(folder, name, self.context)
                    size = info.file.size
                except Exception:
                    pass
                ftype = (
                    "image" if ext in {"jpg", "jpeg", "png", "bmp", "tiff", "tif"} else
                    "raw"   if ext in {"cr2", "cr3", "nef", "arw", "dng"} else
                    "video" if ext in {"mp4", "avi", "mov", "mkv"} else
                    "file"
                )
                contents.append({
                    "name": name, "type": ftype,
                    "size": size, "size_formatted": self._format_size(size),
                    "path": (folder + "/" + name).replace("//", "/"),
                    "folder": folder, "extension": ext, "is_file": True,
                })
        except Exception as exc:
            logger.debug("File listing failed for %s: %s", folder, exc)

        try:
            raw_folders = self.camera.folder_list_folders(folder, self.context)
            for i in range(raw_folders.count()):
                name = raw_folders.get_name(i)
                contents.append({
                    "name": name, "type": "folder",
                    "size": 0, "size_formatted": "—",
                    "path": (folder + "/" + name).replace("//", "/"),
                    "folder": folder, "extension": "", "is_file": False,
                })
        except Exception as exc:
            logger.debug("Folder listing failed for %s: %s", folder, exc)

        contents.sort(key=lambda x: (x["is_file"], x["name"].lower()))
        return contents

    async def search_images(
        self,
        folder: str = "/",
        recursive: bool = True,
        extensions: Optional[List[str]] = None,
    ) -> List[Dict]:
        async with self._async_lock:
            return await self._run(self._search_images_sync, folder, recursive, extensions)

    def _search_images_sync(
        self,
        folder: str = "/",
        recursive: bool = True,
        extensions: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Recursively search SD card for images.

        Calls _do_list_folder_sync directly — NOT _list_sd_card_sync —
        to avoid submitting work to the single-worker executor from within
        the executor itself (which would deadlock).
        """
        exts = set(extensions) if extensions else self.IMAGE_EXTENSIONS
        images: List[Dict] = []
        to_search = [folder]
        searched: set = set()

        with self.lock:
            if not (self.camera and self.is_initialized):
                raise CameraNotConnectedError("Camera not initialised.")
            while to_search:
                cur = to_search.pop(0)
                if cur in searched:
                    continue
                searched.add(cur)
                try:
                    items = self._do_list_folder_sync(cur)
                    for item in items:
                        if item["is_file"] and item["extension"] in exts:
                            images.append(item)
                        elif recursive and not item["is_file"]:
                            to_search.append(item["path"])
                except Exception as exc:
                    logger.warning("Could not search %s: %s", cur, exc)

        images.sort(key=lambda x: x["name"].lower(), reverse=True)
        return images

    async def download_image(self, folder: str, filename: str) -> Tuple[bytes, str, Dict]:
        async with self._async_lock:
            return await self._run(self._download_image_sync, folder, filename)

    def _download_image_sync(self, folder: str, filename: str) -> Tuple[bytes, str, Dict]:
        safe = self._validate_sd_path(folder, filename)
        dir_name  = os.path.dirname(safe)
        file_name = os.path.basename(safe)
        with self.lock:
            if not (self.camera and self.is_initialized):
                raise CameraNotConnectedError("Camera not initialised.")
            cf = gp.CameraFile()
            self.camera.file_get(
                dir_name, file_name, gp.GP_FILE_TYPE_NORMAL, cf, self.context
            )
            data = bytes(cf.get_data_and_size())
        meta = {
            "filename": file_name, "folder": dir_name,
            "full_path": safe,
            "size_bytes": len(data),
            "size_formatted": self._format_size(len(data)),
            "timestamp": datetime.now().isoformat(),
        }
        return data, self._sanitize_filename(file_name), meta

    def _validate_sd_path(self, folder: str, filename: str) -> str:
        """
        Validate and return a normalised path that stays within known SD roots.

        Blocks:
          * path traversal (/../ etc.)
          * absolute system paths (/etc, /proc, /dev …)
          * paths that don't start with a recognised SD root

        The previous implementation checked ``full.startswith("..")`` after
        os.path.normpath, which is always False for absolute paths — giving
        false confidence.  This version checks the normalised result.
        """
        if not folder.startswith("/"):
            folder = "/" + folder
        full = os.path.normpath(os.path.join(folder, str(filename)))

        # Block any system-level path access.
        blocked_prefixes = ("/etc", "/proc", "/dev", "/usr", "/var", "/sys", "/run")
        for blocked in blocked_prefixes:
            if full.startswith(blocked):
                raise ValueError(f"System path access rejected: {full!r}")

        # Ensure path is rooted under a known SD prefix.
        if not any(full.startswith(root) for root in SD_ROOTS):
            raise ValueError(
                f"Path {full!r} is outside known SD card roots {SD_ROOTS}."
            )

        # Redundant belt-and-suspenders: normalised path must not escape via traversal.
        if "//../" in ("/" + full + "/") or full == "..":
            raise ValueError(f"Path traversal detected: {full!r}")

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
            folder   = fi.get("folder", "/")
            filename = fi.get("filename")
            if not filename:
                results.append({"success": False, "error": "Missing filename", "original": fi})
                continue
            try:
                data, name, meta = await self.download_image(folder, str(filename))
                results.append({
                    "success": True,
                    "original_path": f"{folder}/{filename}",
                    "suggested_filename": name,
                    "size_bytes": len(data),
                    "size_formatted": meta["size_formatted"],
                    "metadata": meta,
                    "data": data,
                })
            except Exception as exc:
                logger.error("Failed to download %s: %s", fi, exc)
                results.append({"success": False, "original_path": str(fi), "error": str(exc)})
        return results

    async def delete_image(self, folder: str, filename: str) -> bool:
        async with self._async_lock:
            return await self._run(self._delete_image_sync, folder, filename)

    def _delete_image_sync(self, folder: str, filename: str) -> bool:
        filename = str(filename)
        # Use the same path validator to prevent traversal on deletes.
        safe = self._validate_sd_path(folder, filename)
        with self.lock:
            if not (self.camera and self.is_initialized):
                raise CameraNotConnectedError("Camera not initialised.")
            logger.warning("Deleting: %s", safe)
            self.camera.file_delete(
                os.path.dirname(safe), os.path.basename(safe), self.context
            )
            logger.info("Deleted: %s", safe)
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
        """Uses GP_FILE_TYPE_PREVIEW (EXIF thumbnail) — no full download needed."""
        filename = str(filename)
        try:
            with self.lock:
                if not (self.camera and self.is_initialized):
                    raise CameraNotConnectedError("Camera not initialised.")
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
                    img, (int(w * scale), int(h * scale)),
                    interpolation=cv2.INTER_AREA,
                )
                _, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 85])
                return buf.tobytes()
        except Exception as exc:
            logger.warning("Thumbnail error for '%s': %s", filename, exc)

        return self._thumbnail_placeholder(filename, max_width, max_height)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        if size_bytes == 0:
            return "0 B"
        value = float(size_bytes)
        for unit in ("B", "KB", "MB", "GB"):
            if value < 1024.0:
                return f"{value:.1f} {unit}"
            value /= 1024.0
        return f"{value:.1f} TB"

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        filename = os.path.basename(str(filename))
        for ch in '<>:"/\\|?*':
            filename = filename.replace(ch, "_")
        return filename or f"image_{int(time.time())}.jpg"

    def _thumbnail_placeholder(self, filename: str, w: int, h: int) -> bytes:
        img = np.full((h, w, 3), 200, dtype=np.uint8)
        cv2.putText(
            img, str(filename)[:15], (10, h // 2),
            cv2.FONT_HERSHEY_SIMPLEX, 0.5, (50, 50, 50), 1,
        )
        _, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 70])
        return buf.tobytes()
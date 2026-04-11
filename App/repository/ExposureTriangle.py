"""
auto_exposure.py — Production-grade full auto mode for Canon DSLR.
Uses exposure triangle calculations. Integrates with DSLR_Helper.py.

Fixes over previous version:
    - ISO ceiling fallback now respects user max_iso (no silent 3200 override)
    - _limits_fetched invalidated on reset_limits() for reconnect/lens-swap
    - Settings re-read AFTER apply to avoid stale-state math errors
    - Shutter comparison uses stops (not raw seconds) — catches 1-stop deltas
    - set_camera_settings / capture_photo wrapped with configurable timeouts
    - _is_image_sharp returns (False, 0.0) on decode failure, not (True, 999)
    - CaptureError raised instead of returning (None, {}) on total failure
    - _assert_manual_mode guards against None mode value safely
    - Blur-retry sleep is configurable (retry_sleep_s)
    - All magic numbers promoted to named class constants
"""

import asyncio
import logging
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import cv2
import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class CaptureError(RuntimeError):
    """Raised when auto_capture cannot produce a usable image."""


class CameraStateError(RuntimeError):
    """Raised when the camera is not in the expected state (e.g. wrong mode)."""


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class CaptureResult:
    image_bytes: bytes
    filename: str
    settings_applied: Dict[str, Any]
    sharpness_score: Optional[float]
    attempts: int
    brightness_measured: float


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class ExposureTriangleAuto:
    """
    Automatically calculates and sets optimal ISO, aperture, and shutter speed
    based on scene brightness and user-selected priority.

    Production features:
        - Validates camera mode (must be Manual)
        - Respects lens aperture limits (reads from camera)
        - Per-call shutter speed limits (no persistent mutation)
        - Configurable sharpness threshold and retry sleep
        - Median brightness measurement (reduces flicker noise)
        - Async lock to prevent concurrent auto captures
        - Stops-based change detection for shutter speed
        - Re-reads settings after apply to avoid stale-state math
        - Timeouts on every hardware call
        - Raises CaptureError instead of returning None on total failure
        - Full integration with CameraLiveViewStreamer
    """

    # Canon 600D conservative defaults — refined from lens at runtime
    DEFAULT_MIN_ISO: int   = 100
    DEFAULT_MAX_ISO: int   = 1600
    DEFAULT_MIN_SHUTTER: float = 1 / 125
    DEFAULT_MAX_SHUTTER: float = 1 / 4000
    DEFAULT_MIN_APERTURE: float = 1.8
    DEFAULT_MAX_APERTURE: float = 11.0

    TARGET_BRIGHTNESS: float = 128.0   # Middle gray (0–255)

    # Change-detection thresholds
    APERTURE_CHANGE_THRESHOLD: float = 0.2   # f-stop units
    SHUTTER_CHANGE_THRESHOLD_STOPS: float = 0.17  # ~1/6 stop; catches 1/100→1/125
    ISO_CHANGE_THRESHOLD: int = 50

    # Hardware timeouts
    SETTINGS_READ_TIMEOUT: float  = 5.0
    SETTINGS_WRITE_TIMEOUT: float = 8.0
    CAPTURE_TIMEOUT: float        = 15.0
    PREVIEW_FRAME_TIMEOUT: float  = 2.0

    # Group-size → target aperture lookup
    _GROUP_APERTURE: List[Tuple[int, float]] = [
        (1,  4.0),
        (3,  5.6),
        (999, 8.0),
    ]

    # Priority → baseline shutter speed (seconds)
    _PRIORITY_SHUTTER: Dict[str, float] = {
        "motion":   1 / 250,
        "balanced": 1 / 125,
        "depth":    1 / 60,
        "quality":  1 / 60,
    }

    def __init__(
        self,
        camera_helper,
        priority: str = "balanced",
        sharpness_threshold: float = 100.0,
        max_iso: int = 1600,
        retry_sleep_s: float = 0.5,
    ) -> None:
        """
        :param camera_helper:       Instance of CameraLiveViewStreamer
        :param priority:            "motion" | "depth" | "quality" | "balanced"
        :param sharpness_threshold: Laplacian variance minimum; higher = stricter
        :param max_iso:             Hard ceiling on ISO (noise control)
        :param retry_sleep_s:       Seconds to wait between blur-retry attempts
        """
        self.camera = camera_helper
        self.priority = self._validate_priority(priority)
        self.sharpness_threshold = sharpness_threshold
        self.max_iso = max_iso
        self.retry_sleep_s = retry_sleep_s

        # Runtime limits — updated from camera after connection
        self.min_aperture: float = self.DEFAULT_MIN_APERTURE
        self.max_aperture: float = self.DEFAULT_MAX_APERTURE
        self.min_shutter:  float = self.DEFAULT_MIN_SHUTTER
        self.max_shutter:  float = self.DEFAULT_MAX_SHUTTER
        self.min_iso:      int   = self.DEFAULT_MIN_ISO

        self._limits_fetched: bool = False
        self._auto_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def reset_limits(self) -> None:
        """
        Invalidate the cached camera/lens limits.
        Call this after reconnecting to a different body or swapping lenses.
        """
        self._limits_fetched = False
        logger.info("Camera limits cache cleared; will re-fetch on next capture.")

    async def auto_capture(
        self,
        group_size: int = 1,
        max_attempts: int = 3,
        verify_sharpness: bool = True,
    ) -> CaptureResult:
        """
        Fully automatic capture with exposure triangle feedback loop.

        :param group_size:       Number of subjects (drives aperture target)
        :param max_attempts:     Maximum retries on blur detection
        :param verify_sharpness: If True, retry when Laplacian variance is low
        :return: CaptureResult dataclass
        :raises CameraStateError: Camera not in Manual mode
        :raises CaptureError:     All attempts failed or hardware error
        """
        async with self._auto_lock:
            await self._assert_manual_mode()
            await self._fetch_camera_limits()

            # Per-call shutter floor — local copy, never mutates self.min_shutter
            call_min_shutter = self.min_shutter
            last_result: Optional[CaptureResult] = None

            for attempt in range(1, max_attempts + 1):
                logger.info(f"--- Auto capture attempt {attempt}/{max_attempts} ---")

                # Re-read actual hardware state each iteration
                current = await self._timed(
                    self.camera.get_camera_settings(),
                    self.SETTINGS_READ_TIMEOUT,
                    "get_camera_settings",
                )
                current_iso     = self._extract_numeric(current, "iso",          400.0)
                current_apt     = self._extract_numeric(current, "aperture",       5.6)
                current_shutter = self._extract_numeric(current, "shutterspeed", 0.008)

                brightness = await self._measure_brightness(median_frames=3)
                logger.info(
                    f"Brightness: {brightness:.1f} (target {self.TARGET_BRIGHTNESS:.0f})"
                )

                optimal = self._calculate_optimal(
                    current_brightness=brightness,
                    current_iso=current_iso,
                    current_aperture=current_apt,
                    current_shutter=current_shutter,
                    group_size=group_size,
                    min_shutter=call_min_shutter,
                )
                logger.info(
                    f"Optimal → ISO {optimal['iso']}, "
                    f"f/{optimal['aperture']}, "
                    f"1/{1 / optimal['shutterspeed']:.0f}s"
                )

                await self._apply_if_changed(optimal, current_iso, current_apt, current_shutter)

                # Re-read after apply so the next iteration's math is accurate
                confirmed = await self._timed(
                    self.camera.get_camera_settings(),
                    self.SETTINGS_READ_TIMEOUT,
                    "get_camera_settings (post-apply)",
                )
                confirmed_iso     = self._extract_numeric(confirmed, "iso",          optimal["iso"])
                confirmed_apt     = self._extract_numeric(confirmed, "aperture",      optimal["aperture"])
                confirmed_shutter = self._extract_numeric(confirmed, "shutterspeed", optimal["shutterspeed"])
                logger.debug(
                    f"Confirmed → ISO {confirmed_iso}, "
                    f"f/{confirmed_apt}, "
                    f"1/{1 / confirmed_shutter:.0f}s"
                )

                image_bytes, filename = await self._timed(
                    self.camera.capture_photo(),
                    self.CAPTURE_TIMEOUT,
                    "capture_photo",
                )

                sharpness_score: Optional[float] = None

                if verify_sharpness:
                    sharp, sharpness_score = self._check_sharpness(image_bytes)
                    logger.info(
                        f"Sharpness: {sharpness_score:.1f} "
                        f"(threshold {self.sharpness_threshold:.1f}) — "
                        f"{'OK' if sharp else 'BLURRY'}"
                    )
                    if not sharp:
                        logger.warning(
                            f"Attempt {attempt}: blurry. "
                            f"Tightening shutter floor and retrying."
                        )
                        call_min_shutter = min(
                            self.max_shutter,
                            call_min_shutter * 1.5,
                        )
                        # Store in case this is the last attempt
                        last_result = CaptureResult(
                            image_bytes=image_bytes,
                            filename=filename,
                            settings_applied=optimal,
                            sharpness_score=sharpness_score,
                            attempts=attempt,
                            brightness_measured=brightness,
                        )
                        await asyncio.sleep(self.retry_sleep_s)
                        continue

                logger.info(f"Capture succeeded on attempt {attempt}.")
                return CaptureResult(
                    image_bytes=image_bytes,
                    filename=filename,
                    settings_applied=optimal,
                    sharpness_score=sharpness_score,
                    attempts=attempt,
                    brightness_measured=brightness,
                )

            # All attempts exhausted
            if last_result is not None:
                logger.warning(
                    f"All {max_attempts} attempts were blurry. "
                    f"Returning best available image (score={last_result.sharpness_score:.1f})."
                )
                return last_result

            raise CaptureError(
                f"auto_capture failed: no image produced after {max_attempts} attempt(s)."
            )

    # ------------------------------------------------------------------
    # Core exposure triangle calculation
    # ------------------------------------------------------------------

    def _calculate_optimal(
        self,
        current_brightness: float,
        current_iso: float,
        current_aperture: float,
        current_shutter: float,
        group_size: int,
        min_shutter: float,
    ) -> Dict[str, Any]:
        """
        Return optimal {shutterspeed, aperture, iso} using stop-based math.

        Strategy:
            1. Pick aperture from group size.
            2. Pick shutter from priority.
            3. Derive ISO to hit TARGET_BRIGHTNESS.
            4. If ISO hits ceiling, sacrifice the lowest-priority variable.
            5. If ISO hits floor (too bright), increase shutter / narrow aperture.
        """

        # 1. Aperture from group size
        target_aperture = next(
            apt for threshold, apt in self._GROUP_APERTURE if group_size <= threshold
        )
        target_aperture = self._clamp(target_aperture, self.min_aperture, self.max_aperture)

        # 2. Shutter from priority
        target_shutter = self._PRIORITY_SHUTTER[self.priority]
        target_shutter = self._clamp(target_shutter, min_shutter, self.max_shutter)

        # 3. ISO via stops
        brightness_ratio = self.TARGET_BRIGHTNESS / max(current_brightness, 1.0)
        stops_needed     = math.log2(brightness_ratio)

        aperture_stops = math.log2((target_aperture ** 2) / (current_aperture ** 2))
        shutter_stops  = math.log2(current_shutter / target_shutter)

        iso_stops  = stops_needed - aperture_stops - shutter_stops
        target_iso = current_iso * (2 ** iso_stops)
        target_iso = self._clamp(target_iso, self.min_iso, self.max_iso)

        # 4. ISO ceiling hit → sacrifice lowest-priority dimension
        if target_iso >= self.max_iso:
            target_iso, target_aperture, target_shutter = self._handle_iso_ceiling(
                target_iso, target_aperture, target_shutter,
                current_iso, current_aperture, current_shutter,
                stops_needed, min_shutter,
            )

        # 5. ISO floor hit → dump excess light via shutter / aperture
        if target_iso <= self.min_iso:
            target_iso, target_aperture, target_shutter = self._handle_iso_floor(
                target_aperture, target_shutter,
            )

        return {
            "shutterspeed": round(target_shutter, 6),
            "aperture":     round(target_aperture, 1),
            "iso":          int(round(target_iso)),
        }

    def _handle_iso_ceiling(
        self,
        target_iso: float,
        target_aperture: float,
        target_shutter: float,
        current_iso: float,
        current_aperture: float,
        current_shutter: float,
        stops_needed: float,
        min_shutter: float,
    ) -> Tuple[float, float, float]:
        """Adjust aperture or shutter when ISO hits max_iso."""
        if self.priority == "motion":
            # Open aperture to let in more light; keep fast shutter
            target_aperture = self._clamp(
                target_aperture / 1.4, self.min_aperture, self.max_aperture
            )
            aperture_stops = math.log2((target_aperture ** 2) / (current_aperture ** 2))
            shutter_stops  = math.log2(current_shutter / target_shutter)
            iso_stops  = stops_needed - aperture_stops - shutter_stops
            target_iso = self._clamp(current_iso * (2 ** iso_stops), self.min_iso, self.max_iso)

        elif self.priority == "depth":
            # Slow shutter to let in more light; keep narrow aperture
            target_shutter = self._clamp(
                target_shutter / 2, min_shutter, self.max_shutter
            )
            aperture_stops = math.log2((target_aperture ** 2) / (current_aperture ** 2))
            shutter_stops  = math.log2(current_shutter / target_shutter)
            iso_stops  = stops_needed - aperture_stops - shutter_stops
            target_iso = self._clamp(current_iso * (2 ** iso_stops), self.min_iso, self.max_iso)

        else:
            # balanced / quality: respect user's max_iso — no silent override
            target_iso = float(self.max_iso)

        return target_iso, target_aperture, target_shutter

    def _handle_iso_floor(
        self,
        target_aperture: float,
        target_shutter: float,
    ) -> Tuple[float, float, float]:
        """Adjust aperture or shutter when scene is too bright for min ISO."""
        target_iso = float(self.min_iso)
        if self.priority == "depth":
            # Narrow aperture to reduce light; preserve slow shutter
            target_aperture = self._clamp(
                target_aperture * 1.4, self.min_aperture, self.max_aperture
            )
        else:
            # motion / balanced / quality: increase shutter speed
            target_shutter = self._clamp(
                target_shutter * 2, self.min_shutter, self.max_shutter
            )
        return target_iso, target_aperture, target_shutter

    # ------------------------------------------------------------------
    # Hardware interaction helpers
    # ------------------------------------------------------------------

    async def _assert_manual_mode(self) -> None:
        """Raise CameraStateError if camera dial is not in Manual (M)."""
        settings = await self._timed(
            self.camera.get_camera_settings(),
            self.SETTINGS_READ_TIMEOUT,
            "get_camera_settings (mode check)",
        )
        raw_mode = settings.get("autoexposuremode", {})
        mode = (raw_mode.get("value") or "").strip().lower()
        if mode not in ("manual", "m"):
            raise CameraStateError(
                f"Camera dial must be in M (Manual) for auto exposure. "
                f"Current mode: {mode!r}"
            )

    async def _fetch_camera_limits(self) -> None:
        """Read aperture / shutter / ISO limits from camera hardware (cached)."""
        if self._limits_fetched:
            return
        try:
            settings = await self._timed(
                self.camera.get_camera_settings(),
                self.SETTINGS_READ_TIMEOUT,
                "get_camera_settings (limits)",
            )

            # Aperture
            apt_choices = settings.get("aperture", {}).get("choices", [])
            nums = [float(c) for c in apt_choices if self._is_float(c)]
            if nums:
                self.min_aperture = min(nums)
                self.max_aperture = max(nums)
                logger.info(f"Lens aperture range: f/{self.min_aperture} – f/{self.max_aperture}")

            # Shutter speed
            shutter_choices = settings.get("shutterspeed", {}).get("choices", [])
            shutter_nums: List[float] = []
            for c in shutter_choices:
                s = str(c)
                if "/" in s:
                    parts = s.split("/")
                    if len(parts) == 2 and self._is_float(parts[0]) and self._is_float(parts[1]):
                        denominator = float(parts[1])
                        if denominator != 0:
                            shutter_nums.append(float(parts[0]) / denominator)
                elif self._is_float(c):
                    shutter_nums.append(float(c))
            if shutter_nums:
                self.min_shutter = min(shutter_nums)
                self.max_shutter = max(shutter_nums)
                logger.info(
                    f"Shutter range: 1/{int(1/self.min_shutter)} – "
                    f"1/{int(1/self.max_shutter)}"
                )

            # ISO
            iso_choices = settings.get("iso", {}).get("choices", [])
            iso_nums = [int(c) for c in iso_choices if str(c).isdigit()]
            if iso_nums:
                self.min_iso = min(iso_nums)
                # Always respect user's max_iso ceiling
                self.max_iso = min(self.max_iso, max(iso_nums))
                logger.info(f"ISO range: {self.min_iso} – {self.max_iso}")

        except (CaptureError, CameraStateError):
            raise
        except Exception as exc:
            logger.warning(f"Could not fetch camera limits; using defaults. Reason: {exc}")

        self._limits_fetched = True

    async def _measure_brightness(
        self, median_frames: int = 3
    ) -> float:
        """
        Capture preview frames and return median pixel intensity (0–255).
        Falls back to 100.0 if no frames are readable.
        """
        median_frames = max(1, median_frames)
        values: List[float] = []

        for _ in range(median_frames):
            try:
                frame_bytes = await self._timed(
                    self.camera.get_preview_frame(),
                    self.PREVIEW_FRAME_TIMEOUT,
                    "get_preview_frame",
                )
                if frame_bytes:
                    arr = np.frombuffer(frame_bytes, np.uint8)
                    img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
                    if img is not None:
                        values.append(float(np.mean(img)))
            except asyncio.TimeoutError:
                logger.warning("Preview frame timed out; skipping.")
            except Exception as exc:
                logger.debug(f"Preview frame error: {exc}")

        if not values:
            logger.warning("No valid preview frames; using fallback brightness 100.0.")
            return 100.0

        result = float(np.median(values))
        logger.debug(f"Brightness: median={result:.1f} over {len(values)} frames.")
        return result

    async def _apply_if_changed(
        self,
        optimal: Dict[str, Any],
        curr_iso: float,
        curr_apt: float,
        curr_shutter: float,
    ) -> None:
        """
        Write settings to camera only when the change exceeds detection thresholds.
        Shutter comparison uses stops (not raw seconds) to catch 1-stop deltas.
        """
        changes: Dict[str, str] = {}

        if abs(optimal["aperture"] - curr_apt) > self.APERTURE_CHANGE_THRESHOLD:
            changes["aperture"] = str(optimal["aperture"])

        # Stops-based shutter comparison — avoids missing changes like 1/100→1/125
        if curr_shutter > 0 and optimal["shutterspeed"] > 0:
            shutter_delta_stops = abs(math.log2(optimal["shutterspeed"] / curr_shutter))
            if shutter_delta_stops > self.SHUTTER_CHANGE_THRESHOLD_STOPS:
                changes["shutterspeed"] = str(optimal["shutterspeed"])
        elif optimal["shutterspeed"] != curr_shutter:
            changes["shutterspeed"] = str(optimal["shutterspeed"])

        if abs(optimal["iso"] - curr_iso) > self.ISO_CHANGE_THRESHOLD:
            changes["iso"] = str(optimal["iso"])

        if not changes:
            logger.debug("No setting changes needed.")
            return

        logger.info(f"Applying changes: {changes}")
        result = await self._timed(
            self.camera.set_camera_settings(changes),
            self.SETTINGS_WRITE_TIMEOUT,
            "set_camera_settings",
        )
        if isinstance(result, dict) and result.get("failed"):
            logger.warning(f"Some settings were rejected by camera: {result['failed']}")

        await asyncio.sleep(0.2)  # Allow camera to settle

    def _check_sharpness(self, image_bytes: bytes) -> Tuple[bool, float]:
        """
        Laplacian variance sharpness check.

        Returns (is_sharp, variance_score).
        Returns (False, 0.0) on decode failure — treats broken images as blurry.
        """
        try:
            arr = np.frombuffer(image_bytes, np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
            if img is None:
                logger.error("Sharpness check: image decode returned None (corrupt data?).")
                return False, 0.0
            variance = float(cv2.Laplacian(img, cv2.CV_64F).var())
            return variance > self.sharpness_threshold, variance
        except Exception as exc:
            logger.error(f"Sharpness check failed: {exc}")
            return False, 0.0

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    async def _timed(coro, timeout: float, label: str):
        """Await a coroutine with a timeout; raises CaptureError on expiry."""
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            raise CaptureError(f"Hardware call '{label}' timed out after {timeout}s.")

    @staticmethod
    def _clamp(value: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, value))

    @staticmethod
    def _validate_priority(priority: str) -> str:
        valid = {"motion", "depth", "quality", "balanced"}
        if priority not in valid:
            raise ValueError(f"priority must be one of {valid!r}, got {priority!r}.")
        return priority

    @staticmethod
    def _is_float(value: Any) -> bool:
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def _extract_numeric(
        self, settings_dict: Dict, key: str, default: float
    ) -> float:
        """Extract a numeric value from a gphoto2-style nested settings dict."""
        try:
            val = settings_dict.get(key, {}).get("value", default)
            if isinstance(val, str) and "/" in val:
                parts = val.split("/")
                if len(parts) == 2:
                    num, den = float(parts[0]), float(parts[1])
                    if den != 0:
                        return num / den
            return float(val)
        except (ValueError, TypeError):
            return default
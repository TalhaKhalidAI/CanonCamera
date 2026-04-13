import base64
import time
from typing import Dict, Optional,Any
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Response, status
from fastapi.responses import HTMLResponse, StreamingResponse
from App.api.dependencies.camera import CameraLiveViewStreamer, get_camera_streamer
from App.core.LoggingInit import get_core_logger
from App.repository.DLSR_Helper import CameraHardwareError, CircuitBreakerOpenError, CameraNotConnectedError
from App.api.dependencies.auth import get_current_user 
# TODO: re-enable when auth middleware is wired up
# from App.api.dependencies.auth import get_current_user

logger = get_core_logger(__name__)

cam_route = APIRouter(prefix="/dslr", tags=["DSLR"])

# Base path used in the HTML UI — single place to change if the mount point moves.
API_PREFIX = "/app/v1/dslr"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_detail(exc: Exception, public_message: str) -> str:
    """Log the real error internally; return a safe message to the client."""
    logger.error("%s — %s: %s", public_message, type(exc).__name__, exc)
    return public_message


def _camera_connected(clsr: CameraLiveViewStreamer) -> bool:
    return clsr.get_status()["camera_connected"]


async def _ensure_connected(clsr: CameraLiveViewStreamer) -> None:
    """
    Raise HTTP 503 if the camera is not connected and cannot be auto-started.
    Callers that want to auto-start the stream should call this first.
    """
    if not _camera_connected(clsr):
        if not await clsr.start_streaming():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Camera not connected. Call /dslr/start first.",
            )


def _map_hardware_error(exc: Exception) -> HTTPException:
    """Convert camera-layer exceptions to appropriate HTTP status codes."""
    if isinstance(exc, HTTPException):
        return exc
    if isinstance(exc, CircuitBreakerOpenError):
        return HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Camera offlined by circuit breaker: {exc}",
        )
    if isinstance(exc, CameraNotConnectedError):
        return HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Camera not connected.",
        )
    if isinstance(exc, CameraHardwareError):
        return HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Camera hardware error: {exc}",
        )
    return HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="An internal error occurred. Check server logs.",
    )


def _content_type_from_bytes(data: bytes) -> str:
    if data[:2] == b"\xff\xd8":
        return "image/jpeg"
    if data[:4] == b"\x89PNG":
        return "image/png"
    if data[:2] == b"BM":
        return "image/bmp"
    if data[:4] in (b"II\x2a\x00", b"MM\x00\x2a"):
        return "image/tiff"
    return "application/octet-stream"


# ---------------------------------------------------------------------------
# Detect / Connect
# ---------------------------------------------------------------------------

@cam_route.get("/detect")
async def detect_cameras(
    current_user: Dict[str, Any] = Depends(get_current_user),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    # _user=Depends(get_current_user),  # TODO: enable
):
    """Detect all available USB cameras."""
    try:
        cur=current_user.get("role")
        if cur=="guest" or cur=="viewer":
            raise  HTTPException(403,"Not enough permissions")
        cameras = await clsr.detect_usb_cameras()
        return {
            "status": "success",
            "cameras_found": len(cameras),
            "cameras": cameras,
            "current_status": clsr.get_status(),
        }
    except Exception as exc:
        raise _map_hardware_error(exc)


@cam_route.post("/connect")
async def connect_camera(
    current_user: Dict[str, Any] = Depends(get_current_user),
    port: Optional[str] = Query(None, description="USB port string, e.g. usb:001,005"),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    save_to_sd: bool = Query(True),
    # _user=Depends(get_current_user),  # TODO: enable
):
    """Connect to a specific camera port, or auto-select the first detected."""
    try:
        cur=current_user.get("role")
        if cur=="guest" or cur=="viewer":
            raise  HTTPException(403,"Not enough permissions")
        if not port:
            cameras = await clsr.detect_usb_cameras()
            if not cameras:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No cameras detected.",
                )
            port = cameras[0]["port"]
            logger.info("Auto-selected camera at port %s", port)

        success = await clsr.connect_to_camera(port,save_to_sd=save_to_sd)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Failed to connect to camera at port {port!r}.",
            )
        return {
            "status": "connected",
            "port": port,
            "camera_model": clsr.camera_model,
            "serial_number": clsr.serial_number,
            "firmware_version": clsr.firmware_version,
        }
    except Exception as exc:
        raise _map_hardware_error(exc)


@cam_route.get("/cameras")
async def get_all_cameras(
    current_user: Dict[str, Any] = Depends(get_current_user),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
):
    """Detect and return all connected USB cameras."""
    try:
        cur=current_user.get("role")
        if cur=="guest" or cur=="viewer":
            raise  HTTPException(403,"Not enough permissions")
        cameras = await clsr.get_all_cameras()
        return {
            "status": "success",
            "cameras_found": len(cameras),
            "cameras": cameras,
        }
    except Exception as exc:
        raise _map_hardware_error(exc)


@cam_route.post("/disconnect")
async def disconnect_camera(
    current_user: Dict[str, Any] = Depends(get_current_user),
    port: Optional[str] = Query(None, description="Optional USB port to force-disconnect"),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
):
    """
    Safely disconnect the currently active camera or a specific port.
    Stops any active stream before disconnecting.
    """
    try:
        cur=current_user.get("role")
        if cur=="guest" or cur=="viewer":
            raise  HTTPException(403,"Not enough permissions")
        # disconnect_camera returns True on success, or False if force-release fails
        success = await clsr.disconnect_camera(port)
        if not success and port:
             raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to force-disconnect camera at port {port!r}.",
            )
        return {
            "status": "success",
            "message": "Camera disconnected safely",
            "port": port or clsr.selected_port
        }
    except Exception as exc:
        raise _map_hardware_error(exc)


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

@cam_route.get("/settings")
async def get_settings(
    current_user: Dict[str, Any] = Depends(get_current_user),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    # _user=Depends(get_current_user),  # TODO: enable
):
    """Fetch current camera settings and their allowed values."""
    try:
        cur=current_user.get("role")
        if cur=="guest" or cur=="viewer":
            raise  HTTPException(403,"Not enough permissions")
        settings = await clsr.get_camera_settings()
        return {"status": "success", "settings": settings}
    except Exception as exc:
        raise _map_hardware_error(exc)


@cam_route.patch("/settings")
async def update_settings(
    current_user: Dict[str, Any] = Depends(get_current_user),
    settings: Dict[str, str] = Body(..., example={"iso": "400", "shutterspeed": "1/125"}),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    # _user=Depends(get_current_user),  # TODO: enable
):
    """
    Update one or more camera settings.
    Returns which keys were applied and which were rejected.
    """
    cur=current_user.get("role")
    if cur=="guest" or cur=="viewer":
        raise  HTTPException(403,"Not enough permissions")
    
    if not settings:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Settings body must not be empty.",
        )
    try:
        result = await clsr.set_camera_settings(settings)
        return {"status": "success", **result}
    except Exception as exc:
        raise _map_hardware_error(exc)


# ---------------------------------------------------------------------------
# Stream control
# ---------------------------------------------------------------------------

@cam_route.post("/start")
async def start_stream(
    current_user: Dict[str, Any] = Depends(get_current_user),
    port: Optional[str] = Query(None),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    save_to_sd: bool = Query(True, description="Save photos to SD card"),
    # _user=Depends(get_current_user),  # TODO: enable
):
    """Start the camera live-view stream."""
    cur=current_user.get("role")
    if cur=="guest" or cur=="viewer":
        raise  HTTPException(403,"Not enough permissions")
    if clsr.is_streaming:
        return {
            "status": "already_running",
            "camera_model": clsr.camera_model,
            "port": clsr.selected_port,
            "stream_url": f"{API_PREFIX}/livestream",
        }
    try:
        success = await clsr.start_streaming(port,save_to_sd=save_to_sd)
    except Exception as exc:
        raise _map_hardware_error(exc)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to start camera stream.",
        )
    return {
        "status": "started",
        "camera_model": clsr.camera_model,
        "serial_number": clsr.serial_number,
        "firmware_version": clsr.firmware_version,
        "port": clsr.selected_port,
        "stream_url": f"{API_PREFIX}/livestream",
    }


@cam_route.post("/stop")
async def stop_stream(
    current_user: Dict[str, Any] = Depends(get_current_user),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    # _user=Depends(get_current_user),  # TODO: enable
):
    """Stop the camera live-view stream."""
    try:
        cur=current_user.get("role")
        if cur=="guest" or cur=="viewer":
            raise  HTTPException(403,"Not enough permissions")
        await clsr.stop_streaming()
    except Exception as exc:
        raise _map_hardware_error(exc)
    return {"status": "stopped"}


@cam_route.get("/livestream")
async def live_stream(
    current_user: Dict[str, Any] = Depends(get_current_user),
    port: Optional[str] = Query(None),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    # _user=Depends(get_current_user),  # TODO: enable
):
    """
    MJPEG live-stream endpoint.
    Auto-starts the stream if not already running.
    """
    cur=current_user.get("role")
    if cur=="guest" or cur=="viewer":
        raise  HTTPException(403,"Not enough permissions")
    if not clsr.is_streaming:
        try:
            success = await clsr.start_streaming(port)
        except Exception as exc:
            raise _map_hardware_error(exc)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Failed to start camera stream.",
            )
    return StreamingResponse(
        clsr.generate_mjpeg_stream(),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@cam_route.get("/status")
async def stream_status(
    current_user: Dict[str, Any] = Depends(get_current_user),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    # _user=Depends(get_current_user),  # TODO: enable
):
    cur=current_user.get("role")
    if cur=="guest" or cur=="viewer":
        raise  HTTPException(403,"Not enough permissions")
    """Return full camera and stream status, including circuit-breaker state."""
    info = clsr.get_status()
    return {
        "status": "running" if info["is_streaming"] else "stopped",
        **info,
    }


# ---------------------------------------------------------------------------
# Capture
# ---------------------------------------------------------------------------

@cam_route.post("/capture")
async def capture_photo(
    current_user: Dict[str, Any] = Depends(get_current_user),
    keep_on_sd: bool = Query(False, description="Retain image on SD card after download"),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    # _user=Depends(get_current_user),  # TODO: enable
):
    """
    Capture a full-resolution photo and return it as a binary response.
    Basic image metrics are included as X-Image-* response headers.
    """
    try:
        cur=current_user.get("role")
        if cur=="guest" or cur=="viewer":
            raise  HTTPException(403,"Not enough permissions")
        await _ensure_connected(clsr)

        # capture_photo() returns (bytes, str) — metadata is logged in DLSR_Helper
        image_bytes, filename = await clsr.capture_photo(keep_on_sd=keep_on_sd)

        # Extract metadata for response headers without a second hardware round-trip
        metadata = clsr.extract_image_metadata(image_bytes, filename)

        content_type = _content_type_from_bytes(image_bytes)
        ext = filename.rsplit(".", 1)[-1] if "." in filename else "jpg"
        download_name = f"capture_{int(time.time())}.{ext}"

        logger.info(
            "Returning capture: %d bytes, %s, %dx%d px",
            len(image_bytes), content_type,
            metadata.get("width_px", 0), metadata.get("height_px", 0),
        )
        return Response(
            content=image_bytes,
            media_type=content_type,
            headers={
                # RFC 6266 — filename must be quoted
                "Content-Disposition": f'attachment; filename="{download_name}"',
                "X-Image-Width-PX":  str(metadata.get("width_px",  0)),
                "X-Image-Height-PX": str(metadata.get("height_px", 0)),
                "X-Image-DPI":       str(metadata.get("dpi",       300)),
                "X-Image-Width-MM":  str(metadata.get("width_mm",  0.0)),
                "X-Image-Height-MM": str(metadata.get("height_mm", 0.0)),
                "X-Image-Format":    str(metadata.get("format",    "Unknown")),
            },
        )

    except Exception as exc:
        raise _map_hardware_error(exc)


@cam_route.post("/capture/detailed")
async def capture_photo_detailed(
    current_user: Dict[str, Any] = Depends(get_current_user),
    keep_on_sd: bool = Query(False),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    # _user=Depends(get_current_user),  # TODO: enable
):
    """
    Capture a photo and return metadata + Base64-encoded image as JSON.
    Intended for clients that cannot handle binary responses.
    """
    try:
        cur=current_user.get("role")
        if cur=="guest" or cur=="viewer":
            raise  HTTPException(403,"Not enough permissions")
        await _ensure_connected(clsr)

        image_bytes, filename = await clsr.capture_photo(keep_on_sd=keep_on_sd)
        metadata = clsr.extract_image_metadata(image_bytes, filename)

        return {
            "filename":   filename,
            "format":     metadata.get("format",    "Unknown"),
            "width_px":   metadata.get("width_px",  0),
            "height_px":  metadata.get("height_px", 0),
            "dpi":        metadata.get("dpi",        300),
            "width_mm":   metadata.get("width_mm",  0.0),
            "height_mm":  metadata.get("height_mm", 0.0),
            "image_data": base64.b64encode(image_bytes).decode("utf-8"),
        }

    except Exception as exc:
        raise _map_hardware_error(exc)


# ---------------------------------------------------------------------------
# Health  (public — no auth required)
# ---------------------------------------------------------------------------

@cam_route.get("/health")
async def camera_health(
    current_user: Dict[str, Any] = Depends(get_current_user),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
):
    """
    Lightweight health check.  Read-only — never triggers a capture.
    Returns 200 regardless of camera state so load-balancers don't cycle
    the process; the 'status' field conveys actual health.
    """
    cur=current_user.get("role")
    if cur=="guest" or cur=="viewer":
        raise  HTTPException(403,"Not enough permissions")
    info = clsr.get_status()
    return {
        "status":           "healthy" if info["camera_connected"] else "unhealthy",
        "camera_model":     info["camera_model"],
        "port":             info["selected_port"],
        "is_streaming":     info["is_streaming"],
        "circuit_breaker":  info.get("circuit_breaker", "CLOSED"),
        "timestamp":        time.time(),
    }


@cam_route.get("/test")
async def test_endpoint():
    """Unauthenticated smoke-test endpoint."""
    return {
        "service":   "DSLR Camera Controller",
        "status":    "operational",
        "timestamp": time.time(),
        "endpoints": [
            # auth column reflects actual current state (all TODO: pending)
            {"path": f"{API_PREFIX}/detect",          "method": "GET",   "auth": False},
            {"path": f"{API_PREFIX}/cameras",         "method": "GET",   "auth": False},
            {"path": f"{API_PREFIX}/connect",         "method": "POST",  "auth": False},
            {"path": f"{API_PREFIX}/disconnect",      "method": "POST",  "auth": False},
            {"path": f"{API_PREFIX}/livestream",      "method": "GET",   "auth": False},
            {"path": f"{API_PREFIX}/start",           "method": "POST",  "auth": False},
            {"path": f"{API_PREFIX}/stop",            "method": "POST",  "auth": False},
            {"path": f"{API_PREFIX}/status",          "method": "GET",   "auth": False},
            {"path": f"{API_PREFIX}/capture",         "method": "POST",  "auth": False},
            {"path": f"{API_PREFIX}/capture/detailed","method": "POST",  "auth": False},
            {"path": f"{API_PREFIX}/settings",        "method": "GET",   "auth": False},
            {"path": f"{API_PREFIX}/settings",        "method": "PATCH", "auth": False},
            {"path": f"{API_PREFIX}/health",          "method": "GET",   "auth": False},
        ],
    }


# ---------------------------------------------------------------------------
# Web UI  (public — informational only)
# ---------------------------------------------------------------------------

@cam_route.get("/select", response_class=HTMLResponse)
async def camera_selection_page(current_user: Dict[str, Any] = Depends(get_current_user),):
    cur=current_user.get("role")
    if cur=="guest" or cur=="viewer":
        raise  HTTPException(403,"Not enough permissions")
    """Minimal camera control web UI."""
    # API_PREFIX is injected server-side — no hard-coded paths in JS
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Camera Control</title>
  <style>
    body{{font-family:Arial,sans-serif;margin:20px;max-width:900px}}
    .card{{border:1px solid #ddd;padding:15px;margin:10px 0;border-radius:5px}}
    .camera{{background:#f9f9f9}}.camera.selected{{background:#e3f2fd;border-color:#2196f3}}
    button{{padding:10px 15px;margin:5px;border:none;border-radius:4px;cursor:pointer}}
    .btn-primary{{background:#2196f3;color:#fff}}
    .btn-success{{background:#4caf50;color:#fff}}
    .btn-danger{{background:#f44336;color:#fff}}
    button:disabled{{opacity:.45;cursor:not-allowed}}
    #video{{width:100%;max-width:800px;margin:20px 0;display:block}}
    #msg{{margin-top:8px;font-size:.9em;color:#555}}
  </style>
</head>
<body>
  <h1>&#128247; Camera Control Panel</h1>

  <div class="card">
    <h3>1. Detect Cameras</h3>
    <button onclick="detectCameras()" class="btn-primary">&#128269; Detect</button>
    <div id="cameraList"></div>
  </div>

  <div class="card">
    <h3>2. Stream Controls</h3>
    <button onclick="startStream()" class="btn-success" id="startBtn" disabled>&#9654; Start</button>
    <button onclick="stopStream()"  class="btn-danger"  id="stopBtn"  disabled>&#9209; Stop</button>
    <button onclick="capturePhoto()" class="btn-primary" id="captureBtn" disabled>&#128248; Capture</button>
    <div id="msg"></div>
  </div>

  <div class="card">
    <h3>3. Live View</h3>
    <img id="video" src="" alt="Live stream will appear here after starting.">
  </div>

  <script>
    const API = {repr(API_PREFIX)};
    let selectedPort = null;

    function setMsg(text) {{
      document.getElementById('msg').textContent = text;
    }}

    async function apiFetch(path, opts = {{}}) {{
      const res = await fetch(API + path, opts);
      if (!res.ok) {{
        const err = await res.json().catch(() => ({{detail: res.statusText}}));
        throw new Error(err.detail || res.statusText);
      }}
      return res;
    }}

    async function detectCameras() {{
      setMsg('Detecting…');
      try {{
        const data = await (await apiFetch('/detect')).json();
        const list = document.getElementById('cameraList');
        list.innerHTML = '';
        if (!data.cameras_found) {{
          list.textContent = 'No cameras found.';
          setMsg('');
          return;
        }}
        data.cameras.forEach(cam => {{
          const div = document.createElement('div');
          div.className = 'camera';
          const h4  = document.createElement('h4');
          h4.textContent = cam.model || 'Unknown Camera';   // textContent prevents XSS
          const p   = document.createElement('p');
          p.textContent = 'Port: ' + cam.port;
          const btn = document.createElement('button');
          btn.className = 'btn-primary';
          btn.textContent = 'Select';
          btn.onclick = () => {{
            selectedPort = cam.port;
            document.querySelectorAll('.camera').forEach(d => d.classList.remove('selected'));
            div.classList.add('selected');
            document.getElementById('startBtn').disabled = false;
            setMsg('Selected: ' + cam.model + ' @ ' + cam.port);
          }};
          div.append(h4, p, btn);
          list.appendChild(div);
        }});
        setMsg(data.cameras_found + ' camera(s) found.');
      }} catch (e) {{
        setMsg('Error: ' + e.message);
      }}
    }}

    async function startStream() {{
      setMsg('Starting stream…');
      try {{
        const qs = selectedPort ? '?port=' + encodeURIComponent(selectedPort) : '';
        const data = await (await apiFetch('/start' + qs, {{method: 'POST'}})).json();
        setMsg('Status: ' + data.status);
        document.getElementById('stopBtn').disabled    = false;
        document.getElementById('captureBtn').disabled = false;
        document.getElementById('startBtn').disabled   = true;
        document.getElementById('video').src = API + '/livestream' + qs;
      }} catch (e) {{
        setMsg('Start failed: ' + e.message);
      }}
    }}

    async function stopStream() {{
      setMsg('Stopping…');
      try {{
        await apiFetch('/stop', {{method: 'POST'}});
        document.getElementById('startBtn').disabled   = false;
        document.getElementById('stopBtn').disabled    = true;
        document.getElementById('captureBtn').disabled = true;
        document.getElementById('video').src = '';
        setMsg('Stream stopped.');
      }} catch (e) {{
        setMsg('Stop failed: ' + e.message);
      }}
    }}

    async function capturePhoto() {{
      setMsg('Capturing…');
      try {{
        const res = await apiFetch('/capture', {{method: 'POST'}});
        const blob = await res.blob();
        const a = Object.assign(document.createElement('a'), {{
          href:     URL.createObjectURL(blob),
          download: 'photo_' + Date.now() + '.jpg',
        }});
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        setMsg('Photo downloaded.');
      }} catch (e) {{
        setMsg('Capture failed: ' + e.message);
      }}
    }}

    // Auto-detect on load
    detectCameras();
  </script>
</body>
</html>"""
    return HTMLResponse(content=html)
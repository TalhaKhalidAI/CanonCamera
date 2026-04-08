from fastapi import APIRouter, Depends, HTTPException, status, Body
from fastapi.responses import StreamingResponse, Response, HTMLResponse
from typing import Optional, Dict
import time

from App.api.dependencies.auth import get_current_active_user
from App.api.dependencies.camera import get_camera_streamer, CameraLiveViewStreamer
from App.core.LoggingInit import get_core_logger

logger = get_core_logger(__name__)

cam_route = APIRouter(prefix="/dslr", tags=["DSLR"])


# ---------------------------------------------------------------------------
# Detect / Connect
# ---------------------------------------------------------------------------

@cam_route.get("/detect")
async def detect_cameras(
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
):
    """Detect all available USB cameras."""
    cameras = await clsr.detect_usb_cameras()
    return {
        "status": "success",
        "cameras_found": len(cameras),
        "cameras": cameras,
        "current_status": clsr.get_status(),
    }


@cam_route.post("/connect")
async def connect_camera(
    port: Optional[str] = None,
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
):
    """Connect to a specific camera by port or auto-select."""
    if not port:
        cameras = await clsr.detect_usb_cameras()
        if not cameras:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="No cameras detected"
            )
        port = cameras[0]["port"]
        logger.info(f"Auto-selecting camera at {port}")

    success = await clsr.connect_to_camera(port)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to connect to camera",
        )
    return {"status": "connected", "port": port}


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@cam_route.get("/settings")
async def get_settings(
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
):
    """Fetch current camera settings and allowed values."""
    try:
        settings = await clsr.get_camera_settings()
        return {"status": "success", "settings": settings}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@cam_route.patch("/settings")
async def update_settings(
    settings: Dict[str, str] = Body(...),
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
):
    """Update one or more camera settings."""
    try:
        result = await clsr.set_camera_settings(settings)
        return {"status": "success", **result}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


# ---------------------------------------------------------------------------
# Stream control
# ---------------------------------------------------------------------------


@cam_route.post("/start")
async def start_stream(
    port: Optional[str] = None,
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    # _user=Depends(get_current_active_user),
):
    """Start the camera live stream."""
    if clsr.is_streaming:
        return {
            "status": "already_running",
            "camera_model": clsr.camera_model,
            "port": clsr.selected_port,
        }
    success = await clsr.start_streaming(port)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to start camera stream",
        )
    return {
        "status": "started",
        "camera_model": clsr.camera_model,
        "port": clsr.selected_port,
        "stream_url": "/dslr/livestream",
    }


@cam_route.post("/stop")
async def stop_stream(
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    ## _user=Depends(get_current_active_user),
):
    """Stop the camera live stream."""
    await clsr.stop_streaming()
    return {"status": "stopped", "message": "Live stream stopped successfully"}


@cam_route.get("/livestream")
async def live_stream(
    port: Optional[str] = None,
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    # _user=Depends(get_current_active_user),
):
    """MJPEG live stream endpoint."""
    if not clsr.is_streaming:
        if not await clsr.start_streaming(port):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Failed to start camera stream",
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
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
    # _user=Depends(get_current_active_user),
):
    """Get current streaming status."""
    info = clsr.get_status()
    return {"status": "running" if info["is_streaming"] else "stopped", **info}


# ---------------------------------------------------------------------------
# Capture
# ---------------------------------------------------------------------------


@cam_route.post("/capture")
async def capture_photo(
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
):
    """Capture a high-resolution photo with autonomous hardware synchronization."""
    try:
        if not clsr.camera:
            # Try to connect if not already connected
            if not await clsr.start_streaming():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Camera not connected. Start stream first.",
                )

        file_data, filename = await clsr.capture_photo()


        # Detect content type from magic bytes
        if file_data[:2] == b"\xff\xd8":
            content_type = "image/jpeg"
        elif file_data[:4] == b"\x89PNG":
            content_type = "image/png"
        elif file_data[:2] == b"BM":
            content_type = "image/bmp"
        elif file_data[:4] in (b"II\x2a\x00", b"MM\x00\x2a"):
            content_type = "image/tiff"
        else:
            content_type = "application/octet-stream"

        ts = int(time.time())
        ext = filename.rsplit(".", 1)[-1]
        download_name = f"capture_{ts}.{ext}"

        logger.info(f"Returning photo: {len(file_data)} bytes, {content_type}")
        return Response(
            content=file_data,
            media_type=content_type,
            headers={"Content-Disposition": f"attachment; filename={download_name}"},
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API Capture error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Capture failed: {str(e)}",
        )


# ---------------------------------------------------------------------------
# Health / Test (public — no auth)
# ---------------------------------------------------------------------------


@cam_route.get("/health")
async def camera_health(clsr: CameraLiveViewStreamer = Depends(get_camera_streamer)):
    """Check camera connectivity. Read-only, no auth required."""
    if not clsr.camera:
        return {
            "status": "unhealthy",
            "message": "Camera not connected",
            "timestamp": time.time(),
        }
    # Use streaming status as a lightweight liveness signal — no extra capture
    info = clsr.get_status()
    return {
        "status": "healthy" if info["camera_connected"] else "unhealthy",
        "camera_model": info["camera_model"],
        "port": info["selected_port"],
        "is_streaming": info["is_streaming"],
        "timestamp": time.time(),
    }


@cam_route.get("/test")
async def test_endpoint():
    """Test endpoint — no auth required."""
    return {
        "service": "DSLR Camera Controller",
        "status": "operational",
        "timestamp": time.time(),
        "endpoints": [
            {"path": "/dslr/detect", "method": "GET", "auth": True},
            {"path": "/dslr/connect", "method": "POST", "auth": True},
            {"path": "/dslr/livestream", "method": "GET", "auth": True},
            {"path": "/dslr/start", "method": "POST", "auth": True},
            {"path": "/dslr/stop", "method": "POST", "auth": True},
            {"path": "/dslr/status", "method": "GET", "auth": True},
            {"path": "/dslr/capture", "method": "POST", "auth": True},
            {"path": "/dslr/health", "method": "GET", "auth": False},
        ],
    }


# ---------------------------------------------------------------------------
# Web UI (public — informational only)
# ---------------------------------------------------------------------------


@cam_route.get("/select", response_class=HTMLResponse)
async def camera_selection_page():
    """Minimal camera control web UI."""
    html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Camera Control</title>
  <style>
    body{font-family:Arial,sans-serif;margin:20px}
    .card{border:1px solid #ddd;padding:15px;margin:10px 0;border-radius:5px}
    .camera{background:#f9f9f9}.camera.selected{background:#e3f2fd;border-color:#2196f3}
    button{padding:10px 15px;margin:5px;border:none;border-radius:4px;cursor:pointer}
    .btn-primary{background:#2196f3;color:#fff}.btn-success{background:#4caf50;color:#fff}
    .btn-danger{background:#f44336;color:#fff}
    .ok{color:#2e7d32}.err{color:#c62828}
    #video{width:100%;max-width:800px;margin:20px 0}
  </style>
</head>
<body>
  <h1>📷 Camera Control Panel</h1>
  <div class="card">
    <h3>1. Detect Cameras</h3>
    <button onclick="detectCameras()" class="btn-primary">🔍 Detect</button>
    <div id="cameraList"></div>
  </div>
  <div class="card">
    <h3>2. Stream Controls</h3>
    <button onclick="startStream()" class="btn-success" id="startBtn">▶ Start</button>
    <button onclick="stopStream()" class="btn-danger" id="stopBtn" disabled>⏹ Stop</button>
    <button onclick="capturePhoto()" class="btn-primary" id="captureBtn" disabled>📸 Capture</button>
    <div id="streamStatus"></div>
  </div>
  <div class="card">
    <h3>3. Live View</h3>
    <img id="video" src="" alt="Stream will appear here">
  </div>
  <script>
    let selectedPort = null;

    async function detectCameras() {
      const res = await fetch('/app/v1/dslr/detect');
      if (!res.ok) { alert('Auth required or server error'); return; }
      const data = await res.json();
      const list = document.getElementById('cameraList');
      list.innerHTML = '';
      if (!data.cameras_found) { list.textContent = 'No cameras found.'; return; }
      data.cameras.forEach(cam => {
        const div = document.createElement('div');
        div.className = 'camera';
        const h4 = document.createElement('h4');
        h4.textContent = cam.model || 'Unknown Camera';       // textContent prevents XSS
        const p = document.createElement('p');
        p.textContent = 'Port: ' + cam.port;
        const btn = document.createElement('button');
        btn.className = 'btn-primary';
        btn.textContent = 'Select';
        btn.onclick = () => { selectedPort = cam.port; document.getElementById('startBtn').disabled = false; };
        div.append(h4, p, btn);
        list.appendChild(div);
      });
    }

    async function startStream() {
      let url = '/app/v1/dslr/start';
      if (selectedPort) url += '?port=' + encodeURIComponent(selectedPort);
      const res = await fetch(url, {method: 'POST'});
      const data = await res.json();
      document.getElementById('streamStatus').textContent = 'Status: ' + data.status;
      document.getElementById('stopBtn').disabled = false;
      document.getElementById('captureBtn').disabled = false;
      document.getElementById('video').src = '/app/v1/dslr/livestream';
    }

    async function stopStream() {
      await fetch('/app/v1/dslr/stop', {method: 'POST'});
      document.getElementById('startBtn').disabled = false;
      document.getElementById('stopBtn').disabled = true;
      document.getElementById('captureBtn').disabled = true;
      document.getElementById('video').src = '';
    }

    async function capturePhoto() {
      const res = await fetch('/app/v1/dslr/capture', {method: 'POST'});
      if (!res.ok) { alert('Capture failed'); return; }
      const blob = await res.blob();
      const a = Object.assign(document.createElement('a'), {
        href: URL.createObjectURL(blob),
        download: 'photo_' + Date.now() + '.jpg',
      });
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
    }

    detectCameras();
  </script>
</body>
</html>"""
    return HTMLResponse(content=html)

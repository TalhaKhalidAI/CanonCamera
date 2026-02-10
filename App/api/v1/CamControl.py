from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse, Response, HTMLResponse
from typing import Optional
import time
from contextlib import asynccontextmanager
from App.api.dependencies.auth import get_current_active_user
from App.repository.DLSR_Helper import CameraLiveViewStreamer
from App.core.LoggingInit import get_core_logger
from App.schemas.AuthScheema import TokenResponse
import asyncio

# Initialize logger
logger = get_core_logger(__name__)

# Global camera streamer instance
clsr: Optional[CameraLiveViewStreamer] = None

@asynccontextmanager
async def router_lifespan(app=APIRouter):
    """Lifespan manager for camera streamer"""
    global clsr
    try:
        # Initialize camera streamer but don't start streaming yet
        clsr = CameraLiveViewStreamer()
        logger.info("Camera streamer initialized (not started)")
        
        yield
        
    except Exception as e:
        logger.error(f"Error in camera router lifespan: {e}")
    finally:
        # Clean up on shutdown
        if clsr:
            clsr.cleanup()
            logger.info("Camera streamer cleaned up")

cam_route = APIRouter(
    prefix="/dslr",
    tags=["DSLR"],
    lifespan=router_lifespan
)

@cam_route.get("/detect")
async def detect_cameras():
    """
    Detect all available USB cameras.
    """
    try:
        if not clsr:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Camera controller not initialized"
            )
        
        cameras = clsr.detect_usb_cameras()
        
        # Test each camera
        for camera in cameras:
            test_result = clsr.test_camera_port(camera['port'])
            camera['test_result'] = test_result
        
        return {
            "status": "success",
            "cameras_found": len(cameras),
            "cameras": cameras,
            "current_status": clsr.get_status() if clsr else None
        }
            
    except Exception as e:
        logger.error(f"Error detecting cameras: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to detect cameras: {str(e)}"
        )

@cam_route.post("/connect")
async def connect_camera(port: Optional[str] = None):
    """
    Connect to a specific camera by port or auto-select.
    """
    try:
        if not clsr:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Camera controller not initialized"
            )
        
        # If no port specified, auto-detect
        if not port:
            cameras = clsr.detect_usb_cameras()
            if not cameras:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No cameras detected"
                )
            
            # Try to find working camera
            working_cameras = [c for c in cameras if c.get('test_result', {}).get('success', False)]
            if working_cameras:
                port = working_cameras[0]['port']
                logger.info(f"Auto-selecting camera at {port}")
            else:
                port = cameras[0]['port']
                logger.info(f"Auto-selecting first camera at {port}")
        
        # Connect to camera
        success = clsr.connect_to_camera(port)
        
        if success:
            return {
                "status": "connected",
                "message": f"Connected to camera at {port}",
                "port": port,
                "camera_model": clsr.camera_model
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to connect to camera at {port}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error connecting to camera: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to connect: {str(e)}"
        )

@cam_route.get("/livestream")
async def live_stream(port: Optional[str] = None):
    """
    MJPEG live stream endpoint.
    Optionally specify port to connect to specific camera.
    """
    try:
        if not clsr:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Camera stream is not available"
            )
        
        # If not streaming, start streaming with optional port
        if not clsr.is_streaming:
            success = clsr.start_streaming(port)
            if not success:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Failed to start camera stream"
                )
            logger.info(f"Live stream started on {clsr.camera_model}")
        
        # Return MJPEG stream
        return StreamingResponse(
            clsr.generate_mjpeg_stream(),
            media_type="multipart/x-mixed-replace; boundary=frame",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in live stream endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Live stream failed: {str(e)}"
        )

@cam_route.post("/start")
async def start_stream(port: Optional[str] = None):
    """
    Start the camera live stream manually.
    Optionally specify port to connect to specific camera.
    """
    try:
        if not clsr:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Camera streamer not initialized"
            )
        
        if clsr.is_streaming:
            return {
                "status": "already_running",
                "message": "Stream is already running",
                "camera_model": clsr.camera_model,
                "port": clsr.selected_port
            }
        
        success = clsr.start_streaming(port)
        if success:
            return {
                "status": "started",
                "message": "Live stream started successfully",
                "camera_model": clsr.camera_model,
                "port": clsr.selected_port,
                "stream_url": "/dslr/livestream"
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Failed to start camera stream"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting stream: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start stream: {str(e)}"
        )

@cam_route.post("/stop")
async def stop_stream():
    """
    Stop the camera live stream.
    """
    try:
        if not clsr:
            return {
                "status": "not_initialized",
                "message": "Streamer not initialized"
            }
        
        clsr.stop_streaming()
        return {
            "status": "stopped",
            "message": "Live stream stopped successfully"
        }
            
    except Exception as e:
        logger.error(f"Error stopping stream: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop stream: {str(e)}"
        )

@cam_route.get("/status")
async def stream_status():
    """
    Get current streaming status.
    """
    try:
        if not clsr:
            return {
                "status": "not_initialized",
                "is_streaming": False,
                "message": "Camera streamer not initialized"
            }
        
        status_info = clsr.get_status()
        return {
            "status": "running" if status_info['is_streaming'] else "stopped",
            **status_info
        }
            
    except Exception as e:
        logger.error(f"Error getting stream status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get stream status: {str(e)}"
        )
@cam_route.post("/capture")
async def capture_photo(format: str = "jpg"):
    """
    Capture a high-resolution photo.
    Optional parameter: format (jpg/preview)
    """
    try:
        if not clsr:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Camera streamer not initialized"
            )
        
        # Make sure camera is connected
        if not clsr.camera:
            if not clsr.start_streaming():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Camera not connected. Start stream first."
                )
        
        # Capture photo based on requested format
        if format.lower() == "preview":
            # Use preview method for guaranteed JPEG
            file_data, filename = clsr.capture_photo_as_jpeg()
            download_name = f"preview_{int(time.time())}.jpg"
        else:
            # Try to capture with format handling
            file_data, original_name = clsr.capture_photo(format)
            
            # Check file type
            if file_data[:2] == b'\xff\xd8':
                # Valid JPEG
                extension = "jpg"
            else:
                # Check file magic bytes
                magic = file_data[:4].hex()
                if magic.startswith('424d'):  # BMP
                    extension = "bmp"
                elif magic.startswith('89504e47'):  # PNG
                    extension = "png"
                elif magic.startswith('49492a00') or magic.startswith('4d4d002a'):  # TIFF
                    extension = "tif"
                elif 'cr2' in original_name.lower():
                    extension = "cr2"
                else:
                    extension = "bin"  # Binary/unknown
            
            download_name = f"capture_{int(time.time())}.{extension}"
        
        # Debug log
        logger.info(f"Captured photo: {len(file_data)} bytes, type: {file_data[:4].hex()}")
        
        # Determine content type
        if file_data[:2] == b'\xff\xd8':
            content_type = "image/jpeg"
        elif file_data[:4] == b'\x89PNG':
            content_type = "image/png"
        elif file_data[:2] == b'BM':
            content_type = "image/bmp"
        elif file_data[:4] in [b'II\x2a\x00', b'MM\x00\x2a']:
            content_type = "image/tiff"
        elif file_data[:4] == b'\x49\x49\x2a\x00':  # Canon RAW (CR2 starts with TIFF)
            content_type = "image/x-canon-cr2"
        else:
            content_type = "application/octet-stream"
        
        # Return as downloadable image
        return Response(
            content=file_data,
            media_type=content_type,
            headers={
                "Content-Disposition": f"attachment; filename={download_name}",
                "Content-Type": content_type
            }
        )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error capturing photo: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to capture photo: {str(e)}"
        )

@cam_route.get("/select", response_class=HTMLResponse)
async def camera_selection_page():
    """
    Web interface for selecting and controlling cameras.
    """
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Camera Selection</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .card { border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px; }
            .camera { background-color: #f9f9f9; }
            .camera.selected { background-color: #e3f2fd; border-color: #2196f3; }
            button { padding: 10px 15px; margin: 5px; border: none; border-radius: 4px; cursor: pointer; }
            .btn-primary { background-color: #2196f3; color: white; }
            .btn-success { background-color: #4caf50; color: white; }
            .btn-danger { background-color: #f44336; color: white; }
            .status { padding: 5px 10px; border-radius: 3px; font-size: 0.9em; }
            .status-success { background-color: #e8f5e9; color: #2e7d32; }
            .status-error { background-color: #ffebee; color: #c62828; }
            #video { width: 100%; max-width: 800px; margin: 20px 0; }
        </style>
    </head>
    <body>
        <h1>📷 Camera Control Panel</h1>
        
        <div class="card">
            <h3>1. Detect Cameras</h3>
            <button onclick="detectCameras()" class="btn-primary">🔍 Detect Cameras</button>
            <div id="cameraList"></div>
        </div>
        
        <div class="card">
            <h3>2. Stream Controls</h3>
            <button onclick="startStream()" class="btn-success" id="startBtn">▶ Start Stream</button>
            <button onclick="stopStream()" class="btn-danger" id="stopBtn" disabled>⏹ Stop Stream</button>
            <button onclick="capturePhoto()" class="btn-primary" id="captureBtn" disabled>📸 Capture Photo</button>
            <div id="streamStatus"></div>
        </div>
        
        <div class="card">
            <h3>3. Live View</h3>
            <img id="video" src="" alt="Live stream will appear here">
            <p><a href="/dslr/livestream" target="_blank">Open fullscreen stream</a></p>
        </div>
        
        <script>
            let selectedPort = null;
            
            async function detectCameras() {
                const response = await fetch('/dslr/detect');
                const data = await response.json();
                
                const cameraList = document.getElementById('cameraList');
                cameraList.innerHTML = '';
                
                if (data.cameras_found === 0) {
                    cameraList.innerHTML = '<p>No cameras found. Connect a USB camera and refresh.</p>';
                    return;
                }
                
                data.cameras.forEach(camera => {
                    const div = document.createElement('div');
                    div.className = 'camera';
                    div.innerHTML = `
                        <h4>${camera.model || 'Unknown Camera'}</h4>
                        <p><strong>Port:</strong> ${camera.port}</p>
                        <p><strong>Status:</strong> 
                            <span class="status ${camera.test_result.success ? 'status-success' : 'status-error'}">
                                ${camera.test_result.success ? '✓ Working' : '✗ Failed'}
                            </span>
                        </p>
                        <p><strong>Capabilities:</strong> ${camera.test_result.capabilities?.join(', ') || 'None'}</p>
                        <button onclick="selectCamera('${camera.port}')" class="btn-primary">Select</button>
                    `;
                    cameraList.appendChild(div);
                });
            }
            
            function selectCamera(port) {
                selectedPort = port;
                document.querySelectorAll('.camera').forEach(el => el.classList.remove('selected'));
                event.target.closest('.camera').classList.add('selected');
                document.getElementById('startBtn').disabled = false;
            }
            
            async function startStream() {
                const btn = document.getElementById('startBtn');
                btn.disabled = true;
                
                let url = '/dslr/start';
                if (selectedPort) {
                    url += `?port=${encodeURIComponent(selectedPort)}`;
                }
                
                const response = await fetch(url, { method: 'POST' });
                const data = await response.json();
                
                document.getElementById('streamStatus').innerHTML = 
                    `<p><strong>Status:</strong> ${data.status}</p>
                     <p><strong>Camera:</strong> ${data.camera_model || 'Unknown'}</p>
                     <p><strong>Port:</strong> ${data.port || 'Auto'}</p>`;
                
                document.getElementById('stopBtn').disabled = false;
                document.getElementById('captureBtn').disabled = false;
                
                // Start showing live view
                document.getElementById('video').src = '/dslr/livestream' + (selectedPort ? `?port=${encodeURIComponent(selectedPort)}` : '');
            }
            
            async function stopStream() {
                const response = await fetch('/dslr/stop', { method: 'POST' });
                const data = await response.json();
                
                document.getElementById('streamStatus').innerHTML = 
                    `<p><strong>Status:</strong> ${data.status}</p>`;
                
                document.getElementById('startBtn').disabled = false;
                document.getElementById('stopBtn').disabled = true;
                document.getElementById('captureBtn').disabled = true;
                
                // Stop showing live view
                document.getElementById('video').src = '';
            }
            
            async function capturePhoto() {
                const response = await fetch('/dslr/capture', { method: 'POST' });
                
                if (response.ok) {
                    // Download the photo
                    const blob = await response.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `photo_${Date.now()}.jpg`;
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                } else {
                    alert('Failed to capture photo');
                }
            }
            
            // Initial detection
            detectCameras();
            
            // Check current status
            async function checkStatus() {
                const response = await fetch('/dslr/status');
                const data = await response.json();
                
                if (data.is_streaming) {
                    document.getElementById('startBtn').disabled = true;
                    document.getElementById('stopBtn').disabled = false;
                    document.getElementById('captureBtn').disabled = false;
                    document.getElementById('video').src = '/dslr/livestream';
                    document.getElementById('streamStatus').innerHTML = 
                        `<p><strong>Status:</strong> Streaming</p>
                         <p><strong>Camera:</strong> ${data.camera_model || 'Unknown'}</p>`;
                }
            }
            
            checkStatus();
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

@cam_route.get("/test")
async def test_endpoint():
    """
    Test endpoint to verify camera API is working.
    """
    return {
        "service": "DSLR Camera Controller",
        "status": "operational",
        "timestamp": time.time(),
        "endpoints": [
            {"path": "/dslr/detect", "method": "GET", "desc": "Detect USB cameras"},
            {"path": "/dslr/connect", "method": "POST", "desc": "Connect to specific camera"},
            {"path": "/dslr/livestream", "method": "GET", "desc": "MJPEG live stream"},
            {"path": "/dslr/start", "method": "POST", "desc": "Start stream"},
            {"path": "/dslr/stop", "method": "POST", "desc": "Stop stream"},
            {"path": "/dslr/status", "method": "GET", "desc": "Stream status"},
            {"path": "/dslr/capture", "method": "POST", "desc": "Capture photo"},
            {"path": "/dslr/select", "method": "GET", "desc": "Web interface"}
        ]
    }
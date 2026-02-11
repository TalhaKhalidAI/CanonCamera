from fastapi import Request
from App.repository.DLSR_Helper import CameraLiveViewStreamer

def get_camera_streamer(request: Request) -> CameraLiveViewStreamer:
    """
    FastAPI dependency that returns the shared CameraLiveViewStreamer instance.
    The instance is expected to be stored in the app state.
    """
    return request.app.state.camera_streamer

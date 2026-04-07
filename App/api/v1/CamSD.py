import json
import os
from datetime import datetime
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import Response

from App.api.dependencies.auth import get_current_active_user
from App.api.dependencies.camera import get_camera_streamer, CameraLiveViewStreamer
from App.core.LoggingInit import get_core_logger

logger = get_core_logger(__name__)

# NOTE: prefix is "/sd" — routes below use "/list", "/test" etc.
# Effective paths under the app router: /app/v1/sd/list, /app/v1/sd/test …
camsd_route = APIRouter(prefix="/sd", tags=["DSLR_Storage"])


@camsd_route.get("/list")
async def list_sd_card(
    folder: str = "/",
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
   # _user=Depends(get_current_active_user),
):
    """List contents of the camera's SD card."""
    if not clsr.get_status().get("camera_connected"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Camera not connected. Please connect first.",
        )
    contents = await clsr.list_sd_card_contents(folder)
    return {"status": "success", "folder": folder, "count": len(contents), "contents": contents}


@camsd_route.get("/test")
async def test_sd_card(
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
   # _user=Depends(get_current_active_user),
):
    """Test SD card access."""
    results: Dict = {
        "status": "testing",
        "timestamp": datetime.now().isoformat(),
        "tests": [],
    }

    info = clsr.get_status()
    results["tests"].append({
        "test": "camera_connection",
        "result": info.get("camera_connected", False),
        "details": info,
    })
    if not info.get("camera_connected"):
        results["status"] = "failed"
        results["message"] = "Camera not connected"
        return results

    try:
        root = await clsr.list_sd_card_contents("/")
        files = [c for c in root if c["is_file"]]
        folders = [c for c in root if not c["is_file"]]
        results["tests"].append({"test": "list_root", "result": True, "details": f"{len(root)} items"})
        results["root_sample"] = {
            "total": len(root), "files": len(files), "folders": len(folders),
            "file_names": [f["name"] for f in files[:3]],
            "folder_names": [f["name"] for f in folders[:3]],
        }
    except Exception as e:
        results["tests"].append({"test": "list_root", "result": False, "error": str(e)})

    all_ok = [t["result"] for t in results["tests"]]
    results["status"] = "success" if all(all_ok) else "partial" if any(all_ok) else "failed"
    results["message"] = "All tests passed" if all(all_ok) else "Some tests failed"
    return results


@camsd_route.get("/search")
async def search_images(
    folder: str = "/",
    recursive: bool = True,
    extensions: Optional[str] = None,
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
   # _user=Depends(get_current_active_user),
):
    """Search for images on SD card."""
    ext_list = [e.strip().lower() for e in extensions.split(",")] if extensions else None
    images = await clsr.search_images(folder, recursive, ext_list)
    return {
        "status": "success",
        "search_params": {"folder": folder, "recursive": recursive, "extensions": ext_list},
        "count": len(images),
        "images": images,
    }


@camsd_route.get("/download")
async def download_sd_image(
    file_path: str,
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
   # _user=Depends(get_current_active_user),
):
    """Download an image from SD card by absolute path."""
    file_data, filename, metadata = await clsr.download_image_by_path(file_path)

    if file_data[:2] == b"\xff\xd8":
        ct = "image/jpeg"
    elif file_data[:4] == b"\x89PNG":
        ct = "image/png"
    elif file_data[:2] == b"BM":
        ct = "image/bmp"
    elif file_data[:4] in (b"II\x2a\x00", b"MM\x00\x2a"):
        ct = "image/tiff"
    else:
        ct = "application/octet-stream"

    return Response(
        content=file_data,
        media_type=ct,
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "X-File-Metadata": json.dumps(metadata),
        },
    )


@camsd_route.get("/thumbnail")
async def get_image_thumbnail(
    file_path: str,
    width: int = 320,
    height: int = 240,
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
   # _user=Depends(get_current_active_user),
):
    """Get an embedded EXIF thumbnail from the camera."""
    folder = os.path.dirname(file_path) if "/" in file_path else "/"
    filename = os.path.basename(file_path) if "/" in file_path else file_path
    thumb = await clsr.get_image_thumbnail(folder, filename, width, height)
    return Response(
        content=thumb,
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@camsd_route.post("/batch_download")
async def batch_download_images(
    file_list: List[Dict],
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
   # _user=Depends(get_current_active_user),
):
    """Download multiple images. Returns a ZIP for >1 file."""
    results = await clsr.download_multiple_images(file_list)
    ok = [r for r in results if r["success"]]

    if len(ok) > 1:
        import zipfile
        from io import BytesIO

        buf = BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for r in ok:
                zf.writestr(r["suggested_filename"], r["data"])
        buf.seek(0)
        return Response(
            content=buf.read(),
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=camera_images.zip"},
        )
    elif len(ok) == 1:
        r = ok[0]
        ct = "image/jpeg" if r["data"][:2] == b"\xff\xd8" else "application/octet-stream"
        return Response(
            content=r["data"],
            media_type=ct,
            headers={"Content-Disposition": f"attachment; filename={r['suggested_filename']}"},
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No images were successfully downloaded",
        )


@camsd_route.delete("/delete")
async def delete_sd_image(
    file_path: str,
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer),
   # _user=Depends(get_current_active_user),
):
    """Delete an image from the camera's SD card."""
    folder = os.path.dirname(file_path) if "/" in file_path else "/"
    filename = os.path.basename(file_path) if "/" in file_path else file_path
    success = await clsr.delete_image(folder, filename)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to delete {file_path}",
        )
    return {"status": "success", "message": f"Deleted {file_path}"}
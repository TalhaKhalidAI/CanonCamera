from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse, Response, HTMLResponse
from typing import Optional
import time
from contextlib import asynccontextmanager
from App.api.dependencies.auth import get_current_active_user
from App.api.dependencies.camera import get_camera_streamer, CameraLiveViewStreamer
from App.core.LoggingInit import get_core_logger
from typing import List, Dict

# Initialize logger
logger = get_core_logger(__name__)

camsd_route = APIRouter(
    prefix="/sd",
    tags=["DSLR_Storage"]
)
@camsd_route.get("/sd/list")
async def list_sd_card(folder: str = "/", clsr: CameraLiveViewStreamer = Depends(get_camera_streamer)):
    """
    List contents of camera's SD card.
    """
    try:
        if not clsr:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Camera controller not initialized"
            )
        
        # Check if camera is connected
        status_info = clsr.get_status()
        if not status_info.get('camera_connected', False):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Camera not connected. Please connect to a camera first."
            )
        
        try:
            contents = clsr.list_sd_card_contents(folder)
            
            return {
                "status": "success",
                "folder": folder,
                "count": len(contents),
                "contents": contents
            }
        except Exception as e:
            logger.error(f"Error in list_sd_card_contents: {e}")
            # Try to provide more helpful error message
            if "not initialized" in str(e).lower():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Camera not connected. Please connect to a camera first."
                )
            elif "folder" in str(e).lower():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Folder '{folder}' not found on camera."
                )
            else:
                raise
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing SD card: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list SD card: {str(e)}"
        )

 
@camsd_route.get("/sd/test")
async def test_sd_card(clsr: CameraLiveViewStreamer = Depends(get_camera_streamer)):
    """
    Test SD card access with simple operations.
    """
    try:
        if not clsr:
            return {
                "status": "error",
                "message": "Camera controller not initialized"
            }
        
        results = {
            "status": "testing",
            "timestamp": datetime.now().isoformat(),
            "tests": []
        }
        
        # Test 1: Check camera connection
        status_info = clsr.get_status()
        results['tests'].append({
            "test": "camera_connection",
            "result": status_info.get('camera_connected', False),
            "details": status_info
        })
        
        if not status_info.get('camera_connected', False):
            results['status'] = "failed"
            results['message'] = "Camera not connected"
            return results
        
        # Test 2: Try to list root folder
        try:
            root_contents = clsr.list_sd_card_contents("/")
            results['tests'].append({
                "test": "list_root",
                "result": True,
                "details": f"Found {len(root_contents)} items"
            })
            
            # Show sample of what was found
            files = [c for c in root_contents if c['is_file']]
            folders = [c for c in root_contents if not c['is_file']]
            
            results['root_sample'] = {
                "total_items": len(root_contents),
                "files": len(files),
                "folders": len(folders),
                "file_names": [f['name'] for f in files[:3]],
                "folder_names": [f['name'] for f in folders[:3]]
            }
            
        except Exception as e:
            results['tests'].append({
                "test": "list_root",
                "result": False,
                "error": str(e)
            })
        
        # Test 3: Try to get basic info (non-recursive)
        try:
            with clsr.lock:
                if clsr.camera and clsr.is_initialized:
                    # Try to get camera abilities
                    try:
                        abilities = clsr.camera.get_abilities()
                        results['camera_abilities'] = str(abilities)[:200]
                    except:
                        results['camera_abilities'] = "Not available"
                    
                    # Try to get config
                    try:
                        config = clsr.camera.get_config(clsr.context)
                        results['config_available'] = True
                        
                        # Check for storage config
                        storage_keys = ['capturetarget', 'storage']
                        for key in storage_keys:
                            try:
                                storage_config = config.get_child_by_name(key)
                                results[f'config_{key}'] = storage_config.get_value()
                            except:
                                pass
                    except:
                        results['config_available'] = False
                    
                    results['tests'].append({
                        "test": "camera_config",
                        "result": True
                    })
                else:
                    results['tests'].append({
                        "test": "camera_config",
                        "result": False,
                        "error": "Camera not initialized"
                    })
        except Exception as e:
            results['tests'].append({
                "test": "camera_config",
                "result": False,
                "error": str(e)
            })
        
        # Determine overall status
        all_tests = [t['result'] for t in results['tests']]
        if all(all_tests):
            results['status'] = "success"
            results['message'] = "All tests passed"
        elif any(all_tests):
            results['status'] = "partial"
            results['message'] = "Some tests passed"
        else:
            results['status'] = "failed"
            results['message'] = "All tests failed"
        
        return results
            
    except Exception as e:
        logger.error(f"Error testing SD card: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }
@camsd_route.get("/sd/search")
async def search_images(
    folder: str = "/",
    recursive: bool = True,
    extensions: str = None,
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer)
):
    """
    Search for images on SD card.
    """
    try:
        if not clsr:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Camera not connected"
            )
        
        # Parse extensions parameter
        ext_list = None
        if extensions:
            ext_list = [ext.strip().lower() for ext in extensions.split(',')]
        
        images = clsr.search_images(folder, recursive, ext_list)
        
        return {
            "status": "success",
            "search_params": {
                "folder": folder,
                "recursive": recursive,
                "extensions": ext_list
            },
            "count": len(images),
            "images": images
        }
            
    except Exception as e:
        logger.error(f"Error searching images: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search images: {str(e)}"
        )

@camsd_route.get("/sd/download")
async def download_sd_image(file_path: str, clsr: CameraLiveViewStreamer = Depends(get_camera_streamer)):
    """
    Download an image from SD card by path.
    """
    try:
        if not clsr:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Camera not connected"
            )
        
        file_data, filename, metadata = clsr.download_image_by_path(file_path)
        
        # Determine content type
        if file_data[:2] == b'\xff\xd8':
            content_type = "image/jpeg"
        elif file_data[:4] == b'\x89PNG':
            content_type = "image/png"
        elif file_data[:2] == b'BM':
            content_type = "image/bmp"
        elif file_data[:4] in [b'II\x2a\x00', b'MM\x00\x2a']:
            content_type = "image/tiff"
        else:
            content_type = "application/octet-stream"
        
        # Return as downloadable file
        return Response(
            content=file_data,
            media_type=content_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": content_type,
                "X-File-Metadata": json.dumps(metadata)
            }
        )
            
    except Exception as e:
        logger.error(f"Error downloading image: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to download image: {str(e)}"
        )

@camsd_route.get("/sd/thumbnail")
async def get_image_thumbnail(
    file_path: str,
    width: int = 320,
    height: int = 240,
    clsr: CameraLiveViewStreamer = Depends(get_camera_streamer)
):
    """
    Get a thumbnail of an image from SD card.
    """
    try:
        if not clsr:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Camera not connected"
            )
        
        # Extract folder and filename
        if '/' in file_path:
            folder = os.path.dirname(file_path)
            filename = os.path.basename(file_path)
        else:
            folder = "/"
            filename = file_path
        
        thumbnail_data = clsr.get_image_thumbnail(folder, filename, width, height)
        
        return Response(
            content=thumbnail_data,
            media_type="image/jpeg",
            headers={
                "Cache-Control": "public, max-age=3600",
                "Content-Type": "image/jpeg"
            }
        )
            
    except Exception as e:
        logger.error(f"Error getting thumbnail: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get thumbnail: {str(e)}"
        )

@camsd_route.post("/sd/batch_download")
async def batch_download_images(file_list: List[Dict], clsr: CameraLiveViewStreamer = Depends(get_camera_streamer)):
    """
    Download multiple images at once.
    Expected format: [{"folder": "/DCIM", "filename": "IMG_001.jpg"}, ...]
    """
    try:
        if not clsr:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Camera not connected"
            )
        
        results = clsr.download_multiple_images(file_list)
        
        # Create a zip file if multiple successful downloads
        successful_downloads = [r for r in results if r['success']]
        
        if len(successful_downloads) > 1:
            import zipfile
            from io import BytesIO
            
            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for result in successful_downloads:
                    zip_file.writestr(
                        result['suggested_filename'],
                        result['data']
                    )
            
            zip_buffer.seek(0)
            zip_data = zip_buffer.read()
            
            return Response(
                content=zip_data,
                media_type="application/zip",
                headers={
                    "Content-Disposition": "attachment; filename=camera_images.zip",
                    "Content-Type": "application/zip"
                }
            )
        elif len(successful_downloads) == 1:
            # Return single image
            result = successful_downloads[0]
            content_type = "image/jpeg" if result['data'][:2] == b'\xff\xd8' else "application/octet-stream"
            
            return Response(
                content=result['data'],
                media_type=content_type,
                headers={
                    "Content-Disposition": f"attachment; filename={result['suggested_filename']}",
                    "Content-Type": content_type
                }
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No images were successfully downloaded"
            )
            
    except Exception as e:
        logger.error(f"Error in batch download: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch download failed: {str(e)}"
        )

@camsd_route.delete("/sd/delete")
async def delete_sd_image(file_path: str, clsr: CameraLiveViewStreamer = Depends(get_camera_streamer)):
    """
    Delete an image from camera's SD card.
    """
    try:
        if not clsr:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Camera not connected"
            )
        
        # Extract folder and filename
        if '/' in file_path:
            folder = os.path.dirname(file_path)
            filename = os.path.basename(file_path)
        else:
            folder = "/"
            filename = file_path
        
        success = clsr.delete_image(folder, filename)
        
        if success:
            return {
                "status": "success",
                "message": f"Deleted {file_path} from camera"
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to delete {file_path}"
            )
            
    except Exception as e:
        logger.error(f"Error deleting image: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete image: {str(e)}"
        )
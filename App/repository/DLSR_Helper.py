import gphoto2 as gp
import cv2
import numpy as np
import threading
import time
from queue import Queue, Empty
import logging
from typing import Optional, List, Dict, Tuple
import subprocess
import re

# Set up logging
logger = logging.getLogger(__name__)

class CameraLiveViewStreamer:
    """Non-blocking camera live view streamer for FastAPI with USB detection"""
    
    def __init__(self, max_queue_size=10):
        self.camera = None
        self.context = gp.Context()
        self.is_streaming = False
        self.frame_queue = Queue(maxsize=max_queue_size)
        self.stream_thread = None
        self.lock = threading.Lock()
        self.last_frame_time = 0
        self.frame_count = 0
        self.selected_port = None
        self.camera_model = "Unknown"
        
    def _kill_conflicting_processes(self):
        """Kill processes that might be using the camera"""
        try:
            subprocess.run(['pkill', '-f', 'gvfs-gphoto2'], 
                         capture_output=True, text=True)
            subprocess.run(['pkill', '-f', 'gphoto2'], 
                         capture_output=True, text=True)
            time.sleep(1)
            return True
        except Exception as e:
            logger.warning(f"Error killing processes: {e}")
            return False
    
    def detect_usb_cameras(self) -> List[Dict]:
        """
        Detect all available USB cameras.
        Returns list of camera dictionaries with model and port.
        """
        cameras = []
        
        try:
            # Method 1: Use gphoto2 command line
            try:
                result = subprocess.run(
                    ['gphoto2', '--auto-detect'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                
                lines = result.stdout.strip().split('\n')
                for line in lines[2:]:  # Skip header lines
                    if line and '---' not in line:
                        parts = line.strip().split()
                        if len(parts) >= 2:
                            model = ' '.join(parts[:-1])
                            port = parts[-1]
                            cameras.append({
                                'model': model,
                                'port': port,
                                'type': 'gphoto2_detected',
                                'source': 'gphoto2'
                            })
            except Exception as e:
                logger.debug(f"gphoto2 auto-detect failed: {e}")
            
            # Method 2: Use Python libgphoto2
            try:
                camera_list = gp.Camera.autodetect()
                for name, addr in camera_list:
                    if not any(cam['port'] == addr for cam in cameras):
                        cameras.append({
                            'model': name,
                            'port': addr,
                            'type': 'autodetect',
                            'source': 'libgphoto2'
                        })
            except Exception as e:
                logger.debug(f"Python autodetect failed: {e}")
            
            # Remove duplicates
            unique_cameras = []
            seen_ports = set()
            for cam in cameras:
                if cam['port'] not in seen_ports:
                    seen_ports.add(cam['port'])
                    unique_cameras.append(cam)
            
            return unique_cameras
            
        except Exception as e:
            logger.error(f"Error detecting USB cameras: {e}")
            return []
    
    def test_camera_port(self, port: str) -> Dict:
        """Test if a specific port has a working camera"""
        test_result = {
            'port': port,
            'success': False,
            'model': None,
            'error': None,
            'capabilities': []
        }
        
        try:
            temp_camera = gp.Camera()
            
            # Try to set port
            try:
                camera_list = gp.Camera.autodetect()
                for name, addr in camera_list:
                    if addr == port:
                        temp_camera.set_port_info(addr)
                        break
            except:
                pass
            
            temp_camera.init(self.context)
            
            # Get camera info
            try:
                summary = temp_camera.get_summary(self.context)
                test_result['model'] = str(summary)[:100]  # Limit length
            except:
                pass
            
            # Test capabilities
            try:
                config = temp_camera.get_config(self.context)
                
                # Test live view
                viewfinder_names = ['viewfinder', 'eosviewfinder', 'liveview']
                for name in viewfinder_names:
                    try:
                        config.get_child_by_name(name)
                        test_result['capabilities'].append('live_view')
                        break
                    except:
                        continue
                
                # Test capture
                try:
                    temp_camera.capture(gp.GP_CAPTURE_IMAGE, self.context)
                    test_result['capabilities'].append('capture')
                except:
                    pass
                
            except:
                pass
            
            test_result['success'] = True
            temp_camera.exit(self.context)
            
        except gp.GPhoto2Error as e:
            test_result['error'] = str(e)
        except Exception as e:
            test_result['error'] = f"Unexpected error: {e}"
        
        return test_result
    
    def connect_to_camera(self, port: str = None) -> bool:
        """
        Connect to a specific camera by port.
        If no port specified, auto-select first available.
        """
        try:
            with self.lock:
                # Clean up existing connection
                if self.camera:
                    try:
                        self.camera.exit(self.context)
                    except:
                        pass
                    self.camera = None
                
                self.camera = gp.Camera()
                
                # If port specified, try to use it
                if port:
                    self.selected_port = port
                    try:
                        camera_list = gp.Camera.autodetect()
                        for name, addr in camera_list:
                            if addr == port:
                                self.camera.set_port_info(addr)
                                self.camera_model = name
                                logger.info(f"Connected to {name} at {port}")
                                break
                    except Exception as e:
                        logger.warning(f"Could not set port {port}: {e}")
                        # Try without port
                        self.selected_port = None
                
                # Initialize camera
                self.camera.init(self.context)
                
                # Get camera model
                try:
                    self.camera_model = str(self.camera.get_summary(self.context))
                except:
                    pass
                
                logger.info(f"Camera connected successfully: {self.camera_model}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to connect to camera: {e}")
            return False
    
    def initialize_camera(self, port: str = None):
        """Initialize camera connection with optional port"""
        try:
            # First connect to camera
            if not self.connect_to_camera(port):
                return False
            
            with self.lock:
                # Enable live view
                config = self.camera.get_config(self.context)
                viewfinder_names = ['viewfinder', 'eosviewfinder', 'liveview']
                viewfinder_config = None
                
                for name in viewfinder_names:
                    try:
                        viewfinder_config = config.get_child_by_name(name)
                        logger.info(f"Found viewfinder config: {name}")
                        break
                    except:
                        continue
                
                if viewfinder_config:
                    viewfinder_config.set_value(1)
                    self.camera.set_config(config, self.context)
                    logger.info("Live view enabled on camera")
                else:
                    logger.warning("Could not find viewfinder config")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize camera: {e}")
            return False
    
    def _capture_frame(self):
        """Capture a single frame from camera"""
        try:
            with self.lock:
                if not self.camera:
                    return None
                
                camera_file = gp.CameraFile()
                self.camera.capture_preview(camera_file, self.context)
                file_data = camera_file.get_data_and_size()
                return file_data
        except Exception as e:
            logger.error(f"Frame capture error: {e}")
            return None
    
    def _stream_frames(self):
        """Background thread function to continuously capture frames"""
        logger.info("Starting frame capture thread")
        
        while self.is_streaming:
            try:
                # Capture frame
                frame_data = self._capture_frame()
                
                if frame_data is not None:
                    # Add to queue, drop old frames if queue is full
                    if self.frame_queue.full():
                        try:
                            self.frame_queue.get_nowait()
                        except Empty:
                            pass
                    
                    self.frame_queue.put(frame_data)
                    self.frame_count += 1
                    
                    # Log FPS every 30 frames
                    if self.frame_count % 30 == 0:
                        current_time = time.time()
                        elapsed = current_time - self.last_frame_time
                        if elapsed > 0:
                            fps = 30 / elapsed
                            logger.debug(f"Stream FPS: {fps:.1f}")
                        self.last_frame_time = current_time
                
                # Small delay to prevent CPU overload
                time.sleep(0.01)
                
            except Exception as e:
                logger.error(f"Error in stream thread: {e}")
                time.sleep(0.1)
        
        logger.info("Frame capture thread stopped")
    
    def start_streaming(self, port: str = None):
        """Start the live view stream with optional port"""
        if self.is_streaming:
            logger.warning("Stream is already running")
            return False
        
        # Kill conflicting processes
        self._kill_conflicting_processes()
        
        # Initialize camera with optional port
        if not self.initialize_camera(port):
            return False
        
        self.is_streaming = True
        self.last_frame_time = time.time()
        self.frame_count = 0
        
        # Clear queue
        while not self.frame_queue.empty():
            try:
                self.frame_queue.get_nowait()
            except Empty:
                break
        
        # Start background thread
        self.stream_thread = threading.Thread(target=self._stream_frames, daemon=True)
        self.stream_thread.start()
        
        logger.info(f"Live view streaming started on {self.camera_model}")
        return True
    
    def stop_streaming(self):
        """Stop the live view stream"""
        if not self.is_streaming:
            return
        
        self.is_streaming = False
        
        # Wait for thread to finish
        if self.stream_thread and self.stream_thread.is_alive():
            self.stream_thread.join(timeout=2.0)
        
        # Disable live view on camera
        try:
            with self.lock:
                if self.camera:
                    config = self.camera.get_config(self.context)
                    viewfinder_names = ['viewfinder', 'eosviewfinder', 'liveview']
                    
                    for name in viewfinder_names:
                        try:
                            viewfinder_config = config.get_child_by_name(name)
                            viewfinder_config.set_value(0)
                            self.camera.set_config(config, self.context)
                            logger.info("Live view disabled on camera")
                            break
                        except:
                            continue
        except Exception as e:
            logger.error(f"Error disabling live view: {e}")
        
        logger.info("Live view streaming stopped")
    
    def get_latest_frame(self, timeout=0.1) -> Optional[bytes]:
        """Get the latest frame from the queue"""
        try:
            return self.frame_queue.get(timeout=timeout)
        except Empty:
            return None
    
    def generate_mjpeg_stream(self):
        """Generator function for MJPEG streaming"""
        boundary = 'frame'
        
        while self.is_streaming:
            frame_data = self.get_latest_frame(timeout=0.5)
            
            if frame_data is not None:
                yield (b'--' + boundary.encode() + b'\r\n'
                       b'Content-Type: image/jpeg\r\n'
                       b'Content-Length: ' + str(len(frame_data)).encode() + b'\r\n\r\n' +
                       frame_data + b'\r\n')
            else:
                # Send a placeholder frame if no data
                placeholder = self._create_placeholder_frame()
                yield (b'--' + boundary.encode() + b'\r\n'
                       b'Content-Type: image/jpeg\r\n'
                       b'Content-Length: ' + str(len(placeholder)).encode() + b'\r\n\r\n' +
                       placeholder + b'\r\n')
                time.sleep(0.1)
    
    def _create_placeholder_frame(self) -> bytes:
        """Create a placeholder frame when camera is not available"""
        img = np.zeros((480, 640, 3), dtype=np.uint8)
        cv2.putText(img, f"Camera: {self.camera_model}", (50, 100), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
        cv2.putText(img, f"Port: {self.selected_port or 'Auto'}", (50, 150), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
        cv2.putText(img, "Status: Waiting for frames...", (50, 200), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
        
        _, buffer = cv2.imencode('.jpg', img)
        return buffer.tobytes()
    
    def get_status(self) -> dict:
        """Get current streaming status"""
        return {
            'is_streaming': self.is_streaming,
            'camera_model': self.camera_model,
            'selected_port': self.selected_port,
            'queue_size': self.frame_queue.qsize(),
            'frame_count': self.frame_count,
            'camera_connected': self.camera is not None
        }
    
    def capture_photo(self, format: str = "jpg") -> Tuple[bytes, str]:
        """Capture a high-resolution photo with format selection"""
        try:
            with self.lock:
                if not self.camera:
                    raise Exception("Camera not initialized. Call start_streaming() first.")
                
                # Get camera config to check current settings
                config = self.camera.get_config(self.context)
                
                # Try to set image format if possible
                try:
                    # Common format setting names
                    format_names = ['imageformat', 'imagequality', 'capturetarget']
                    for name in format_names:
                        try:
                            format_config = config.get_child_by_name(name)
                            # Try to set to JPEG if available
                            for i in range(format_config.count_choices()):
                                choice = format_config.get_choice(i)
                                if 'jpeg' in choice.lower() or 'jpg' in choice.lower():
                                    format_config.set_value(choice)
                                    self.camera.set_config(config, self.context)
                                    logger.info(f"Set image format to: {choice}")
                                    break
                            break
                        except:
                            continue
                except Exception as e:
                    logger.warning(f"Could not set image format: {e}")
                
                # Capture full image
                capture_info = self.camera.capture(gp.GP_CAPTURE_IMAGE, self.context)
                logger.info(f"Captured image: {capture_info.folder}/{capture_info.name}")
                
                # Determine actual file type
                file_extension = capture_info.name.split('.')[-1].lower()
                
                # Download from camera
                camera_file = gp.CameraFile()
                self.camera.file_get(
                    capture_info.folder,
                    capture_info.name,
                    gp.GP_FILE_TYPE_NORMAL,
                    camera_file,
                    self.context
                )
                
                # Get image data
                file_data = camera_file.get_data_and_size()
                
                # Check if it's actually JPEG data
                if file_data[:2] == b'\xff\xd8':
                    # It's a valid JPEG file
                    logger.info(f"Captured valid JPEG image ({len(file_data)} bytes)")
                    return file_data, "photo.jpg"
                else:
                    # Not JPEG - could be RAW or other format
                    logger.warning(f"Image is not JPEG (starts with {file_data[:2].hex()})")
                    
                    # Try to convert using OpenCV if possible
                    try:
                        # Try to decode as various formats
                        img_array = np.frombuffer(file_data, dtype=np.uint8)
                        
                        # First try direct decode
                        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
                        
                        if img is not None:
                            # Successfully decoded - convert to JPEG
                            logger.info(f"Decoded image, converting to JPEG")
                            _, jpeg_data = cv2.imencode('.jpg', img, [cv2.IMWRITE_JPEG_QUALITY, 95])
                            return jpeg_data.tobytes(), "photo.jpg"
                        
                        # If that failed, try different approach for RAW files
                        logger.info("Attempting to read file from camera storage directly")
                        
                        # Alternative: Use camera's preview capture which is always JPEG
                        preview_file = gp.CameraFile()
                        self.camera.capture_preview(preview_file, self.context)
                        preview_data = preview_file.get_data_and_size()
                        
                        if preview_data[:2] == b'\xff\xd8':
                            logger.info("Using preview capture as fallback")
                            return preview_data, "preview.jpg"
                        
                    except Exception as decode_error:
                        logger.error(f"Failed to decode image: {decode_error}")
                    
                    # Last resort: return raw data with correct extension
                    logger.warning(f"Returning raw file data with extension .{file_extension}")
                    return file_data, f"photo.{file_extension}"
                    
        except Exception as e:
            logger.error(f"Capture error: {e}")
            raise

    def capture_photo_as_jpeg(self) -> Tuple[bytes, str]:
        """Force capture as JPEG by first taking a preview"""
        try:
            with self.lock:
                if not self.camera:
                    raise Exception("Camera not initialized.")
                
                # Method 1: Use preview which is always JPEG
                camera_file = gp.CameraFile()
                self.camera.capture_preview(camera_file, self.context)
                preview_data = camera_file.get_data_and_size()
                
                # Method 2: Also capture full image for quality
                capture_info = self.camera.capture(gp.GP_CAPTURE_IMAGE, self.context)
                
                # Try to download and check if it's JPEG
                full_file = gp.CameraFile()
                try:
                    self.camera.file_get(
                        capture_info.folder,
                        capture_info.name,
                        gp.GP_FILE_TYPE_NORMAL,
                        full_file,
                        self.context
                    )
                    full_data = full_file.get_data_and_size()
                    
                    if full_data[:2] == b'\xff\xd8':
                        # Full image is JPEG
                        logger.info("Full image is JPEG format")
                        return full_data, "photo.jpg"
                    else:
                        # Full image is not JPEG, use preview
                        logger.info("Using preview as JPEG (full image is not JPEG)")
                        return preview_data, "photo_preview.jpg"
                        
                except:
                    # If full image download fails, use preview
                    logger.warning("Full image download failed, using preview")
                    return preview_data, "photo_preview.jpg"
                    
        except Exception as e:
            logger.error(f"JPEG capture error: {e}")
            raise
    
    def cleanup(self):
        """Clean up resources"""
        self.stop_streaming()
        
        with self.lock:
            if self.camera:
                try:
                    self.camera.exit(self.context)
                    logger.info("Camera connection closed")
                except Exception as e:
                    logger.error(f"Error closing camera: {e}")
                finally:
                    self.camera = None
import gphoto2 as gp
import cv2
import numpy as np
import threading
import time
from queue import Queue, Empty
import logging
from typing import Optional, List, Dict, Tuple, Any
import subprocess
import re
import os
from datetime import datetime
import json

# Set up logging
logger = logging.getLogger(__name__)

class CameraLiveViewStreamer:
    """Non-blocking camera live view streamer for FastAPI with USB detection"""
    
    def __init__(self, max_queue_size=10):
        self.camera = None
        self.context = None
        self.is_streaming = False
        self.frame_queue = Queue(maxsize=max_queue_size)
        self.stream_thread = None
        self.lock = threading.Lock()
        self.last_frame_time = 0
        self.frame_count = 0
        self.selected_port = None
        self.camera_model = "Unknown"
        self.is_initialized = False
        
    def _initialize_context(self):
        """Initialize gphoto2 context"""
        if self.context is None:
            try:
                self.context = gp.Context()
                logger.info("Initialized gphoto2 context")
            except Exception as e:
                logger.error(f"Failed to initialize context: {e}")
                self.context = None
                return False
        return True
    
    def _kill_conflicting_processes(self):
        """Kill processes that might be using the camera"""
        try:
            # Kill common gphoto2 processes
            processes = ['gvfs-gphoto2', 'gphoto2', 'ptpcamera', 'usbmuxd']
            for proc in processes:
                try:
                    subprocess.run(['pkill', '-9', '-f', proc], 
                                 capture_output=True, text=True, timeout=2)
                except:
                    pass
            
            # Also try fuser to kill processes using USB
            try:
                # Check for USB camera processes
                result = subprocess.run(['fuser', '-v', '/dev/bus/usb/*'], 
                                      capture_output=True, text=True)
                for line in result.stdout.split('\n'):
                    if 'gphoto' in line.lower() or 'ptp' in line.lower():
                        pid = line.split()[0]
                        if pid.isdigit():
                            os.kill(int(pid), 9)
            except:
                pass
            
            time.sleep(0.5)
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
            # Method 1: Use Python libgphoto2 autodetect
            try:
                if not self._initialize_context():
                    return []
                
                camera_list = gp.Camera.autodetect(self.context)
                for name, addr in camera_list:
                    cameras.append({
                        'model': name,
                        'port': addr,
                        'type': 'autodetect',
                        'source': 'libgphoto2'
                    })
                logger.info(f"Found {len(camera_list)} cameras via libgphoto2")
            except Exception as e:
                logger.debug(f"Python autodetect failed: {e}")
            
            # Method 2: Use gphoto2 command line
            try:
                result = subprocess.run(
                    ['gphoto2', '--auto-detect'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                
                lines = result.stdout.strip().split('\n')
                for line in lines[2:]:  # Skip header lines
                    if line and '---' not in line and 'Port' not in line:
                        # Parse model and port
                        parts = line.strip().split()
                        if len(parts) >= 2:
                            # Port is usually last element
                            port = parts[-1]
                            model = ' '.join(parts[:-1])
                            
                            # Check if we already have this camera
                            if not any(cam['port'] == port for cam in cameras):
                                cameras.append({
                                    'model': model,
                                    'port': port,
                                    'type': 'gphoto2_detected',
                                    'source': 'gphoto2'
                                })
            except Exception as e:
                logger.debug(f"gphoto2 auto-detect failed: {e}")
            
            # Method 3: Check USB devices directly
            try:
                result = subprocess.run(
                    ['lsusb'],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                
                for line in result.stdout.split('\n'):
                    if 'Camera' in line or 'photo' in line.lower():
                        parts = line.split()
                        if len(parts) > 5:
                            bus = parts[1]
                            device = parts[3].strip(':')
                            cameras.append({
                                'model': ' '.join(parts[6:]),
                                'port': f"usb:{bus},{device}",
                                'type': 'lsusb_detected',
                                'source': 'lsusb'
                            })
            except Exception as e:
                logger.debug(f"lsusb failed: {e}")
            
            # Remove duplicates
            unique_cameras = []
            seen_ports = set()
            for cam in cameras:
                if cam['port'] not in seen_ports:
                    seen_ports.add(cam['port'])
                    unique_cameras.append(cam)
            
            logger.info(f"Total unique cameras detected: {len(unique_cameras)}")
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
        
        temp_camera = None
        
        try:
            # Kill any conflicting processes first
            self._kill_conflicting_processes()
            
            # Initialize context if needed
            if not self._initialize_context():
                test_result['error'] = "Failed to initialize context"
                return test_result
            
            # Create temporary camera
            temp_camera = gp.Camera()
            
            # Try to set port
            try:
                # Get autodetect list to match port
                camera_list = gp.Camera.autodetect(self.context)
                for name, addr in camera_list:
                    if addr == port:
                        temp_camera.set_port_info(addr)
                        break
            except Exception as e:
                logger.debug(f"Could not set port info for {port}: {e}")
            
            # Try to initialize
            try:
                temp_camera.init(self.context)
                test_result['success'] = True
            except gp.GPhoto2Error as e:
                test_result['error'] = f"GPhoto2Error: {e}"
                return test_result
            except Exception as e:
                test_result['error'] = f"Init error: {e}"
                return test_result
            
            # Get camera info
            try:
                summary = temp_camera.get_summary(self.context)
                # Extract model from summary
                summary_str = str(summary)
                # Look for model information
                model_match = re.search(r'Model:\s*(.+)', summary_str)
                if model_match:
                    test_result['model'] = model_match.group(1).strip()
                else:
                    test_result['model'] = summary_str[:100]  # Limit length
            except Exception as e:
                logger.debug(f"Could not get summary: {e}")
            
            # Test capabilities
            try:
                config = temp_camera.get_config(self.context)
                
                # Test live view
                viewfinder_names = ['viewfinder', 'eosviewfinder', 'liveview', 'capturetarget']
                for name in viewfinder_names:
                    try:
                        config.get_child_by_name(name)
                        test_result['capabilities'].append('live_view')
                        break
                    except:
                        continue
                
                # Test capture
                try:
                    # Quick capture test
                    capture_info = temp_camera.capture(gp.GP_CAPTURE_IMAGE, self.context)
                    test_result['capabilities'].append('capture')
                    
                    # Clean up captured file
                    try:
                        temp_camera.file_delete(capture_info.folder, capture_info.name, self.context)
                    except:
                        pass
                except:
                    pass
                
                # Test preview capture
                try:
                    camera_file = gp.CameraFile()
                    temp_camera.capture_preview(camera_file, self.context)
                    test_result['capabilities'].append('preview')
                except:
                    pass
                
            except Exception as e:
                logger.debug(f"Capabilities test failed: {e}")
            
            # Clean up
            try:
                temp_camera.exit(self.context)
            except:
                pass
            
            logger.info(f"Port test successful for {port}")
            
        except Exception as e:
            test_result['error'] = f"Unexpected error: {e}"
            # Clean up if camera was initialized
            if temp_camera:
                try:
                    temp_camera.exit(self.context)
                except:
                    pass
        
        return test_result
    
    def _safe_camera_init(self, port: str = None) -> bool:
        """Safely initialize camera connection"""
        try:
            # Kill conflicting processes
            self._kill_conflicting_processes()
            
            # Initialize context if needed
            if not self._initialize_context():
                logger.error("Failed to initialize gphoto2 context")
                return False
            
            # Create new camera instance
            camera = gp.Camera()
            
            # Set port if specified
            if port:
                try:
                    # Get autodetect list
                    camera_list = gp.Camera.autodetect(self.context)
                    for name, addr in camera_list:
                        if addr == port:
                            camera.set_port_info(addr)
                            self.selected_port = addr
                            self.camera_model = name
                            logger.info(f"Setting camera port to: {addr} ({name})")
                            break
                except Exception as e:
                    logger.warning(f"Could not set port {port}: {e}")
                    self.selected_port = None
            
            # Initialize camera with timeout protection
            camera.init(self.context)
            
            # Get camera info
            try:
                summary = camera.get_summary(self.context)
                summary_str = str(summary)
                model_match = re.search(r'Model:\s*(.+)', summary_str)
                if model_match:
                    self.camera_model = model_match.group(1).strip()
                else:
                    self.camera_model = summary_str[:100]
                logger.info(f"Camera model: {self.camera_model}")
            except Exception as e:
                logger.warning(f"Could not get camera summary: {e}")
            
            # Store camera instance
            self.camera = camera
            self.is_initialized = True
            
            logger.info("Camera initialized successfully")
            return True
            
        except gp.GPhoto2Error as e:
            logger.error(f"GPhoto2Error during camera init: {e}")
            if 'camera not found' in str(e).lower() or 'no camera' in str(e).lower():
                logger.error("No camera detected. Check USB connection.")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during camera init: {e}")
            return False
    
    def connect_to_camera(self, port: str = None) -> bool:
        """
        Connect to a specific camera by port.
        If no port specified, auto-select first available.
        """
        try:
            with self.lock:
                # Clean up existing connection
                self._safe_camera_cleanup()
                
                # If no port specified, try to auto-detect
                if port is None:
                    logger.info("No port specified, attempting auto-detect")
                    cameras = self.detect_usb_cameras()
                    if cameras:
                        # Try each camera until one works
                        for cam in cameras:
                            logger.info(f"Trying camera: {cam['model']} at {cam['port']}")
                            if self._safe_camera_init(cam['port']):
                                return True
                        logger.error("No cameras could be initialized")
                        return False
                    else:
                        logger.error("No cameras detected")
                        return False
                
                # Try to connect to specified port
                logger.info(f"Attempting to connect to port: {port}")
                return self._safe_camera_init(port)
                
        except Exception as e:
            logger.error(f"Failed to connect to camera: {e}")
            return False
    
    def _safe_camera_cleanup(self):
        """Safely clean up camera resources"""
        try:
            if self.camera:
                # Exit camera if initialized
                if self.is_initialized:
                    try:
                        self.camera.exit(self.context)
                        logger.info("Camera exited successfully")
                    except Exception as e:
                        logger.warning(f"Error exiting camera: {e}")
                
                self.camera = None
                self.is_initialized = False
                
        except Exception as e:
            logger.error(f"Error in camera cleanup: {e}")
        finally:
            self.camera = None
            self.is_initialized = False
    
    def initialize_camera(self, port: str = None):
        """Initialize camera connection with optional port"""
        try:
            # First connect to camera
            if not self.connect_to_camera(port):
                logger.error("Failed to connect to camera in initialize_camera")
                return False
            
            with self.lock:
                if not self.camera or not self.is_initialized:
                    logger.error("Camera not initialized properly")
                    return False
                
                # Try to enable live view if supported
                try:
                    config = self.camera.get_config(self.context)
                    viewfinder_names = ['viewfinder', 'eosviewfinder', 'liveview']
                    viewfinder_config = None
                    
                    for name in viewfinder_names:
                        try:
                            viewfinder_config = config.get_child_by_name(name)
                            logger.info(f"Found viewfinder config: {name}")
                            
                            # Try to enable viewfinder
                            try:
                                viewfinder_config.set_value(1)
                                self.camera.set_config(config, self.context)
                                logger.info("Live view enabled on camera")
                                break
                            except Exception as e:
                                logger.warning(f"Could not set viewfinder value: {e}")
                                continue
                        except:
                            continue
                    
                    if viewfinder_config is None:
                        logger.warning("Could not find viewfinder config. Some cameras don't support live view.")
                        # Continue anyway - some cameras work without explicit viewfinder
                
                except Exception as e:
                    logger.warning(f"Could not configure live view: {e}")
                    # Continue anyway - some cameras don't need this
            
            logger.info("Camera initialization complete")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize camera: {e}")
            self._safe_camera_cleanup()
            return False
    
    def _capture_frame(self):
        """Capture a single frame from camera"""
        try:
            with self.lock:
                if not self.camera or not self.is_initialized:
                    logger.debug("Camera not ready for frame capture")
                    return None
                
                camera_file = gp.CameraFile()
                self.camera.capture_preview(camera_file, self.context)
                file_data = camera_file.get_data_and_size()
                return file_data
                
        except gp.GPhoto2Error as e:
            logger.error(f"GPhoto2Error in frame capture: {e}")
            # Check if camera disconnected
            if 'I/O' in str(e) or 'not found' in str(e):
                logger.error("Camera may have been disconnected")
                self._safe_camera_cleanup()
            return None
        except Exception as e:
            logger.error(f"Frame capture error: {e}")
            return None
    
    def _stream_frames(self):
        """Background thread function to continuously capture frames"""
        logger.info("Starting frame capture thread")
        
        retry_count = 0
        max_retries = 3
        
        while self.is_streaming:
            try:
                # Check if camera needs reconnection
                if not self.camera or not self.is_initialized:
                    if retry_count < max_retries:
                        logger.info(f"Attempting to reconnect camera (attempt {retry_count + 1}/{max_retries})")
                        if self.initialize_camera(self.selected_port):
                            logger.info("Camera reconnected successfully")
                            retry_count = 0
                        else:
                            retry_count += 1
                            time.sleep(1)
                            continue
                    else:
                        logger.error("Max reconnection attempts reached. Stopping stream.")
                        self.is_streaming = False
                        break
                
                # Capture frame
                frame_data = self._capture_frame()
                
                if frame_data is not None:
                    # Reset retry count on successful capture
                    retry_count = 0
                    
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
                else:
                    # Frame capture failed
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(f"Frame capture failed {retry_count} times. Camera may be disconnected.")
                        # Don't break, let reconnection logic handle it
                
                # Small delay to prevent CPU overload
                time.sleep(0.033)  # ~30 FPS
                
            except Exception as e:
                logger.error(f"Error in stream thread: {e}")
                retry_count += 1
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
            logger.error("Failed to initialize camera for streaming")
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
        
        logger.info("Stopping stream...")
        self.is_streaming = False
        
        # Wait for thread to finish
        if self.stream_thread and self.stream_thread.is_alive():
            self.stream_thread.join(timeout=2.0)
            if self.stream_thread.is_alive():
                logger.warning("Stream thread did not stop gracefully")
        
        # Disable live view on camera if possible
        try:
            with self.lock:
                if self.camera and self.is_initialized:
                    try:
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
                        logger.warning(f"Error disabling live view: {e}")
        except Exception as e:
            logger.error(f"Exception during stream stop: {e}")
        
        # Clean up camera
        self._safe_camera_cleanup()
        
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
        
        _, buffer = cv2.imencode('.jpg', img, [cv2.IMWRITE_JPEG_QUALITY, 80])
        return buffer.tobytes()
    
    def get_status(self) -> dict:
        """Get current streaming status"""
        return {
            'is_streaming': self.is_streaming,
            'camera_model': self.camera_model,
            'selected_port': self.selected_port,
            'queue_size': self.frame_queue.qsize(),
            'frame_count': self.frame_count,
            'camera_connected': self.camera is not None and self.is_initialized,
            'is_initialized': self.is_initialized
        }
    
    def capture_photo(self, format: str = "jpg") -> Tuple[bytes, str]:
        """Capture a high-resolution photo with format selection"""
        try:
            with self.lock:
                if not self.camera or not self.is_initialized:
                    raise Exception("Camera not initialized. Call start_streaming() first.")
                
                # Get camera config
                config = self.camera.get_config(self.context)
                
                # Try to set image format if possible
                try:
                    format_names = ['imageformat', 'imagequality', 'capturetarget']
                    for name in format_names:
                        try:
                            format_config = config.get_child_by_name(name)
                            # Look for JPEG options
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
                
                # Check if it's JPEG
                if file_data[:2] == b'\xff\xd8':
                    return file_data, "photo.jpg"
                else:
                    # Try to convert using OpenCV
                    try:
                        img_array = np.frombuffer(file_data, dtype=np.uint8)
                        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
                        
                        if img is not None:
                            _, jpeg_data = cv2.imencode('.jpg', img, [cv2.IMWRITE_JPEG_QUALITY, 95])
                            return jpeg_data.tobytes(), "photo.jpg"
                    except Exception as decode_error:
                        logger.error(f"Failed to decode image: {decode_error}")
                    
                    # Return raw data
                    file_extension = capture_info.name.split('.')[-1].lower()
                    return file_data, f"photo.{file_extension}"
                    
        except Exception as e:
            logger.error(f"Capture error: {e}")
            raise

    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up camera streamer...")
        self.stop_streaming()
        
        # Clean up context
        if self.context:
            # Note: gphoto2 Python bindings don't require explicit context cleanup
            self.context = None
        
        logger.info("Camera streamer cleanup complete")
    def _safe_camera_cleanup(self):
            """Safely clean up camera resources"""
            try:
                if self.camera:
                    # Exit camera if initialized
                    if self.is_initialized:
                        try:
                            self.camera.exit(self.context)
                            logger.info("Camera exited successfully")
                        except Exception as e:
                            logger.warning(f"Error exiting camera: {e}")
                    
                    self.camera = None
                    self.is_initialized = False
                    
            except Exception as e:
                logger.error(f"Error in camera cleanup: {e}")
            finally:
                self.camera = None
                self.is_initialized = False
    
    # ============================================
    # SD CARD ACCESS FUNCTIONS
    # ============================================
    
    def list_sd_card_contents(self, folder: str = "/") -> List[Dict]:
        """
        List all files and directories on the camera's SD card.
        
        Args:
            folder: Path to list (default is root "/")
            
        Returns:
            List of dictionaries containing file/directory info
        """
        try:
            with self.lock:
                if not self.camera or not self.is_initialized:
                    raise Exception("Camera not initialized. Connect to camera first.")
                
                contents = []
                
                # List files in the folder
                try:
                    files = self.camera.folder_list_files(folder, self.context)
                    for file_info in files:
                        # Get file size if possible
                        size_bytes = 0
                        try:
                            camera_file = gp.CameraFile()
                            self.camera.file_get(
                                folder,
                                file_info.name,
                                gp.GP_FILE_TYPE_NORMAL,
                                camera_file,
                                self.context
                            )
                            file_data = camera_file.get_data_and_size()
                            size_bytes = len(file_data)
                        except:
                            size_bytes = 0
                        
                        # Determine file type
                        file_name = str(file_info.name)  # Ensure it's a string
                        if '.' in file_name:
                            file_ext = file_name.lower().split('.')[-1]
                        else:
                            file_ext = ''
                            
                        if file_ext in ['jpg', 'jpeg', 'png', 'bmp', 'tiff', 'tif']:
                            file_type = "image"
                        elif file_ext in ['cr2', 'nef', 'arw', 'dng']:
                            file_type = "raw"
                        elif file_ext in ['mp4', 'avi', 'mov', 'mkv']:
                            file_type = "video"
                        else:
                            file_type = "file"
                        
                        contents.append({
                            'name': file_name,
                            'type': file_type,
                            'size': size_bytes,
                            'size_formatted': self._format_file_size(size_bytes),
                            'path': f"{folder}/{file_name}",
                            'folder': folder,
                            'extension': file_ext,
                            'is_file': True
                        })
                except Exception as e:
                    logger.warning(f"Could not list files in {folder}: {e}")
                
                # List folders
                try:
                    folders = self.camera.folder_list_folders(folder, self.context)
                    for folder_name in folders:
                        contents.append({
                            'name': str(folder_name),  # Ensure it's a string
                            'type': "folder",
                            'size': 0,
                            'size_formatted': "-",
                            'path': f"{folder}/{folder_name}",
                            'folder': folder,
                            'extension': "",
                            'is_file': False
                        })
                except Exception as e:
                    logger.warning(f"Could not list folders in {folder}: {e}")
                
                # Sort contents: folders first, then files alphabetically
                contents.sort(key=lambda x: (not x['is_file'], x['name'].lower()))
                
                logger.info(f"Listed {len(contents)} items from SD card folder: {folder}")
                return contents
                
        except Exception as e:
            logger.error(f"Error listing SD card contents: {e}")
            raise
 
    
    def search_images(self, 
                     folder: str = "/", 
                     recursive: bool = True,
                     extensions: List[str] = None) -> List[Dict]:
        """
        Search for images on the SD card.
        
        Args:
            folder: Starting folder path
            recursive: Whether to search subfolders
            extensions: List of file extensions to include (default: common image formats)
            
        Returns:
            List of image file information dictionaries
        """
        try:
            with self.lock:
                if not self.camera or not self.is_initialized:
                    raise Exception("Camera not initialized. Connect to camera first.")
                
                if extensions is None:
                    extensions = ['jpg', 'jpeg', 'png', 'bmp', 'tiff', 'tif', 'cr2', 'nef', 'arw', 'dng']
                
                images = []
                folders_to_search = [folder]
                searched_folders = set()
                
                while folders_to_search:
                    current_folder = folders_to_search.pop(0)
                    
                    # Avoid infinite loops
                    if current_folder in searched_folders:
                        continue
                    searched_folders.add(current_folder)
                    
                    try:
                        contents = self.list_sd_card_contents(current_folder)
                        
                        for item in contents:
                            if item['is_file']:
                                # Check if file has image extension
                                ext = item.get('extension', '').lower()
                                if ext in extensions:
                                    # Try to get more metadata if it's an image
                                    metadata = self._get_image_metadata(current_folder, item['name'])
                                    item.update(metadata)
                                    images.append(item)
                            elif recursive and item['type'] == 'folder':
                                # Add subfolder to search list
                                folders_to_search.append(item['path'])
                    except Exception as e:
                        logger.warning(f"Could not search folder {current_folder}: {e}")
                
                # Sort images by name (which often includes timestamp)
                images.sort(key=lambda x: x['name'].lower(), reverse=True)
                
                logger.info(f"Found {len(images)} images in search")
                return images
                
        except Exception as e:
            logger.error(f"Error searching images: {e}")
            raise
    
    def download_image(self, folder: str, filename: str) -> Tuple[bytes, str, Dict]:
        """
        Download a specific image from the camera's SD card.
        
        Args:
            folder: Folder path on camera
            filename: Name of the file to download
            
        Returns:
            Tuple of (file_data, suggested_filename, metadata)
        """
        try:
            with self.lock:
                if not self.camera or not self.is_initialized:
                    raise Exception("Camera not initialized. Connect to camera first.")
                
                # Ensure filename is string
                filename = str(filename)
                logger.info(f"Downloading image: {folder}/{filename}")
                
                # Download the file
                camera_file = gp.CameraFile()
                self.camera.file_get(
                    folder,
                    filename,
                    gp.GP_FILE_TYPE_NORMAL,
                    camera_file,
                    self.context
                )
                
                # Get file data
                file_data = camera_file.get_data_and_size()
                
                # Get metadata
                metadata = self._get_image_metadata(folder, filename)
                
                # Get file info for size
                metadata['size_bytes'] = len(file_data)
                metadata['size_formatted'] = self._format_file_size(len(file_data))
                
                # Determine suggested filename
                # Use original filename but sanitize it
                safe_filename = self._sanitize_filename(filename)
                
                logger.info(f"Successfully downloaded {filename} ({metadata['size_formatted']})")
                return file_data, safe_filename, metadata
                
        except Exception as e:
            logger.error(f"Error downloading image {folder}/{filename}: {e}")
            raise
    
    def download_image_by_path(self, file_path: str) -> Tuple[bytes, str, Dict]:
        """
        Download an image by full path.
        
        Args:
            file_path: Full path to file (e.g., "/DCIM/100CANON/image.jpg")
            
        Returns:
            Tuple of (file_data, suggested_filename, metadata)
        """
        try:
            # Extract folder and filename from path
            if '/' in file_path:
                folder = os.path.dirname(file_path)
                filename = os.path.basename(file_path)
            else:
                folder = "/"
                filename = file_path
            
            return self.download_image(folder, filename)
            
        except Exception as e:
            logger.error(f"Error downloading image by path {file_path}: {e}")
            raise
    
    def download_multiple_images(self, file_list: List[Dict]) -> List[Dict]:
        """
        Download multiple images at once.
        
        Args:
            file_list: List of dictionaries with 'folder' and 'filename' keys
            
        Returns:
            List of download results
        """
        try:
            results = []
            
            for i, file_info in enumerate(file_list):
                try:
                    folder = file_info.get('folder', '/')
                    filename = file_info.get('filename')
                    
                    if not filename:
                        logger.warning(f"Missing filename in item {i}")
                        results.append({
                            'success': False,
                            'error': 'Missing filename',
                            'original_path': file_info
                        })
                        continue
                    
                    # Ensure filename is string
                    filename = str(filename)
                    logger.info(f"Downloading {i+1}/{len(file_list)}: {folder}/{filename}")
                    
                    file_data, suggested_name, metadata = self.download_image(folder, filename)
                    
                    results.append({
                        'success': True,
                        'original_path': f"{folder}/{filename}",
                        'suggested_filename': suggested_name,
                        'size_bytes': len(file_data),
                        'size_formatted': metadata['size_formatted'],
                        'metadata': metadata,
                        'data': file_data  # Note: might be large, consider saving to disk instead
                    })
                    
                except Exception as e:
                    logger.error(f"Failed to download {file_info}: {e}")
                    results.append({
                        'success': False,
                        'original_path': str(file_info),
                        'error': str(e)
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"Error downloading multiple images: {e}")
            raise
    
    def delete_image(self, folder: str, filename: str) -> bool:
        """
        Delete an image from the camera's SD card.
        
        Args:
            folder: Folder path on camera
            filename: Name of the file to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.lock:
                if not self.camera or not self.is_initialized:
                    raise Exception("Camera not initialized. Connect to camera first.")
                
                # Ensure filename is string
                filename = str(filename)
                logger.warning(f"Deleting image from camera: {folder}/{filename}")
                
                # Delete the file
                self.camera.file_delete(folder, filename, self.context)
                
                logger.info(f"Successfully deleted {folder}/{filename}")
                return True
                
        except Exception as e:
            logger.error(f"Error deleting image {folder}/{filename}: {e}")
            return False
    
    def get_image_thumbnail(self, folder: str, filename: str, 
                           max_width: int = 320, max_height: int = 240) -> bytes:
        """
        Get a thumbnail version of an image.
        
        Args:
            folder: Folder path on camera
            filename: Name of the file
            max_width: Maximum thumbnail width
            max_height: Maximum thumbnail height
            
        Returns:
            Thumbnail image data as JPEG bytes
        """
        try:
            # Ensure filename is string
            filename = str(filename)
            
            # Download the full image
            file_data, _, _ = self.download_image(folder, filename)
            
            # Try to decode and create thumbnail
            try:
                img_array = np.frombuffer(file_data, dtype=np.uint8)
                img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
                
                if img is not None:
                    # Create thumbnail
                    height, width = img.shape[:2]
                    
                    # Calculate aspect ratio
                    if width > height:
                        new_width = min(max_width, width)
                        new_height = int(height * (new_width / width))
                    else:
                        new_height = min(max_height, height)
                        new_width = int(width * (new_height / height))
                    
                    # Resize
                    thumbnail = cv2.resize(img, (new_width, new_height), interpolation=cv2.INTER_AREA)
                    
                    # Encode as JPEG
                    _, thumbnail_data = cv2.imencode('.jpg', thumbnail, 
                                                    [cv2.IMWRITE_JPEG_QUALITY, 85])
                    
                    return thumbnail_data.tobytes()
                
            except Exception as decode_error:
                logger.warning(f"Could not decode image for thumbnail: {decode_error}")
            
            # If we can't create thumbnail, return a placeholder
            return self._create_thumbnail_placeholder(filename, max_width, max_height)
            
        except Exception as e:
            logger.error(f"Error getting thumbnail for {folder}/{filename}: {e}")
            # Return placeholder on error
            return self._create_thumbnail_placeholder(filename, max_width, max_height)
    
    def _get_image_metadata(self, folder: str, filename: str) -> Dict:
        """
        Extract metadata from an image file.
        
        Args:
            folder: Folder path on camera
            filename: Name of the file
            
        Returns:
            Dictionary with metadata
        """
        metadata = {
            'filename': filename,
            'folder': folder,
            'full_path': f"{folder}/{filename}",
            'file_type': 'unknown',
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # Ensure filename is string
            filename = str(filename)
            
            # Download a small portion to check file type
            camera_file = gp.CameraFile()
            self.camera.file_get(
                folder,
                filename,
                gp.GP_FILE_TYPE_NORMAL,
                camera_file,
                self.context
            )
            
            # Get first few bytes to detect file type
            file_data = camera_file.get_data_and_size()
            
            # Check file magic bytes
            magic = file_data[:4] if len(file_data) >= 4 else b''
            magic_hex = magic.hex()
            
            if file_data[:2] == b'\xff\xd8':
                metadata['file_type'] = 'jpeg'
            elif magic == b'\x89PNG':
                metadata['file_type'] = 'png'
            elif magic == b'BM':
                metadata['file_type'] = 'bmp'
            elif magic in [b'II\x2a\x00', b'MM\x00\x2a']:
                if filename.lower().endswith('.cr2'):
                    metadata['file_type'] = 'cr2'
                else:
                    metadata['file_type'] = 'tiff'
            elif magic_hex.startswith('4e494b4f4e'):  # NIKON
                metadata['file_type'] = 'nef'
            elif magic_hex.startswith('49492a00') and filename.lower().endswith('.arw'):
                metadata['file_type'] = 'arw'
            
            # Extract timestamp from filename (common pattern)
            try:
                # Look for patterns like IMG_20231225_123456.jpg
                date_patterns = [
                    r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})',
                    r'IMG_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})',
                    r'DSC_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})'
                ]
                
                for pattern in date_patterns:
                    match = re.search(pattern, filename)
                    if match:
                        year, month, day, hour, minute, second = match.groups()
                        metadata['timestamp'] = f"{year}-{month}-{day}T{hour}:{minute}:{second}"
                        break
            except:
                pass
            
        except Exception as e:
            logger.debug(f"Could not extract metadata for {filename}: {e}")
        
        return metadata
    
    def _format_file_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format"""
        if size_bytes == 0:
            return "0 B"
            
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for safe downloading"""
        # Ensure it's a string
        filename = str(filename)
        
        # Remove any path components
        filename = os.path.basename(filename)
        
        # Replace or remove problematic characters
        problematic_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
        for char in problematic_chars:
            filename = filename.replace(char, '_')
        
        # Ensure filename is not empty
        if not filename:
            filename = f"image_{int(time.time())}.jpg"
        
        return filename
    
    def _create_thumbnail_placeholder(self, filename: str, 
                                     width: int = 320, height: int = 240) -> bytes:
        """Create a placeholder thumbnail when image cannot be decoded"""
        img = np.zeros((height, width, 3), dtype=np.uint8)
        img.fill(200)  # Light gray background
        
        # Ensure filename is string
        filename = str(filename)
        
        # Add text
        text = filename[:15] + "..." if len(filename) > 15 else filename
        cv2.putText(img, text, (10, height//2 - 20), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.5, (50, 50, 50), 1)
        cv2.putText(img, "Thumbnail", (10, height//2 + 10), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.5, (50, 50, 50), 1)
        cv2.putText(img, "Unavailable", (10, height//2 + 40), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.5, (50, 50, 50), 1)
        
        _, buffer = cv2.imencode('.jpg', img, [cv2.IMWRITE_JPEG_QUALITY, 70])
        return buffer.tobytes()

    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up camera streamer...")
        self.stop_streaming()
        
        # Clean up context
        if self.context:
            # Note: gphoto2 Python bindings don't require explicit context cleanup
            self.context = None
        
        logger.info("Camera streamer cleanup complete")
import asyncio
import logging
from App.repository.DLSR_Helper import CameraLiveViewStreamer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_camera_methods():
    streamer = CameraLiveViewStreamer()
    
    logger.info("Testing get_all_cameras()...")
    try:
        cameras = await streamer.get_all_cameras()
        logger.info(f"Found {len(cameras)} cameras: {cameras}")
    except Exception as e:
        logger.error(f"get_all_cameras failed: {e}")

    logger.info("Testing disconnect_camera() with a specific (unused) port...")
    try:
        # This should trigger the _force_disconnect_port_sync path
        await streamer.disconnect_camera(port="usb:999,999")
        logger.info("Force-disconnect (unused port) handled safely (logged failure is expected).")
    except Exception as e:
        logger.error(f"disconnect_camera (specific port) failed: {e}")

    logger.info("Testing disconnect_camera() while not streaming...")
    try:
        await streamer.disconnect_camera()
        logger.info("Disconnected (not streaming) safely.")
    except Exception as e:
        logger.error(f"disconnect_camera (not streaming) failed: {e}")

    # Note: We can't easily test with actual streaming without a camera,
    # but the logic should be sound if the individual methods work.
    
    await streamer.cleanup()
    logger.info("Test complete.")

if __name__ == "__main__":
    asyncio.run(test_camera_methods())

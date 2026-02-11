from fastapi import APIRouter
from .UserAuth import router
from .Admin import admin_router
from .Users import user_router
from .CamControl import cam_route
from .CamSD import camsd_route
app_router=APIRouter()
app_router.include_router(router,prefix="/auth")
app_router.include_router(admin_router,prefix="/admin")
app_router.include_router(user_router,prefix="/users")
app_router.include_router(cam_route,prefix="/camera")
app_router.include_router(camsd_route,prefix="/camera_sd")
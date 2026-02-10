from fastapi import APIRouter
from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from typing import Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from App.repository.UserRepository import UserRepository
from App.core.LoggingInit import get_core_logger
from App.schemas.AuthScheema import TokenResponse
from App.api.dependencies.auth import (
    refresh_access_token,
    get_current_active_user,
    get_admin_user,
)
from App.schemas.AuthScheema import UserResponse
from App.core.Connector import get_db
user_router=APIRouter(prefix="/users_config",tags=["Users"])
logger = get_core_logger(__name__)
@user_router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Refresh access token",
    description="Get new access token using refresh token"
)
async def refresh_token(
    refresh_token: str = Body(..., embed=True),
    db: AsyncSession = Depends(get_db)
):
    """Refresh access token"""
    try:
        new_access_token = await refresh_access_token(refresh_token, db)
        
        if not new_access_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid refresh token"
            )
        
        return TokenResponse(
            access_token=new_access_token,
            expires_in=3600
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Token refresh error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Token refresh failed"
        )

@user_router.get(
    "/me",
    response_model=UserResponse,
    summary="Get current user profile",
    description="Get detailed information about the currently authenticated user"
)
async def get_my_profile(
    current_user: Dict[str, Any] = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """Get current user's profile"""
    try:
        repo = UserRepository(db)
        user = await repo.get_by_id(current_user["id"])
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        return UserResponse.model_validate(user)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Profile fetch error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch profile"
        )

# Helper endpoint for admin users
@user_router.get(
    "/users",
    summary="List users (Admin only)",
    description="Get list of all users. Admin access required."
)
async def list_users(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    search: Optional[str] = None,
    current_user: Dict[str, Any] = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db)
):
    """List users (admin only)"""
    try:
        repo = UserRepository(db)
        users = await repo.search_users(
            skip=skip,
            limit=limit,
            active_only=False,
            search=search
        )
        
        user_count = await repo.count_users(active_only=False)
        
        return {
            "users": [UserResponse.model_validate(user) for user in users],
            "total": user_count,
            "skip": skip,
            "limit": limit
        }
        
    except Exception as e:
        logger.error(f"Users list error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch users list"
        )

 
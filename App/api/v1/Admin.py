from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from typing import Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from App.repository.UserRepository import UserRepository
from App.core.LoggingInit import get_core_logger
from App.schemas.AuthScheema import TokenResponse
from App.api.dependencies.auth import (
     verify_password,
    get_current_user,
    get_password_hash,
 
)
from App.schemas.AuthScheema import UserResponse
from App.models.UserAuthModel import User,UpdateUser
from App.core.Connector import get_db
admin_router=APIRouter(prefix="/admin_access",tags=["Admin"])
logger = get_core_logger(__name__)
@admin_router.patch(
    "/account/{user_id}",
    response_model=UserResponse,
    summary="Update user account",
    description="Update user information. Admin can update any user, users can only update themselves."
)
async def update_account(
    user_id: int,
    update_data: UpdateUser,
    current_user: Dict[str, Any] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Update user account"""
    try:
        repo = UserRepository(db)
        
        # Check permissions
        is_admin = current_user.get("role") == "admin"
        can_update = is_admin or current_user.get("id") == user_id
        
        if not can_update:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions"
            )
        
        # Get user to update
        user_to_update = await repo.get_by_id(user_id)
        if not user_to_update:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Prepare update data
        update_dict = update_data.model_dump(exclude_unset=True)
        
        # Security restrictions for non-admin users
        if not is_admin:
            restricted_fields = ["user_role", "is_active"]
            for field in restricted_fields:
                if field in update_dict:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Cannot update {field} field"
                    )
        
        # Check email uniqueness if changing email
        if "email" in update_dict and update_dict["email"] != user_to_update.email:
            existing = await repo.get_by_email(update_dict["email"])
            if existing and existing.id != user_id:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Email already in use"
                )
        
        # Hash password if updating
        if "password" in update_dict:
            update_dict["password_hash"] = get_password_hash(update_dict["password"])
            del update_dict["password"]
        
        # Perform update
        updated_user = await repo.update(user_id, update_dict)
        
        if not updated_user:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update user"
            )
        
        logger.info(f"User {user_id} updated by user {current_user.get('id')}")
        
        return UserResponse.model_validate(updated_user)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Account update error: {e}")
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Update failed: {str(e)}"
        )

@admin_router.delete(
    "/account/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete user account",
    description="Delete user account. Admin can delete any user, users can only delete themselves with password verification."
)
async def delete_account(
    user_id: int,
    password: Optional[str] = Query(None, description="Required for non-admin users"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Delete user account"""
    try:
        repo = UserRepository(db)
        
        # Check if trying to delete self
        is_self_deletion = current_user.get("id") == user_id
        is_admin = current_user.get("role") == "admin"
        
        # Get user to delete
        user_to_delete = await repo.get_by_id(user_id)
        if not user_to_delete:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Admin can delete any user (except themselves without verification)
        if is_admin and not is_self_deletion:
            await repo.delete(user_id)
            logger.info(f"Admin {current_user.get('id')} deleted user {user_id}")
            return
        
        # Self-deletion requires password verification
        if is_self_deletion:
            if not password:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Password required for self-deletion"
                )
            
            if not verify_password(password, user_to_delete.password_hash):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Incorrect password"
                )
            
            await repo.delete(user_id)
            logger.info(f"User {user_id} deleted their own account")
            return
        
        # If we get here, user doesn't have permission
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Account deletion error: {e}")
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Deletion failed: {str(e)}"
        )

@admin_router.post(
    "/reset-auto-kill",
    status_code=status.HTTP_200_OK,
    summary="Reset Auto-Kill Switch",
    description="Allows administrators to reset the system from safety mode after an internal error."
)
async def reset_auto_kill(
    request: Request,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Reset the auto-kill protection flag"""
    if current_user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only administrators can reset the safety mode"
        )
    
    request.app.state.auto_kill_enabled = False
    logger.info(f"System safety mode reset by admin: {current_user.get('email')}")
    return {"status": "success", "message": "System safety mode has been reset."}


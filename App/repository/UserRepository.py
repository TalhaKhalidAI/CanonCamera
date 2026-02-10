# App/repository/UserRepository.py - CLEANER VERSION
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_
from typing import Optional, List, Dict, Any, Tuple
import logging

# Import ONLY SQLAlchemy model
from App.api.databases.MigrateTable import User as UserModel

logger = logging.getLogger(__name__)

class UserRepository:
    """Repository for user database operations - NO SCHEMA DEPENDENCIES"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    # ========== BASIC CRUD ==========
    
    async def get_by_id(self, user_id: int) -> Optional[UserModel]:
        """Get user by ID"""
        try:
            result = await self.session.execute(
                select(UserModel).where(UserModel.id == user_id)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting user by ID {user_id}: {e}")
            return None
    
    async def get_by_email(self, email: str) -> Optional[UserModel]:
        """Get user by email"""
        try:
            result = await self.session.execute(
                select(UserModel).where(UserModel.email == email)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting user by email {email}: {e}")
            return None
    
    async def get_by_name(self, name: str) -> Optional[UserModel]:
        """Get user by name"""
        try:
            result = await self.session.execute(
                select(UserModel).where(UserModel.name == name)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting user by name {name}: {e}")
            return None
    
    async def create(self, user_data: Dict[str, Any]) -> Optional[UserModel]:
        """Create new user from dictionary"""
        try:
            user = UserModel(**user_data)
            self.session.add(user)
            await self.session.commit()
            await self.session.refresh(user)
            
            logger.info(f"Created new user: {user.email}")
            return user
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error creating user: {e}")
            return None
    
    async def update(self, user_id: int, update_data: Dict[str, Any]) -> Optional[UserModel]:
        """Update user with dictionary"""
        try:
            user = await self.get_by_id(user_id)
            if not user:
                return None
            
            for field, value in update_data.items():
                if hasattr(user, field):
                    setattr(user, field, value)
            
            await self.session.commit()
            await self.session.refresh(user)
            
            logger.info(f"Updated user {user_id}")
            return user
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating user {user_id}: {e}")
            return None
    
    async def update_password_hash(self, user_id: int, password_hash: str) -> bool:
        """Update only password hash"""
        try:
            user = await self.get_by_id(user_id)
            if not user:
                return False
            
            user.password_hash = password_hash
            await self.session.commit()
            logger.info(f"Updated password hash for user {user_id}")
            return True
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating password for user {user_id}: {e}")
            return False
    
    # FIXED delete method - allows account restoration
    async def delete(self, user_id: int) -> bool:
        """Soft delete user (disable only)"""
        try:
            user = await self.get_by_id(user_id)
            if not user:
                return False
            
            # Only set disabled flag, keep is_active for possible restoration
            user.disabled = True
            await self.session.commit()
            
            logger.info(f"Soft deleted (disabled) user {user_id}")
            return True
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error deleting user {user_id}: {e}")
            return False

    # Add restoration method
    async def restore(self, user_id: int) -> bool:
        """Restore disabled user"""
        try:
            user = await self.get_by_id(user_id)
            if not user:
                return False
            
            user.disabled = False
            # Optionally: user.is_active = True
            await self.session.commit()
            
            logger.info(f"Restored user {user_id}")
            return True
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error restoring user {user_id}: {e}")
            return False
    
    # ========== QUERIES ==========
    
    async def get_user_role(self, user_id: int) -> Optional[str]:
        """Get user role by ID"""
        user = await self.get_by_id(user_id)
        return user.user_role if user else None
    
    async def is_user_active(self, user_id: int) -> bool:
        """Check if user is active and not disabled"""
        user = await self.get_by_id(user_id)
        return bool(user and user.is_active and not user.disabled)
    
    async def is_user_disabled(self, user_id: int) -> bool:
        """Check if user is disabled"""
        user = await self.get_by_id(user_id)
        return bool(user and user.disabled)
    
    async def search_users(
        self, 
        skip: int = 0, 
        limit: int = 100,
        active_only: bool = True,
        search: Optional[str] = None
    ) -> List[UserModel]:
        """Search users with pagination"""
        try:
            query = select(UserModel)
            
            # Apply filters
            if active_only:
                query = query.where(
                    and_(
                        UserModel.is_active == True,
                        UserModel.disabled == False
                    )
                )
            
            # Apply search
            if search:
                search_filter = UserModel.name.ilike(f"%{search}%")
                if '@' in search:  # Likely an email search
                    search_filter = search_filter | UserModel.email.ilike(f"%{search}%")
                query = query.where(search_filter)
            
            # Apply pagination and ordering
            query = query.offset(skip).limit(limit).order_by(UserModel.created_at.desc())
            
            result = await self.session.execute(query)
            return result.scalars().all()
            
        except Exception as e:
            logger.error(f"Error searching users: {e}")
            return []
    
    async def count_users(self, active_only: bool = True) -> int:
        """Count total users"""
        try:
            query = select(UserModel)
            
            if active_only:
                query = query.where(
                    and_(
                        UserModel.is_active == True,
                        UserModel.disabled == False
                    )
                )
            
            result = await self.session.execute(query)
            return len(result.scalars().all())
            
        except Exception as e:
            logger.error(f"Error counting users: {e}")
            return 0
    
    async def exists_by_email(self, email: str) -> bool:
        """Check if user exists by email"""
        user = await self.get_by_email(email)
        return user is not None
    
    async def get_with_profile(self, user_id: int) -> Optional[Tuple[UserModel, Any]]:
        """Get user with related profile data (if you have profiles)"""
        try:
            from sqlalchemy.orm import joinedload
            
            result = await self.session.execute(
                select(UserModel)
                .options(joinedload(UserModel.profile))  # If you have relationship
                .where(UserModel.id == user_id)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting user with profile {user_id}: {e}")
            return None
    
    # ========== UTILITIES ==========
    
    def to_dict(self, user: UserModel, exclude: List[str] = None) -> Dict[str, Any]:
        """Convert SQLAlchemy model to dictionary (NO Pydantic dependency)"""
        if exclude is None:
            exclude = []
        
        result = {}
        for column in user.__table__.columns:
            col_name = column.name
            if col_name not in exclude:
                result[col_name] = getattr(user, col_name)
        
        return result
    
    def to_safe_dict(self, user: UserModel) -> Dict[str, Any]:
        """Convert to dictionary excluding sensitive fields"""
        exclude_fields = ['password_hash', 'secret_key', 'token']
        return self.to_dict(user, exclude=exclude_fields)
    
    async def bulk_create(self, users_data: List[Dict[str, Any]]) -> List[UserModel]:
        """Create multiple users"""
        users = []
        try:
            for user_data in users_data:
                user = UserModel(**user_data)
                self.session.add(user)
                users.append(user)
            
            await self.session.commit()
            
            # Refresh all users
            for user in users:
                await self.session.refresh(user)
            
            logger.info(f"Created {len(users)} users")
            return users
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error in bulk create: {e}")
            return []
    
    async def bulk_update(self, user_updates: List[Tuple[int, Dict[str, Any]]]) -> int:
        """Bulk update users"""
        updated_count = 0
        try:
            for user_id, update_data in user_updates:
                user = await self.get_by_id(user_id)
                if user:
                    for field, value in update_data.items():
                        if hasattr(user, field):
                            setattr(user, field, value)
                    updated_count += 1
            
            await self.session.commit()
            logger.info(f"Bulk updated {updated_count} users")
            return updated_count
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error in bulk update: {e}")
            return 0
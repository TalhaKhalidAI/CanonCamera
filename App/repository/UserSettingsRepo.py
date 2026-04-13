# App/repository/UserSettingsRepo.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_
from typing import Optional, List, Dict, Any
import logging
from App.api.databases.MigrateTable import User, userSettings as UserSettingsModel

logger = logging.getLogger(__name__)


class UserSettingsRepository:
    """Repository for user settings database operations."""

    def __init__(self, session: AsyncSession):
        self.session = session

    # ========== HELPER METHODS ==========

    async def _get_raw(self, id: int) -> Optional[UserSettingsModel]:
        """Internal helper to get settings by ID without deleted check."""
        result = await self.session.execute(
            select(UserSettingsModel).where(UserSettingsModel.id == id)
        )
        return result.scalar_one_or_none()

    async def _get_raw_by_user(self, user_id: int) -> Optional[UserSettingsModel]:
        """Internal helper to get settings by user_id without deleted check."""
        # Using a join to ensure the user exists
        query = (
            select(UserSettingsModel)
            .join(User, UserSettingsModel.user_id == User.id)
            .where(UserSettingsModel.user_id == user_id)
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    # ========== READ OPERATIONS ==========

    async def get_by_id(
        self, id: int, include_deleted: bool = False
    ) -> Optional[UserSettingsModel]:
        """Get settings by its primary ID."""
        try:
            # Join with User to ensure settings are linked to a valid user account
            query = (
                select(UserSettingsModel)
                .join(User, UserSettingsModel.user_id == User.id)
                .where(UserSettingsModel.id == id)
            )
            if not include_deleted:
                query = query.where(UserSettingsModel.deleted == False)

            result = await self.session.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting settings by ID {id}: {e}")
            return None

    async def get_by_user_id(
        self, user_id: int, include_deleted: bool = False
    ) -> Optional[UserSettingsModel]:
        """Get settings for a specific user using an explicit join."""
        try:
            # Performing a JOIN with User table as requested to ensure integrity
            query = (
                select(UserSettingsModel)
                .join(User, UserSettingsModel.user_id == User.id)
                .where(UserSettingsModel.user_id == user_id)
            )
            if not include_deleted:
                query = query.where(UserSettingsModel.deleted == False)

            result = await self.session.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting settings for user {user_id}: {e}")
            return None

    # ========== WRITE OPERATIONS ==========

    async def create(self, data: Dict[str, Any]) -> Optional[UserSettingsModel]:
        """Create new user settings."""
        try:
            user_id = data.get("user_id")
            if not user_id:
                raise ValueError("user_id is required")

            # Check if user actually exists before creating settings
            user_check = await self.session.execute(
                select(User).where(User.id == user_id)
            )
            if not user_check.scalar_one_or_none():
                raise ValueError(f"User with ID {user_id} does not exist")

            # Check if settings already exist for this user (even in trash)
            existing = await self.get_by_user_id(user_id, include_deleted=True)
            if existing:
                if existing.deleted:
                    # Restore deleted settings instead of creating new
                    for key, value in data.items():
                        setattr(existing, key, value)
                    existing.deleted = False
                    existing.is_active = True
                    existing.disabled = False
                    await self.session.commit()
                    await self.session.refresh(existing)
                    return existing
                raise ValueError(f"Settings for user {user_id} already exist")

            new_settings = UserSettingsModel(**data)
            self.session.add(new_settings)
            await self.session.commit()
            await self.session.refresh(new_settings)

            logger.info(f"Created settings for user: {user_id}")
            return new_settings

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error creating user settings: {e}")
            raise e

    async def update(
        self, user_id: int, data: Dict[str, Any]
    ) -> Optional[UserSettingsModel]:
        """Update settings for a user."""
        try:
            # Fetch using join to ensure user integrity
            settings = await self.get_by_user_id(user_id, include_deleted=True)

            if not settings:
                raise ValueError(f"Settings for user {user_id} not found")

            if settings.deleted:
                raise ValueError(
                    f"Settings for user {user_id} are deleted and cannot be updated"
                )

            if settings.disabled:
                raise ValueError(
                    f"Settings for user {user_id} are disabled and cannot be updated"
                )

            for key, value in data.items():
                if (
                    hasattr(settings, key) and key != "user_id"
                ):  # Prevent changing user_id
                    setattr(settings, key, value)

            await self.session.commit()
            await self.session.refresh(settings)

            logger.info(f"Updated settings for user {user_id}")
            return settings

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating settings for user {user_id}: {e}")
            raise e

    async def delete(self, user_id: int) -> bool:
        """Soft delete user settings."""
        try:
            settings = await self.get_by_user_id(user_id, include_deleted=True)
            if not settings:
                raise ValueError(f"Settings for user {user_id} not found")

            if settings.deleted:
                raise ValueError(f"Settings for user {user_id} are already deleted")

            settings.deleted = True
            await self.session.commit()
            await self.session.refresh(settings)

            logger.info(f"Soft deleted settings for user {user_id}")
            return True
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error deleting settings for user {user_id}: {e}")
            raise e

    async def restore(self, user_id: int) -> bool:
        """Restore soft-deleted user settings."""
        try:
            settings = await self._get_raw_by_user(user_id)
            if not settings:
                raise ValueError(f"Settings for user {user_id} not found")

            if not settings.deleted:
                raise ValueError(
                    f"Settings for user {user_id} are not deleted and cannot be restored"
                )

            settings.deleted = False
            settings.is_active = True
            await self.session.commit()
            await self.session.refresh(settings)

            logger.info(f"Restored settings for user {user_id}")
            return True
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error restoring settings for user {user_id}: {e}")
            raise e

    async def set_status(
        self, user_id: int, is_active: bool = True, disabled: bool = False
    ) -> bool:
        """Enable or disable user settings."""
        try:
            settings = await self.get_by_user_id(user_id)
            if not settings:
                raise ValueError(f"Settings for user {user_id} not found")

            settings.is_active = is_active
            settings.disabled = disabled

            await self.session.commit()
            await self.session.refresh(settings)
            logger.info(
                f"Set status for user {user_id} settings: active={is_active}, disabled={disabled}"
            )
            return True
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error setting status for user {user_id} settings: {e}")
            raise e

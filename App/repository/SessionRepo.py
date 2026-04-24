# App/repository/SessionRepo.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, and_, or_, func
from typing import Optional, List, Dict, Any
from datetime import datetime
import logging
import uuid

from App.api.databases.MigrateTable import (
    Sessions,
    Events,
    Photoes,
    Prints,
    Orders,
    User
)

logger = logging.getLogger(__name__)


class SessionRepo:
    """Repository for guest session management - Production Ready"""

    def __init__(self, session: AsyncSession):
        self.session = session
        self._default_timeout = 5.0

    # ========== HELPER METHODS ==========

    def _generate_session_code(self) -> str:
        """Generate a unique session code using UUID4."""
        return str(uuid.uuid4())





    # ========== CREATE OPERATIONS ==========

    async def create_session(
        self,
        event_id: int,
        guest_name: str,
        guest_email: Optional[str] = None,
        guest_phone: Optional[str] = None,
        guest_address: Optional[str] = None,
        created_by: Optional[int] = None
    ) -> Optional[Sessions]:
        """Create a new guest session for an event."""
        try:
            if not guest_name:
                raise ValueError("Guest name is required")

            # Validate event exists and is active
            event_check = await self.session.execute(
                select(Events.id).where(
                    and_(
                        Events.id == event_id,
                        Events.deleted == False,
                        Events.is_active == True,
                        Events.disabled == False
                    )
                ).execution_options(timeout=self._default_timeout)
            )
            if not event_check.scalar():
                raise ValueError(f"Active event with ID {event_id} not found")

            session_code = self._generate_session_code()

            session = Sessions(
                event_id=event_id,
                session_code=session_code,
                guest_name=guest_name.strip(),
                guest_email=guest_email.lower().strip() if guest_email else None,
                guest_phone=guest_phone,
                guest_address=guest_address,
                is_active=True,
                disabled=False,
                deleted=False
            )

            self.session.add(session)
            await self.session.commit()
            await self.session.refresh(session)
            logger.info(f"Created session '{session_code}' for event {event_id}")
            return session

        except ValueError:
            raise
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error creating session: {e}")
            return None

    # ========== READ OPERATIONS ==========

    async def get_by_id(
        self,
        session_id: int,
        include_deleted: bool = False,
        check_active: bool = True
    ) -> Optional[Sessions]:
        """Get session by ID with optional active status check."""
        try:
            conditions = [Sessions.id == session_id]

            if not include_deleted:
                conditions.append(Sessions.deleted == False)

            if check_active:
                conditions.extend([
                    Sessions.is_active == True,
                    Sessions.disabled == False
                ])

            query = select(Sessions).where(and_(*conditions))

            result = await self.session.execute(
                query.execution_options(timeout=self._default_timeout)
            )
            return result.scalar_one_or_none()

        except Exception as e:
            logger.error(f"Error getting session {session_id}: {e}")
            return None

    async def get_by_code(
        self,
        session_code: str,
        include_deleted: bool = False
    ) -> Optional[Sessions]:
        """Get session by unique code."""
        try:
            conditions = [Sessions.session_code == session_code]

            if not include_deleted:
                conditions.append(Sessions.deleted == False)

            query = select(Sessions).where(and_(*conditions))

            result = await self.session.execute(
                query.execution_options(timeout=self._default_timeout)
            )
            return result.scalar_one_or_none()

        except Exception as e:
            logger.error(f"Error getting session by code '{session_code}': {e}")
            return None

    async def get_by_event(
        self,
        event_id: int,
        include_inactive: bool = False,
        include_deleted: bool = False,
        limit: int = 100,
        offset: int = 0
    ) -> List[Sessions]:
        """Get all sessions for an event with pagination."""
        try:
            conditions = [Sessions.event_id == event_id]

            if not include_deleted:
                conditions.append(Sessions.deleted == False)

            if not include_inactive:
                conditions.extend([
                    Sessions.is_active == True,
                    Sessions.disabled == False
                ])

            query = (
                select(Sessions)
                .where(and_(*conditions))
                .order_by(Sessions.created_at.desc())
                .limit(limit)
                .offset(offset)
            )

            result = await self.session.execute(
                query.execution_options(timeout=self._default_timeout)
            )
            return list(result.scalars().all())

        except Exception as e:
            logger.error(f"Error getting sessions for event {event_id}: {e}")
            return []

    async def get_by_guest_email(
        self,
        email: str,
        event_id: Optional[int] = None,
        limit: int = 50
    ) -> List[Sessions]:
        """Get all sessions for a guest email, optionally filtered by event."""
        try:
            conditions = [
                Sessions.guest_email == email.lower(),
                Sessions.deleted == False
            ]

            if event_id:
                conditions.append(Sessions.event_id == event_id)

            query = (
                select(Sessions)
                .where(and_(*conditions))
                .order_by(Sessions.created_at.desc())
                .limit(limit)
            )

            result = await self.session.execute(
                query.execution_options(timeout=self._default_timeout)
            )
            return list(result.scalars().all())

        except Exception as e:
            logger.error(f"Error getting sessions for email {email}: {e}")
            return []

    async def get_active_sessions(
        self,
        event_id: Optional[int] = None,
        limit: int = 100
    ) -> List[Sessions]:
        """Get all active sessions (not ended, not disabled)."""
        try:
            conditions = [
                Sessions.deleted == False,
                Sessions.is_active == True,
                Sessions.disabled == False
            ]

            if event_id:
                conditions.append(Sessions.event_id == event_id)

            query = (
                select(Sessions)
                .where(and_(*conditions))
                .order_by(Sessions.created_at.desc())
                .limit(limit)
            )

            result = await self.session.execute(
                query.execution_options(timeout=self._default_timeout)
            )
            return list(result.scalars().all())

        except Exception as e:
            logger.error(f"Error getting active sessions: {e}")
            return []

    async def get_deleted_sessions(
        self,
        event_id: Optional[int] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Sessions]:
        """Get all soft-deleted sessions."""
        try:
            conditions = [Sessions.deleted == True]

            if event_id:
                conditions.append(Sessions.event_id == event_id)

            query = (
                select(Sessions)
                .where(and_(*conditions))
                .order_by(Sessions.updated_at.desc())
                .limit(limit)
                .offset(offset)
            )

            result = await self.session.execute(
                query.execution_options(timeout=self._default_timeout)
            )
            return list(result.scalars().all())

        except Exception as e:
            logger.error(f"Error getting deleted sessions: {e}")
            return []

    async def search_sessions(
        self,
        search_term: str,
        event_id: Optional[int] = None,
        limit: int = 50
    ) -> List[Sessions]:
        """Search sessions by guest name, email, or phone."""
        try:
            search_pattern = f"%{search_term}%"
            conditions = [
                Sessions.deleted == False,
                or_(
                    Sessions.guest_name.ilike(search_pattern),
                    Sessions.guest_email.ilike(search_pattern),
                    Sessions.guest_phone.ilike(search_pattern),
                    Sessions.session_code.ilike(search_pattern)
                )
            ]

            if event_id:
                conditions.append(Sessions.event_id == event_id)

            query = (
                select(Sessions)
                .where(and_(*conditions))
                .order_by(Sessions.created_at.desc())
                .limit(limit)
            )

            result = await self.session.execute(
                query.execution_options(timeout=self._default_timeout)
            )
            return list(result.scalars().all())

        except Exception as e:
            logger.error(f"Error searching sessions: {e}")
            return []

    # ========== UPDATE OPERATIONS ==========

    async def update_guest_info(
        self,
        session_id: int,
        guest_name: Optional[str] = None,
        guest_email: Optional[str] = None,
        guest_phone: Optional[str] = None,
        guest_address: Optional[str] = None
    ) -> Optional[Sessions]:
        """Update guest information for a session."""
        try:
            session = await self.get_by_id(session_id, check_active=False)
            if not session:
                raise ValueError(f"Session {session_id} not found")

            if session.deleted:
                raise ValueError(f"Session {session_id} is deleted")

            if session.disabled:
                raise ValueError(f"Session {session_id} is disabled")

            if guest_name:
                session.guest_name = guest_name.strip()
            if guest_email:
                session.guest_email = guest_email.lower().strip()
            if guest_phone:
                session.guest_phone = guest_phone
            if guest_address:
                session.guest_address = guest_address

            session.updated_at = func.now()

            await self.session.commit()
            await self.session.refresh(session)

            logger.info(f"Updated guest info for session {session_id}")
            return session

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating session {session_id}: {e}")
            return None

    async def update_session_status(
        self,
        session_id: int,
        is_active: bool
    ) -> Optional[Sessions]:
        """Activate or deactivate a session."""
        try:
            session = await self.get_by_id(session_id, include_deleted=True, check_active=False)
            if not session:
                raise ValueError(f"Session {session_id} not found")

            if session.deleted:
                raise ValueError(f"Session {session_id} is deleted")

            session.is_active = is_active
            session.updated_at = func.now()

            await self.session.commit()
            await self.session.refresh(session)

            logger.info(f"Session {session_id} status updated to active={is_active}")
            return session

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating session status {session_id}: {e}")
            return None

    async def disable_session(self, session_id: int) -> bool:
        """Disable a session (soft disable without deletion)."""
        try:
            session = await self.get_by_id(session_id, check_active=False)
            if not session:
                return False

            if session.deleted:
                return False

            session.disabled = True
            session.is_active = False
            session.updated_at = func.now()

            await self.session.commit()
            logger.info(f"Disabled session {session_id}")
            return True

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error disabling session {session_id}: {e}")
            return False

    async def enable_session(self, session_id: int) -> bool:
        """Enable a disabled session."""
        try:
            session = await self.get_by_id(session_id, include_deleted=True, check_active=False)
            if not session:
                return False

            if session.deleted:
                return False

            session.disabled = False
            session.is_active = True
            session.updated_at = func.now()

            await self.session.commit()
            logger.info(f"Enabled session {session_id}")
            return True

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error enabling session {session_id}: {e}")
            return False

    # ========== DELETE & RESTORE OPERATIONS ==========

    async def delete_session(self, session_id: int, hard_delete: bool = False) -> bool:
        """Delete a session (soft or hard)."""
        try:
            session = await self.get_by_id(session_id, check_active=False)
            if not session:
                raise ValueError(f"Session {session_id} not found")

            if hard_delete:
                # Hard delete - remove all related records first
                await self.session.execute(
                    delete(Photoes).where(Photoes.session_id == session_id)
                )
                await self.session.execute(
                    delete(Prints).where(Prints.session_id == session_id)
                )
                await self.session.execute(
                    delete(Orders).where(Orders.session_id == session_id)
                )
                result = await self.session.execute(
                    delete(Sessions).where(Sessions.id == session_id)
                )
                await self.session.commit()
                success = result.rowcount > 0
                if success:
                    logger.info(f"Hard deleted session {session_id}")
                return success
            else:
                if session.deleted:
                    return False

                # Soft cascade to children
                await self.session.execute(
                    update(Photoes).where(Photoes.session_id == session_id)
                    .values(deleted=True, updated_at=func.now())
                )
                await self.session.execute(
                    update(Prints).where(Prints.session_id == session_id)
                    .values(deleted=True, updated_at=func.now())
                )
                await self.session.execute(
                    update(Orders).where(Orders.session_id == session_id)
                    .values(deleted=True, updated_at=func.now())
                )

                session.deleted = True
                session.is_active = False
                session.updated_at = func.now()

                await self.session.commit()
                logger.info(f"Soft deleted session {session_id} and its children")
                return True

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error deleting session {session_id}: {e}")
            return False

    async def restore_session(self, session_id: int) -> bool:
        """Restore a soft-deleted session."""
        try:
            session = await self.get_by_id(session_id, include_deleted=True, check_active=False)
            if not session or not session.deleted:
                return False

            # Check if session code conflicts with existing active session
            conflict = await self.session.execute(
                select(Sessions.id).where(
                    and_(
                        Sessions.session_code == session.session_code,
                        Sessions.deleted == False,
                        Sessions.id != session_id
                    )
                )
            )
            if conflict.scalar():
                # Generate new code for restored session (UUID)
                session.session_code = self._generate_session_code()
                logger.warning(f"Session {session_id} restored with new code {session.session_code}")

            # Cascade restore child records
            await self.session.execute(
                update(Photoes).where(Photoes.session_id == session_id)
                .values(deleted=False, updated_at=func.now())
            )
            await self.session.execute(
                update(Prints).where(Prints.session_id == session_id)
                .values(deleted=False, updated_at=func.now())
            )
            await self.session.execute(
                update(Orders).where(Orders.session_id == session_id)
                .values(deleted=False, updated_at=func.now())
            )

            session.deleted = False
            session.is_active = True
            session.disabled = False
            session.updated_at = func.now()

            await self.session.commit()
            logger.info(f"Restored session {session_id}")
            return True

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error restoring session {session_id}: {e}")
            return False

    # ========== STATUS CHECK METHODS ==========

    async def is_session_active(self, session_id: int) -> bool:
        """Check if session is active (not deleted, not disabled, is_active=True)."""
        try:
            session = await self.get_by_id(session_id, check_active=False)
            if not session:
                return False
            return session.is_active and not session.disabled and not session.deleted
        except Exception as e:
            logger.error(f"Error checking session active status: {e}")
            return False

    async def is_session_valid(self, session_id: int) -> bool:
        """Check if session exists and is usable (not deleted, not disabled)."""
        try:
            session = await self.get_by_id(session_id, check_active=False)
            return bool(session and not session.deleted and not session.disabled)
        except Exception as e:
            logger.error(f"Error checking session validity: {e}")
            return False

    # ========== STATISTICS METHODS ==========

    async def get_session_count(
        self,
        event_id: Optional[int] = None,
        only_active: bool = True
    ) -> int:
        """Get total session count with filters."""
        try:
            conditions = [Sessions.deleted == False]

            if event_id:
                conditions.append(Sessions.event_id == event_id)

            if only_active:
                conditions.extend([
                    Sessions.is_active == True,
                    Sessions.disabled == False
                ])

            result = await self.session.execute(
                select(func.count()).select_from(Sessions).where(and_(*conditions))
            )
            return result.scalar() or 0

        except Exception as e:
            logger.error(f"Error getting session count: {e}")
            return 0

    async def get_session_summary(self, session_id: int, only_active: bool = True) -> Optional[Dict[str, Any]]:
        """Get a summary of session with related counts. If only_active is True, only return if session is not deleted or disabled."""
        try:
            session = await self.get_by_id(session_id, check_active=False)
            if not session:
                return None

            if only_active and (session.deleted or session.disabled):
                return None


            # Get photo count
            photo_count = await self.session.scalar(
                select(func.count()).select_from(Photoes).where(
                    and_(
                        Photoes.session_id == session_id,
                        Photoes.deleted == False,
                        Photoes.disabled == False
                    )
                )
            )

            # Get print count
            print_count = await self.session.scalar(
                select(func.count()).select_from(Prints).where(
                    and_(
                        Prints.session_id == session_id,
                        Prints.deleted == False,
                        Prints.disabled == False
                    )
                )
            )

            # Get order count and total
            order_result = await self.session.execute(
                select(
                    func.count().label("order_count"),
                    func.coalesce(func.sum(Orders.total_amount), 0).label("total_spent")
                ).select_from(Orders).where(
                    and_(
                        Orders.session_id == session_id,
                        Orders.deleted == False,
                        Orders.disabled == False,
                        Orders.payment_status == "paid"
                    )
                )
            )
            order_stats = order_result.one()

            return {
                "session_id": session.id,
                "session_code": session.session_code,
                "guest_name": session.guest_name,
                "guest_email": session.guest_email,
                "guest_phone": session.guest_phone,
                "event_id": session.event_id,
                "is_active": session.is_active and not session.disabled and not session.deleted,
                "created_at": session.created_at,
                "updated_at": session.updated_at,
                "photos_count": photo_count or 0,
                "prints_count": print_count or 0,
                "orders_count": order_stats.order_count or 0,
                "total_spent": float(order_stats.total_spent or 0)
            }

        except Exception as e:
            logger.error(f"Error getting session summary: {e}")
            return None
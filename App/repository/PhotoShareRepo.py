# App/repository/PhotoShareRepo.py
import asyncio
import logging
from typing import Any, Dict, List, Optional
from contextvars import ContextVar
from enum import Enum

from sqlalchemy import and_, func, select, update
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import false, true

from App.api.databases.MigrateTable import PhotoShares

logger = logging.getLogger(__name__)
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="unknown")
_PG_DEADLOCK_CODE = "40P01"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ShareStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SENT = "sent"
    FAILED = "failed"
    OPENED = "opened"


# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class PhotoShareRepoError(Exception):
    pass

class ShareNotFoundError(PhotoShareRepoError):
    pass

class InvalidShareStateError(PhotoShareRepoError):
    pass


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class PhotoShareRepo:
    """
    Production-ready repository for PhotoShares.
    Handles sharing via email, SMS, etc. with retry logic and tracking.
    """

    def __init__(self, session: AsyncSession):
        self.session = session
        self._default_timeout = 5.0
        self._bulk_timeout = 30.0
        self._max_retries = 3

    def _log(self, level: str, msg: str, **extra) -> None:
        getattr(logger, level)(
            msg, extra={"correlation_id": correlation_id_var.get(), **extra}
        )

    async def _with_deadlock_retry(self, func, operation: str = "operation"):
        """Retry on PostgreSQL deadlock with exponential backoff."""
        last_exc = None
        for attempt in range(self._max_retries):
            try:
                return await func()
            except (OperationalError, SQLAlchemyError) as exc:
                last_exc = exc
                pg_code = getattr(getattr(exc, "orig", None), "pgcode", None)
                if pg_code != _PG_DEADLOCK_CODE:
                    raise
                if attempt < self._max_retries - 1:
                    wait = (2 ** attempt) * 0.1
                    self._log("warning", f"Deadlock in {operation}, retry in {wait}s")
                    await asyncio.sleep(wait)
                    await self.session.rollback()
        raise last_exc

    # ==================================================================
    # CREATE
    # ==================================================================

    async def create_share(
        self,
        photo_id: int,
        session_id: int,
        share_method: str,
        recipient_target: str,
        recipient_name: str,
        template_id: Optional[str] = None,
    ) -> PhotoShares:
        """Create a new photo share request."""
        async def _do_create():
            share = PhotoShares(
                photo_id=photo_id,
                session_id=session_id,
                share_method=share_method.strip(),
                recipient_target=recipient_target.strip(),
                recipient_name=recipient_name.strip(),
                template_id=template_id,
                status=ShareStatus.PENDING.value,
                retry_count=0,
                is_active=True,
                deleted=False,
            )
            self.session.add(share)
            await self.session.flush()
            return share

        share = await self._with_deadlock_retry(_do_create, "create_share")
        await self.session.commit()
        await self.session.refresh(share)

        self._log(
            "info",
            f"Created share request {share.id} for photo {photo_id} via {share_method}"
        )
        return share

    # ==================================================================
    # READ
    # ==================================================================

    async def get_share_by_id(
        self, share_id: int, include_deleted: bool = False
    ) -> Optional[PhotoShares]:
        """Get share by ID."""
        conditions = [PhotoShares.id == share_id]
        if not include_deleted:
            conditions.append(PhotoShares.deleted.is_(false()))

        result = await self.session.execute(
            select(PhotoShares).where(and_(*conditions))
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_shares_by_session(
        self, session_id: int, limit: int = 100, offset: int = 0
    ) -> List[PhotoShares]:
        """Get all shares for a session."""
        result = await self.session.execute(
            select(PhotoShares).where(
                PhotoShares.session_id == session_id,
                PhotoShares.deleted.is_(false())
            )
            .order_by(PhotoShares.created_at.desc())
            .limit(limit).offset(offset)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_shares_by_photo(
        self, photo_id: int, limit: int = 100
    ) -> List[PhotoShares]:
        """Get all shares for a specific photo."""
        result = await self.session.execute(
            select(PhotoShares).where(
                PhotoShares.photo_id == photo_id,
                PhotoShares.deleted.is_(false())
            )
            .order_by(PhotoShares.created_at.desc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_pending_shares(self, limit: int = 50) -> List[PhotoShares]:
        """Get shares that are pending to be processed."""
        result = await self.session.execute(
            select(PhotoShares).where(
                PhotoShares.status == ShareStatus.PENDING.value,
                PhotoShares.is_active.is_(true()),
                PhotoShares.deleted.is_(false())
            )
            .order_by(PhotoShares.created_at.asc())
            .limit(limit)
            .execution_options(timeout=self._bulk_timeout)
        )
        return list(result.scalars().all())

    async def get_failed_shares_for_retry(
        self, max_retries: int = 3, limit: int = 50
    ) -> List[PhotoShares]:
        """Get failed shares that haven't exceeded max retries."""
        result = await self.session.execute(
            select(PhotoShares).where(
                PhotoShares.status == ShareStatus.FAILED.value,
                PhotoShares.retry_count < max_retries,
                PhotoShares.is_active.is_(true()),
                PhotoShares.deleted.is_(false())
            )
            .order_by(PhotoShares.last_retry_at.asc().nullsfirst())
            .limit(limit)
            .execution_options(timeout=self._bulk_timeout)
        )
        return list(result.scalars().all())

    # ==================================================================
    # UPDATE
    # ==================================================================

    async def update_share_status(
        self,
        share_id: int,
        status: ShareStatus,
        error_message: Optional[str] = None
    ) -> PhotoShares:
        """Update the status of a share request atomically."""
        async def _do_update():
            result = await self.session.execute(
                select(PhotoShares)
                .where(PhotoShares.id == share_id, PhotoShares.deleted.is_(false()))
                .with_for_update()
                .execution_options(timeout=self._default_timeout)
            )
            share = result.scalar_one_or_none()
            if not share:
                raise ShareNotFoundError(f"Share {share_id} not found")

            share.status = status.value
            
            if status == ShareStatus.SENT:
                share.sent_at = func.now()
                share.error_message = None
            elif status == ShareStatus.OPENED:
                share.opened_at = func.now()
            elif status == ShareStatus.FAILED and error_message:
                share.error_message = error_message[:500]

            share.updated_at = func.now()
            return share

        share = await self._with_deadlock_retry(_do_update, "update_share_status")
        await self.session.commit()
        await self.session.refresh(share)
        self._log("info", f"Share {share_id} status updated to {status.value}")
        return share

    async def record_retry(
        self,
        share_id: int,
        error_message: Optional[str] = None
    ) -> PhotoShares:
        """Increment the retry count and record the failure."""
        async def _do_record():
            result = await self.session.execute(
                select(PhotoShares)
                .where(PhotoShares.id == share_id, PhotoShares.deleted.is_(false()))
                .with_for_update()
                .execution_options(timeout=self._default_timeout)
            )
            share = result.scalar_one_or_none()
            if not share:
                raise ShareNotFoundError(f"Share {share_id} not found")

            if share.retry_count >= self._max_retries:
                raise InvalidShareStateError(
                    f"Max retries ({self._max_retries}) exceeded for share {share_id}"
                )

            share.retry_count += 1
            share.last_retry_at = func.now()
            share.status = ShareStatus.FAILED.value
            if error_message:
                share.error_message = error_message[:500]

            share.updated_at = func.now()
            return share

        share = await self._with_deadlock_retry(_do_record, "record_retry")
        await self.session.commit()
        await self.session.refresh(share)
        self._log("warning", f"Recorded retry {share.retry_count} for share {share_id}")
        return share

    # ==================================================================
    # DELETE
    # ==================================================================

    async def soft_delete_share(self, share_id: int) -> bool:
        """Soft delete a share record."""
        async def _do_delete():
            result = await self.session.execute(
                select(PhotoShares)
                .where(PhotoShares.id == share_id, PhotoShares.deleted.is_(false()))
                .with_for_update()
                .execution_options(timeout=self._default_timeout)
            )
            share = result.scalar_one_or_none()
            if not share:
                return False

            share.deleted = True
            share.is_active = False
            share.updated_at = func.now()
            return True

        success = await self._with_deadlock_retry(_do_delete, "soft_delete_share")
        if success:
            await self.session.commit()
            self._log("info", f"Soft deleted share {share_id}")
        return success

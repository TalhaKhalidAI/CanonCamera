# App/repository/PrinterRepo.py - PRODUCTION HARDENED (Architect's Version)
from sqlalchemy import select, update, delete, and_, or_, func, CASE, case
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import logging

from App.api.databases.MigrateTable import (
    printerConfig,
    Prints,
    Events,
    User,
    Photoes,
    Sessions
)

logger = logging.getLogger(__name__)

class PrinterRepo:
    """Repository for Printer configurations and Print job management.
    
    Hardened for high-concurrency booth environments with row-level locking 
    and optimized query paths.
    """

    def __init__(self, session: AsyncSession):
        self.session = session
        self._default_timeout = 5.0

    # ========== PRINTER CONFIGURATION CRUD ==========

    async def create_printer_config(
        self,
        event_id: int,
        owned_by: int,
        printer_name: str,
        printer_settings: Dict[str, Any] = None
    ) -> Optional[printerConfig]:
        """Create a new printer configuration for an event.
        
        Includes existence checks for Event and duplicate name protection.
        """
        try:
            if not printer_name:
                raise ValueError("Printer name is required")

            # Validate Event exists and is not deleted
            event_check = await self.session.execute(
                select(Events.id)
                .where(and_(Events.id == event_id, Events.deleted == False))
                .execution_options(timeout=self._default_timeout)
            )
            if not event_check.scalar():
                raise ValueError(f"Active event with ID {event_id} not found")

            # Check for duplicate printer name in the same event
            dup_check = await self.session.execute(
                select(printerConfig.id)
                .where(
                    and_(
                        printerConfig.event_id == event_id,
                        printerConfig.printer_name == printer_name,
                        printerConfig.deleted == False
                    )
                )
                .execution_options(timeout=self._default_timeout)
            )
            if dup_check.scalar():
                raise ValueError(f"Printer '{printer_name}' already exists for event {event_id}")

            config = printerConfig(
                event_id=event_id,
                owned_by=owned_by,
                printer_name=printer_name,
                printer_settings=printer_settings or {},
                is_active=True,
                disabled=False,
                deleted=False
            )
            self.session.add(config)
            await self.session.commit()
            await self.session.refresh(config)
            
            logger.info(f"Created printer config {config.id} for event {event_id}")
            return config
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error creating printer config: {e}")
            return None

    async def get_printer_config(
        self,
        config_id: int,
        include_deleted: bool = False
    ) -> Optional[printerConfig]:
        """Retrieve a specific printer configuration by ID."""
        try:
            query = select(printerConfig).where(printerConfig.id == config_id)
            if not include_deleted:
                query = query.where(printerConfig.deleted == False)

            result = await self.session.execute(
                query.execution_options(timeout=self._default_timeout)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting printer config {config_id}: {e}")
            return None

    async def get_event_printer_configs(
        self,
        event_id: int,
        active_only: bool = True
    ) -> List[printerConfig]:
        """List all printer configurations for a specific event."""
        try:
            query = select(printerConfig).where(
                and_(
                    printerConfig.event_id == event_id,
                    printerConfig.deleted == False
                )
            )
            if active_only:
                query = query.where(
                    and_(
                        printerConfig.is_active == True,
                        printerConfig.disabled == False
                    )
                )

            result = await self.session.execute(
                query.execution_options(timeout=self._default_timeout)
            )
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error listing printer configs for event {event_id}: {e}")
            return []

    async def update_printer_config(
        self,
        config_id: int,
        update_data: Dict[str, Any]
    ) -> Optional[printerConfig]:
        """Update printer configuration details and return updated object.
        
        Includes session refresh to maintain object state consistency.
        """
        try:
            config = await self.get_printer_config(config_id)
            if not config:
                return None

            forbidden_fields = {"id", "event_id", "owned_by", "created_at"}
            for field, value in update_data.items():
                if field not in forbidden_fields and hasattr(config, field):
                    setattr(config, field, value)

            config.updated_at = func.now()
            
            await self.session.commit()
            await self.session.refresh(config)
            
            logger.info(f"Updated printer config {config_id}")
            return config
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating printer config {config_id}: {e}")
            return None

    async def delete_printer_config(self, config_id: int, soft: bool = True) -> bool:
        """Delete a printer configuration."""
        try:
            if soft:
                stmt = (
                    update(printerConfig)
                    .where(printerConfig.id == config_id)
                    .values(deleted=True, is_active=False, updated_at=func.now())
                )
            else:
                stmt = delete(printerConfig).where(printerConfig.id == config_id)

            await self.session.execute(
                stmt.execution_options(timeout=self._default_timeout)
            )
            await self.session.commit()
            logger.info(f"Deleted printer config {config_id} (soft={soft})")
            return True
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error deleting printer config {config_id}: {e}")
            return False

    async def restore_printer_config(self, config_id: int) -> bool:
        """Restore a soft-deleted printer configuration with name conflict check."""
        try:
            config = await self.get_printer_config(config_id, include_deleted=True)
            if not config or not config.deleted:
                return False

            # Check if name is already taken by another active config
            conflict_check = await self.session.execute(
                select(printerConfig.id).where(
                    and_(
                        printerConfig.event_id == config.event_id,
                        printerConfig.printer_name == config.printer_name,
                        printerConfig.deleted == False,
                        printerConfig.id != config_id
                    )
                )
            )
            if conflict_check.scalar():
                logger.warning(f"Restoration failed: Name '{config.printer_name}' conflict")
                return False

            config.deleted = False
            config.is_active = True
            config.disabled = False
            config.updated_at = func.now()
            
            await self.session.commit()
            logger.info(f"Restored printer config {config_id}")
            return True
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error restoring printer config {config_id}: {e}")
            return False

    # ========== PRINT JOB MANAGEMENT (Prints) ==========

    async def create_print_job(
        self,
        photo_id: int,
        session_id: int,
        layout_type: str = "single",
        copies: int = 1
    ) -> Optional[Prints]:
        """Queue a new print job with existence validation for photo and session."""
        try:
            # Validate Photo and Session exist and aren't deleted
            # validation = await self.session.execute(
            #     select(Photoes.id, Sessions.id)
            #     .join(Sessions, Photoes.session_id == Sessions.id)
            #     .where(
            #         and_(
            #             Photoes.id == photo_id,
            #             Sessions.id == session_id,
            #             Photoes.deleted == False,
            #             Photoes.is_active == True,      # ← ADD THIS
            #             Sessions.deleted == False,
            #             Sessions.is_active == True,     # ← ADD THIS
            #             Sessions.disabled == False      # ← ADD THIS
            #         )
            #     )
            #     .execution_options(timeout=self._default_timeout)
            # )
            validation = await self.session.execute(
                select(Photoes.id, Sessions.id)
                .join(Sessions, Photoes.session_id == Sessions.id)
                .join(Events, Sessions.event_id == Events.id)  # ← JOIN event
                .where(
                    and_(
                        Photoes.id == photo_id,
                        Sessions.id == session_id,
                        Photoes.deleted == False,
                        Photoes.is_active == True,
                        Sessions.deleted == False,
                        Sessions.is_active == True,
                        Sessions.disabled == False,
                        Events.deleted == False,      # ← ADD
                        Events.is_active == True,     # ← ADD
                        Events.disabled == False      # ← ADD
                    )
                )
            )
            if not validation.first():
                raise ValueError("Valid active Photo and Session pair required")

            job = Prints(
                photo_id=photo_id,
                session_id=session_id,
                layout_type=layout_type,
                copies=copies,
                status="pending",
                is_active=True,
                disabled=False,
                deleted=False
            )
            self.session.add(job)
            await self.session.commit()
            await self.session.refresh(job)
            
            logger.info(f"Created print job {job.id} for photo {photo_id}")
            return job
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error creating print job: {e}")
            return None

    async def claim_next_job(self) -> Optional[Prints]:
        """Atomic claim for worker processing using FOR UPDATE SKIP LOCKED.
        
        This prevents multiple parallel workers from processing the same job.
        The first worker to successfully lock and update becomes the exclusive owner.
        """
        try:
            # Select next pending job and lock it
            select_stmt = (
                select(Prints)
                .where(
                    and_(
                        Prints.status == "pending",
                        Prints.deleted == False,
                        Prints.disabled == False,
                        Prints.is_active == True
                    )
                )
                .order_by(Prints.created_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            
            result = await self.session.execute(
                select_stmt.execution_options(timeout=self._default_timeout)
            )
            job = result.scalar_one_or_none()
            
            if job:
                job.status = "processing"
                job.started_at = func.now()
                job.updated_at = func.now()
                await self.session.commit()
                await self.session.refresh(job)
                logger.debug(f"Worker claimed print job {job.id}")
            
            return job
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error claiming next print job: {e}")
            return None

    async def update_print_status(
        self,
        job_id: int,
        status: str,
        error_message: str = None
    ) -> bool:
        """Update the status of a print job using row-level locking."""
        try:
            valid_statuses = {"pending", "processing", "completed", "failed", "cancelled"}
            if status not in valid_statuses:
                raise ValueError(f"Invalid status: {status}")

            # Lock the row before update to prevent race conditions
            select_stmt = (
                select(Prints)
                .where(Prints.id == job_id)
                .with_for_update()
            )
            result = await self.session.execute(
                select_stmt.execution_options(timeout=self._default_timeout)
            )
            job = result.scalar_one_or_none()
            
            if not job:
                return False

            # State transition validation
            terminal_states = {"completed", "cancelled"}
            if job.status in terminal_states and status != job.status:
                raise ValueError(f"Cannot transition from terminal state '{job.status}' to '{status}'")

            job.status = status
            job.error_message = error_message
            job.updated_at = func.now()
            
            if status == "completed":
                job.completed_at = func.now()
            elif status == "processing":
                job.started_at = func.now()

            await self.session.commit()
            logger.info(f"Updated print job {job_id} status to {status}")
            return True
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating print job {job_id} status: {e}")
            return False

    async def get_session_prints(
        self, 
        session_id: int,
        limit: int = 50,
        offset: int = 0
    ) -> List[Prints]:
        """Get paginated print jobs for a guest session."""
        try:
            query = (
                select(Prints)
                .where(and_(
                    Prints.session_id == session_id, 
                    Prints.deleted == False,
                    Prints.disabled == False
                ))
                .order_by(Prints.created_at.desc())
                .limit(limit)
                .offset(offset)
            )

            result = await self.session.execute(
                query.execution_options(timeout=self._default_timeout)
            )
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error getting prints for session {session_id}: {e}")
            return []

    async def get_print_stats(self, event_id: Optional[int] = None) -> Dict[str, Any]:
        """Get basic print statistics using optimized JOIN for event-level filtering.
        
        Replaces vulnerable IN (SELECT...) subqueries with a performant INNER JOIN.
        """
        try:
            # Base counters
            total_stmt = func.count(Prints.id).label("total")
            completed_stmt = func.sum(case((Prints.status == "completed", 1), else_=0)).label("completed")
            failed_stmt = func.sum(case((Prints.status == "failed", 1), else_=0)).label("failed")

            query = select(total_stmt, completed_stmt, failed_stmt).where(Prints.deleted == False)

            if event_id:
                # JOIN to Sessions to filter by event_id - much faster than IN (SELECT...)
                query = query.join(Sessions, Prints.session_id == Sessions.id)\
                             .where(and_(Sessions.event_id == event_id, Sessions.deleted == False))

            result = await self.session.execute(
                query.execution_options(timeout=self._default_timeout)
            )
            row = result.first()

            if not row or row.total == 0:
                return {"total_jobs": 0, "completed": 0, "failed": 0, "pending": 0}

            total = row.total or 0
            comp = row.completed or 0
            fail = row.failed or 0

            return {
                "total_jobs": total,
                "completed": comp,
                "failed": fail,
                "pending": max(0, total - comp - fail)
            }
        except Exception as e:
            logger.error(f"Error getting print stats: {e}")
            return {"total_jobs": 0, "completed": 0, "failed": 0, "pending": 0}

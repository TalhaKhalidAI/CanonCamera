# App/repository/CameraRepo.py
import asyncio
import logging
from typing import Any, Dict, List, Optional
from contextvars import ContextVar
from enum import Enum

from sqlalchemy import and_, delete, func, select, update
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import false, true

from App.api.databases.MigrateTable import (
    Cameras, CamerasPresets, EventCameraConfig, SessionCameraSettings,
    Events, Sessions, User,
)

logger = logging.getLogger(__name__)
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="unknown")
_PG_DEADLOCK_CODE = "40P01"


# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class CameraRepoError(Exception):
    pass

class CameraNotFoundError(CameraRepoError):
    pass

class PresetNotFoundError(CameraRepoError):
    pass

class EventConfigNotFoundError(CameraRepoError):
    pass

class SessionSettingsNotFoundError(CameraRepoError):
    pass

class DuplicateCameraError(CameraRepoError):
    pass

class DuplicatePresetError(CameraRepoError):
    pass


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class CameraRepo:
    """
    Production-ready Camera repository with full CRUD for:
        - Cameras
        - CamerasPresets
        - EventCameraConfig
        - SessionCameraSettings
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
    # CAMERAS - CRUD
    # ==================================================================

    async def create_camera(
        self,
        name: str,
        model: str,
        serial_number: Optional[str] = None,
        usb_port: Optional[str] = None,
        firmware_version: Optional[str] = None,
        capabilities: Optional[Dict] = None,
        notes: Optional[str] = None,
    ) -> Cameras:
        """Create a new camera with deadlock retry."""
        async def _do_create():
            if serial_number:
                existing = await self.session.execute(
                    select(Cameras.id).where(
                        Cameras.serial_number == serial_number,
                        Cameras.deleted.is_(false()),
                    ).execution_options(timeout=self._default_timeout)
                )
                if existing.scalar():
                    raise DuplicateCameraError(
                        f"Camera with serial '{serial_number}' already exists"
                    )

            camera = Cameras(
                name=name.strip(),
                model=model.strip(),
                serial_number=serial_number,
                usb_port=usb_port,
                firmware_version=firmware_version,
                capabilities=capabilities or {},
                is_active=True,
                notes=notes,
                deleted=False,
            )
            self.session.add(camera)
            await self.session.flush()
            return camera

        camera = await self._with_deadlock_retry(_do_create, "create_camera")
        await self.session.commit()
        await self.session.refresh(camera)
        self._log("info", f"Created camera '{name}' (model={model})")
        return camera

    async def get_camera_by_id(
        self, camera_id: int, include_deleted: bool = False
    ) -> Optional[Cameras]:
        """Get camera by ID."""
        conditions = [Cameras.id == camera_id]
        if not include_deleted:
            conditions.append(Cameras.deleted.is_(false()))

        result = await self.session.execute(
            select(Cameras).where(and_(*conditions))
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_camera_by_serial(self, serial_number: str) -> Optional[Cameras]:
        """Get camera by serial number."""
        result = await self.session.execute(
            select(Cameras).where(
                Cameras.serial_number == serial_number,
                Cameras.deleted.is_(false()),
            ).execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_all_cameras(
        self, active_only: bool = True, limit: int = 100, offset: int = 0
    ) -> List[Cameras]:
        """Get all cameras with optional active filter."""
        conditions = [Cameras.deleted.is_(false())]
        if active_only:
            conditions.append(Cameras.is_active.is_(true()))

        result = await self.session.execute(
            select(Cameras).where(and_(*conditions))
            .order_by(Cameras.created_at.desc())
            .limit(limit).offset(offset)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def update_camera(
        self,
        camera_id: int,
        name: Optional[str] = None,
        model: Optional[str] = None,
        serial_number: Optional[str] = None,
        usb_port: Optional[str] = None,
        firmware_version: Optional[str] = None,
        capabilities: Optional[Dict] = None,
        is_active: Optional[bool] = None,
        notes: Optional[str] = None,
    ) -> Cameras:
        """Update camera with FOR UPDATE lock."""
        async def _do_update():
            result = await self.session.execute(
                select(Cameras)
                .where(Cameras.id == camera_id, Cameras.deleted.is_(false()))
                .with_for_update()
                .execution_options(timeout=self._default_timeout)
            )
            camera = result.scalar_one_or_none()
            if not camera:
                raise CameraNotFoundError(f"Camera {camera_id} not found")

            if serial_number and serial_number != camera.serial_number:
                dup = await self.session.execute(
                    select(Cameras.id).where(
                        Cameras.serial_number == serial_number,
                        Cameras.id != camera_id,
                        Cameras.deleted.is_(false()),
                    )
                )
                if dup.scalar():
                    raise DuplicateCameraError(
                        f"Serial '{serial_number}' already in use"
                    )
                camera.serial_number = serial_number

            if name is not None:
                camera.name = name.strip()
            if model is not None:
                camera.model = model.strip()
            if usb_port is not None:
                camera.usb_port = usb_port
            if firmware_version is not None:
                camera.firmware_version = firmware_version
            if capabilities is not None:
                camera.capabilities = capabilities
            if is_active is not None:
                camera.is_active = is_active
            if notes is not None:
                camera.notes = notes

            camera.updated_at = func.now()
            return camera

        camera = await self._with_deadlock_retry(_do_update, "update_camera")
        await self.session.commit()
        await self.session.refresh(camera)
        self._log("info", f"Updated camera {camera_id}")
        return camera

    async def soft_delete_camera(self, camera_id: int) -> bool:
        """Soft delete a camera."""
        result = await self.session.execute(
            select(Cameras)
            .where(Cameras.id == camera_id, Cameras.deleted.is_(false()))
            .with_for_update()
            .execution_options(timeout=self._default_timeout)
        )
        camera = result.scalar_one_or_none()
        if not camera:
            return False

        camera.deleted = True
        camera.is_active = False
        camera.updated_at = func.now()
        await self.session.commit()
        self._log("info", f"Soft deleted camera {camera_id}")
        return True

    # ==================================================================
    # CAMERA PRESETS - CRUD
    # ==================================================================

    async def create_preset(
        self,
        name: str,
        settings: Dict,
        description: Optional[str] = None,
        camera_model: Optional[str] = None,
        created_by: Optional[int] = None,
    ) -> CamerasPresets:
        """Create a camera preset with deadlock retry."""
        async def _do_create():
            existing = await self.session.execute(
                select(CamerasPresets.id).where(
                    CamerasPresets.name == name,
                    CamerasPresets.deleted.is_(false()),
                ).execution_options(timeout=self._default_timeout)
            )
            if existing.scalar():
                raise DuplicatePresetError(f"Preset '{name}' already exists")

            preset = CamerasPresets(
                name=name.strip(),
                description=description,
                camera_model=camera_model,
                settings=settings,
                is_active=True,
                created_by=created_by,
                deleted=False,
            )
            self.session.add(preset)
            await self.session.flush()
            return preset

        preset = await self._with_deadlock_retry(_do_create, "create_preset")
        await self.session.commit()
        await self.session.refresh(preset)
        self._log("info", f"Created preset '{name}'")
        return preset

    async def get_preset_by_id(
        self, preset_id: int, include_deleted: bool = False
    ) -> Optional[CamerasPresets]:
        """Get preset by ID."""
        conditions = [CamerasPresets.id == preset_id]
        if not include_deleted:
            conditions.append(CamerasPresets.deleted.is_(false()))

        result = await self.session.execute(
            select(CamerasPresets).where(and_(*conditions))
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_preset_by_name(self, name: str) -> Optional[CamerasPresets]:
        """Get preset by name."""
        result = await self.session.execute(
            select(CamerasPresets).where(
                CamerasPresets.name == name,
                CamerasPresets.deleted.is_(false()),
            ).execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_presets_by_model(
        self, camera_model: str, limit: int = 100
    ) -> List[CamerasPresets]:
        """Get all presets for a camera model."""
        result = await self.session.execute(
            select(CamerasPresets).where(
                CamerasPresets.camera_model == camera_model,
                CamerasPresets.deleted.is_(false()),
                CamerasPresets.is_active.is_(true()),
            ).order_by(CamerasPresets.name.asc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_all_presets(
        self, active_only: bool = True, limit: int = 100, offset: int = 0
    ) -> List[CamerasPresets]:
        """Get all presets."""
        conditions = [CamerasPresets.deleted.is_(false())]
        if active_only:
            conditions.append(CamerasPresets.is_active.is_(true()))

        result = await self.session.execute(
            select(CamerasPresets).where(and_(*conditions))
            .order_by(CamerasPresets.name.asc())
            .limit(limit).offset(offset)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def update_preset(
        self,
        preset_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
        camera_model: Optional[str] = None,
        settings: Optional[Dict] = None,
        is_active: Optional[bool] = None,
    ) -> CamerasPresets:
        """Update a preset with FOR UPDATE lock."""
        async def _do_update():
            result = await self.session.execute(
                select(CamerasPresets)
                .where(CamerasPresets.id == preset_id, CamerasPresets.deleted.is_(false()))
                .with_for_update()
                .execution_options(timeout=self._default_timeout)
            )
            preset = result.scalar_one_or_none()
            if not preset:
                raise PresetNotFoundError(f"Preset {preset_id} not found")

            if name is not None and name != preset.name:
                dup = await self.session.execute(
                    select(CamerasPresets.id).where(
                        CamerasPresets.name == name,
                        CamerasPresets.id != preset_id,
                        CamerasPresets.deleted.is_(false()),
                    )
                )
                if dup.scalar():
                    raise DuplicatePresetError(f"Preset '{name}' already exists")
                preset.name = name.strip()

            if description is not None:
                preset.description = description
            if camera_model is not None:
                preset.camera_model = camera_model
            if settings is not None:
                preset.settings = settings
            if is_active is not None:
                preset.is_active = is_active

            preset.updated_at = func.now()
            return preset

        preset = await self._with_deadlock_retry(_do_update, "update_preset")
        await self.session.commit()
        await self.session.refresh(preset)
        self._log("info", f"Updated preset {preset_id}")
        return preset

    async def soft_delete_preset(self, preset_id: int) -> bool:
        """Soft delete a preset."""
        result = await self.session.execute(
            select(CamerasPresets)
            .where(CamerasPresets.id == preset_id, CamerasPresets.deleted.is_(false()))
            .with_for_update()
            .execution_options(timeout=self._default_timeout)
        )
        preset = result.scalar_one_or_none()
        if not preset:
            return False

        preset.deleted = True
        preset.is_active = False
        preset.updated_at = func.now()
        await self.session.commit()
        self._log("info", f"Soft deleted preset {preset_id}")
        return True

    # ==================================================================
    # EVENT CAMERA CONFIG - CRUD
    # ==================================================================

    async def create_event_config(
        self,
        event_id: int,
        camera_id: int,
        preset_id: Optional[int] = None,
        override_settings: Optional[Dict] = None,
        applied_by: Optional[int] = None,
    ) -> EventCameraConfig:
        """Assign a camera to an event with deadlock retry."""
        async def _do_create():
            camera = await self.get_camera_by_id(camera_id)
            if not camera:
                raise CameraNotFoundError(f"Camera {camera_id} not found")

            if preset_id:
                preset = await self.get_preset_by_id(preset_id)
                if not preset:
                    raise PresetNotFoundError(f"Preset {preset_id} not found")

            config = EventCameraConfig(
                event_id=event_id,
                camera_id=camera_id,
                preset_id=preset_id,
                override_settings=override_settings or {},
                is_active=True,
                applied_by=applied_by,
                deleted=False,
            )
            self.session.add(config)
            await self.session.flush()
            return config

        config = await self._with_deadlock_retry(_do_create, "create_event_config")
        await self.session.commit()
        await self.session.refresh(config)
        self._log("info", f"Created event config: event={event_id} camera={camera_id}")
        return config

    async def get_event_config_by_id(
        self, config_id: int, include_deleted: bool = False
    ) -> Optional[EventCameraConfig]:
        """Get event camera config by ID."""
        conditions = [EventCameraConfig.id == config_id]
        if not include_deleted:
            conditions.append(EventCameraConfig.deleted.is_(false()))

        result = await self.session.execute(
            select(EventCameraConfig).where(and_(*conditions))
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_configs_by_event(
        self, event_id: int, active_only: bool = True, limit: int = 100
    ) -> List[EventCameraConfig]:
        """Get all camera configs for an event."""
        conditions = [
            EventCameraConfig.event_id == event_id,
            EventCameraConfig.deleted.is_(false()),
        ]
        if active_only:
            conditions.append(EventCameraConfig.is_active.is_(true()))

        result = await self.session.execute(
            select(EventCameraConfig).where(and_(*conditions))
            .order_by(EventCameraConfig.created_at.desc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_configs_by_camera(
        self, camera_id: int, active_only: bool = True, limit: int = 100
    ) -> List[EventCameraConfig]:
        """Get all event configs for a specific camera."""
        conditions = [
            EventCameraConfig.camera_id == camera_id,
            EventCameraConfig.deleted.is_(false()),
        ]
        if active_only:
            conditions.append(EventCameraConfig.is_active.is_(true()))

        result = await self.session.execute(
            select(EventCameraConfig).where(and_(*conditions))
            .order_by(EventCameraConfig.created_at.desc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_active_config_for_event_camera(
        self, event_id: int, camera_id: int
    ) -> Optional[EventCameraConfig]:
        """Get the active config for a specific event+camera combination."""
        result = await self.session.execute(
            select(EventCameraConfig).where(
                EventCameraConfig.event_id == event_id,
                EventCameraConfig.camera_id == camera_id,
                EventCameraConfig.is_active.is_(true()),
                EventCameraConfig.deleted.is_(false()),
            ).execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def update_event_config(
        self,
        config_id: int,
        preset_id: Optional[int] = None,
        override_settings: Optional[Dict] = None,
        is_active: Optional[bool] = None,
        applied_by: Optional[int] = None,
    ) -> EventCameraConfig:
        """Update event camera config."""
        async def _do_update():
            result = await self.session.execute(
                select(EventCameraConfig)
                .where(EventCameraConfig.id == config_id, EventCameraConfig.deleted.is_(false()))
                .with_for_update()
                .execution_options(timeout=self._default_timeout)
            )
            config = result.scalar_one_or_none()
            if not config:
                raise EventConfigNotFoundError(f"Event config {config_id} not found")

            if preset_id is not None:
                if preset_id > 0:
                    preset = await self.get_preset_by_id(preset_id)
                    if not preset:
                        raise PresetNotFoundError(f"Preset {preset_id} not found")
                config.preset_id = preset_id if preset_id > 0 else None

            if override_settings is not None:
                config.override_settings = override_settings
            if is_active is not None:
                config.is_active = is_active
            if applied_by is not None:
                config.applied_by = applied_by

            config.applied_at = func.now()
            config.updated_at = func.now()
            return config

        config = await self._with_deadlock_retry(_do_update, "update_event_config")
        await self.session.commit()
        await self.session.refresh(config)
        self._log("info", f"Updated event config {config_id}")
        return config

    async def soft_delete_event_config(self, config_id: int) -> bool:
        """Soft delete an event camera config."""
        result = await self.session.execute(
            select(EventCameraConfig)
            .where(EventCameraConfig.id == config_id, EventCameraConfig.deleted.is_(false()))
            .with_for_update()
            .execution_options(timeout=self._default_timeout)
        )
        config = result.scalar_one_or_none()
        if not config:
            return False

        config.deleted = True
        config.is_active = False
        config.updated_at = func.now()
        await self.session.commit()
        self._log("info", f"Soft deleted event config {config_id}")
        return True

    # ==================================================================
    # SESSION CAMERA SETTINGS - CRUD
    # ==================================================================

    async def record_session_settings(
        self,
        session_id: int,
        event_id: int,
        camera_id: int,
        settings_used: Dict,
        preset_id: Optional[int] = None,
        applied_via: str = "preset",
        success: bool = True,
        error_message: Optional[str] = None,
    ) -> SessionCameraSettings:
        """Record the camera settings used for a photo session."""
        # Validate camera exists
        camera = await self.get_camera_by_id(camera_id)
        if not camera:
            raise CameraNotFoundError(f"Camera {camera_id} not found")

        record = SessionCameraSettings(
            session_id=session_id,
            event_id=event_id,
            camera_id=camera_id,
            preset_id=preset_id,
            settings_used=settings_used,
            applied_via=applied_via,
            success=success,
            error_message=error_message[:500] if error_message else None,
            deleted=False,
        )
        self.session.add(record)
        await self.session.flush()
        await self.session.commit()
        await self.session.refresh(record)

        self._log(
            "info",
            f"Recorded session settings: session={session_id} camera={camera_id} "
            f"success={success}",
        )
        return record

    async def get_session_settings_by_id(
        self, record_id: int, include_deleted: bool = False
    ) -> Optional[SessionCameraSettings]:
        """Get session camera settings by ID."""
        conditions = [SessionCameraSettings.id == record_id]
        if not include_deleted:
            conditions.append(SessionCameraSettings.deleted.is_(false()))

        result = await self.session.execute(
            select(SessionCameraSettings).where(and_(*conditions))
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_settings_by_session(
        self, session_id: int
    ) -> List[SessionCameraSettings]:
        """Get all camera settings records for a session."""
        result = await self.session.execute(
            select(SessionCameraSettings).where(
                SessionCameraSettings.session_id == session_id,
                SessionCameraSettings.deleted.is_(false()),
            ).order_by(SessionCameraSettings.created_at.desc())
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_settings_by_event(
        self, event_id: int, limit: int = 100, offset: int = 0
    ) -> List[SessionCameraSettings]:
        """Get all camera settings records for an event."""
        result = await self.session.execute(
            select(SessionCameraSettings).where(
                SessionCameraSettings.event_id == event_id,
                SessionCameraSettings.deleted.is_(false()),
            ).order_by(SessionCameraSettings.created_at.desc())
            .limit(limit).offset(offset)
            .execution_options(timeout=self._bulk_timeout)
        )
        return list(result.scalars().all())

    async def get_failed_settings(
        self, event_id: Optional[int] = None, limit: int = 100
    ) -> List[SessionCameraSettings]:
        """Get all failed camera settings applications."""
        conditions = [
            SessionCameraSettings.success.is_(false()),
            SessionCameraSettings.deleted.is_(false()),
        ]
        if event_id:
            conditions.append(SessionCameraSettings.event_id == event_id)

        result = await self.session.execute(
            select(SessionCameraSettings).where(and_(*conditions))
            .order_by(SessionCameraSettings.created_at.desc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def soft_delete_session_settings(self, record_id: int) -> bool:
        """Soft delete a session camera settings record."""
        result = await self.session.execute(
            select(SessionCameraSettings)
            .where(
                SessionCameraSettings.id == record_id,
                SessionCameraSettings.deleted.is_(false()),
            )
            .with_for_update()
            .execution_options(timeout=self._default_timeout)
        )
        record = result.scalar_one_or_none()
        if not record:
            return False

        record.deleted = True
        await self.session.commit()
        self._log("info", f"Soft deleted session settings {record_id}")
        return True

    # ==================================================================
    # CONVENIENCE / WORKFLOW METHODS
    # ==================================================================

    async def apply_preset_to_event(
        self,
        event_id: int,
        camera_id: int,
        preset_id: int,
        applied_by: int,
        override_settings: Optional[Dict] = None,
    ) -> EventCameraConfig:
        """
        Apply a preset to a camera for an event atomically.
        Locks existing configs with FOR UPDATE, deactivates them, then creates new.
        """
        async def _do_apply():
            # Lock + deactivate existing active configs for this event+camera
            existing = await self.session.execute(
                select(EventCameraConfig)
                .where(
                    EventCameraConfig.event_id == event_id,
                    EventCameraConfig.camera_id == camera_id,
                    EventCameraConfig.is_active.is_(true()),
                    EventCameraConfig.deleted.is_(false()),
                )
                .with_for_update()
                .execution_options(timeout=self._default_timeout)
            )
            for old_config in existing.scalars().all():
                old_config.is_active = False
                old_config.updated_at = func.now()

            # Validate camera and preset
            camera = await self.get_camera_by_id(camera_id)
            if not camera:
                raise CameraNotFoundError(f"Camera {camera_id} not found")
            if preset_id:
                preset = await self.get_preset_by_id(preset_id)
                if not preset:
                    raise PresetNotFoundError(f"Preset {preset_id} not found")

            # Create new config
            config = EventCameraConfig(
                event_id=event_id,
                camera_id=camera_id,
                preset_id=preset_id,
                override_settings=override_settings or {},
                is_active=True,
                applied_by=applied_by,
                deleted=False,
            )
            self.session.add(config)
            await self.session.flush()
            return config

        config = await self._with_deadlock_retry(_do_apply, "apply_preset_to_event")
        await self.session.commit()
        await self.session.refresh(config)
        self._log(
            "info",
            f"Applied preset {preset_id} to camera {camera_id} for event {event_id}",
        )
        return config

    async def get_resolved_settings(
        self, event_id: int, camera_id: int
    ) -> Dict[str, Any]:
        """
        Resolve the final settings for a camera at an event:
            1. Start with preset settings
            2. Merge override_settings on top

        Returns dict with 'settings', 'preset_id', 'config_id' keys.
        """
        config = await self.get_active_config_for_event_camera(event_id, camera_id)
        if not config:
            return {"settings": {}, "preset_id": None, "config_id": None}

        base_settings = {}

        # Load preset settings if a preset is assigned
        if config.preset_id:
            preset = await self.get_preset_by_id(config.preset_id)
            if preset:
                base_settings = dict(preset.settings) if preset.settings else {}

        # Merge overrides on top
        if config.override_settings:
            base_settings.update(config.override_settings)

        return {
            "settings": base_settings,
            "preset_id": config.preset_id,
            "config_id": config.id,
        }

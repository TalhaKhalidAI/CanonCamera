# App/repository/PhotoRepo.py
import asyncio
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import aiofiles
from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from App.api.databases.MigrateTable import Events, OrderItems, Photoes, Sessions
from App.core.settings import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Resolve MEDIA_ROOT once at import time.
# All path operations are validated against this resolved absolute path.
# ---------------------------------------------------------------------------
try:
    MEDIA_ROOT: Path = Path(settings.MEDIA_PATH).resolve()
except AttributeError:
    raise RuntimeError(
        "MEDIA_PATH is not configured in settings. "
        "Add MEDIA_PATH=/absolute/path/to/media to your .env file."
    )

if not MEDIA_ROOT.is_absolute():
    raise RuntimeError(f"MEDIA_PATH must be an absolute path, got: {MEDIA_ROOT}")

if MEDIA_ROOT == Path("/"):
    raise RuntimeError("MEDIA_PATH cannot be the filesystem root (/).")

# Maximum file size: 50MB default, configurable via settings
MAX_FILE_SIZE_BYTES: int = getattr(settings, "MAX_PHOTO_SIZE_BYTES", 50 * 1024 * 1024)

# Supported image magic numbers for content validation
MAGIC_NUMBERS: Dict[str, Set[bytes]] = {
    "jpeg": {b"\xff\xd8\xff"},
    "jpg": {b"\xff\xd8\xff"},
    "png": {b"\x89PNG\r\n\x1a\n"},
    "gif": {b"GIF87a", b"GIF89a"},
    "webp": {b"RIFF"},  # RIFF....WEBP
    "bmp": {b"BM"},
    "tiff": {b"II*\x00", b"MM\x00*"},
}


class PhotoRepoError(Exception):
    """Base exception for PhotoRepo errors."""
    pass


class PhotoNotFoundError(PhotoRepoError):
    """Raised when a photo is not found."""
    pass


class SessionNotFoundError(PhotoRepoError):
    """Raised when a session is not found or inactive."""
    pass


class DuplicatePhotoError(PhotoRepoError):
    """Raised when a photo with the same path already exists."""
    pass


class FileValidationError(PhotoRepoError):
    """Raised when file content validation fails."""
    pass


class PhotoRepo:
    """
    Repository for Photo management.

    File-system contract
    --------------------
    * All photos are stored under MEDIA_ROOT.
    * Relative path stored in DB: {sanitized_guest}/{session_code}/{session_id}/{filename}
    * Absolute path on disk:      MEDIA_ROOT / <relative path above>
    * The two are always derived from the same source — they can never drift.
    * Soft delete  → DB flag only; the file is NEVER touched.
    * Hard delete  → DB row removed AND the file is deleted from disk.
    * Path traversal is blocked at _validate_path(); any path that resolves
      outside MEDIA_ROOT raises ValueError before any I/O occurs.
    """

    def __init__(self, session: AsyncSession):
        self.session = session
        self._default_timeout = 5.0

    # ------------------------------------------------------------------
    # Internal path helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize_name(name: str) -> str:
        """
        Convert an arbitrary string into a safe directory-name component.
        Keeps alphanumerics, hyphens, and underscores; collapses runs of
        unsafe characters into a single underscore; strips leading/trailing
        underscores; caps at 60 chars.
        """
        name = name.strip()
        name = re.sub(r"[^\w\-]", "_", name)
        name = re.sub(r"_+", "_", name).strip("_")
        return name[:60] or "unknown"

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        """
        Ensure the filename itself contains no path separators or shell chars.
        Only the basename is kept; unsafe chars are replaced with underscores.
        Strips leading dots to prevent hidden files.
        """
        filename = os.path.basename(filename)
        filename = re.sub(r"[^\w\-.]", "_", filename)
        filename = filename.lstrip(".")
        return filename or "image.jpg"

    def _build_abs_path(
        self,
        guest_name: str,
        session_code: str,
        session_id: int,
        filename: str,
    ) -> Path:
        """
        Build the ABSOLUTE path for a photo.

        Layout: MEDIA_ROOT / {guest} / {session_code} / {session_id} / {filename}

        The result is always validated against MEDIA_ROOT before being returned.
        """
        safe_guest    = self._sanitize_name(guest_name)
        safe_code     = self._sanitize_name(session_code)
        safe_filename = self._sanitize_filename(filename)

        abs_path = (
            MEDIA_ROOT / safe_guest / safe_code / str(session_id) / safe_filename
        )
        self._validate_path(abs_path)
        return abs_path

    @staticmethod
    def _validate_path(path: Path) -> None:
        """
        Raise ValueError if *path* resolves to anything outside MEDIA_ROOT.
        This is the single, authoritative traversal guard — called before
        every file read, write, or delete.
        """
        try:
            path.resolve().relative_to(MEDIA_ROOT)
        except ValueError:
            raise ValueError(
                f"Path traversal blocked: '{path}' resolves outside "
                f"MEDIA_ROOT ('{MEDIA_ROOT}')."
            )

    @staticmethod
    def _rel(abs_path: Path) -> str:
        """Return the path stored in the DB (relative to MEDIA_ROOT)."""
        return str(abs_path.relative_to(MEDIA_ROOT))

    @staticmethod
    def _abs(rel_path: str) -> Path:
        """Reconstruct the absolute path from a DB-stored relative path."""
        abs_path = (MEDIA_ROOT / rel_path).resolve()
        # Guard: reconstructed path must still be inside MEDIA_ROOT
        try:
            abs_path.relative_to(MEDIA_ROOT)
        except ValueError:
            raise ValueError(
                f"Stored path '{rel_path}' resolves outside MEDIA_ROOT. "
                "Possible DB tampering."
            )
        return abs_path

    # ------------------------------------------------------------------
    # Content validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_file_content(file_bytes: bytes, claimed_format: str) -> None:
        """
        Validate that file bytes match the claimed image format via magic numbers.
        Raises FileValidationError if validation fails.
        """
        if not file_bytes:
            raise FileValidationError("Empty file bytes provided.")

        if len(file_bytes) > MAX_FILE_SIZE_BYTES:
            raise FileValidationError(
                f"File size {len(file_bytes)} bytes exceeds maximum "
                f"{MAX_FILE_SIZE_BYTES} bytes."
            )

        claimed_format = claimed_format.lower().strip(".")
        magic_set = MAGIC_NUMBERS.get(claimed_format)

        if magic_set is None:
            logger.warning(
                f"No magic number validation available for format '{claimed_format}'. "
                "Skipping content validation."
            )
            return

        for magic in magic_set:
            if file_bytes.startswith(magic):
                return

        # Special case for webp: RIFF header + WEBP at offset 8
        if claimed_format == "webp" and file_bytes.startswith(b"RIFF"):
            if len(file_bytes) >= 12 and file_bytes[8:12] == b"WEBP":
                return

        raise FileValidationError(
            f"File content does not match claimed format '{claimed_format}'. "
            f"Expected magic number for {claimed_format}."
        )

    # ------------------------------------------------------------------
    # Async file I/O helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _async_write_file(abs_path: Path, file_bytes: bytes) -> None:
        """Write file bytes asynchronously, creating parent directories if needed."""
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(abs_path, "wb") as f:
            await f.write(file_bytes)

    @staticmethod
    async def _async_delete_file(abs_path: Path) -> None:
        """Delete a file asynchronously if it exists."""
        if await asyncio.to_thread(abs_path.exists):
            await asyncio.to_thread(abs_path.unlink, missing_ok=True)

    # ------------------------------------------------------------------
    # Internal session-info helper
    # ------------------------------------------------------------------

    async def _get_session_info(
        self, session_id: int
    ) -> Optional[Tuple[str, str]]:
        """
        Return (guest_name, session_code) for an active, non-deleted session.
        Returns None if the session is not found or is inactive.
        """
        result = await self.session.execute(
            select(Sessions.guest_name, Sessions.session_code).where(
                and_(
                    Sessions.id == session_id,
                    Sessions.deleted == False,
                    Sessions.is_active == True,
                    Sessions.disabled == False,
                )
            ).execution_options(timeout=self._default_timeout)
        )
        row = result.first()
        if row is None:
            return None
        return row.guest_name, row.session_code

    async def _verify_session_active(self, session_id: int) -> None:
        """Verify a session is active. Raises SessionNotFoundError if not."""
        session_info = await self._get_session_info(session_id)
        if session_info is None:
            raise SessionNotFoundError(f"Active session {session_id} not found")

    # ------------------------------------------------------------------
    # Duplicate detection
    # ------------------------------------------------------------------

    async def _path_exists_in_db(self, rel_path: str) -> bool:
        """Return True if this relative path is already recorded in the DB."""
        result = await self.session.execute(
            select(Photoes.id)
            .where(
                and_(
                    Photoes.path == rel_path,
                    Photoes.deleted == False,
                )
            )
            .limit(1)
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none() is not None

    # ------------------------------------------------------------------
    # CREATE
    # ------------------------------------------------------------------

    async def create_photo(
        self,
        session_id: int,
        filename: str,
        image_type: str,
        image_format: str,
        file_bytes: Optional[bytes] = None,
        thumb_path: Optional[str] = None,
        width_px: Optional[int] = None,
        height_px: Optional[int] = None,
        width_mm: Optional[float] = None,
        height_mm: Optional[float] = None,
        dpi: int = 400,
        file_size_bytes: Optional[int] = None,
        is_active: bool = True,
    ) -> Photoes:
        """
        Create a photo record and, if *file_bytes* is supplied, write the file.

        Path is always derived from session info + filename — callers never
        supply a raw path. This guarantees DB path == disk path.

        Raises:
            ValueError: missing required fields, inactive/deleted session,
                       duplicate path, path traversal attempt
            FileValidationError: file content doesn't match claimed format
                               or exceeds size limit
            SQLAlchemyError: database errors (propagated, not swallowed)
        """
        # --- basic validation ---
        if not session_id:
            raise ValueError("session_id is required")
        if not filename:
            raise ValueError("filename is required")
        if not image_type:
            raise ValueError("image_type is required")
        if not image_format:
            raise ValueError("image_format is required")

        # --- session must exist and be active ---
        session_info = await self._get_session_info(session_id)
        if session_info is None:
            raise SessionNotFoundError(
                f"Active session with ID {session_id} not found"
            )

        guest_name, session_code = session_info

        # --- build & validate path ---
        abs_path = self._build_abs_path(guest_name, session_code, session_id, filename)
        rel_path = self._rel(abs_path)

        # --- duplicate guard (DB is the single arbiter via unique constraint) ---
        if await self._path_exists_in_db(rel_path):
            raise DuplicatePhotoError(
                f"Photo '{filename}' already exists for session {session_id} "
                f"at path '{rel_path}'."
            )
        if await asyncio.to_thread(abs_path.exists):
            raise DuplicatePhotoError(
                f"File already exists on disk at '{rel_path}' but has no DB record. "
                "Resolve the orphan file before creating a new record."
            )

        # --- validate file content if bytes provided ---
        if file_bytes is not None:
            self._validate_file_content(file_bytes, image_format)
            file_size_bytes = file_size_bytes or len(file_bytes)

        # --- write file to disk if bytes provided ---
        if file_bytes is not None:
            await self._async_write_file(abs_path, file_bytes)
            logger.info(f"Wrote photo to disk: {rel_path}")

        # --- persist DB record ---
        photo = Photoes(
            session_id=session_id,
            filename=self._sanitize_filename(filename),
            path=rel_path,
            thumb_path=thumb_path,
            image_type=image_type,
            image_format=image_format,
            width_px=width_px,
            height_px=height_px,
            width_mm=width_mm,
            height_mm=height_mm,
            dpi=dpi,
            file_size_bytes=file_size_bytes,
            is_active=is_active,
            disabled=False,
            deleted=False,
        )
        self.session.add(photo)

        try:
            await self.session.commit()
        except IntegrityError as e:
            await self.session.rollback()
            # Clean up file since DB insert failed
            if file_bytes is not None:
                await self._async_delete_file(abs_path)
            raise DuplicatePhotoError(
                f"Photo path '{rel_path}' conflicted with existing record. "
                "Possible race condition."
            ) from e
        except SQLAlchemyError:
            await self.session.rollback()
            if file_bytes is not None:
                await self._async_delete_file(abs_path)
            raise

        await self.session.refresh(photo)
        logger.info(
            f"Created photo record id={photo.id} path='{rel_path}' "
            f"session={session_id}"
        )
        return photo

    async def bulk_create_photos(
        self,
        photos_data: List[Dict[str, Any]],
    ) -> List[Photoes]:
        """
        Bulk-create photo records. Each entry may include 'file_bytes' to write
        the file to disk atomically before the batch DB insert.

        On any failure the whole batch is rolled back and all written files
        are cleaned up.

        Raises:
            ValueError: missing required fields, inactive session
            DuplicatePhotoError: duplicate path within batch or in DB
            FileValidationError: content validation failure
            SQLAlchemyError: database errors (propagated)
        """
        if not photos_data:
            return []

        required = {"session_id", "filename", "image_type", "image_format"}
        written_files: List[Path] = []
        seen_paths: Set[str] = set()

        try:
            photos: List[Photoes] = []

            for i, data in enumerate(photos_data):
                missing = required - data.keys()
                if missing:
                    raise ValueError(f"Photo {i}: missing fields {missing}")

                session_info = await self._get_session_info(data["session_id"])
                if session_info is None:
                    raise SessionNotFoundError(
                        f"Photo {i}: active session {data['session_id']} not found"
                    )

                guest_name, session_code = session_info
                abs_path = self._build_abs_path(
                    guest_name, session_code, data["session_id"], data["filename"]
                )
                rel_path = self._rel(abs_path)

                # Intra-batch duplicate detection
                if rel_path in seen_paths:
                    raise DuplicatePhotoError(
                        f"Photo {i}: duplicate path '{rel_path}' within batch"
                    )
                seen_paths.add(rel_path)

                if await self._path_exists_in_db(rel_path):
                    raise DuplicatePhotoError(
                        f"Photo {i}: duplicate path '{rel_path}' already in DB"
                    )

                file_bytes = data.get("file_bytes")
                if file_bytes is not None:
                    self._validate_file_content(file_bytes, data["image_format"])
                    if await asyncio.to_thread(abs_path.exists):
                        raise DuplicatePhotoError(
                            f"Photo {i}: orphan file already exists at '{rel_path}'"
                        )
                    await self._async_write_file(abs_path, file_bytes)
                    written_files.append(abs_path)

                photos.append(
                    Photoes(
                        session_id=data["session_id"],
                        filename=self._sanitize_filename(data["filename"]),
                        path=rel_path,
                        thumb_path=data.get("thumb_path"),
                        image_type=data["image_type"],
                        image_format=data["image_format"],
                        width_px=data.get("width_px"),
                        height_px=data.get("height_px"),
                        width_mm=data.get("width_mm"),
                        height_mm=data.get("height_mm"),
                        dpi=data.get("dpi", 400),
                        file_size_bytes=data.get("file_size_bytes")
                        or (len(file_bytes) if file_bytes else None),
                        is_active=data.get("is_active", True),
                        disabled=False,
                        deleted=False,
                    )
                )

            for photo in photos:
                self.session.add(photo)

            try:
                await self.session.commit()
            except IntegrityError as e:
                await self.session.rollback()
                raise DuplicatePhotoError(
                    f"Bulk insert failed due to path conflict. "
                    f"Possible race condition or intra-batch collision."
                ) from e
            except SQLAlchemyError:
                await self.session.rollback()
                raise

            for photo in photos:
                await self.session.refresh(photo)

            logger.info(f"Bulk created {len(photos)} photos")
            return photos

        except Exception:
            # Roll back every file written during this batch
            for f in written_files:
                try:
                    await self._async_delete_file(f)
                except OSError as unlink_err:
                    logger.error(f"Bulk rollback: could not remove '{f}': {unlink_err}")
            raise

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------

    async def get_by_id(
        self,
        photo_id: int,
        include_deleted: bool = False,
    ) -> Optional[Photoes]:
        """Get photo by ID. Deleted photos are excluded by default."""
        conditions = [Photoes.id == photo_id]
        if not include_deleted:
            conditions.append(Photoes.deleted == False)

        result = await self.session.execute(
            select(Photoes)
            .where(and_(*conditions))
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_by_session(
        self,
        session_id: int,
        include_inactive: bool = False,
        include_deleted: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Photoes]:
        """Get all photos for a session with pagination."""
        conditions = [Photoes.session_id == session_id]
        if not include_deleted:
            conditions.append(Photoes.deleted == False)
        if not include_inactive:
            conditions += [Photoes.is_active == True, Photoes.disabled == False]

        result = await self.session.execute(
            select(Photoes)
            .where(and_(*conditions))
            .order_by(Photoes.created_at.desc())
            .limit(limit)
            .offset(offset)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_by_event(
        self,
        event_id: int,
        include_inactive: bool = False,
        include_deleted: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Photoes]:
        """Get all photos for an event via session join."""
        conditions = [Sessions.event_id == event_id]
        if not include_deleted:
            conditions += [Photoes.deleted == False, Sessions.deleted == False]
        if not include_inactive:
            conditions += [
                Photoes.is_active == True,
                Photoes.disabled == False,
                Sessions.is_active == True,
                Sessions.disabled == False,
            ]

        result = await self.session.execute(
            select(Photoes)
            .join(Sessions, Photoes.session_id == Sessions.id)
            .where(and_(*conditions))
            .order_by(Photoes.created_at.desc())
            .limit(limit)
            .offset(offset)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_by_filename(
        self,
        filename: str,
        session_id: Optional[int] = None,
    ) -> Optional[Photoes]:
        """
        Get photo by filename, optionally scoped to a session.

        WARNING: Without session_id, this returns an arbitrary photo if
        multiple sessions contain the same filename. Always provide session_id
        for deterministic results.
        """
        conditions = [
            Photoes.filename == self._sanitize_filename(filename),
            Photoes.deleted == False,
        ]
        if session_id:
            conditions.append(Photoes.session_id == session_id)

        result = await self.session.execute(
            select(Photoes)
            .where(and_(*conditions))
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_active_photos(
        self,
        session_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[Photoes]:
        """Get all active (not deleted, not disabled) photos."""
        conditions = [
            Photoes.deleted == False,
            Photoes.disabled == False,
            Photoes.is_active == True,
        ]
        if session_id:
            conditions.append(Photoes.session_id == session_id)

        result = await self.session.execute(
            select(Photoes)
            .where(and_(*conditions))
            .order_by(Photoes.created_at.desc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_deleted_photos(
        self,
        session_id: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Photoes]:
        """Get soft-deleted photo records (files are untouched on disk)."""
        conditions = [Photoes.deleted == True]
        if session_id:
            conditions.append(Photoes.session_id == session_id)

        result = await self.session.execute(
            select(Photoes)
            .where(and_(*conditions))
            .order_by(Photoes.updated_at.desc())
            .limit(limit)
            .offset(offset)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_photos_without_thumbnails(self, limit: int = 50) -> List[Photoes]:
        """Get active photos that still need thumbnail generation."""
        result = await self.session.execute(
            select(Photoes)
            .where(
                and_(
                    Photoes.deleted == False,
                    Photoes.disabled == False,
                    Photoes.is_active == True,
                    Photoes.thumb_path.is_(None),
                )
            )
            .order_by(Photoes.created_at.asc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    def get_absolute_path(self, photo: Photoes) -> Optional[Path]:
        """
        Return the absolute path for a photo from its DB-stored relative path.

        Returns None — and logs a warning — if the photo is soft-deleted,
        so callers are never handed a path to a 'deleted' file.
        Access is always blocked for soft-deleted photos.
        """
        if photo.deleted:
            logger.warning(
                f"Attempted to access file for soft-deleted photo id={photo.id}. "
                "Access denied."
            )
            return None
        try:
            return self._abs(photo.path)
        except ValueError as e:
            logger.error(f"Invalid path for photo id={photo.id}: {e}")
            return None

    # ------------------------------------------------------------------
    # UPDATE
    # ------------------------------------------------------------------

    async def update_photo(
        self,
        photo_id: int,
        update_data: Dict[str, Any],
    ) -> Photoes:
        """
        Update photo metadata.
        'path', 'id', 'session_id', 'created_at' are immutable.

        Raises:
            PhotoNotFoundError: photo not found
            ValueError: photo is deleted or immutable field provided
            SQLAlchemyError: database errors (propagated)
        """
        photo = await self.get_by_id(photo_id)
        if not photo:
            raise PhotoNotFoundError(f"Photo {photo_id} not found")
        if photo.deleted:
            raise ValueError(f"Photo {photo_id} is deleted")

        # Verify session is still active
        await self._verify_session_active(photo.session_id)

        immutable = {"id", "session_id", "path", "created_at"}
        ignored = []
        for field, value in update_data.items():
            if field in immutable:
                ignored.append(field)
                continue
            if hasattr(photo, field):
                setattr(photo, field, value)

        if ignored:
            logger.warning(
                f"Ignored immutable fields in update for photo {photo_id}: {ignored}"
            )

        photo.updated_at = func.now()

        try:
            await self.session.commit()
            await self.session.refresh(photo)
        except SQLAlchemyError:
            await self.session.rollback()
            raise

        logger.info(f"Updated photo {photo_id}")
        return photo

    async def update_thumbnail_path(
        self,
        photo_id: int,
        thumb_path: str,
    ) -> bool:
        """
        Update thumbnail path.
        The thumbnail path is validated to stay within MEDIA_ROOT.
        """
        # Validate thumb path before storing
        self._validate_path((MEDIA_ROOT / thumb_path).resolve())

        try:
            result = await self.session.execute(
                update(Photoes)
                .where(Photoes.id == photo_id)
                .values(thumb_path=thumb_path, updated_at=func.now())
                .execution_options(timeout=self._default_timeout)
            )
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            raise

        if result.rowcount > 0:
            logger.info(f"Updated thumbnail for photo {photo_id}")
            return True
        return False

    async def reassign_session(
        self,
        photo_id: int,
        new_session_id: int,
    ) -> bool:
        """
        Move a photo record to a different session.
        NOTE: this updates the DB record only — the file is NOT moved on disk.
        If you need the file in the new path structure, handle the file move
        in a service layer before calling this.
        """
        photo = await self.get_by_id(photo_id)
        if not photo:
            raise PhotoNotFoundError(f"Photo {photo_id} not found")
        if photo.deleted:
            raise ValueError(f"Photo {photo_id} is deleted")

        session_info = await self._get_session_info(new_session_id)
        if session_info is None:
            raise SessionNotFoundError(f"Active session {new_session_id} not found")

        photo.session_id = new_session_id
        photo.updated_at = func.now()

        try:
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            raise

        logger.info(f"Reassigned photo {photo_id} to session {new_session_id}")
        return True

    async def toggle_active_status(
        self,
        photo_id: int,
        is_active: bool,
    ) -> bool:
        """Toggle photo active status. Returns new status."""
        photo = await self.get_by_id(photo_id)
        if not photo:
            raise PhotoNotFoundError(f"Photo {photo_id} not found")
        if photo.deleted:
            raise ValueError(f"Photo {photo_id} is deleted")

        # Verify session is still active
        await self._verify_session_active(photo.session_id)

        photo.is_active = is_active
        photo.updated_at = func.now()

        try:
            await self.session.commit()
            await self.session.refresh(photo)
        except SQLAlchemyError:
            await self.session.rollback()
            raise

        logger.info(f"Photo {photo_id} active status set to {is_active}")
        return photo.is_active

    async def disable_photo(self, photo_id: int) -> bool:
        photo = await self.get_by_id(photo_id)
        if not photo:
            raise PhotoNotFoundError(f"Photo {photo_id} not found")
        if photo.deleted:
            raise ValueError(f"Photo {photo_id} is deleted")

        # Verify session is still active
        await self._verify_session_active(photo.session_id)

        photo.disabled = True
        photo.is_active = False
        photo.updated_at = func.now()

        try:
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            raise

        logger.info(f"Disabled photo {photo_id}")
        return True

    async def enable_photo(self, photo_id: int) -> bool:
        photo = await self.get_by_id(photo_id, include_deleted=True)
        if not photo:
            raise PhotoNotFoundError(f"Photo {photo_id} not found")
        if photo.deleted:
            raise ValueError(f"Photo {photo_id} is deleted")

        # Verify session is still active
        await self._verify_session_active(photo.session_id)

        photo.disabled = False
        photo.is_active = True
        photo.updated_at = func.now()

        try:
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            raise

        logger.info(f"Enabled photo {photo_id}")
        return True

    # ------------------------------------------------------------------
    # DELETE & RESTORE
    # ------------------------------------------------------------------

    async def delete_photo(
        self,
        photo_id: int,
        hard_delete: bool = False,
    ) -> bool:
        """
        Delete a photo.

        Soft delete (default):
          - Sets deleted=True in DB only.
          - The file on disk is NEVER touched.
          - get_absolute_path() will return None for this record, blocking access.

        Hard delete:
          - Checks for linked order items first (refuses if any exist).
          - Deletes the DB record.
          - Deletes the file from disk.
          - If the file delete fails, the DB row is still removed and the
            orphan is logged for manual cleanup.

        Raises:
            PhotoNotFoundError: photo not found
            ValueError: hard delete blocked by order items
            SQLAlchemyError: database errors (propagated)
        """
        photo = await self.get_by_id(photo_id, include_deleted=True)
        if not photo:
            raise PhotoNotFoundError(f"Photo {photo_id} not found")

        if hard_delete:
            # Guard: block hard delete if order items reference this photo
            order_item = await self.session.execute(
                select(OrderItems.id)
                .where(
                    and_(
                        OrderItems.photo_id == photo_id,
                        OrderItems.deleted == False,
                    )
                )
                .limit(1)
                .execution_options(timeout=self._default_timeout)
            )
            if order_item.scalar():
                raise ValueError(
                    f"Cannot hard delete photo {photo_id}: "
                    "active order items reference it. "
                    "Soft delete or handle order items first."
                )

            rel_path = photo.path
            await self.session.execute(
                delete(Photoes).where(Photoes.id == photo_id)
            )

            try:
                await self.session.commit()
            except SQLAlchemyError:
                await self.session.rollback()
                raise

            logger.info(f"Hard deleted photo DB record {photo_id}")

            # Remove file from disk after DB commit
            try:
                abs_path = self._abs(rel_path)
                await self._async_delete_file(abs_path)
                logger.info(f"Deleted photo file from disk: {rel_path}")
            except (ValueError, OSError) as file_err:
                logger.error(
                    f"DB row for photo {photo_id} removed but file "
                    f"'{rel_path}' could not be deleted: {file_err}. "
                    "Manual cleanup required."
                )
            return True

        else:
            # Soft delete — file is NEVER touched
            if photo.deleted:
                return False

            photo.deleted = True
            photo.is_active = False
            photo.updated_at = func.now()

            try:
                await self.session.commit()
            except SQLAlchemyError:
                await self.session.rollback()
                raise

            logger.info(
                f"Soft deleted photo {photo_id} "
                f"(file preserved at '{photo.path}')"
            )
            return True

    async def restore_photo(self, photo_id: int) -> bool:
        """
        Restore a soft-deleted photo record.
        The file was never deleted, so no disk operation is needed.
        Verifies the file still exists before restoring.
        """
        photo = await self.get_by_id(photo_id, include_deleted=True)
        if not photo:
            raise PhotoNotFoundError(f"Photo {photo_id} not found")
        if not photo.deleted:
            return False

        # Confirm the file is still on disk before restoring the record
        try:
            abs_path = self._abs(photo.path)
            if not await asyncio.to_thread(abs_path.exists):
                logger.error(
                    f"Cannot restore photo {photo_id}: "
                    f"file '{photo.path}' no longer exists on disk."
                )
                return False
        except ValueError as e:
            logger.error(f"Cannot restore photo {photo_id}: {e}")
            return False

        # Verify session is still active before restoring
        await self._verify_session_active(photo.session_id)

        photo.deleted = False
        photo.is_active = True
        photo.disabled = False
        photo.updated_at = func.now()

        try:
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            raise

        logger.info(f"Restored photo {photo_id}")
        return True

    async def bulk_delete_by_session(
        self,
        session_id: int,
        hard_delete: bool = False,
    ) -> int:
        """
        Delete all photos for a session.

        Soft delete: DB flags only, no files touched.
        Hard delete: checks for order items, removes DB rows, then deletes files.
        """
        if hard_delete:
            # Guard: any order items referencing photos in this session?
            order_item = await self.session.execute(
                select(OrderItems.id)
                .where(
                    OrderItems.photo_id.in_(
                        select(Photoes.id).where(
                            Photoes.session_id == session_id
                        )
                    )
                )
                .limit(1)
                .execution_options(timeout=self._default_timeout)
            )
            if order_item.scalar():
                raise ValueError(
                    f"Cannot hard delete photos for session {session_id}: "
                    "some photos have associated order items"
                )

            # Fetch paths before deleting records
            paths_result = await self.session.execute(
                select(Photoes.path).where(Photoes.session_id == session_id)
            )
            rel_paths = [row[0] for row in paths_result.all()]

            result = await self.session.execute(
                delete(Photoes).where(Photoes.session_id == session_id)
            )

            try:
                await self.session.commit()
            except SQLAlchemyError:
                await self.session.rollback()
                raise

            count = result.rowcount

            # Delete files after DB commit
            for rel_path in rel_paths:
                try:
                    abs_path = self._abs(rel_path)
                    await self._async_delete_file(abs_path)
                except (ValueError, OSError) as file_err:
                    logger.error(
                        f"Could not delete file '{rel_path}' "
                        f"during session bulk delete: {file_err}"
                    )

            logger.info(f"Hard deleted {count} photos for session {session_id}")
            return count
        else:
            result = await self.session.execute(
                update(Photoes)
                .where(Photoes.session_id == session_id)
                .values(deleted=True, is_active=False, updated_at=func.now())
                .execution_options(timeout=self._default_timeout)
            )

            try:
                await self.session.commit()
            except SQLAlchemyError:
                await self.session.rollback()
                raise

            count = result.rowcount
            logger.info(
                f"Soft deleted {count} photos for session {session_id} "
                "(files preserved on disk)"
            )
            return count

    # ------------------------------------------------------------------
    # STATUS CHECKS
    # ------------------------------------------------------------------

    async def is_photo_active(self, photo_id: int) -> bool:
        try:
            photo = await self.get_by_id(photo_id)
            return bool(
                photo
                and photo.is_active
                and not photo.disabled
                and not photo.deleted
            )
        except SQLAlchemyError:
            raise
        except Exception as e:
            logger.error(f"Error checking photo active status: {e}")
            return False

    async def has_thumbnail(self, photo_id: int) -> bool:
        try:
            photo = await self.get_by_id(photo_id)
            return bool(photo and photo.thumb_path)
        except SQLAlchemyError:
            raise
        except Exception as e:
            logger.error(f"Error checking thumbnail for photo {photo_id}: {e}")
            return False

    # ------------------------------------------------------------------
    # STATISTICS
    # ------------------------------------------------------------------

    async def get_photo_count(
        self,
        session_id: Optional[int] = None,
        event_id: Optional[int] = None,
        only_active: bool = True,
    ) -> int:
        conditions = (
            [Photoes.deleted == False, Photoes.disabled == False, Photoes.is_active == True]
            if only_active
            else [Photoes.deleted == False]
        )
        if session_id:
            conditions.append(Photoes.session_id == session_id)

        query = select(func.count()).select_from(Photoes).where(and_(*conditions))

        if event_id:
            query = query.where(
                Photoes.session_id.in_(
                    select(Sessions.id).where(Sessions.event_id == event_id)
                )
            )

        try:
            result = await self.session.execute(
                query.execution_options(timeout=self._default_timeout)
            )
            return result.scalar() or 0
        except SQLAlchemyError:
            raise
        except Exception as e:
            logger.error(f"Error getting photo count: {e}")
            return 0

    async def get_session_photo_stats(self, session_id: int) -> Dict[str, Any]:
        try:
            total  = await self.get_photo_count(session_id=session_id, only_active=False)
            active = await self.get_photo_count(session_id=session_id, only_active=True)

            fmt_result = await self.session.execute(
                select(Photoes.image_format, func.count().label("count"))
                .where(and_(Photoes.session_id == session_id, Photoes.deleted == False))
                .group_by(Photoes.image_format)
            )
            formats = {row[0]: row[1] for row in fmt_result.all()}

            return {
                "total_photos":   total,
                "active_photos":  active,
                "deleted_photos": total - active,
                "formats":        formats,
            }
        except SQLAlchemyError:
            raise
        except Exception as e:
            logger.error(f"Error getting photo stats for session {session_id}: {e}")
            return {"total_photos": 0, "active_photos": 0, "deleted_photos": 0, "formats": {}}

    async def get_photo_summary(self, photo_id: int) -> Optional[Dict[str, Any]]:
        """Detailed photo summary with session/event info. Blocked for deleted photos."""
        try:
            photo = await self.get_by_id(photo_id)
            if not photo:
                return None

            session_result = await self.session.execute(
                select(Sessions.session_code, Sessions.guest_name, Sessions.event_id)
                .where(Sessions.id == photo.session_id)
            )
            session_info = session_result.first()

            event_name = None
            if session_info:
                ev = await self.session.execute(
                    select(Events.name).where(Events.id == session_info.event_id)
                )
                event_name = ev.scalar()

            return {
                "photo_id":      photo.id,
                "filename":      photo.filename,
                "path":          photo.path,
                "thumb_path":    photo.thumb_path,
                "image_type":    photo.image_type,
                "image_format":  photo.image_format,
                "dimensions_px": {"width": photo.width_px,  "height": photo.height_px},
                "dimensions_mm": {"width": photo.width_mm,  "height": photo.height_mm},
                "dpi":           photo.dpi,
                "file_size_bytes": photo.file_size_bytes,
                "is_active":     photo.is_active and not photo.disabled and not photo.deleted,
                "created_at":    photo.created_at,
                "session_code":  session_info.session_code if session_info else None,
                "guest_name":    session_info.guest_name   if session_info else None,
                "event_name":    event_name,
            }
        except SQLAlchemyError:
            raise
        except Exception as e:
            logger.error(f"Error getting photo summary for {photo_id}: {e}")
            return None
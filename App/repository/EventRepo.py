# App/repository/EventRepo.py - PRODUCTION READY
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, and_, or_, func
from sqlalchemy.orm import aliased
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import Optional, List, Dict, Any, Tuple, Set
from datetime import datetime
import logging

from App.api.databases.MigrateTable import (
    Events, 
    eventShares, 
    User as UserModel,
    Sessions,
    Photoes,
    Orders
)

logger = logging.getLogger(__name__)


class EventRepo:
    """Repository for Event management operations - PRODUCTION READY"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    # ========== CREATE OPERATIONS ==========
    
    async def create_event(
        self, 
        event_data: Dict[str, Any],
        created_by: int,
        auto_assign_owner: bool = True
    ) -> Optional[Events]:
        """Create a new event and optionally assign owner."""
        try:
            # Validate required fields
            if not event_data.get("name"):
                raise ValueError("Event name is required")
            
            if not event_data.get("location"):
                raise ValueError("Event location is required")
            
            # Build event params without mutating the caller's dict
            event_params = {
                "is_active": True,
                "disabled": False,
                "deleted": False,
                "config": {},
                "created_by": created_by,
                "owned_by": created_by,
                **event_data,  # caller values take precedence over defaults
            }
            
            # Create event
            event = Events(**event_params)
            self.session.add(event)
            await self.session.flush()
            
            # Auto-assign owner access if requested
            if auto_assign_owner:
                # FIXED: Only use fields that exist in your eventShares model
                owner_share = eventShares(
                    event_id=event.id,
                    user_id=created_by,
                    permission="owner",
                    is_active=True,
                    granted_by=created_by
                    # REMOVED: can_edit - doesn't exist in your model
                )
                self.session.add(owner_share)
            
            await self.session.commit()
            await self.session.refresh(event)
            
            logger.info(f"Created event '{event.name}' (ID: {event.id}) by user {created_by}")
            return event
            
        except ValueError as e:
            await self.session.rollback()
            logger.error(f"Validation error: {e}")
            raise
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error creating event: {e}")
            return None
    
    # ========== READ OPERATIONS ==========
    
    async def get_event(
        self, 
        event_id: int,
        include_shares: bool = False,
        include_camera_configs: bool = False
    ) -> Optional[Events]:
        """Get event by ID with optional eager loading."""
        try:
            query = select(Events).where(
                and_(
                    Events.id == event_id,
                    Events.deleted == False
                )
            )
            # NOTE: include_shares / include_camera_configs eager loading requires
            # SQLAlchemy relationship() definitions on the Events model. Add them
            # to MigrateTable.py to activate (e.g. shares = relationship("eventShares")).
            result = await self.session.execute(query)
            return result.scalar_one_or_none()
            
        except Exception as e:
            logger.error(f"Error getting event {event_id}: {e}")
            return None
    
    async def get_event_by_name(
        self, 
        event_name: str,
        case_sensitive: bool = False
    ) -> Optional[Events]:
        """Get event by name."""
        try:
            if case_sensitive:
                condition = Events.name == event_name
            else:
                condition = func.lower(Events.name) == func.lower(event_name)
            
            result = await self.session.execute(
                select(Events).where(
                    and_(
                        condition,
                        Events.deleted == False
                    )
                )
            )
            return result.scalar_one_or_none()
            
        except Exception as e:
            logger.error(f"Error getting event by name '{event_name}': {e}")
            return None
    
    async def get_event_by_location(
        self, 
        event_location: str,
        exact_match: bool = True
    ) -> List[Events]:
        """Get events by location."""
        try:
            if exact_match:
                condition = Events.location == event_location
            else:
                condition = Events.location.ilike(f"%{event_location}%")
            
            result = await self.session.execute(
                select(Events).where(
                    and_(
                        condition,
                        Events.deleted == False
                    )
                )
            )
            return result.scalars().all()
            
        except Exception as e:
            logger.error(f"Error getting events by location '{event_location}': {e}")
            return []
    
    async def get_all_events(
        self,
        include_deleted: bool = False,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "created_at",
        order_desc: bool = True
    ) -> List[Events]:
        """Get all events with pagination."""
        try:
            query = select(Events)
            
            if not include_deleted:
                query = query.where(Events.deleted == False)
            
            order_column = getattr(Events, order_by, Events.created_at)
            if order_desc:
                query = query.order_by(order_column.desc())
            else:
                query = query.order_by(order_column.asc())
            
            query = query.limit(limit).offset(offset)
            
            result = await self.session.execute(query)
            return result.scalars().all()
            
        except Exception as e:
            logger.error(f"Error getting all events: {e}")
            return []
    
    async def get_user_events(
        self,
        user_id: int,
        permission: Optional[str] = None,
        include_deleted: bool = False,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get all events accessible by a user with their permission level."""
        try:
            query = (
                select(
                    Events,
                    eventShares.permission,
                    eventShares.is_active.label("share_active"),
                    eventShares.granted_by
                )
                .join(eventShares, Events.id == eventShares.event_id)
                .where(
                    and_(
                        eventShares.user_id == user_id,
                        eventShares.deleted == False
                    )
                )
            )
            
            if not include_deleted:
                query = query.where(Events.deleted == False)
            
            if permission:
                query = query.where(eventShares.permission == permission)
            
            query = query.order_by(Events.created_at.desc())
            query = query.limit(limit).offset(offset)
            
            result = await self.session.execute(query)
            rows = result.all()
            
            return [
                {
                    "event": row[0],
                    "permission": row[1],
                    "share_active": row[2],
                    "granted_by": row[3]
                }
                for row in rows
            ]
            
        except Exception as e:
            logger.error(f"Error getting user events for user {user_id}: {e}")
            return []
    
    async def get_active_events(
        self,
        include_disabled: bool = False,
        limit: int = 100
    ) -> List[Events]:
        """Get currently active events."""
        try:
            conditions = [
                Events.deleted == False,
                Events.is_active == True
            ]
            
            if not include_disabled:
                conditions.append(Events.disabled == False)
            
            result = await self.session.execute(
                select(Events)
                .where(and_(*conditions))
                .order_by(Events.created_at.desc())
                .limit(limit)
            )
            return result.scalars().all()
            
        except Exception as e:
            logger.error(f"Error getting active events: {e}")
            return []
    
    async def get_deleted_events(self) -> List[Events]:
        """Get all soft-deleted events."""
        try:
            result = await self.session.execute(
                select(Events).where(Events.deleted == True)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting deleted events: {e}")
            return []
    
    async def search_events(
        self,
        search_term: str,
        fields: Optional[List[str]] = None,
        limit: int = 50
    ) -> List[Events]:
        """Search events across specified fields."""
        try:
            if fields is None:
                fields = ['name', 'location']
            
            conditions = []
            search_pattern = f"%{search_term}%"
            
            for field in fields:
                if hasattr(Events, field):
                    column = getattr(Events, field)
                    conditions.append(column.ilike(search_pattern))
            
            if not conditions:
                return []
            
            result = await self.session.execute(
                select(Events)
                .where(
                    and_(
                        or_(*conditions),
                        Events.deleted == False
                    )
                )
                .limit(limit)
            )
            return result.scalars().all()
            
        except Exception as e:
            logger.error(f"Error searching events with '{search_term}': {e}")
            return []
    
    # ========== PERMISSION CHECK METHODS ==========
    
    async def user_has_event_access(self, user_id: int, event_id: int) -> bool:
        """Check if user has any active access to this event. LIMIT 1 avoids full scan."""
        try:
            result = await self.session.execute(
                select(eventShares.id).where(
                    and_(
                        eventShares.event_id == event_id,
                        eventShares.user_id == user_id,
                        eventShares.deleted == False,
                        eventShares.is_active == True
                    )
                ).limit(1)
            )
            return result.scalar_one_or_none() is not None
        except Exception as e:
            logger.error(f"Error checking event access: {e}")
            return False
    async def user_can_edit_event(
        self, 
        user_id: int, 
        event_id: int
    ) -> bool:
        """Check if user can EDIT an event (owner or manager role)."""
        try:
            result = await self.session.execute(
                select(eventShares).where(
                    and_(
                        eventShares.event_id == event_id,
                        eventShares.user_id == user_id,
                        eventShares.permission.in_(["owner", "manager"]),
                        eventShares.deleted == False,
                        eventShares.is_active == True
                    )
                )
            )
            return result.first() is not None
            
        except Exception as e:
            logger.error(f"Error checking edit permission: {e}")
            return False
    
    async def user_can_manage_camera(
        self, 
        user_id: int, 
        event_id: int
    ) -> bool:
        """Check if user can MANAGE CAMERA (owner, manager, or photographer)."""
        try:
            result = await self.session.execute(
                select(eventShares).where(
                    and_(
                        eventShares.event_id == event_id,
                        eventShares.user_id == user_id,
                        eventShares.permission.in_(["owner", "manager", "photographer"]),
                        eventShares.deleted == False,
                        eventShares.is_active == True
                    )
                )
            )
            return result.first() is not None
            
        except Exception as e:
            logger.error(f"Error checking camera permission: {e}")
            return False
    
    async def user_is_event_owner(
        self, 
        user_id: int, 
        event_id: int
    ) -> bool:
        """Check if user is the OWNER of an event."""
        try:
            result = await self.session.execute(
                select(eventShares).where(
                    and_(
                        eventShares.event_id == event_id,
                        eventShares.user_id == user_id,
                        eventShares.permission == "owner",
                        eventShares.deleted == False
                    )
                )
            )
            return result.first() is not None
            
        except Exception as e:
            logger.error(f"Error checking owner status: {e}")
            return False
    
    async def get_user_event_permission(
        self, 
        user_id: int, 
        event_id: int
    ) -> Optional[str]:
        """Get user's permission level for an event."""
        try:
            result = await self.session.execute(
                select(eventShares.permission).where(
                    and_(
                        eventShares.event_id == event_id,
                        eventShares.user_id == user_id,
                        eventShares.deleted == False,
                        eventShares.is_active == True
                    )
                )
            )
            return result.scalar_one_or_none()
            
        except Exception as e:
            logger.error(f"Error getting user permission: {e}")
            return None
    
    # ========== UPDATE OPERATIONS ==========
    
    async def update_event(
        self, 
        event_id: int, 
        update_data: Dict[str, Any],
        updated_by: Optional[int] = None
    ) -> Optional[Events]:
        """Update event information."""
        try:
            event = await self.get_event(event_id)
            if not event:
                logger.warning(f"Event {event_id} not found for update")
                return None
            
            # Prevent updating certain fields
            forbidden_fields = ['id', 'created_at', 'deleted']
            for field in forbidden_fields:
                update_data.pop(field, None)
            
            # Update allowed fields
            for field, value in update_data.items():
                if hasattr(event, field):
                    setattr(event, field, value)
            
            event.updated_at = func.now()
            
            await self.session.commit()
            await self.session.refresh(event)
            
            logger.info(f"Updated event {event_id}")
            return event
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating event {event_id}: {e}")
            return None
    
    async def toggle_event_active(self, event_id: int) -> Optional[bool]:
        """Toggle event active status. Returns new status or None if error."""
        try:
            event = await self.get_event(event_id)
            if not event:
                return None
            
            event.is_active = not event.is_active
            event.updated_at = func.now()
            
            await self.session.commit()
            
            new_status = event.is_active
            logger.info(f"Event {event_id} active status toggled to {new_status}")
            return new_status
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error toggling event active status: {e}")
            return None
    
    async def disable_event(self, event_id: int) -> bool:
        """Disable an event (soft disable without deletion)."""
        try:
            event = await self.get_event(event_id)
            if not event:
                return False
            
            event.disabled = True
            event.is_active = False
            event.updated_at = func.now()
            
            await self.session.commit()
            
            logger.info(f"Disabled event {event_id}")
            return True
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error disabling event {event_id}: {e}")
            return False
    
    async def enable_event(self, event_id: int) -> bool:
        """Enable a disabled event."""
        try:
            event = await self.get_event(event_id)
            if not event:
                return False
            
            event.disabled = False
            event.is_active = True
            event.updated_at = func.now()
            
            await self.session.commit()
            
            logger.info(f"Enabled event {event_id}")
            return True
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error enabling event {event_id}: {e}")
            return False
    
    async def update_user_permission(
        self,
        event_id: int,
        user_id: int,
        new_permission: str,
        updated_by: int
    ) -> bool:
        """Update a user's permission for an event."""
        try:
            valid_permissions = ["owner", "manager", "photographer", "viewer"]
            if new_permission not in valid_permissions:
                raise ValueError(f"Invalid permission. Must be one of: {valid_permissions}")
            
            # Check if trying to set owner when one already exists
            if new_permission == "owner":
                existing_owner = await self.session.execute(
                    select(eventShares).where(
                        and_(
                            eventShares.event_id == event_id,
                            eventShares.permission == "owner",
                            eventShares.user_id != user_id,
                            eventShares.deleted == False
                        )
                    )
                )
                if existing_owner.first():
                    raise ValueError("Event already has an owner. Transfer ownership instead.")
            
            # Find existing share record
            result = await self.session.execute(
                select(eventShares).where(
                    and_(
                        eventShares.event_id == event_id,
                        eventShares.user_id == user_id,
                        eventShares.deleted == False
                    )
                )
            )
            share = result.scalar_one_or_none()
            
            if share:
                share.permission = new_permission
                share.granted_by = updated_by
                share.updated_at = func.now()
            else:
                # FIXED: Only use fields that exist
                share = eventShares(
                    event_id=event_id,
                    user_id=user_id,
                    permission=new_permission,
                    granted_by=updated_by,
                    is_active=True
                )
                self.session.add(share)
            
            await self.session.commit()
            logger.info(f"Updated permission for user {user_id} on event {event_id} to {new_permission}")
            return True
            
        except ValueError as e:
            await self.session.rollback()
            logger.error(f"Validation error: {e}")
            raise
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating user permission: {e}")
            return False
    
    async def transfer_event_ownership(
        self,
        event_id: int,
        current_owner_id: int,
        new_owner_id: int,
        transferred_by: int
    ) -> bool:
        """Transfer event ownership with FOR UPDATE lock."""
        try:
            # Lock the event row to prevent race conditions
            event = await self.session.execute(
                select(Events).where(Events.id == event_id).with_for_update()
            )
            event_row = event.scalar_one_or_none()
            
            if not event_row:
                raise ValueError(f"Event {event_id} not found")
            
            # Single atomic operation - demote old owner, promote new owner
            await self.session.execute(
                update(eventShares)
                .where(
                    and_(
                        eventShares.event_id == event_id,
                        eventShares.user_id == current_owner_id,
                        eventShares.permission == "owner",
                        eventShares.deleted == False
                    )
                )
                .values(
                    permission="manager",
                    updated_at=func.now()
                )
            )
            
            # Upsert new owner
            existing = await self.session.execute(
                select(eventShares).where(
                    and_(
                        eventShares.event_id == event_id,
                        eventShares.user_id == new_owner_id,
                        eventShares.deleted == False
                    )
                ).with_for_update()
            )
            new_owner_share = existing.scalar_one_or_none()
            
            if new_owner_share:
                new_owner_share.permission = "owner"
                new_owner_share.granted_by = transferred_by
            else:
                self.session.add(eventShares(
                    event_id=event_id,
                    user_id=new_owner_id,
                    permission="owner",
                    granted_by=transferred_by,
                    is_active=True
                ))
            
            # Update event owned_by
            event_row.owned_by = new_owner_id
            event_row.updated_at = func.now()
            
            await self.session.commit()
            logger.info(f"Transferred event {event_id} ownership")
            return True
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error transferring event ownership: {e}")
            return False
        
    async def remove_user_from_event(
        self,
        event_id: int,
        user_id: int,
        removed_by: int
    ) -> bool:
        """Remove a user's access to an event (soft delete their share)."""
        try:
            # Prevent removing the last owner
            if await self.user_is_event_owner(user_id, event_id):
                result = await self.session.execute(
                    select(eventShares).where(
                        and_(
                            eventShares.event_id == event_id,
                            eventShares.permission == "owner",
                            eventShares.user_id != user_id,
                            eventShares.deleted == False
                        )
                    )
                )
                if not result.first():
                    raise ValueError("Cannot remove the only owner of an event. Transfer ownership first.")
            
            result = await self.session.execute(
                update(eventShares)
                .where(
                    and_(
                        eventShares.event_id == event_id,
                        eventShares.user_id == user_id,
                        eventShares.deleted == False
                    )
                )
                .values(
                    deleted=True,
                    updated_at=func.now()
                )
            )
            
            await self.session.commit()
            
            if result.rowcount > 0:
                logger.info(f"Removed user {user_id} from event {event_id}")
                return True
            return False
            
        except ValueError as e:
            await self.session.rollback()
            logger.error(f"Validation error: {e}")
            raise
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error removing user from event: {e}")
            return False
    
    # ========== DELETE & RESTORE OPERATIONS ==========
    
    async def delete_event(self, event_id: int, hard_delete: bool = False) -> bool:
        """Delete an event with complete cascade."""
        try:
            if hard_delete:
                # Delete in correct order (child to parent)
                await self.session.execute(delete(Photoes).where(
                    Photoes.session_id.in_(select(Sessions.id).where(Sessions.event_id == event_id))
                ))
                await self.session.execute(delete(Orders).where(Orders.event_id == event_id))
                await self.session.execute(delete(Sessions).where(Sessions.event_id == event_id))
                await self.session.execute(delete(eventShares).where(eventShares.event_id == event_id))
                await self.session.execute(delete(Events).where(Events.id == event_id))
                await self.session.commit()
                return True
            else:
                # Soft delete all related records
                event = await self.get_event(event_id)
                if not event:
                    return False
                
                event.deleted = True
                event.is_active = False
                
                # Cascade soft delete to ALL child tables
                await self.session.execute(
                    update(Sessions).where(Sessions.event_id == event_id)
                    .values(deleted=True, updated_at=func.now())
                )
                await self.session.execute(
                    update(Photoes).where(
                        Photoes.session_id.in_(select(Sessions.id).where(Sessions.event_id == event_id))
                    ).values(deleted=True, updated_at=func.now())
                )
                await self.session.execute(
                    update(Orders).where(Orders.event_id == event_id)
                    .values(deleted=True, updated_at=func.now())
                )
                await self.session.execute(
                    update(eventShares).where(eventShares.event_id == event_id)
                    .values(deleted=True, updated_at=func.now())
                )
                
                await self.session.commit()
                return True
                
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error deleting event: {e}")
            return False
        
    async def restore_event(self, event_id: int) -> bool:
        """Restore a soft-deleted event and ALL child records."""
        try:
            result = await self.session.execute(
                select(Events).where(Events.id == event_id)
            )
            event = result.scalar_one_or_none()
            
            if not event or not event.deleted:
                return False
            
            # Only block restore on EXACT NAME collision with a different active event.
            # Location is not unique (multiple events can share a venue).
            conflict_count = await self.session.scalar(
                select(func.count()).select_from(Events).where(
                    and_(
                        Events.deleted == False,
                        Events.id != event_id,
                        Events.name == event.name
                    )
                )
            )
            if conflict_count:
                logger.warning(
                    f"Cannot restore event {event_id}: name '{event.name}' already in use"
                )
                return False
            
            # Restore event
            event.deleted = False
            event.is_active = True
            event.disabled = False
            event.updated_at = func.now()
            
            # Restore ALL child records
            await self.session.execute(
                update(Sessions)
                .where(Sessions.event_id == event_id)
                .values(deleted=False, updated_at=func.now())
            )
            
            # Restore photos (must happen after sessions)
            await self.session.execute(
                update(Photoes)
                .where(
                    Photoes.session_id.in_(
                        select(Sessions.id).where(Sessions.event_id == event_id)
                    )
                )
                .values(deleted=False, updated_at=func.now())
            )
            
            # Restore orders
            await self.session.execute(
                update(Orders)
                .where(Orders.event_id == event_id)
                .values(deleted=False, updated_at=func.now())
            )
            
            # Restore shares
            await self.session.execute(
                update(eventShares)
                .where(eventShares.event_id == event_id)
                .values(deleted=False, updated_at=func.now())
            )
            
            await self.session.commit()
            logger.info(f"Restored event {event_id} and ALL child records")
            return True
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error restoring event {event_id}: {e}")
            return False
    
    # ========== STATUS CHECK METHODS ==========
    
    async def is_event_active(self, event_id: int) -> bool:
        """Check if event is active."""
        try:
            event = await self.get_event(event_id)
            if not event:
                return False
            return event.is_active and not event.disabled and not event.deleted
        except Exception as e:
            logger.error(f"Error checking active status: {e}")
            return False
    
    async def is_event_disabled(self, event_id: int) -> bool:
        """Check if event is explicitly disabled."""
        try:
            event = await self.get_event(event_id)
            return bool(event and event.disabled)
        except Exception as e:
            logger.error(f"Error checking disabled status: {e}")
            return False
    
    # ========== USER MANAGEMENT METHODS ==========
    
    async def get_event_users(
        self,
        event_id: int,
        permission_filter: Optional[str] = None,
        include_inactive: bool = False,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get all users with access to an event and their permission levels."""
        try:
            query = (
                select(
                    UserModel.id,
                    UserModel.name,
                    UserModel.email,
                    UserModel.user_role,
                    eventShares.permission,
                    eventShares.granted_by,
                    eventShares.access_date,
                    eventShares.is_active.label("share_active")
                )
                .join(eventShares, UserModel.id == eventShares.user_id)
                .where(
                    and_(
                        eventShares.event_id == event_id,
                        eventShares.deleted == False
                    )
                )
            )
            
            if not include_inactive:
                query = query.where(
                    and_(
                        eventShares.is_active == True,
                        UserModel.disabled == False,
                        UserModel.deleted == False
                    )
                )
            
            if permission_filter:
                query = query.where(eventShares.permission == permission_filter)
            
            query = query.limit(limit).offset(offset)
            
            result = await self.session.execute(query)
            rows = result.all()
            
            return [
                {
                    "user_id": row[0],
                    "name": row[1],
                    "email": row[2],
                    "user_role": row[3],
                    "permission": row[4],
                    "granted_by": row[5],
                    "access_date": row[6],
                    "share_active": row[7]
                }
                for row in rows
            ]
            
        except Exception as e:
            logger.error(f"Error getting event users: {e}")
            return []
    
    # ========== BULK OPERATIONS ==========
    
    async def bulk_update_status(
        self, 
        event_ids: List[int], 
        is_active: bool
    ) -> int:
        """Bulk update active status for multiple events."""
        try:
            result = await self.session.execute(
                update(Events)
                .where(Events.id.in_(event_ids))
                .values(
                    is_active=is_active,
                    updated_at=func.now()
                )
            )
            await self.session.commit()
            
            count = result.rowcount
            logger.info(f"Bulk updated {count} events to active={is_active}")
            return count
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error bulk updating events: {e}")
            return 0
    
    # ========== STATISTICS ==========
    
    async def get_event_count(
        self,
        include_deleted: bool = False,
        only_active: bool = False
    ) -> int:
        """Get total event count with filters."""
        try:
            conditions = []
            
            if not include_deleted:
                conditions.append(Events.deleted == False)
            
            if only_active:
                conditions.extend([
                    Events.is_active == True,
                    Events.disabled == False
                ])
            
            result = await self.session.execute(
                select(func.count()).select_from(Events).where(and_(*conditions))
            )
            return result.scalar() or 0
            
        except Exception as e:
            logger.error(f"Error getting event count: {e}")
            return 0
    
    async def get_event_summary(self, event_id: int) -> Optional[Dict[str, Any]]:
        """Get event summary with sequential queries.
        
        IMPORTANT: asyncio.gather() is UNSAFE with a shared AsyncSession — the session
        is not concurrency-safe. Queries must be sequential within a single session scope.
        """
        try:
            event = await self.get_event(event_id)
            if not event:
                return None

            session_count = await self.session.scalar(
                select(func.count()).select_from(Sessions).where(
                    and_(Sessions.event_id == event_id, Sessions.deleted == False)
                )
            )

            photo_count = await self.session.scalar(
                select(func.count()).select_from(Photoes).where(
                    and_(
                        Photoes.session_id.in_(
                            select(Sessions.id).where(Sessions.event_id == event_id)
                        ),
                        Photoes.deleted == False
                    )
                )
            )

            order_stats_result = await self.session.execute(
                select(
                    func.count().label("order_count"),
                    func.coalesce(func.sum(Orders.total_amount), 0).label("total_revenue")
                )
                .select_from(Orders)
                .where(
                    and_(
                        Orders.event_id == event_id,
                        Orders.deleted == False,
                        Orders.payment_status == "paid"
                    )
                )
            )
            order_row = order_stats_result.first()

            return {
                "event_id": event.id,
                "event_name": event.name,
                "location": event.location,
                "is_active": event.is_active and not event.disabled and not event.deleted,
                "sessions_count": session_count or 0,
                "photos_count": photo_count or 0,
                "orders_count": order_row.order_count if order_row else 0,
                "total_revenue": float(order_row.total_revenue or 0) if order_row else 0.0,
                "created_at": event.created_at,
                "updated_at": event.updated_at,
            }

        except Exception as e:
            logger.error(f"Error getting event summary: {e}")
            return None
            
    # ========== EVENT SHARE MANAGEMENT METHODS ==========

    async def get_event_share(
        self,
        share_id: int,
        include_deleted: bool = False
    ) -> Optional[eventShares]:
        """Get a specific event share by ID."""
        try:
            query = select(eventShares).where(eventShares.id == share_id)
            
            if not include_deleted:
                query = query.where(eventShares.deleted == False)
            
            result = await self.session.execute(query)
            return result.scalar_one_or_none()
            
        except Exception as e:
            logger.error(f"Error getting event share {share_id}: {e}")
            return None


    async def get_event_shares(
        self,
        event_id: Optional[int] = None,
        user_id: Optional[int] = None,
        permission: Optional[str] = None,
        include_deleted: bool = False,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get event shares with filters.
        
        Args:
            event_id: Filter by event ID
            user_id: Filter by user ID
            permission: Filter by permission level
            include_deleted: Include soft-deleted shares
            limit: Max results
            offset: Pagination offset
        """
        try:
            # Create aliases for UserModel to avoid conflicts
            Grantor = aliased(UserModel)
            
            query = (
                select(
                    eventShares.id,
                    eventShares.event_id,
                    eventShares.user_id,
                    eventShares.permission,
                    eventShares.granted_by,
                    eventShares.access_date,
                    eventShares.is_active,
                    eventShares.deleted,
                    eventShares.created_at,
                    eventShares.updated_at,
                    Events.name.label("event_name"),
                    UserModel.name.label("user_name"),
                    UserModel.email.label("user_email"),
                    Grantor.name.label("grantor_name")
                )
                .outerjoin(Events, eventShares.event_id == Events.id)
                .outerjoin(UserModel, eventShares.user_id == UserModel.id)
                .outerjoin(Grantor, eventShares.granted_by == Grantor.id)
            )
            
            # Apply filters
            if event_id:
                query = query.where(eventShares.event_id == event_id)
            
            if user_id:
                query = query.where(eventShares.user_id == user_id)
            
            if permission:
                query = query.where(eventShares.permission == permission)
            
            if not include_deleted:
                query = query.where(eventShares.deleted == False)
            
            query = query.order_by(eventShares.created_at.desc())
            query = query.limit(limit).offset(offset)
            
            result = await self.session.execute(query)
            rows = result.all()
            
            return [
                {
                    "id": row[0],
                    "event_id": row[1],
                    "user_id": row[2],
                    "permission": row[3],
                    "granted_by": row[4],
                    "access_date": row[5],
                    "is_active": row[6],
                    "deleted": row[7],
                    "created_at": row[8],
                    "updated_at": row[9],
                    "event_name": row[10],
                    "user_name": row[11],
                    "user_email": row[12],
                    "grantor_name": row[13]
                }
                for row in rows
            ]
            
        except Exception as e:
            logger.error(f"Error getting event shares: {e}")
            return []


    async def update_event_share(
        self,
        share_id: int,
        update_data: Dict[str, Any],
        updated_by: int
    ) -> Optional[eventShares]:
        """
        Update an event share (permission, is_active).
        
        Args:
            share_id: Share record ID
            update_data: Dict with fields to update (permission, is_active)
            updated_by: User ID making the change
        """
        try:
            share = await self.get_event_share(share_id, include_deleted=False)
            if not share:
                logger.warning(f"Event share {share_id} not found")
                return None
            
            # Validate permission if being updated
            if "permission" in update_data:
                valid_permissions = ["owner", "manager", "photographer", "viewer"]
                if update_data["permission"] not in valid_permissions:
                    raise ValueError(f"Invalid permission. Must be one of: {valid_permissions}")
                
                # Prevent demoting the last owner
                if share.permission == "owner" and update_data["permission"] != "owner":
                    # Check if this is the only owner
                    result = await self.session.execute(
                        select(eventShares).where(
                            and_(
                                eventShares.event_id == share.event_id,
                                eventShares.permission == "owner",
                                eventShares.deleted == False,
                                eventShares.id != share_id
                            )
                        )
                    )
                    if not result.first():
                        raise ValueError("Cannot demote the only owner of an event. Transfer ownership first.")
            
            # Update fields
            for field, value in update_data.items():
                if hasattr(share, field) and field not in ['id', 'created_at', 'granted_by']:
                    setattr(share, field, value)
            
            share.updated_at = func.now()
            
            await self.session.commit()
            await self.session.refresh(share)
            
            logger.info(f"Updated event share {share_id} by user {updated_by}")
            return share
            
        except ValueError as e:
            await self.session.rollback()
            logger.error(f"Validation error: {e}")
            raise
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating event share {share_id}: {e}")
            return None


    async def delete_event_share(
        self,
        share_id: int,
        deleted_by: int,
        hard_delete: bool = False
    ) -> bool:
        """
        Delete an event share (soft or hard).
        
        Args:
            share_id: Share record ID
            deleted_by: User ID performing deletion
            hard_delete: If True, permanently delete; if False, soft delete
        """
        try:
            share = await self.get_event_share(share_id, include_deleted=True)
            if not share:
                logger.warning(f"Event share {share_id} not found")
                return False
            
            # Prevent deleting the last owner
            if share.permission == "owner":
                result = await self.session.execute(
                    select(eventShares).where(
                        and_(
                            eventShares.event_id == share.event_id,
                            eventShares.permission == "owner",
                            eventShares.deleted == False,
                            eventShares.id != share_id
                        )
                    )
                )
                if not result.first():
                    raise ValueError("Cannot delete the only owner of an event. Transfer ownership first.")
            
            if hard_delete:
                # Permanent deletion
                await self.session.execute(
                    delete(eventShares).where(eventShares.id == share_id)
                )
                await self.session.commit()
                logger.info(f"Hard deleted event share {share_id} by user {deleted_by}")
                return True
            else:
                # Soft delete
                share.deleted = True
                share.is_active = False
                share.updated_at = func.now()
                await self.session.commit()
                logger.info(f"Soft deleted event share {share_id} by user {deleted_by}")
                return True
                
        except ValueError as e:
            await self.session.rollback()
            logger.error(f"Validation error: {e}")
            raise
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error deleting event share {share_id}: {e}")
            return False


    async def restore_event_share(
        self,
        share_id: int,
        restored_by: int
    ) -> bool:
        """
        Restore a soft-deleted event share.
        
        Args:
            share_id: Share record ID
            restored_by: User ID performing restoration
        """
        try:
            share = await self.get_event_share(share_id, include_deleted=True)
            if not share or not share.deleted:
                logger.warning(f"Event share {share_id} not found or not deleted")
                return False
            
            # Check if restoring owner would create duplicate owner
            if share.permission == "owner":
                result = await self.session.execute(
                    select(eventShares).where(
                        and_(
                            eventShares.event_id == share.event_id,
                            eventShares.permission == "owner",
                            eventShares.deleted == False
                        )
                    )
                )
                if result.first():
                    raise ValueError("Cannot restore owner share - event already has an owner")
            
            share.deleted = False
            share.is_active = True
            share.updated_at = func.now()
            
            await self.session.commit()
            
            logger.info(f"Restored event share {share_id} by user {restored_by}")
            return True
            
        except ValueError as e:
            await self.session.rollback()
            logger.error(f"Validation error: {e}")
            raise
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error restoring event share {share_id}: {e}")
            return False


    async def get_deleted_event_shares(
        self,
        event_id: Optional[int] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get all soft-deleted event shares.
        
        Args:
            event_id: Optional filter by event ID
            limit: Max results
            offset: Pagination offset
        """
        try:
            query = (
                select(
                    eventShares.id,
                    eventShares.event_id,
                    eventShares.user_id,
                    eventShares.permission,
                    eventShares.granted_by,
                    eventShares.access_date,
                    eventShares.deleted,
                    eventShares.updated_at.label("deleted_at"),
                    Events.name.label("event_name"),
                    UserModel.name.label("user_name"),
                    UserModel.email.label("user_email")
                )
                .outerjoin(Events, eventShares.event_id == Events.id)
                .outerjoin(UserModel, eventShares.user_id == UserModel.id)
                .where(eventShares.deleted == True)
            )
            
            if event_id:
                query = query.where(eventShares.event_id == event_id)
            
            query = query.order_by(eventShares.updated_at.desc())
            query = query.limit(limit).offset(offset)
            
            result = await self.session.execute(query)
            rows = result.all()
            
            return [
                {
                    "id": row[0],
                    "event_id": row[1],
                    "user_id": row[2],
                    "permission": row[3],
                    "granted_by": row[4],
                    "access_date": row[5],
                    "deleted": row[6],
                    "deleted_at": row[7],
                    "event_name": row[8],
                    "user_name": row[9],
                    "user_email": row[10]
                }
                for row in rows
            ]
            
        except Exception as e:
            logger.error(f"Error getting deleted event shares: {e}")
            return []


    async def bulk_grant_event_access(
        self,
        event_id: int,
        user_ids: List[int],
        permission: str,
        granted_by: int,
        chunk_size: int = 500
    ) -> Dict[str, Any]:
        """Bulk-grant event access via PostgreSQL INSERT ... ON CONFLICT DO UPDATE (upsert).

        Replaces previous N+1 SELECT-per-user loop with a single statement per chunk.
        Requires the uq_event_shares_event_user unique constraint on (event_id, user_id).
        Owner bulk-grant is blocked — use transfer_event_ownership() instead.
        """
        results: Dict[str, Any] = {"success_count": 0, "failed_users": [], "errors": []}

        valid_permissions = ["manager", "photographer", "viewer"]
        if permission not in valid_permissions:
            raise ValueError(
                f"bulk_grant only supports {valid_permissions}. "
                "Use transfer_event_ownership() to assign 'owner'."
            )

        if not user_ids:
            return results

        for i in range(0, len(user_ids), chunk_size):
            chunk = user_ids[i:i + chunk_size]
            chunk_num = i // chunk_size + 1
            try:
                stmt = pg_insert(eventShares).values([
                    {
                        "event_id": event_id,
                        "user_id": uid,
                        "permission": permission,
                        "granted_by": granted_by,
                        "is_active": True,
                        "deleted": False,
                    }
                    for uid in chunk
                ]).on_conflict_do_update(
                    constraint="uq_event_shares_event_user",
                    set_={
                        "permission": permission,
                        "granted_by": granted_by,
                        "is_active": True,
                        "deleted": False,
                        "updated_at": func.now(),
                    }
                )
                await self.session.execute(stmt)
                await self.session.commit()
                results["success_count"] += len(chunk)
                logger.info(f"bulk_grant chunk {chunk_num}: {len(chunk)} users upserted")
            except Exception as e:
                await self.session.rollback()
                err = f"Chunk {chunk_num} failed: {e}"
                results["errors"].append(err)
                for uid in chunk:
                    results["failed_users"].append({"user_id": uid, "error": str(e)})
                logger.error(err)

        return results

    async def bulk_revoke_event_access(
        self,
        event_id: int,
        user_ids: List[int],
        revoked_by: int,
        chunk_size: int = 500
    ) -> Dict[str, Any]:
        """Bulk-revoke event access using a single batch UPDATE per chunk.

        Replaces previous N individual UPDATE statements with one UPDATE ... WHERE IN (...).
        The sole owner of an event is protected and reported in failed_users.
        """
        results: Dict[str, Any] = {"success_count": 0, "failed_users": [], "errors": []}

        if not user_ids:
            return results

        # Fetch all current owner IDs once — avoids repeated queries in the loop
        owners_result = await self.session.execute(
            select(eventShares.user_id).where(
                and_(
                    eventShares.event_id == event_id,
                    eventShares.permission == "owner",
                    eventShares.deleted == False
                )
            )
        )
        owner_ids: Set[int] = {row[0] for row in owners_result.all()}
        # Only protect if there is exactly one owner (multiple owners can all be revoked)
        sole_owner_id: Optional[int] = next(iter(owner_ids)) if len(owner_ids) == 1 else None

        for i in range(0, len(user_ids), chunk_size):
            chunk = user_ids[i:i + chunk_size]
            chunk_num = i // chunk_size + 1

            protected = [uid for uid in chunk if uid == sole_owner_id]
            safe      = [uid for uid in chunk if uid != sole_owner_id]

            for uid in protected:
                results["failed_users"].append(
                    {"user_id": uid, "error": "Cannot revoke the only owner of this event"}
                )

            if not safe:
                continue

            try:
                result = await self.session.execute(
                    update(eventShares)
                    .where(
                        and_(
                            eventShares.event_id == event_id,
                            eventShares.user_id.in_(safe),
                            eventShares.deleted == False
                        )
                    )
                    .values(deleted=True, is_active=False, updated_at=func.now())
                )
                await self.session.commit()
                results["success_count"] += result.rowcount

                not_found = len(safe) - result.rowcount
                if not_found > 0:
                    logger.warning(
                        f"bulk_revoke chunk {chunk_num}: {not_found} users had no active share"
                    )
                logger.info(f"bulk_revoke chunk {chunk_num}: {result.rowcount} shares revoked")

            except Exception as e:
                await self.session.rollback()
                err = f"Chunk {chunk_num} failed: {e}"
                results["errors"].append(err)
                for uid in safe:
                    results["failed_users"].append({"user_id": uid, "error": str(e)})
                logger.error(err)

        return results
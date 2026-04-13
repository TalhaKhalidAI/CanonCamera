# App/repository/CurrencyRepo.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_, or_
from typing import Optional, List, Dict, Any
import logging
from App.api.databases.MigrateTable import Currencies

logger = logging.getLogger(__name__)

class CurrencyRepository:
    """Repository for currency database operations with availability checks."""
    
    def __init__(self, session: AsyncSession):
        self.session = session

    # ========== HELPER METHODS ==========

    async def _get_raw(self, currency_id: int) -> Optional[Currencies]:
        """Internal helper to get currency by ID without deleted check."""
        result = await self.session.execute(
            select(Currencies).where(Currencies.id == currency_id)
        )
        return result.scalar_one_or_none()

    async def is_available(self, currency_id: int) -> bool:
        """Check if currency exists, is active, and not disabled/deleted."""
        currency = await self.get_by_id(currency_id)
        return bool(currency and currency.is_active and not currency.disabled and not currency.deleted)

    # ========== READ OPERATIONS ==========

    async def get_by_id(self, currency_id: int, include_deleted: bool = False) -> Optional[Currencies]:
        """Get currency by ID."""
        try:
            query = select(Currencies).where(Currencies.id == currency_id)
            if not include_deleted:
                query = query.where(Currencies.deleted == False)
            
            result = await self.session.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting currency by ID {currency_id}: {e}")
            return None

    async def get_by_code(self, code: str, include_deleted: bool = False) -> Optional[Currencies]:
        """Get currency by its ISO code (e.g., USD, EUR)."""
        try:
            query = select(Currencies).where(Currencies.code == code.upper())
            if not include_deleted:
                query = query.where(Currencies.deleted == False)
            
            result = await self.session.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting currency by code {code}: {e}")
            return None

    async def list_all(self, include_deleted: bool = False) -> List[Currencies]:
        """List all currencies in the database."""
        try:
            query = select(Currencies)
            if not include_deleted:
                query = query.where(Currencies.deleted == False)
            
            result = await self.session.execute(query.order_by(Currencies.code))
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error listing currencies: {e}")
            return []

    async def get_all(self) -> List[Currencies]:
        """Fetch all non-deleted currencies."""
        return await self.list_all(include_deleted=False)

    async def get_available(self) -> List[Currencies]:
        """Get all currencies that are active, not disabled, and not deleted."""
        try:
            result = await self.session.execute(
                select(Currencies).where(
                    and_(
                        Currencies.is_active == True,
                        Currencies.disabled == False,
                        Currencies.deleted == False
                    )
                ).order_by(Currencies.is_default.desc(), Currencies.code)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error fetching available currencies: {e}")
            return []

    # ========== WRITE OPERATIONS ==========

    async def add(self, data: Dict[str, Any]) -> Optional[Currencies]:
        """Add a new currency."""
        try:
            # Check all required fields
            required_fields = ["code", "name", "symbol"]
            for field in required_fields:
                if not data.get(field):
                    raise ValueError(f"{field} is required")

            code = data.get("code", "").upper()

            # Check if exists (even if deleted, we might want to restore or avoid duplicates)
            existing = await self.get_by_code(code, include_deleted=True)
            if existing:
                if existing.deleted:
                    # If it was deleted, restore it instead of creating new
                    for key, value in data.items():
                        setattr(existing, key, value)
                    existing.deleted = False
                    existing.is_active = True
                    existing.disabled = False
                    await self.session.commit()
                    await self.session.refresh(existing)
                    return existing
                raise ValueError(f"Currency with code {code} already exists")

            # If this is set as default, unset previous default
            if data.get("is_default"):
                await self.session.execute(
                    update(Currencies).values(is_default=False)
                )

            new_currency = Currencies(**data)
            # Ensure code is upper case if not already
            new_currency.code = code
            
            self.session.add(new_currency)
            await self.session.commit()
            await self.session.refresh(new_currency)
            
            logger.info(f"Added new currency: {code}")
            return new_currency
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error adding currency: {e}")
            raise e

    async def update(self, currency_id: int, data: Dict[str, Any]) -> Optional[Currencies]:
        """Update an existing currency. Checks if not deleted/disabled first."""
        try:
            # Include deleted to provide a better error message if it is deleted
            currency = await self.get_by_id(currency_id, include_deleted=True)
            
            if not currency:
                raise ValueError(f"Currency with ID {currency_id} not found")
            
            if currency.deleted:
                raise ValueError(f"Currency {currency.code} is deleted and cannot be updated")
            
            if currency.disabled:
                raise ValueError(f"Currency {currency.code} is disabled and cannot be updated")

            # Handle default flag logic
            if data.get("is_default") and not currency.is_default:
                await self.session.execute(
                    update(Currencies).where(Currencies.id != currency_id).values(is_default=False)
                )

            for key, value in data.items():
                if hasattr(currency, key):
                    setattr(currency, key, value)
            
            code = currency.code
            await self.session.commit()
            await self.session.refresh(currency)
            
            logger.info(f"Updated currency: {code}")
            return currency
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating currency {currency_id}: {e}")
            raise e

    async def delete(self, currency_id: int) -> bool:
        """Soft delete a currency. Checks if not already deleted."""
        try:
            # Fetch including deleted for a specific error message
            currency = await self.get_by_id(currency_id, include_deleted=True)
            
            if not currency:
                raise ValueError(f"Currency with ID {currency_id} not found")
            
            if currency.deleted:
                raise ValueError(f"Currency {currency.code} is already deleted")
            
            if currency.is_default:
                raise ValueError("Cannot delete the default currency")

            currency.deleted = True
            await self.session.commit()
            await self.session.refresh(currency)
            
            logger.info(f"Soft deleted currency ID {currency_id}")
            return True
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error deleting currency {currency_id}: {e}")
            raise e

    async def restore(self, currency_id: int) -> bool:
        """Restore a soft-deleted currency."""
        try:
            currency = await self._get_raw(currency_id)
            if not currency:
                raise ValueError(f"Currency with ID {currency_id} not found")
            if not currency.deleted:
                raise ValueError(f"Currency {currency.code} is not deleted and cannot be restored")
            
            currency.deleted = False
            currency.is_active = True
            await self.session.commit()
            
            await self.session.refresh(currency)
            logger.info(f"Restored currency: {currency.code}")
            return True
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error restoring currency {currency_id}: {e}")
            raise e

    async def set_status(self, currency_id: int, is_active: bool = True, disabled: bool = False) -> bool:
        """Enable or disable a currency."""
        try:
            currency = await self.get_by_id(currency_id)
            if not currency:
                raise ValueError(f"Currency with ID {currency_id} not found")  # ← Raise, not return False
            
            if currency.is_default and (not is_active or disabled):
                raise ValueError("Cannot disable or deactivate the default currency")

            currency.is_active = is_active
            currency.disabled = disabled
            
            await self.session.commit()
            await self.session.refresh(currency)
            logger.info(f"Set status for currency {currency.code}: active={is_active}, disabled={disabled}")
            return True
            
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error setting status for currency {currency_id}: {e}")
            raise e  # ← Raise, not return False
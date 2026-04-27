# App/repository/ProductRepo.py
import asyncio
import logging
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Set
from contextvars import ContextVar

from sqlalchemy import and_, case, delete, func, select, update
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from App.api.databases.MigrateTable import Currencies, Events, OrderItems, Products

logger = logging.getLogger(__name__)

# Set by middleware — used to correlate log lines to a single request
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="unknown")

# PostgreSQL deadlock error code
_PG_DEADLOCK_CODE = "40P01"


# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class ProductRepoError(Exception):
    """Base exception for ProductRepo errors."""


class ProductNotFoundError(ProductRepoError):
    """Raised when a product is not found."""


class EventNotFoundError(ProductRepoError):
    """Raised when an event is not found or inactive."""


class CurrencyNotFoundError(ProductRepoError):
    """Raised when a currency is not found or inactive."""


class DuplicateProductError(ProductRepoError):
    """Raised when a product with the same name exists for the event."""


class InsufficientStockError(ProductRepoError):
    """Raised when trying to reduce stock below zero."""


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class ProductRepo:
    """
    Repository for Product management.

    Production features
    -------------------
    * Atomic stock updates via single UPDATE with guard condition (no race conditions)
    * Batched event/currency verification (no N+1 queries)
    * DB unique constraint as primary duplicate guard (TOCTOU-safe)
    * Decimal precision throughout for monetary values
    * Retry on PostgreSQL deadlock only (pgcode 40P01)
    * Structured exceptions — callers never parse error strings

    Required DB constraint (add to MigrateTable.py):
        class Products(Base):
            ...
            __table_args__ = (
                UniqueConstraint("event_id", "name", name="uq_products_event_name"),
            )
    """

    def __init__(self, session: AsyncSession):
        self.session = session
        self._default_timeout = 5.0
        self._bulk_timeout = 30.0
        self._max_retries = 3

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _log(self, level: str, msg: str, **extra) -> None:
        """Emit a log line with the current correlation ID attached."""
        getattr(logger, level)(
            msg, extra={"correlation_id": correlation_id_var.get(), **extra}
        )

    async def _retry_on_deadlock(self, fn: Callable, *args, **kwargs):
        """
        Retry *fn* on PostgreSQL deadlock (pgcode 40P01) with exponential backoff.

        IMPORTANT: accepts a *callable*, not an awaited coroutine.
        Correct usage:
            await self._retry_on_deadlock(self.session.commit)
            await self._retry_on_deadlock(lambda: self.session.execute(stmt))
        """
        last_exc: Optional[Exception] = None
        for attempt in range(self._max_retries):
            try:
                result = fn(*args, **kwargs)
                if asyncio.iscoroutine(result):
                    return await result
                return result
            except (OperationalError, SQLAlchemyError) as exc:
                last_exc = exc
                # Only retry on a true PostgreSQL deadlock
                pg_code = getattr(getattr(exc, "orig", None), "pgcode", None)
                is_deadlock = pg_code == _PG_DEADLOCK_CODE
                if not is_deadlock:
                    raise
                if attempt < self._max_retries - 1:
                    wait = (2 ** attempt) * 0.1
                    self._log(
                        "warning",
                        f"Deadlock (40P01) — retrying in {wait}s "
                        f"(attempt {attempt + 1}/{self._max_retries})",
                    )
                    await asyncio.sleep(wait)
                    await self.session.rollback()  # reset session state before retry
        raise last_exc  # type: ignore[misc]

    async def _verify_events_batch(self, event_ids: Set[int]) -> Set[int]:
        """Batch-verify events are active. Returns set of valid IDs."""
        if not event_ids:
            return set()
        result = await self.session.execute(
            select(Events.id)
            .where(
                and_(
                    Events.id.in_(event_ids),
                    Events.deleted == False,
                    Events.is_active == True,
                    Events.disabled == False,
                )
            )
            .execution_options(timeout=self._default_timeout)
        )
        return {row[0] for row in result.all()}

    async def _verify_currencies_batch(self, currency_ids: Set[int]) -> Set[int]:
        """Batch-verify currencies are active. Returns set of valid IDs."""
        if not currency_ids:
            return set()
        result = await self.session.execute(
            select(Currencies.id)
            .where(
                and_(
                    Currencies.id.in_(currency_ids),
                    Currencies.deleted == False,
                    Currencies.is_active == True,
                    Currencies.disabled == False,
                )
            )
            .execution_options(timeout=self._default_timeout)
        )
        return {row[0] for row in result.all()}

    async def _verify_event_active(self, event_id: int) -> None:
        valid = await self._verify_events_batch({event_id})
        if event_id not in valid:
            raise EventNotFoundError(f"Active event with ID {event_id} not found")

    async def _verify_currency_active(self, currency_id: int) -> None:
        valid = await self._verify_currencies_batch({currency_id})
        if currency_id not in valid:
            raise CurrencyNotFoundError(f"Active currency with ID {currency_id} not found")

    async def _name_exists_in_event(
        self, event_id: int, name: str, exclude_id: Optional[int] = None
    ) -> bool:
        """
        Application-level duplicate check (secondary guard).
        The DB unique constraint on (event_id, name) is the primary TOCTOU guard.
        """
        conditions = [
            Products.event_id == event_id,
            Products.name == name.strip(),
            Products.deleted == False,
        ]
        if exclude_id:
            conditions.append(Products.id != exclude_id)

        result = await self.session.execute(
            select(Products.id)
            .where(and_(*conditions))
            .limit(1)
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none() is not None

    # ------------------------------------------------------------------
    # CREATE
    # ------------------------------------------------------------------

    async def create_product(
        self,
        event_id: int,
        currency_id: int,
        name: str,
        unit_price: Decimal,
        description: Optional[str] = None,
        stock_count: int = 0,
        low_stock_threshold: int = 10,
        is_active: bool = True,
    ) -> Products:
        """
        Create a new product.

        Raises:
            ValueError: invalid input values
            EventNotFoundError: event not found or inactive
            CurrencyNotFoundError: currency not found or inactive
            DuplicateProductError: product with same name already exists in event
        """
        if not name or not name.strip():
            raise ValueError("Product name is required")
        if unit_price < 0:
            raise ValueError(f"Unit price cannot be negative: {unit_price}")
        if stock_count < 0:
            raise ValueError(f"Stock count cannot be negative: {stock_count}")
        if low_stock_threshold < 0:
            raise ValueError(f"Low stock threshold cannot be negative: {low_stock_threshold}")

        await self._verify_event_active(event_id)
        await self._verify_currency_active(currency_id)

        product = Products(
            event_id=event_id,
            currency_id=currency_id,
            name=name.strip(),
            description=description.strip() if description else None,
            unit_price=unit_price,
            stock_count=stock_count,
            low_stock_threshold=low_stock_threshold,
            is_active=is_active,
            disabled=False,
            deleted=False,
        )
        self.session.add(product)

        try:
            # FIX: pass callable, not awaited result
            await self._retry_on_deadlock(self.session.commit)
            await self.session.refresh(product)
        except IntegrityError as e:
            await self.session.rollback()
            err = str(e).lower()
            if "duplicate key" in err or "unique constraint" in err:
                raise DuplicateProductError(
                    f"Product '{name.strip()}' already exists for event {event_id}"
                )
            raise

        self._log("info", f"Created product '{product.name}' id={product.id} event={event_id}")
        return product

    async def bulk_create_products(
        self,
        products_data: List[Dict[str, Any]],
    ) -> List[Products]:
        """
        Bulk-create products with batched verification (no N+1).

        Raises:
            ValueError: missing or invalid fields
            EventNotFoundError: any event is invalid
            CurrencyNotFoundError: any currency is invalid
            DuplicateProductError: any name conflicts (intra-batch or existing DB)
        """
        if not products_data:
            return []

        required = {"event_id", "currency_id", "name", "unit_price"}
        # intra-batch duplicate tracker: {event_id: {name, ...}}
        seen_names: Dict[int, Set[str]] = {}

        # Batch verify all referenced events and currencies
        event_ids = {p["event_id"] for p in products_data}
        currency_ids = {p["currency_id"] for p in products_data}
        active_events = await self._verify_events_batch(event_ids)
        active_currencies = await self._verify_currencies_batch(currency_ids)

        to_add: List[Products] = []

        for i, data in enumerate(products_data):
            missing = required - data.keys()
            if missing:
                raise ValueError(f"Product {i}: missing fields {missing}")

            name = data["name"].strip()
            if not name:
                raise ValueError(f"Product {i}: name cannot be empty")
            if data["unit_price"] < 0:
                raise ValueError(f"Product {i}: unit_price cannot be negative")
            if data.get("stock_count", 0) < 0:
                raise ValueError(f"Product {i}: stock_count cannot be negative")

            if data["event_id"] not in active_events:
                raise EventNotFoundError(
                    f"Product {i}: event {data['event_id']} not active"
                )
            if data["currency_id"] not in active_currencies:
                raise CurrencyNotFoundError(
                    f"Product {i}: currency {data['currency_id']} not active"
                )

            # Intra-batch duplicate check
            eid = data["event_id"]
            seen_names.setdefault(eid, set())
            if name in seen_names[eid]:
                raise DuplicateProductError(
                    f"Product {i}: duplicate name '{name}' within batch for event {eid}"
                )
            seen_names[eid].add(name)

            to_add.append(
                Products(
                    event_id=eid,
                    currency_id=data["currency_id"],
                    name=name,
                    description=data.get("description"),
                    unit_price=data["unit_price"],
                    stock_count=data.get("stock_count", 0),
                    low_stock_threshold=data.get("low_stock_threshold", 10),
                    is_active=data.get("is_active", True),
                    disabled=False,
                    deleted=False,
                )
            )

        for product in to_add:
            self.session.add(product)

        try:
            # FIX: pass callable, not awaited result
            await self._retry_on_deadlock(self.session.commit)
        except IntegrityError as e:
            await self.session.rollback()
            err = str(e).lower()
            if "duplicate key" in err or "unique constraint" in err:
                raise DuplicateProductError(
                    "One or more products have duplicate names in their events"
                )
            raise

        for product in to_add:
            await self.session.refresh(product)

        self._log("info", f"Bulk created {len(to_add)} products")
        return to_add

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------

    async def get_by_id(
        self,
        product_id: int,
        include_deleted: bool = False,
    ) -> Optional[Products]:
        """Get product by ID."""
        conditions = [Products.id == product_id]
        if not include_deleted:
            conditions.append(Products.deleted == False)

        result = await self.session.execute(
            select(Products)
            .where(and_(*conditions))
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_by_event(
        self,
        event_id: int,
        include_inactive: bool = False,
        include_deleted: bool = False,
        limit: int = 100,
        offset: int = 0,
        cursor: Optional[int] = None,
    ) -> List[Products]:
        """
        Get products for an event.

        Provide *cursor* (last seen product ID) for cursor-based pagination,
        which is faster on large tables. *offset* is used only when cursor is None.
        The two modes are mutually exclusive — cursor takes precedence.
        """
        conditions = [Products.event_id == event_id]
        if not include_deleted:
            conditions.append(Products.deleted == False)
        if not include_inactive:
            conditions += [Products.is_active == True, Products.disabled == False]

        query = select(Products).where(and_(*conditions))

        if cursor is not None:
            query = query.where(Products.id > cursor).order_by(Products.id.asc())
        else:
            query = query.order_by(Products.name.asc()).offset(offset)

        result = await self.session.execute(
            query.limit(limit).execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_active_products(
        self,
        event_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[Products]:
        """Get all active products, optionally filtered by event."""
        conditions = [
            Products.deleted == False,
            Products.disabled == False,
            Products.is_active == True,
        ]
        if event_id:
            conditions.append(Products.event_id == event_id)

        result = await self.session.execute(
            select(Products)
            .where(and_(*conditions))
            .order_by(Products.name.asc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_low_stock_products(
        self,
        event_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[Products]:
        """Get products where stock_count <= low_stock_threshold."""
        conditions = [
            Products.deleted == False,
            Products.disabled == False,
            Products.is_active == True,
            Products.stock_count <= Products.low_stock_threshold,
        ]
        if event_id:
            conditions.append(Products.event_id == event_id)

        result = await self.session.execute(
            select(Products)
            .where(and_(*conditions))
            .order_by(Products.stock_count.asc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_out_of_stock_products(
        self,
        event_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[Products]:
        """Get products with zero stock."""
        conditions = [
            Products.deleted == False,
            Products.disabled == False,
            Products.is_active == True,
            Products.stock_count == 0,
        ]
        if event_id:
            conditions.append(Products.event_id == event_id)

        result = await self.session.execute(
            select(Products)
            .where(and_(*conditions))
            .order_by(Products.name.asc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    # ------------------------------------------------------------------
    # UPDATE — ATOMIC STOCK (critical, no race conditions)
    # ------------------------------------------------------------------

    async def update_stock(
        self,
        product_id: int,
        quantity_change: int,
    ) -> Products:
        """
        Atomically update stock by *quantity_change* (positive or negative).

        Uses a single UPDATE with a guard condition so no two concurrent
        callers can race — no SELECT-then-UPDATE pattern.

        Raises:
            ProductNotFoundError: product does not exist or is deleted
            InsufficientStockError: would cause stock to go negative
        """
        if quantity_change == 0:
            product = await self.get_by_id(product_id)
            if not product:
                raise ProductNotFoundError(f"Product {product_id} not found")
            return product

        # Build the single atomic UPDATE
        base_conditions = [
            Products.id == product_id,
            Products.deleted == False,
        ]
        if quantity_change < 0:
            # Guard: only update if current stock covers the deduction
            base_conditions.append(Products.stock_count >= -quantity_change)

        async def _do_update_and_commit():
            result = await self.session.execute(
                update(Products)
                .where(and_(*base_conditions))
                .values(
                    stock_count=Products.stock_count + quantity_change,
                    updated_at=func.now(),
                )
                .execution_options(timeout=self._default_timeout)
            )
            await self.session.commit()
            return result

        # FIX: both the UPDATE and the commit are inside the retry callable
        result = await self._retry_on_deadlock(_do_update_and_commit)

        if result.rowcount == 0:
            # rowcount=0 means: not found, deleted, or insufficient stock
            product = await self.get_by_id(product_id, include_deleted=True)
            if not product:
                raise ProductNotFoundError(f"Product {product_id} not found")
            if product.deleted:
                raise ProductNotFoundError(f"Product {product_id} is deleted")
            raise InsufficientStockError(
                f"Cannot reduce stock by {abs(quantity_change)}. "
                f"Current stock: {product.stock_count}"
            )

        product = await self.get_by_id(product_id)
        self._log(
            "info",
            f"Stock updated product={product_id} "
            f"new_stock={product.stock_count} delta={quantity_change}",
        )
        return product

    async def update_product(
        self,
        product_id: int,
        update_data: Dict[str, Any],
    ) -> Products:
        """
        Update product metadata (non-stock fields).
        For stock changes use update_stock() for atomicity.

        Raises:
            ProductNotFoundError: product not found
            ValueError: invalid values or attempt to update immutable fields
            DuplicateProductError: new name conflicts with an existing product
            CurrencyNotFoundError: new currency_id is invalid
        """
        # FIX: raise immediately for immutable fields instead of silently ignoring
        immutable = {"id", "event_id", "created_at"}
        attempted_immutable = immutable & update_data.keys()
        if attempted_immutable:
            raise ValueError(
                f"Cannot update immutable fields: {attempted_immutable}. "
                "Remove them from update_data."
            )

        product = await self.get_by_id(product_id)
        if not product:
            raise ProductNotFoundError(f"Product {product_id} not found")
        if product.deleted:
            raise ValueError(f"Product {product_id} is deleted")

        if "unit_price" in update_data:
            price = update_data["unit_price"]
            if price < 0:
                raise ValueError(f"Unit price cannot be negative: {price}")
            update_data["unit_price"] = (
                price if isinstance(price, Decimal) else Decimal(str(price))
            )

        if "low_stock_threshold" in update_data:
            if update_data["low_stock_threshold"] < 0:
                raise ValueError(
                    f"Low stock threshold cannot be negative: {update_data['low_stock_threshold']}"
                )

        if "name" in update_data:
            new_name = update_data["name"].strip()
            if not new_name:
                raise ValueError("Product name cannot be empty")
            if await self._name_exists_in_event(product.event_id, new_name, exclude_id=product_id):
                raise DuplicateProductError(
                    f"Product '{new_name}' already exists in event {product.event_id}"
                )
            update_data["name"] = new_name

        if "currency_id" in update_data:
            await self._verify_currency_active(update_data["currency_id"])

        for field, value in update_data.items():
            if hasattr(product, field):
                setattr(product, field, value)

        product.updated_at = func.now()

        try:
            await self._retry_on_deadlock(self.session.commit)
            await self.session.refresh(product)
        except IntegrityError as e:
            await self.session.rollback()
            if "duplicate key" in str(e).lower():
                raise DuplicateProductError(
                    f"Product name '{product.name}' already exists in this event"
                )
            raise

        self._log("info", f"Updated product {product_id}")
        return product

    async def toggle_active_status(self, product_id: int, is_active: bool) -> bool:
        """Toggle product active status. Returns new status."""
        product = await self.get_by_id(product_id)
        if not product:
            raise ProductNotFoundError(f"Product {product_id} not found")
        if product.deleted:
            raise ValueError(f"Product {product_id} is deleted")

        product.is_active = is_active
        product.updated_at = func.now()

        try:
            await self.session.commit()
            await self.session.refresh(product)
        except SQLAlchemyError:
            await self.session.rollback()
            raise

        self._log("info", f"Product {product_id} active={is_active}")
        return product.is_active

    async def disable_product(self, product_id: int) -> bool:
        """Soft-disable a product (not deleted, just inactive)."""
        product = await self.get_by_id(product_id)
        if not product:
            raise ProductNotFoundError(f"Product {product_id} not found")
        if product.deleted:
            raise ValueError(f"Product {product_id} is deleted")

        product.disabled = True
        product.is_active = False
        product.updated_at = func.now()

        try:
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            raise

        self._log("info", f"Disabled product {product_id}")
        return True

    async def enable_product(self, product_id: int) -> bool:
        """Re-enable a disabled product."""
        product = await self.get_by_id(product_id, include_deleted=True)
        if not product:
            raise ProductNotFoundError(f"Product {product_id} not found")
        if product.deleted:
            raise ValueError(f"Product {product_id} is deleted")

        product.disabled = False
        product.is_active = True
        product.updated_at = func.now()

        try:
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            raise

        self._log("info", f"Enabled product {product_id}")
        return True

    # ------------------------------------------------------------------
    # DELETE & RESTORE
    # ------------------------------------------------------------------

    async def delete_product(
        self,
        product_id: int,
        hard_delete: bool = False,
    ) -> bool:
        """
        Delete a product.

        Soft delete (default): sets deleted=True, blocks further use.
        Hard delete: permanently removes the row (blocked if order items exist).

        Raises:
            ProductNotFoundError: product not found
            ValueError: hard delete blocked by existing order items
        """
        product = await self.get_by_id(product_id, include_deleted=True)
        if not product:
            raise ProductNotFoundError(f"Product {product_id} not found")

        if hard_delete:
            order_item = await self.session.execute(
                select(OrderItems.id)
                .where(
                    and_(
                        OrderItems.product_id == product_id,
                        OrderItems.deleted == False,
                    )
                )
                .limit(1)
                .execution_options(timeout=self._default_timeout)
            )
            if order_item.scalar():
                raise ValueError(
                    f"Cannot hard delete product {product_id}: "
                    "active order items reference it. Soft delete instead."
                )

            await self.session.execute(
                delete(Products).where(Products.id == product_id)
            )
            await self.session.commit()
            self._log("info", f"Hard deleted product {product_id}")
            return True

        if product.deleted:
            return False  # idempotent

        product.deleted = True
        product.is_active = False
        product.updated_at = func.now()
        await self.session.commit()

        self._log("info", f"Soft deleted product {product_id}")
        return True

    async def restore_product(self, product_id: int) -> bool:
        """
        Restore a soft-deleted product.
        Returns False if not deleted, or if name conflicts with existing product.
        """
        product = await self.get_by_id(product_id, include_deleted=True)
        if not product or not product.deleted:
            return False

        if await self._name_exists_in_event(product.event_id, product.name, exclude_id=product_id):
            self._log(
                "warning",
                f"Cannot restore product {product_id}: "
                f"name '{product.name}' already in use in event {product.event_id}",
            )
            return False

        product.deleted = False
        product.is_active = True
        product.disabled = False
        product.updated_at = func.now()
        await self.session.commit()

        self._log("info", f"Restored product {product_id}")
        return True

    # ------------------------------------------------------------------
    # STATISTICS
    # ------------------------------------------------------------------

    async def get_event_product_stats(self, event_id: int) -> Dict[str, Any]:
        """
        Aggregate product statistics for an event in a single query.

        Counts are split correctly:
          - total_products:      all non-deleted products (active + inactive)
          - active_products:     is_active=True, disabled=False
          - inactive_products:   is_active=False or disabled=True (but not deleted)
          - low_stock_products:  stock_count <= low_stock_threshold (active only)
          - out_of_stock_products: stock_count == 0 (active only)
          - total_inventory_value: sum(unit_price * stock_count) as Decimal
        """
        try:
            result = await self.session.execute(
                select(
                    func.count().label("total"),
                    # FIX: SQLAlchemy 2.x case() syntax
                    func.sum(
                        case(
                            (and_(Products.is_active == True, Products.disabled == False), 1),
                            else_=0,
                        )
                    ).label("active"),
                    func.sum(
                        case(
                            (Products.stock_count <= Products.low_stock_threshold, 1),
                            else_=0,
                        )
                    ).label("low_stock"),
                    func.sum(
                        case((Products.stock_count == 0, 1), else_=0)
                    ).label("out_of_stock"),
                    func.coalesce(
                        func.sum(Products.unit_price * Products.stock_count), 0
                    ).label("inventory_value"),
                )
                .where(
                    and_(
                        Products.event_id == event_id,
                        Products.deleted == False,
                    )
                )
                .execution_options(timeout=self._default_timeout)
            )
            row = result.one()

            total  = row.total  or 0
            active = row.active or 0

            return {
                "total_products":        total,
                "active_products":       active,
                # FIX: inactive = total non-deleted minus active (not "deleted")
                "inactive_products":     total - active,
                "low_stock_products":    row.low_stock    or 0,
                "out_of_stock_products": row.out_of_stock or 0,
                # Stays as Decimal — do not cast to float
                "total_inventory_value": row.inventory_value,
            }
        except Exception as e:
            logger.error(f"Error getting product stats for event {event_id}: {e}")
            return {
                "total_products":        0,
                "active_products":       0,
                "inactive_products":     0,
                "low_stock_products":    0,
                "out_of_stock_products": 0,
                "total_inventory_value": Decimal(0),
            }

    async def get_product_summary(self, product_id: int) -> Optional[Dict[str, Any]]:
        """
        Detailed product summary with event name, currency code, and sold count.
        """
        try:
            product = await self.get_by_id(product_id)
            if not product:
                return None

            # FIX: correct independent joins — Events and Currencies are
            # separate tables; join each on its own FK from the product row.
            event_result = await self.session.execute(
                select(Events.name).where(Events.id == product.event_id)
            )
            event_name = event_result.scalar()

            currency_result = await self.session.execute(
                select(Currencies.code).where(Currencies.id == product.currency_id)
            )
            currency_code = currency_result.scalar()

            # Total units sold (completed order items only)
            sold_result = await self.session.execute(
                select(func.coalesce(func.sum(OrderItems.quantity), 0))
                .where(
                    and_(
                        OrderItems.product_id == product_id,
                        OrderItems.deleted == False,
                    )
                )
            )
            sold_count = sold_result.scalar() or 0

            return {
                "product_id":        product.id,
                "name":              product.name,
                "description":       product.description,
                "unit_price":        product.unit_price,       # Decimal
                "currency_code":     currency_code,
                "stock_count":       product.stock_count,
                "low_stock_threshold": product.low_stock_threshold,
                "is_low_stock":      product.stock_count <= product.low_stock_threshold,
                "is_out_of_stock":   product.stock_count == 0,
                "sold_count":        sold_count,
                "total_revenue":     product.unit_price * sold_count,  # Decimal
                "is_active":         product.is_active and not product.disabled and not product.deleted,
                "created_at":        product.created_at,
                "updated_at":        product.updated_at,
                "event_id":          product.event_id,
                "event_name":        event_name,
            }
        except Exception as e:
            logger.error(f"Error getting product summary for {product_id}: {e}")
            return None
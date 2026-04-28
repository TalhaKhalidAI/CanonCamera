# App/repository/OrderRepo.py
import asyncio
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from contextvars import ContextVar
from datetime import datetime
from uuid import uuid4
from enum import Enum

from sqlalchemy import and_, delete, func, select, update, text
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError, NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import false, true

from App.api.databases.MigrateTable import (
    Orders, OrderItems, Products, Photoes, Sessions, Events, Currencies, User,
    PaymentTransactions, InventoryTransactions
)

logger = logging.getLogger(__name__)

correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="unknown")
_PG_DEADLOCK_CODE = "40P01"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class OrderStatus(str, Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    PAID = "paid"
    SHIPPED = "shipped"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    PAYMENT_FAILED = "payment_failed"


class PaymentStatus(str, Enum):
    UNPAID = "unpaid"
    PAID = "paid"
    FAILED = "failed"
    REFUNDED = "refunded"


class PaymentTransactionStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    REFUNDED = "refunded"


class InventoryTransactionType(str, Enum):
    STOCK_IN = "stock_in"
    SALE = "sale"
    RETURN = "return"
    LOSS = "loss"
    DAMAGE = "damage"
    ADJUSTMENT = "adjustment"
    VOID = "void"


# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class OrderRepoError(Exception):
    pass


class OrderNotFoundError(OrderRepoError):
    pass


class OrderItemNotFoundError(OrderRepoError):
    pass


class InvalidOrderStateError(OrderRepoError):
    pass


class InsufficientStockError(OrderRepoError):
    pass


class PaymentTransactionNotFoundError(OrderRepoError):
    pass


class InventoryTransactionNotFoundError(OrderRepoError):
    pass


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class OrderRepo:
    """
    Production-ready Order repository with full CRUD for:
        - Orders
        - OrderItems
        - PaymentTransactions
        - InventoryTransactions
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

    # ------------------------------------------------------------------
    # Deadlock Retry (FIXED - uniform pattern)
    # ------------------------------------------------------------------

    async def _with_deadlock_retry(self, func, operation: str = "operation"):
        """Retry on PostgreSQL deadlock with exponential backoff."""
        last_exc = None
        for attempt in range(self._max_retries):
            try:
                # Execute the callable to get a fresh coroutine each attempt
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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _generate_order_number(self) -> str:
        """Generate unique order number with collision handling."""
        max_attempts = 3
        for _ in range(max_attempts):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            unique_id = str(uuid4())[:8].upper()
            order_number = f"ORD-{timestamp}-{unique_id}"
            
            existing = await self.session.execute(
                select(Orders.id).where(Orders.order_number == order_number)
                .execution_options(timeout=self._default_timeout)
            )
            if not existing.scalar():
                return order_number
        
        return f"ORD-{uuid4().hex[:16].upper()}"

    async def _validate_order_status(self, order: Orders, allowed: List[OrderStatus]):
        """Validate order status with proper error message."""
        if order.status not in [s.value for s in allowed]:
            raise InvalidOrderStateError(
                f"Order status '{order.status}' not in allowed: {[s.value for s in allowed]}"
            )

    async def _get_order_with_lock(self, order_id: int) -> Orders:
        """Get order with FOR UPDATE lock."""
        result = await self.session.execute(
            select(Orders)
            .where(Orders.id == order_id, Orders.deleted.is_(false()))
            .with_for_update()
            .execution_options(timeout=self._default_timeout)
        )
        order = result.scalar_one_or_none()
        if not order:
            raise OrderNotFoundError(f"Order {order_id} not found")
        return order

    async def _get_order_item_with_lock(self, order_item_id: int) -> OrderItems:
        """Get order item with FOR UPDATE lock."""
        result = await self.session.execute(
            select(OrderItems)
            .where(OrderItems.id == order_item_id, OrderItems.deleted.is_(false()))
            .with_for_update()
            .execution_options(timeout=self._default_timeout)
        )
        order_item = result.scalar_one_or_none()
        if not order_item:
            raise OrderItemNotFoundError(f"Order item {order_item_id} not found")
        return order_item

    # ==================================================================
    # ORDERS - CRUD
    # ==================================================================

    async def create_order(
        self,
        session_id: int,
        event_id: int,
        currency_id: int,
        created_by: int,
        items: List[Dict[str, Any]],
        subtotal_amount: Decimal = Decimal(0),
        total_amount: Optional[Decimal] = None,
    ) -> Orders:
        """Create order with items atomically. Stock NOT reduced here."""
        if not items:
            raise ValueError("Order must have at least one item")

        if total_amount is None:
            total_amount = subtotal_amount

        # Advisory stock check (not atomic, just early validation)
        for item in items:
            result = await self.session.execute(
                select(Products.id, Products.is_active, Products.disabled)
                .where(Products.id == item["product_id"], Products.deleted.is_(false()))
                .execution_options(timeout=self._default_timeout)
            )
            product = result.first()
            if not product:
                raise ValueError(f"Product {item['product_id']} not found")
            if not product.is_active or product.disabled:
                raise ValueError(f"Product {item['product_id']} is not active")

        # Create order
        order_number = await self._generate_order_number()
        order = Orders(
            session_id=session_id,
            event_id=event_id,
            currency_id=currency_id,
            created_by=created_by,
            order_number=order_number,
            status=OrderStatus.PENDING.value,
            payment_status=PaymentStatus.UNPAID.value,
            subtotal_amount=subtotal_amount,
            total_amount=total_amount,
            is_active=True,
            disabled=False,
            deleted=False,
        )
        self.session.add(order)
        await self.session.flush()

        # Create order items
        for item in items:
            line_total = item["unit_price"] * item["quantity"]
            order_item = OrderItems(
                order_id=order.id,
                photo_id=item.get("photo_id"),
                product_id=item["product_id"],
                print_id=item.get("print_id"),
                quantity=item["quantity"],
                unit_price=item["unit_price"],
                line_total=line_total,
                disabled=False,
                deleted=False,
            )
            self.session.add(order_item)

        await self._with_deadlock_retry(
            self.session.commit,
            operation="create_order"
        )
        await self.session.refresh(order)

        self._log("info", f"Created order {order.order_number} with {len(items)} items")
        return order

    async def get_order_by_id(
        self, 
        order_id: int, 
        include_deleted: bool = False
    ) -> Optional[Orders]:
        """Get order by ID."""
        conditions = [Orders.id == order_id]
        if not include_deleted:
            conditions.append(Orders.deleted.is_(false()))

        result = await self.session.execute(
            select(Orders).where(and_(*conditions))
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_order_by_number(self, order_number: str) -> Optional[Orders]:
        """Get order by order number."""
        result = await self.session.execute(
            select(Orders).where(
                Orders.order_number == order_number,
                Orders.deleted.is_(false())
            ).execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_orders_by_session(
        self,
        session_id: int,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Orders]:
        """Get all orders for a session."""
        result = await self.session.execute(
            select(Orders)
            .where(Orders.session_id == session_id, Orders.deleted.is_(false()))
            .order_by(Orders.created_at.desc())
            .limit(limit)
            .offset(offset)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_orders_by_status(
        self,
        status: OrderStatus,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Orders]:
        """Get orders by status with pagination."""
        result = await self.session.execute(
            select(Orders)
            .where(Orders.status == status.value, Orders.deleted.is_(false()))
            .order_by(Orders.created_at.asc())
            .limit(limit)
            .offset(offset)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_orders_by_event(
        self,
        event_id: int,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Orders]:
        """Get all orders for an event."""
        result = await self.session.execute(
            select(Orders)
            .where(Orders.event_id == event_id, Orders.deleted.is_(false()))
            .order_by(Orders.created_at.desc())
            .limit(limit)
            .offset(offset)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_pending_orders(self, limit: int = 100) -> List[Orders]:
        """Get all pending orders."""
        return await self.get_orders_by_status(OrderStatus.PENDING, limit)

    async def get_confirmed_orders(self, limit: int = 100) -> List[Orders]:
        """Get all confirmed orders."""
        return await self.get_orders_by_status(OrderStatus.CONFIRMED, limit)

    # ------------------------------------------------------------------
    # UPDATE
    # ------------------------------------------------------------------

    async def confirm_order(self, order_id: int) -> Orders:
        """Confirm order and reduce stock atomically with audit trail."""
        async def _do_confirm():
            order = await self._get_order_with_lock(order_id)
            await self._validate_order_status(order, [OrderStatus.PENDING])

            # Get order items
            items = await self.session.execute(
                select(OrderItems)
                .where(OrderItems.order_id == order_id, OrderItems.deleted.is_(false()))
                .execution_options(timeout=self._bulk_timeout)
            )
            items = list(items.scalars().all())

            if not items:
                raise ValueError(f"Order {order_id} has no items")

            # Atomic stock reduction and inventory audit
            for item in items:
                result = await self.session.execute(
                    update(Products)
                    .where(
                        Products.id == item.product_id,
                        Products.deleted.is_(false()),
                        Products.stock_count >= item.quantity
                    )
                    .values(
                        stock_count=Products.stock_count - item.quantity,
                        updated_at=func.now()
                    )
                )
                if result.rowcount == 0:
                    product = await self.session.execute(
                        select(Products.name).where(Products.id == item.product_id)
                    )
                    product_name = product.scalar() or f"ID {item.product_id}"
                    raise InsufficientStockError(
                        f"Insufficient stock for product '{product_name}'. "
                        f"Requested: {item.quantity}"
                    )

                # Record inventory transaction
                await self.create_inventory_transaction(
                    event_id=order.event_id,
                    product_id=item.product_id,
                    currency_id=order.currency_id,
                    transaction_type=InventoryTransactionType.SALE.value,
                    quantity_change=-item.quantity,
                    unit_value=item.unit_price,
                    order_id=order.id,
                    order_item_id=item.id,
                    photo_id=item.photo_id,
                    created_by=order.created_by
                )

            # Update order status
            order.status = OrderStatus.CONFIRMED.value
            order.updated_at = func.now()
            await self.session.flush()
            return order

        # ✅ FIX: assign return value and refresh
        order = await self._with_deadlock_retry(_do_confirm, operation="confirm_order")
        await self.session.refresh(order)
        self._log("info", f"Confirmed order {order_id}")
        return order

    async def update_order_status(
        self,
        order_id: int,
        status: OrderStatus,
        cancel_reason: Optional[str] = None
    ) -> Orders:
        """Update order status."""
        async def _do_update():
            order = await self._get_order_with_lock(order_id)
            order.status = status.value
            if cancel_reason:
                order.cancel_reason = cancel_reason[:500]  # Truncate with warning
                if len(cancel_reason) > 500:
                    self._log("warning", f"Cancel reason truncated: original length {len(cancel_reason)}")
            order.updated_at = func.now()
            return order

        # ✅ FIX: assign return value
        order = await self._with_deadlock_retry(_do_update, operation="update_order_status")
        await self.session.refresh(order)
        self._log("info", f"Order {order_id} status: {status.value}")
        return order

    async def cancel_order(
        self, 
        order_id: int, 
        cancel_reason: str,
        restore_stock: bool = True
    ) -> Orders:
        """Cancel order and optionally restore stock."""
        async def _do_cancel():
            order = await self._get_order_with_lock(order_id)

            if order.status in [OrderStatus.CANCELLED.value, OrderStatus.COMPLETED.value]:
                raise InvalidOrderStateError(f"Cannot cancel order with status {order.status}")

            if restore_stock and order.status == OrderStatus.CONFIRMED.value:
                items = await self.session.execute(
                    select(OrderItems)
                    .where(OrderItems.order_id == order_id, OrderItems.deleted.is_(false()))
                    .execution_options(timeout=self._bulk_timeout)
                )
                for item in items.scalars().all():
                    await self.session.execute(
                        update(Products)
                        .where(Products.id == item.product_id, Products.deleted.is_(false()))
                        .values(
                            stock_count=Products.stock_count + item.quantity,
                            updated_at=func.now()
                        )
                    )
                    
                    await self.create_inventory_transaction(
                        event_id=order.event_id,
                        product_id=item.product_id,
                        currency_id=order.currency_id,
                        transaction_type=InventoryTransactionType.RETURN.value,
                        quantity_change=item.quantity,
                        unit_value=item.unit_price,
                        order_id=order.id,
                        order_item_id=item.id,
                        photo_id=item.photo_id,
                        reason=f"Cancelled order: {cancel_reason[:200]}",
                        created_by=order.created_by
                    )

            order.status = OrderStatus.CANCELLED.value
            order.cancel_reason = cancel_reason[:500]
            if len(cancel_reason) > 500:
                self._log("warning", f"Cancel reason truncated for order {order_id}")
            order.cancelled_at = func.now()
            order.updated_at = func.now()
            return order

        # ✅ FIX: assign return value
        order = await self._with_deadlock_retry(_do_cancel, operation="cancel_order")
        await self.session.refresh(order)
        self._log("info", f"Cancelled order {order_id}")
        return order

    # ------------------------------------------------------------------
    # DELETE
    # ------------------------------------------------------------------

    async def soft_delete_order(self, order_id: int) -> bool:
        """Soft delete an order with lock."""
        try:
            order = await self._get_order_with_lock(order_id)
            order.deleted = True
            order.is_active = False
            order.updated_at = func.now()
            await self.session.commit()
            
            self._log("info", f"Soft deleted order {order_id}")
            return True
        except OrderNotFoundError:
            return False

    async def hard_delete_order(self, order_id: int) -> bool:
        """Hard delete an order (check all references first)."""
        # ✅ Check order items
        items = await self.get_order_items(order_id)
        if items:
            raise ValueError(f"Cannot hard delete order {order_id}: has {len(items)} order items")
        
        # ✅ Check payment transactions
        payment = await self.get_payment_transaction_by_order(order_id)
        if payment:
            raise ValueError(f"Cannot hard delete order {order_id}: has payment transaction")
        
        # ✅ Check inventory transactions
        inv_transactions = await self.get_inventory_transactions_by_order(order_id, limit=1)
        if inv_transactions:
            raise ValueError(f"Cannot hard delete order {order_id}: has inventory transactions")
        
        result = await self.session.execute(
            delete(Orders).where(Orders.id == order_id)
        )
        await self.session.commit()
        
        success = result.rowcount > 0
        if success:
            self._log("info", f"Hard deleted order {order_id}")
        return success

    # ==================================================================
    # ORDER ITEMS - CRUD
    # ==================================================================

    async def get_order_items(
        self,
        order_id: int,
        include_deleted: bool = False,
    ) -> List[OrderItems]:
        """Get all items for an order."""
        conditions = [OrderItems.order_id == order_id]
        if not include_deleted:
            conditions.append(OrderItems.deleted.is_(false()))

        result = await self.session.execute(
            select(OrderItems).where(and_(*conditions))
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_order_item_by_id(self, order_item_id: int) -> Optional[OrderItems]:
        """Get order item by ID."""
        result = await self.session.execute(
            select(OrderItems).where(
                OrderItems.id == order_item_id,
                OrderItems.deleted.is_(false())
            ).execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def add_order_item(
        self,
        order_id: int,
        photo_id: int,
        product_id: int,
        quantity: int,
        unit_price: Decimal,
        print_id: Optional[int] = None,
    ) -> OrderItems:
        """Add item to order with FOR UPDATE lock."""
        async def _do_add():
            order = await self._get_order_with_lock(order_id)
            await self._validate_order_status(order, [OrderStatus.PENDING])

            line_total = unit_price * quantity

            order_item = OrderItems(
                order_id=order_id,
                photo_id=photo_id,
                product_id=product_id,
                print_id=print_id,
                quantity=quantity,
                unit_price=unit_price,
                line_total=line_total,
            )
            self.session.add(order_item)

            order.subtotal_amount += line_total
            order.total_amount += line_total
            order.updated_at = func.now()

            await self.session.flush()
            return order_item

        # ✅ FIX: assign return value
        order_item = await self._with_deadlock_retry(_do_add, operation="add_order_item")
        await self.session.refresh(order_item)
        self._log("info", f"Added item to order {order_id}")
        return order_item

    async def update_order_item_quantity(
        self,
        order_item_id: int,
        new_quantity: int,
    ) -> OrderItems:
        """Update order item quantity with FOR UPDATE lock."""
        async def _do_update():
            order_item = await self._get_order_item_with_lock(order_item_id)
            order = await self._get_order_with_lock(order_item.order_id)
            await self._validate_order_status(order, [OrderStatus.PENDING])

            if new_quantity <= 0:
                order.subtotal_amount -= order_item.line_total
                order.total_amount -= order_item.line_total
                order_item.deleted = True
            else:
                old_total = order_item.line_total
                new_line_total = order_item.unit_price * new_quantity
                delta = new_line_total - old_total

                order_item.quantity = new_quantity
                order_item.line_total = new_line_total
                order.subtotal_amount += delta
                order.total_amount += delta

            order_item.updated_at = func.now()
            order.updated_at = func.now()
            return order_item

        # ✅ FIX: assign return value
        order_item = await self._with_deadlock_retry(_do_update, operation="update_order_item_quantity")
        await self.session.refresh(order_item)
        self._log("info", f"Updated order item {order_item_id}")
        return order_item

    async def remove_order_item(self, order_item_id: int) -> bool:
        """Remove order item (soft delete)."""
        await self.update_order_item_quantity(order_item_id, 0)
        return True

    # ==================================================================
    # PAYMENT TRANSACTIONS - CRUD
    # ==================================================================

    async def get_payment_transaction_by_order(
        self, order_id: int
    ) -> Optional[PaymentTransactions]:
        """Get payment transaction for an order."""
        result = await self.session.execute(
            select(PaymentTransactions)
            .where(
                PaymentTransactions.order_id == order_id,
                PaymentTransactions.deleted.is_(false())  # ✅ Fixed: is_(false())
            )
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_payment_transaction_by_id(
        self, transaction_id: int
    ) -> Optional[PaymentTransactions]:
        """Get payment transaction by ID."""
        result = await self.session.execute(
            select(PaymentTransactions)
            .where(
                PaymentTransactions.id == transaction_id,
                PaymentTransactions.deleted.is_(false())
            )
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def get_payment_transaction_by_reference(
        self, provider_reference: str
    ) -> Optional[PaymentTransactions]:
        """Get payment transaction by provider reference."""
        result = await self.session.execute(
            select(PaymentTransactions)
            .where(
                PaymentTransactions.provider_reference == provider_reference,
                PaymentTransactions.deleted.is_(false())
            )
            .execution_options(timeout=self._default_timeout)
        )
        return result.scalar_one_or_none()

    async def list_payment_transactions_by_order_status(
        self, status: PaymentTransactionStatus, limit: int = 100
    ) -> List[PaymentTransactions]:
        """List payment transactions by status."""
        result = await self.session.execute(
            select(PaymentTransactions)
            .where(
                PaymentTransactions.status == status.value,
                PaymentTransactions.deleted.is_(false())
            )
            .order_by(PaymentTransactions.created_at.desc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def create_payment_transaction(
        self,
        order_id: int,
        currency_id: int,
        method: str,
        amount: Decimal,
        provider_reference: Optional[str] = None,
        processed_by: Optional[int] = None
    ) -> PaymentTransactions:
        """Create a payment transaction record."""
        transaction = PaymentTransactions(
            order_id=order_id,
            currency_id=currency_id,
            method=method,
            status=PaymentTransactionStatus.PENDING.value,
            amount=amount,
            provider_reference=provider_reference,
            processed_by=processed_by,
            deleted=False
        )
        self.session.add(transaction)
        await self.session.flush()
        
        self._log("info", f"Created payment transaction for order {order_id}")
        return transaction

    async def update_payment_transaction_status(
        self,
        transaction_id: int,
        status: PaymentTransactionStatus,
        failure_reason: Optional[str] = None,
        provider_reference: Optional[str] = None
    ) -> PaymentTransactions:
        """Update payment transaction status."""
        async def _do_update():
            result = await self.session.execute(
                update(PaymentTransactions)
                .where(
                    PaymentTransactions.id == transaction_id,
                    PaymentTransactions.deleted.is_(false())
                )
                .values(
                    status=status.value,
                    failure_reason=failure_reason,
                    provider_reference=provider_reference if provider_reference else PaymentTransactions.provider_reference,
                    processed_at=func.now(),
                    updated_at=func.now()
                )
                .returning(PaymentTransactions)
            )
            # ✅ FIX: use scalar_one_or_none() and check
            transaction = result.scalar_one_or_none()
            if transaction is None:
                raise PaymentTransactionNotFoundError(f"Transaction {transaction_id} not found")
            return transaction
        
        transaction = await self._with_deadlock_retry(_do_update, operation="update_payment_transaction")
        await self.session.refresh(transaction)
        
        self._log("info", f"Payment transaction {transaction_id} status: {status.value}")
        return transaction

    async def mark_payment_success(
        self,
        transaction_id: int,
        provider_reference: Optional[str] = None
    ) -> PaymentTransactions:
        """Mark payment as successful."""
        return await self.update_payment_transaction_status(
            transaction_id=transaction_id,
            status=PaymentTransactionStatus.SUCCESS,
            provider_reference=provider_reference
        )

    async def mark_payment_failed(
        self,
        transaction_id: int,
        failure_reason: str
    ) -> PaymentTransactions:
        """Mark payment as failed."""
        return await self.update_payment_transaction_status(
            transaction_id=transaction_id,
            status=PaymentTransactionStatus.FAILED,
            failure_reason=failure_reason
        )

    async def soft_delete_payment_transaction(self, transaction_id: int) -> bool:
        """Soft delete a payment transaction."""
        result = await self.session.execute(
            update(PaymentTransactions)
            .where(
                PaymentTransactions.id == transaction_id,
                PaymentTransactions.deleted.is_(false())
            )
            .values(deleted=True, updated_at=func.now())
        )
        await self.session.commit()
        
        success = result.rowcount > 0
        if success:
            self._log("info", f"Soft deleted payment transaction {transaction_id}")
        return success

    # ==================================================================
    # INVENTORY TRANSACTIONS - CRUD
    # ==================================================================

    async def create_inventory_transaction(
        self,
        event_id: int,
        product_id: int,
        currency_id: int,
        transaction_type: str,
        quantity_change: int,
        unit_value: Optional[Decimal] = None,
        order_id: Optional[int] = None,
        order_item_id: Optional[int] = None,
        photo_id: Optional[int] = None,
        reason: Optional[str] = None,
        created_by: Optional[int] = None
    ) -> InventoryTransactions:
        """Record inventory transaction for audit trail."""
        transaction = InventoryTransactions(
            event_id=event_id,
            product_id=product_id,
            currency_id=currency_id,
            transaction_type=transaction_type,
            quantity_change=quantity_change,
            unit_value=unit_value,
            order_id=order_id,
            order_item_id=order_item_id,
            photo_id=photo_id,
            reason=reason,
            created_by=created_by,
            deleted=False
        )
        self.session.add(transaction)
        await self.session.flush()
        
        self._log("info", f"Inventory transaction: {transaction_type} x{quantity_change} for product {product_id}")
        return transaction

    async def get_inventory_transactions_by_product(
        self,
        product_id: int,
        limit: int = 100,
        offset: int = 0
    ) -> List[InventoryTransactions]:
        """Get inventory transactions for a product."""
        result = await self.session.execute(
            select(InventoryTransactions)
            .where(
                InventoryTransactions.product_id == product_id,
                InventoryTransactions.deleted == False
            )
            .order_by(InventoryTransactions.created_at.desc())
            .limit(limit)
            .offset(offset)
            .execution_options(timeout=self._bulk_timeout)  # ✅ Added timeout
        )
        return list(result.scalars().all())

    async def get_inventory_transactions_by_order(
        self,
        order_id: int,
        limit: int = 100
    ) -> List[InventoryTransactions]:
        """Get inventory transactions for an order."""
        result = await self.session.execute(
            select(InventoryTransactions)
            .where(
                InventoryTransactions.order_id == order_id,
                InventoryTransactions.deleted == False
            )
            .order_by(InventoryTransactions.created_at.desc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_inventory_transactions_by_type(
        self,
        transaction_type: InventoryTransactionType,
        limit: int = 100
    ) -> List[InventoryTransactions]:
        """Get inventory transactions by type."""
        result = await self.session.execute(
            select(InventoryTransactions)
            .where(
                InventoryTransactions.transaction_type == transaction_type.value,
                InventoryTransactions.deleted == False
            )
            .order_by(InventoryTransactions.created_at.desc())
            .limit(limit)
            .execution_options(timeout=self._default_timeout)
        )
        return list(result.scalars().all())

    async def get_product_inventory_summary(
        self,
        product_id: int
    ) -> Dict[str, Any]:
        """Get inventory summary for a product."""
        result = await self.session.execute(
            select(
                func.sum(InventoryTransactions.quantity_change).label("net_change"),
                func.count().label("transaction_count")
            )
            .where(
                InventoryTransactions.product_id == product_id,
                InventoryTransactions.deleted == False
            )
            .execution_options(timeout=self._default_timeout)  # ✅ Added timeout
        )
        row = result.one()
        
        return {
            "product_id": product_id,
            "net_change": row.net_change or 0,
            "transaction_count": row.transaction_count or 0
        }

    async def get_event_inventory_summary(
        self,
        event_id: int
    ) -> List[Dict[str, Any]]:
        """Get inventory summary for all products in an event."""
        result = await self.session.execute(
            select(
                InventoryTransactions.product_id,
                func.sum(InventoryTransactions.quantity_change).label("net_change"),
                func.count().label("transaction_count")
            )
            .where(
                InventoryTransactions.event_id == event_id,
                InventoryTransactions.deleted == False
            )
            .group_by(InventoryTransactions.product_id)
            .execution_options(timeout=self._bulk_timeout)  # ✅ Added timeout
        )
        
        return [
            {
                "product_id": row.product_id,
                "net_change": row.net_change or 0,
                "transaction_count": row.transaction_count or 0
            }
            for row in result.all()
        ]

    async def soft_delete_inventory_transaction(self, transaction_id: int) -> bool:
        """Soft delete an inventory transaction."""
        result = await self.session.execute(
            update(InventoryTransactions)
            .where(
                InventoryTransactions.id == transaction_id,
                InventoryTransactions.deleted == False
            )
            .values(deleted=True)
        )
        await self.session.commit()
        
        success = result.rowcount > 0
        if success:
            self._log("info", f"Soft deleted inventory transaction {transaction_id}")
        return success

    # ==================================================================
    # ORDER SUMMARY (Combined Data)
    # ==================================================================

    async def get_order_summary(self, order_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed order summary with items, payment, and inventory."""
        try:
            order = await self.get_order_by_id(order_id)
            if not order:
                return None

            items = await self.get_order_items(order_id)
            payment = await self.get_payment_transaction_by_order(order_id)

            return {
                "order": {
                    "id": order.id,
                    "order_number": order.order_number,
                    "status": order.status,
                    "payment_status": order.payment_status,
                    "subtotal_amount": order.subtotal_amount,
                    "total_amount": order.total_amount,
                    "created_at": order.created_at,
                    "updated_at": order.updated_at,
                },
                "items": [
                    {
                        "item_id": item.id,
                        "product_id": item.product_id,
                        "photo_id": item.photo_id,
                        "quantity": item.quantity,
                        "unit_price": item.unit_price,
                        "line_total": item.line_total,
                    }
                    for item in items
                ],
                "payment": {
                    "id": payment.id if payment else None,
                    "status": payment.status if payment else None,
                    "method": payment.method if payment else None,
                    "amount": payment.amount if payment else None,
                    "provider_reference": payment.provider_reference if payment else None,
                } if payment else None,
                "item_count": len(items),
            }
        except SQLAlchemyError as e:
            self._log("error", f"Error getting order summary: {e}")
            return None
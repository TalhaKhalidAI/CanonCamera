# App/models/user.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime, func, VARCHAR, DECIMAL, FLOAT, ForeignKey, Float, Text, BigInteger, Numeric, UniqueConstraint
from sqlalchemy.orm import relationship
from App.core.Connector import Base  # Import from new connector
from sqlalchemy.dialects.postgresql import JSONB
class User(Base):  # Changed from Users to User (singular, PEP8)
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    email = Column(String(255), nullable=False, unique=True, index=True)
    password_hash = Column(String(255), nullable=False)  # Renamed for clarity
    profile_pic = Column(String(500), nullable=True)
    user_role = Column(String(50), default="user", nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)  # Better name than 'disable'
    is_superuser = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    disabled=Column(Boolean,default=False)
    deleted=Column(Boolean,default=False,nullable=False)

class Currencies(Base):
    __tablename__="currencies"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    code=Column(VARCHAR,unique=True,nullable=False)
    name=Column(VARCHAR(50),nullable=False)
    symbol=Column(VARCHAR(5),nullable=False)
    decimal_places=Column(Integer,nullable=False,default=2)
    exchange_rate_to_default=Column(Numeric(10,2),default=1.000000,nullable=False)
    is_default=Column(Boolean,nullable=False,default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    disabled=Column(Boolean,default=False)
    is_active = Column(Boolean, default=True, nullable=False)  # Better name than 'disable'
    deleted=Column(Boolean,default=False,nullable=False)

class userSettings(Base):
    __tablename__="user_settings"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    user_id=Column(Integer,ForeignKey(User.id))
    settings=Column(JSONB,nullable=False,default={})
    is_active=Column(Boolean,default=True,nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    disabled=Column(Boolean,default=False)
    deleted=Column(Boolean,default=False,nullable=False)



class Events(Base):
    __tablename__="events"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    name=Column(String,nullable=False)
    location=Column(VARCHAR,nullable=False)
    config=Column(JSONB,nullable=False,default={})
    is_active=Column(Boolean,nullable=False,default=True)
    created_by=Column(Integer,ForeignKey(User.id),nullable=False)
    owned_by=Column(Integer,ForeignKey(User.id),nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    disabled=Column(Boolean,default=False)
    deleted=Column(Boolean,default=False,nullable=False)

class eventShares(Base):
    __tablename__="event_shares"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    event_id=Column(Integer,ForeignKey(Events.id),nullable=False)
    user_id=Column(Integer,ForeignKey(User.id),nullable=False)
    permission=Column(VARCHAR,nullable=False,default="viewer")
    granted_by=Column(Integer,ForeignKey(User.id),nullable=True)       # who granted this access
    access_date=Column(DateTime(timezone=True),server_default=func.now(),nullable=True)  # when access was granted
    is_active=Column(Boolean,nullable=False,default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    disabled=Column(Boolean,default=False)
    deleted=Column(Boolean,default=False,nullable=False)

    __table_args__ = (
        # Enforces one share record per user per event — required for ON CONFLICT DO UPDATE
        UniqueConstraint("event_id", "user_id", name="uq_event_shares_event_user"),
    )


class printerConfig(Base):
    __tablename__="printer_config"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    event_id=Column(Integer,ForeignKey(Events.id),nullable=False)
    owned_by=Column(Integer,ForeignKey(User.id),nullable=False)
    printer_name=Column(VARCHAR,nullable=False)
    printer_settings=Column(JSONB,nullable=False,default={})
    is_active=Column(Boolean,nullable=False,default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    disabled=Column(Boolean,default=False)
    deleted=Column(Boolean,default=False,nullable=False)

class Sessions(Base):
    __tablename__="sessions"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    event_id=Column(Integer,ForeignKey(Events.id),nullable=False)
    session_code=Column(VARCHAR, unique=True, index=True, nullable=False)
    guest_name=Column(VARCHAR,nullable=False)
    guest_email=Column(VARCHAR)
    guest_phone=Column(VARCHAR)
    guest_address=Column(VARCHAR)
    is_active=Column(Boolean,nullable=False,default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    disabled=Column(Boolean,default=False)
    deleted=Column(Boolean,default=False,nullable=False)

class Photoes(Base):
    __tablename__="photoes"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    session_id=Column(Integer,ForeignKey(Sessions.id),nullable=False)
    filename=Column(VARCHAR,nullable=False)
    path=Column(VARCHAR,nullable=False)
    thumb_path=Column(VARCHAR)
    image_type=Column(VARCHAR,nullable=False)
    image_format=Column(VARCHAR,nullable=False)
    width_px=Column(Integer)
    height_px=Column(Integer)
    width_mm=Column(Float)
    height_mm=Column(Float)
    dpi=Column(Integer,nullable=False,default=400)
    file_size_bytes=Column(Integer)
    is_active=Column(Boolean,nullable=False,default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    disabled=Column(Boolean,default=False)
    deleted=Column(Boolean,default=False,nullable=False)


class Prints(Base):
    __tablename__="prints"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    photo_id=Column(Integer,ForeignKey(Photoes.id),nullable=False)
    session_id=Column(Integer,ForeignKey(Sessions.id),nullable=False)
    layout_type=Column(VARCHAR,nullable=False)
    copies=Column(Integer,nullable=False,default=1)
    status=Column(VARCHAR,nullable=False,default="pending")
    started_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    completed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    error_message=Column(Text)
    is_active=Column(Boolean,nullable=False,default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    disabled=Column(Boolean,default=False)
    deleted=Column(Boolean,default=False,nullable=False)

       
class Products(Base):
    __tablename__="products"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    event_id=Column(Integer,ForeignKey(Events.id),nullable=False)   
    currency_id=Column(Integer,ForeignKey(Currencies.id),nullable=False)
    name=Column(VARCHAR,nullable=False)
    description=Column(Text)
    unit_price=Column(Numeric(10,2),nullable=False)
    stock_count=Column(Integer,nullable=False,default=0)
    low_stock_threshold=Column(Integer,nullable=False,default=10)
    is_active=Column(Boolean,nullable=False,default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    disabled=Column(Boolean,default=False)
    deleted=Column(Boolean,default=False,nullable=False)
          
class Orders(Base):
    __tablename__="orders"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    session_id=Column(Integer,ForeignKey(Sessions.id),nullable=False)
    event_id=Column(Integer,ForeignKey(Events.id),nullable=False)
    currency_id=Column(Integer,ForeignKey(Currencies.id),nullable=False)
    created_by=Column(Integer,ForeignKey(User.id),nullable=False)
    order_number=Column(VARCHAR,unique=True,nullable=False)
    status=Column(VARCHAR,nullable= False,default="pending")
    payment_status=Column(VARCHAR,nullable= False,default="unpaid")
    payment_method=Column(VARCHAR)
    subtotal_amount=Column(Numeric(10,2),nullable=False,default=0)
    total_amount=Column(Numeric(10,2),nullable=False,default=0)
    is_active=Column(Boolean,nullable=False,default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    cancel_reason=Column(VARCHAR)
    cancelled_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)

    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    disabled=Column(Boolean,default=False)
    deleted=Column(Boolean,default=False,nullable=False)



class OrderItems(Base):
    __tablename__="order_items"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    order_id=Column(Integer,ForeignKey(Orders.id),nullable=False)
    photo_id=Column(Integer,ForeignKey(Photoes.id),nullable=False)
    product_id=Column(Integer,ForeignKey(Products.id),nullable=False)
    print_id=Column(Integer)
    quantity=Column(Integer,nullable=False,default=1)
    unit_price=Column(Numeric(10,2),nullable=False)
    line_total=Column(Numeric(10,2),nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    disabled=Column(Boolean,default=False)
    deleted=Column(Boolean,default=False,nullable=False)

class PaymentTransactions(Base):
    __tablename__="payment_transactions"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    order_id=Column(Integer,ForeignKey(Orders.id),nullable=False)
    currency_id=Column(Integer,ForeignKey(Currencies.id),nullable=False)
    method=Column(VARCHAR,nullable=False)
    status=Column(VARCHAR,nullable=False,default="pending")
    amount=Column(Numeric(10,2),nullable=False)
    provider_reference=Column(VARCHAR)
    failure_reason=Column(Text)
    processed_by=Column(Integer,ForeignKey(User.id))
    processed_at=Column(DateTime(timezone=True), onupdate=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    deleted=Column(Boolean,default=False,nullable=False)


class Refunds(Base):
    __tablename__="refunds"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    order_id=Column(Integer,nullable=False)
    order_item_id=Column(Integer,ForeignKey(OrderItems.id))
    payment_txn_id=Column(Integer,ForeignKey(PaymentTransactions.id),nullable=False)
    currency_id=Column(Integer,ForeignKey(Currencies.id),nullable=False)
    amount=Column(Numeric(10,2),nullable=False)
    reason=Column(Text,nullable=False)
    status=Column(Text,nullable=False,default="pending")
    approved_by=Column(Integer,ForeignKey(User.id))
    processed_by=Column(Integer,ForeignKey(User.id))
    approved_at=Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    processed_at=Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    deleted=Column(Boolean,default=False,nullable=False)

class PhotoShares(Base):
    __tablename__="photo_shares"
    id=Column(Integer,primary_key=True,index=True,autoincrement=True)
    photo_id=Column(Integer,ForeignKey(Photoes.id),nullable=False)
    session_id=Column(Integer,ForeignKey(Sessions.id),nullable=False)
    share_method=Column(VARCHAR,nullable=False)
    recipient_target=Column(VARCHAR,nullable=False)
    recipient_name=Column(VARCHAR,nullable=False)
    template_id=Column(VARCHAR)
    status=Column(VARCHAR,nullable=False,default="pending")
    retry_count=Column(Integer,nullable=False,default=0)
    last_retry_at= Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    sent_at= Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    opened_at= Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    error_message=Column(Text)
    is_active=Column(Boolean,nullable=False,default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    deleted=Column(Boolean,default=False,nullable=False)

class InventoryTransactions(Base):
    __tablename__="inventory_transactions"
   
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    event_id = Column(Integer, ForeignKey("events.id"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=False)
    transaction_type = Column(VARCHAR, nullable=False)  # stock_in, sale, return, loss, damage, adjustment, void
    quantity_change = Column(Integer, nullable=False)
    unit_value = Column(DECIMAL(10, 2), nullable=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=True)
    order_item_id = Column(Integer, ForeignKey("order_items.id"), nullable=True)
    photo_id = Column(Integer, ForeignKey("photoes.id"), nullable=True)
    reason = Column(Text, nullable=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    deleted = Column(Boolean, default=False, nullable=False)

class StockAlerts(Base):
    __tablename__ = "stock_alerts"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    event_id = Column(Integer, ForeignKey("events.id"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    alert_type = Column(VARCHAR, nullable=False)  # CHECK: low_stock, out_of_stock, overstock, expiry
    is_resolved = Column(Boolean, nullable=False, default=False)
    resolved_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    deleted = Column(Boolean, default=False, nullable=False)
    

class AuditLog(Base):
    __tablename__="audit_log"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    table_name = Column(VARCHAR, nullable=False)
    record_id = Column(BigInteger, nullable=False)
    action = Column(VARCHAR, nullable=False)  # INSERT, UPDATE, DELETE, LOGIN, LOGOUT, etc.
    old_value = Column(JSONB, nullable=True)
    new_value = Column(JSONB, nullable=True)
    performed_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    ip_address = Column(VARCHAR(45), nullable=True)
    user_agent = Column(Text, nullable=True)
    performed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    

class Cameras(Base):
    __tablename__="cameras"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(VARCHAR, nullable=False)
    model = Column(VARCHAR, nullable=False)
    serial_number = Column(VARCHAR, unique=True, nullable=True)
    usb_port = Column(VARCHAR, nullable=True)
    firmware_version = Column(VARCHAR, nullable=True)
    capabilities = Column(JSONB, nullable=False, default={})
    is_active = Column(Boolean, default=True, nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    deleted = Column(Boolean, default=False, nullable=False)

class CamerasPresets(Base):
    __tablename__="camera_presets"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(VARCHAR, nullable=False, unique=True)
    description = Column(Text, nullable=True)
    camera_model = Column(VARCHAR, nullable=True)
    settings = Column(JSONB, nullable=False, default={})
    is_active = Column(Boolean, default=True, nullable=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    deleted = Column(Boolean, default=False, nullable=False)

class EventCameraConfig(Base):
    __tablename__="event_camera_config"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    event_id = Column(Integer, ForeignKey("events.id"), nullable=False)
    camera_id = Column(Integer, ForeignKey("cameras.id"), nullable=False)
    preset_id = Column(Integer, ForeignKey("camera_presets.id"), nullable=True)
    override_settings = Column(JSONB, default={}, nullable=True)
    is_active = Column(Boolean, default=True, nullable=True)
    applied_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    applied_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    deleted = Column(Boolean, default=False, nullable=False)

class SessionCameraSettings(Base):
    __tablename__="session_camera_settings"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("sessions.id"), nullable=False)
    event_id = Column(Integer, ForeignKey("events.id"), nullable=False)
    camera_id = Column(Integer, ForeignKey("cameras.id"), nullable=False)
    preset_id = Column(Integer, ForeignKey("camera_presets.id"), nullable=True)
    settings_used = Column(JSONB, nullable=False, default={})
    applied_via = Column(VARCHAR, default='preset', nullable=True)
    success = Column(Boolean, default=True, nullable=True)
    error_message = Column(Text, nullable=True)
    deleted = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
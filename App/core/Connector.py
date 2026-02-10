# App/core/Connector.py - CORRECTED VERSION
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy import text  # ADD THIS IMPORT
from typing import AsyncGenerator, Optional
from contextlib import asynccontextmanager
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

from App.core.settings import settings

logger = logging.getLogger(__name__)

class Database:
    """Professional PostgreSQL database connector"""
    
    def __init__(self, db_url: Optional[str] = None):
        self.db_url = db_url or settings.database_url
        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[async_sessionmaker] = None
        self._is_connected = False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def connect(self) -> None:
        """Connect to database with retry logic"""
        if self._is_connected:
            return
        
        try:
            # Mask password in logs
            safe_url = self.db_url
            if '@' in safe_url:
                parts = safe_url.split('@')
                if ':' in parts[0]:
                    user_pass = parts[0].split(':')
                    if len(user_pass) > 1:
                        safe_url = f"{user_pass[0]}:****@{parts[1]}"
            
            logger.info(f"Connecting to database: {safe_url}")
            
            pool_params = {
                "pool_size": 10,
                "max_overflow": 20,
                "pool_recycle": 1800,
                "pool_timeout": 30
            }
            
            # Create engine with proper configuration
            self._engine = create_async_engine(
                self.db_url,
                echo=settings.DATABASE_ECHO if hasattr(settings, 'DATABASE_ECHO') else False,
                poolclass=AsyncAdaptedQueuePool,
                pool_size=pool_params["pool_size"],
                max_overflow=pool_params["max_overflow"],
                pool_recycle=pool_params["pool_recycle"],
                pool_timeout=pool_params["pool_timeout"],
                pool_pre_ping=True,  # Verify connections before use
                connect_args={
                    "server_settings": {
                        "search_path": settings.DATABASE_SCHEMA if hasattr(settings, 'DATABASE_SCHEMA') else "public",
                        "application_name": settings.DATABASE_NAME if hasattr(settings, 'DATABASE_NAME') else "fastapi_app",
                        "timezone": "UTC"
                    },
                    "command_timeout": settings.DATABASE_CONNECT_TIMEOUT if hasattr(settings, 'DATABASE_CONNECT_TIMEOUT') else 30,
                },
                # Performance optimizations
                future=True,
                execution_options={
                    "isolation_level": "REPEATABLE READ"
                }
            )
            
            # Test connection - FIXED: Wrap raw SQL in text()
            async with self._engine.connect() as conn:
                await conn.execute(text("SELECT 1"))  # WRAP IN text()
                logger.debug("Database connection test successful")
            
            # Create session factory
            self._session_factory = async_sessionmaker(
                bind=self._engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autocommit=False,
                autoflush=False,
            )
            
            self._is_connected = True
            logger.info("✅ Database connected successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            self._is_connected = False
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from database"""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
            self._is_connected = False
            logger.info("Database disconnected")
    
    async def health_check(self) -> bool:
        """Check database health"""
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(text("SELECT 1"))  # WRAP IN text()
                return result.scalar() == 1
        except Exception as e:
            logger.warning(f"Database health check failed: {e}")
            return False
    
    @property
    def is_connected(self) -> bool:
        """Check if database is connected"""
        return self._is_connected
    
    @property
    def engine(self) -> AsyncEngine:
        """Get database engine"""
        if not self._engine:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._engine
    
    @property
    def session_factory(self) -> async_sessionmaker:
        """Get session factory"""
        if not self._session_factory:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._session_factory
    
    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Context manager for database sessions
        Automatically handles commit/rollback
        """
        if not self._is_connected:
            await self.connect()
        
        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"Database session error: {e}")
                raise
            finally:
                await session.close()
    
    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Context manager for transactions
        Provides stronger isolation
        """
        async with self.session() as session:
            async with session.begin():
                yield session

# SQLAlchemy Base for models
Base = declarative_base()

# Database instance (singleton for the application)
database = Database()

# FastAPI dependency
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency injection for database sessions
    Usage: async def endpoint(db: AsyncSession = Depends(get_db))
    """
    async with database.session() as session:
        yield session

# Repository base class
class BaseRepository:
    """Base repository for database operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def execute_raw(self, query: str, params: dict = None):
        """Execute raw SQL query"""
        from sqlalchemy import text
        return await self.session.execute(text(query), params or {})
# settings.py - PostgreSQL version
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr, Field, PostgresDsn, field_validator
from typing import Optional, Any
import os


class Settings(BaseSettings):

 

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Security
    SECRET_KEY: Optional[SecretStr] = Field(
        default=None,
        min_length=32,
        description="Secret key for JWT token signing"
    )
    
    ALGORITHM: str = Field(
        default="HS256",
        pattern="^(HS256|HS384|HS512|RS256|RS384|RS512|ES256|ES384|ES512|PS256|PS384|PS512)$"
    )
    
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(
        default=790,
        ge=1,
        le=10080,
        description="Access token expiration time in minutes"
    )
    
    # Advanced Production Features
    KILL_SWITCH_ENABLED: bool = Field(
        default=False,
        description="Global kill switch to disable the API (Maintenance Mode)"
    )
    
    RATE_LIMIT_DEFAULT: str = Field(
        default="100/minute",
        description="Default rate limit for all endpoints"
    )
    
    # PostgreSQL Configuration
    DATABASE_HOST: str = Field(
        default="localhost",
        description="PostgreSQL host"
    )
    
    DATABASE_PORT: str = Field(
        default="5432",
        pattern="^\d+$",
        description="PostgreSQL port"
    )
    
    DATABASE_USER: str = Field(
        default="postgres",
        description="PostgreSQL username"
    )
    
    DATABASE_PASSWORD: SecretStr = Field(
        default="",
        description="PostgreSQL password"
    )
    
    DATABASE_NAME: str = Field(
        default="myapp_db",
        description="PostgreSQL database name"
    )
    
    DATABASE_SCHEMA: str = Field(
        default="public",
        description="PostgreSQL schema"
    )
    
    # Async settings
    ASYNC_MODE: bool = Field(
        default=True,
        description="Enable async database operations"
    )
    
    # Connection Pool Settings
    DATABASE_POOL_SIZE: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Connection pool size"
    )
    
    DATABASE_MAX_OVERFLOW: int = Field(
        default=40,
        ge=0,
        description="Max overflow connections"
    )
    
    DATABASE_POOL_RECYCLE: int = Field(
        default=3600,
        ge=60,
        description="Connection recycle time in seconds"
    )
    
    DATABASE_POOL_TIMEOUT: int = Field(
        default=30,
        ge=1,
        description="Connection timeout in seconds"
    )
    
    DATABASE_ECHO: bool = Field(
        default=False,
        description="Enable SQL query logging"
    )
    
    # SSL Configuration
    DATABASE_SSLMODE: str = Field(
        default="prefer",
        pattern="^(disable|allow|prefer|require|verify-ca|verify-full)$",
        description="PostgreSQL SSL mode"
    )
    
    DATABASE_CONNECT_TIMEOUT: int = Field(
        default=10,
        ge=1,
        le=60,
        description="Connection timeout in seconds"
    )
    
    # Logging
    LOG_FILEPATH: str = Field(
        default="./logs/",
        description="Path to log files directory"
    )
    
    # Argon2 Hashing
    MEMORY_COST: int = Field(
        default=65536,
        ge=1024,
        le=131072,
        description="Memory cost for Argon2 hashing"
    )
    
    PARALLELISM: int = Field(
        default=2,
        ge=1,
        le=8,
        description="Parallelism factor for Argon2"
    )
    
    HASH_LENGTH: int = Field(
        default=32,
        ge=16,
        le=64,
        description="Hash length for Argon2"
    )
    SALT_LENGTH: int = Field(
        default=16,
        ge=8,
        le=64,
        description="Length of salt for password hashing"
    )
    @field_validator('SECRET_KEY', mode='before')
    @classmethod
    def validate_secret_key(cls, v: Any) -> Any:
        """Ensure SECRET_KEY is set in production"""
        env = os.getenv("ENVIRONMENT", "development")
        if env == "production" and (v is None or v == ""):
            raise ValueError("SECRET_KEY must be set in production")
        return v or "bca38b24a804aa37d821d31af00f5598230122c5bbfc4c4ad5ed40e4258f04ca"

    @field_validator('DATABASE_PASSWORD', mode='before')
    @classmethod
    def validate_password(cls, v: Any) -> Any:
        """Validate password is provided for production"""
        import os
        env = os.getenv("ENVIRONMENT", "development")
        
        if env == "production" and (v is None or v == ""):
            raise ValueError("DATABASE_PASSWORD must be set in production")
        
        return v
    
    @field_validator('RATE_LIMIT_DEFAULT', mode='before')
    @classmethod
    def validate_rate_limit(cls, v: Any) -> Any:
        """Ensure RATE_LIMIT_DEFAULT is in a valid format (e.g., '100/minute')"""
        if v is None:
            return "100/minute"
        v_str = str(v).strip()
        if v_str.isdigit():
            # If user provided a raw number, default it to per minute
            return f"{v_str}/minute"
        if "/" not in v_str:
            # Fallback if no unit provided
            return f"{v_str}/minute"
        return v_str
    
    @property
    def database_url(self) -> str:
        """Get PostgreSQL database URL"""
        password = self.DATABASE_PASSWORD.get_secret_value()
        
        # Construct the URL
        url = (
            f"postgresql+asyncpg://"
            f"{self.DATABASE_USER}:{password}@"
            f"{self.DATABASE_HOST}:{self.DATABASE_PORT}/"
            f"{self.DATABASE_NAME}"
        )
        
        # Add optional parameters
        params = []
        if self.DATABASE_SCHEMA != "public":
            params.append(f"search_path={self.DATABASE_SCHEMA}")
        if self.DATABASE_SSLMODE != "prefer":
            params.append(f"sslmode={self.DATABASE_SSLMODE}")
        if self.DATABASE_CONNECT_TIMEOUT != 10:
            params.append(f"connect_timeout={self.DATABASE_CONNECT_TIMEOUT}")
        
        if params:
            url += "?" + "&".join(params)
        
        return url
    
    @property
    def sync_database_url(self) -> str:
        """Get sync PostgreSQL database URL (for Alembic)"""
        password = self.DATABASE_PASSWORD.get_secret_value()
        
        url = (
            f"postgresql://"
            f"{self.DATABASE_USER}:{password}@"
            f"{self.DATABASE_HOST}:{self.DATABASE_PORT}/"
            f"{self.DATABASE_NAME}"
        )
        
        return url
    
    # Computed properties
    @property
    def secret_key_str(self) -> str:
        """Get the secret key as string (use carefully)"""
        return self.SECRET_KEY.get_secret_value()
    
    def get_argon2_params(self) -> dict:
        """Get Argon2 parameters as a dictionary"""
        return {
            "memory_cost": self.MEMORY_COST,
            "parallelism": self.PARALLELISM,
            "hash_len": self.HASH_LENGTH
        }
    
    def get_connection_pool_params(self) -> dict:
        """Get connection pool parameters"""
        return {
            "pool_size": self.DATABASE_POOL_SIZE,
            "max_overflow": self.DATABASE_MAX_OVERFLOW,
            "pool_recycle": self.DATABASE_POOL_RECYCLE,
            "pool_timeout": self.DATABASE_POOL_TIMEOUT,
        }


# Create singleton instance
settings = Settings()
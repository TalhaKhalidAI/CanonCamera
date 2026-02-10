# App/api/dependencies/auth.py - OPTIMIZED VERSION WITH OAUTH2
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from argon2 import PasswordHasher, exceptions as argon2_exceptions
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from App.core.settings import settings
from App.core.Connector import get_db
from App.repository.UserRepository import UserRepository
from App.core.LoggingInit import get_core_logger
from fastapi.security import HTTPAuthorizationCredentials  # Add this import

# Initialize logger
logger = get_core_logger(__name__)

# Password hasher
pwd_context = PasswordHasher(
    memory_cost=settings.MEMORY_COST,
    parallelism=settings.PARALLELISM,
    hash_len=settings.HASH_LENGTH,
    salt_len=settings.SALT_LENGTH
)

# Use OAuth2PasswordBearer for standard OAuth2 flows
oauth2_scheme =HTTPBearer()

# ========== PASSWORD FUNCTIONS ==========

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify password using Argon2"""
    try:
        return pwd_context.verify(hash=hashed_password, password=plain_password)
    except argon2_exceptions.VerifyMismatchError:
        return False
    except Exception as e:
        logger.error(f"Password verification error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Password verification failed"
        )

def get_password_hash(password: str) -> str:
    """Hash password using Argon2"""
    return pwd_context.hash(password=password)

# ========== TOKEN FUNCTIONS ==========

def create_access_token(
    data: Dict[str, Any], 
    expires_delta: Optional[timedelta] = None
) -> str:
    """Create JWT access token"""
    try:
        to_encode = data.copy()
        
        expire = datetime.now(timezone.utc) + (
            expires_delta if expires_delta else timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "type": "access"
        })
        
        encoded_jwt = jwt.encode(
            to_encode, 
            settings.secret_key_str, 
            algorithm=settings.ALGORITHM
        )
        
        logger.debug(f"Created access token for user: {data.get('sub', 'unknown')}")
        return encoded_jwt
        
    except Exception as e:
        logger.error(f"Token creation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create access token"
        )

def decode_jwt(token: str) -> Optional[Dict[str, Any]]:
    """Decode and validate JWT token - FIXED"""
    try:
        print(f"Decoding token: {token[:30]}...")  # Show first 30 chars for debugging
        
        # Decode with verification
        payload = jwt.decode(
            token, 
            settings.secret_key_str, 
            algorithms=[settings.ALGORITHM]
        )
        
        # Log token type for debugging
        token_type = payload.get("type", "unknown")
        print(f"Token decoded successfully. Type: {token_type}")
        
        return payload
        
    except jwt.ExpiredSignatureError:
        print("Token has expired")
        logger.debug("Token expired")
        return None
    except jwt.JWTError as e:
        print(f"JWT Error: {e}")
        logger.debug(f"JWT decode failed: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
        logger.error(f"Unexpected token decode error: {e}")
        return None

# ========== DEPENDENCY INJECTIONS ==========
bearer_scheme = HTTPBearer(auto_error=False)  # auto_error=False allows optional auth
async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get current authenticated user from token - FIXED"""
    
    # Check if token was provided
    if not credentials:
        logger.warning("No authorization credentials provided")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Extract token from credentials object
    token = credentials.credentials
    print(f"Extracted token: {token[:30]}...")
    
    # Decode token
    payload = decode_jwt(token)
    
    if payload is None:
        logger.warning("Invalid or malformed token received")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or malformed token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Check token type - REJECT REFRESH TOKENS!
    token_type = payload.get("type")
    if token_type == "refresh":
        logger.warning("Refresh token used for authentication")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh tokens cannot be used for authentication. Use an access token.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Check expiration
    exp = payload.get("exp")
    if exp is None:
        logger.warning("Token has no expiration time")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has no expiration",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    expiration_datetime = datetime.fromtimestamp(exp, timezone.utc)
    if expiration_datetime <= datetime.now(timezone.utc):
        logger.warning(f"Expired token used: expired at {expiration_datetime}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Get user info from token
    user_id = payload.get("user_id")
    if not user_id:
        logger.warning("Token missing user_id")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Get user from database using repository
    try:
        repo = UserRepository(db)
        user = await repo.get_by_id(user_id)
        
        if not user:
            logger.warning(f"User not found for ID: {user_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        if user.disabled or not user.is_active:
            logger.warning(f"Inactive user tried to authenticate: {user_id}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Account is disabled"
            )
        
        logger.debug(f"Authenticated user: {user.email} (ID: {user.id})")
        
        return {
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "role": user.user_role,
            "is_active": user.is_active,
            "disabled": user.disabled
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting current user: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication service error"
        )

async def get_current_active_user(
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Check if current user is active"""
    if current_user.get("disabled") or not current_user.get("is_active"):
        logger.warning(f"Inactive user access attempted: {current_user.get('email')}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user"
        )
    return current_user

async def get_admin_user(
    current_user: Dict[str, Any] = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """Check if current user is admin"""
    if current_user.get("role") != "admin":
        logger.warning(f"Non-admin user tried admin action: {current_user.get('email')}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions"
        )
    return current_user

# ========== HELPER FUNCTIONS ==========

async def authenticate_user(
    uname: str,
    password: str,
    db: AsyncSession
) -> Optional[Dict[str, Any]]:
    """Authenticate user by username and password - UPDATED"""
    try:
        repo = UserRepository(db)
        user = await repo.get_by_name(uname)
        
        # Check if user exists
        if not user:
            logger.debug(f"Authentication failed: user not found - {uname}")
            return None
        
        # Verify password
        if not verify_password(password, user.password_hash):
            logger.debug(f"Authentication failed: wrong password - {uname}")
            return None
        
        # Check if active
        if user.disabled or not user.is_active:
            logger.debug(f"Authentication failed: account disabled - {uname}")
            return None
        
        logger.info(f"User authenticated successfully: {uname}")
        
        return {
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "role": user.user_role
        }
        
    except Exception as e:
        logger.error(f"Authentication error for {uname}: {e}")
        return None

 
# ========== ADDITIONAL UTILITIES ==========

def validate_password_strength(password: str) -> bool:
    """Validate password strength with comprehensive checks"""
    if len(password) < 8:
        return False
    
    # Check for at least one uppercase letter
    if not any(c.isupper() for c in password):
        return False
    
    # Check for at least one lowercase letter
    if not any(c.islower() for c in password):
        return False
    
    # Check for at least one digit
    if not any(c.isdigit() for c in password):
        return False
    
    # Check for at least one special character
    special_chars = '!@#$%^&*()_+-=[]{}|;:,.<>?`~'
    if not any(c in special_chars for c in password):
        return False
    
    return True

def create_refresh_token(data: Dict[str, Any]) -> str:
    """Create refresh token (longer expiry)"""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(days=7)  # 7 days
    
    to_encode.update({
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "type": "refresh"
    })
    
    return jwt.encode(
        to_encode, 
        settings.secret_key_str, 
        algorithm=settings.ALGORITHM
    )

async def refresh_access_token(refresh_token: str, db: AsyncSession) -> Optional[str]:
    """Refresh access token using refresh token"""
    try:
        payload = decode_jwt(refresh_token)
        if not payload or payload.get("type") != "refresh":
            return None
        
        user_id = payload.get("user_id")
        if not user_id:
            return None
        
        repo = UserRepository(db)
        user = await repo.get_by_id(user_id)
        
        if not user or user.disabled or not user.is_active:
            return None
        
        # Create new access token
        new_access_token = create_access_token({
            "sub": user.email,
            "user_id": user.id,
            "name": user.name,
            "role": user.user_role
        })
        
        return new_access_token
        
    except Exception as e:
        logger.error(f"Token refresh error: {e}")
        return None

# Additional dependency for optional authentication
async def get_current_user_optional(
    token: Optional[str] = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db)
) -> Optional[Dict[str, Any]]:
    """Optional authentication - returns user if authenticated, None otherwise"""
    if not token:
        return None
    
    try:
        return await get_current_user(token, db)
    except HTTPException:
        return None
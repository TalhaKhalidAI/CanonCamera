"""
Authentication and authorization dependencies.

Exports:
    - verify_password: Verify Argon2 hashed passwords
    - get_password_hash: Hash passwords with Argon2
    - create_access_token: Create JWT tokens
    - get_current_user: FastAPI dependency for authenticated users
    - get_current_active_user: Dependency for active users only
    - get_admin_user: Dependency for admin users only
    - authenticate_user: Authenticate by email/password
    - oauth2_scheme: HTTPBearer scheme
"""

# ✅ Re-export everything from auth.py
from .auth import (
    # Password functions
    verify_password,
    get_password_hash,
    
    # Token functions
    create_access_token,
    decode_jwt,
    
    # FastAPI dependencies
    get_current_user,
    get_current_active_user,
    get_admin_user,
    
    # Authentication
    authenticate_user,
    
    # Security scheme
    oauth2_scheme,
    
    # Additional utilities (if you add them)
    validate_password_strength,
    create_refresh_token,
    refresh_access_token,
)

# ✅ Define __all__ for explicit exports
__all__ = [
    # Password
    "verify_password",
    "get_password_hash",
    
    # Token
    "create_access_token",
    "decode_jwt",
    
    # Dependencies
    "get_current_user",
    "get_current_active_user",
    "get_admin_user",
    
    # Authentication
    "authenticate_user",
    
    # Scheme
    "oauth2_scheme",
    
    # Utilities
    "validate_password_strength",
    "create_refresh_token",
    "refresh_access_token",
]

# ✅ Version info
__version__ = "1.0.0"
__author__ = "IoT Noob"
__description__ = "Authentication and authorization dependencies for FastAPI"
"""
Authentication and authorization dependencies.

Exports:
    - Pydantic model for logni signup etc
    - SIgn up and other
  
"""

# ✅ Re-export everything from auth.py
from .UserAuthModel import (
    User,
    UpdateUser,
    UserResponse,
    LoginUser
)

# ✅ Define __all__ for explicit exports
__all__ = [
"User",
"UpdateUser",
"UserResponse",
"LoginUser"
]

# ✅ Version info
__version__ = "1.0.0"
__author__ = "IoT Noob"
__description__ = "Pydantic model FastAPI"
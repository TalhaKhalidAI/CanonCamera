from pydantic import BaseModel,EmailStr,ConfigDict
from typing import Optional


class TokenResponse(BaseModel):
    """Response model for authentication tokens"""
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "bearer"
    expires_in: int


class UpdateUser(BaseModel):
    name: str | None = None
    email: EmailStr | None = None
    password: str | None = None
    profile_pic: str | None = None
    disable: bool | None = None
    user_role: str | None = None

class UserResponse(BaseModel):
    """For API responses"""
    model_config = ConfigDict(from_attributes=True)  # Add this
    
    id: int
    name: str
    email: EmailStr
    profile_pic: Optional[str] = None  # Make this Optional
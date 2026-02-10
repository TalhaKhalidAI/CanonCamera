from pydantic import BaseModel, EmailStr, field_validator,SecretStr,ConfigDict
from typing import Optional
import re

class LoginUser(BaseModel):
    """Login model with proper validation"""
    username: str
    password: SecretStr
    
    @field_validator("username")
    def validate_username(cls, v):
        """Validate username/email format"""
        if not v or not v.strip():
            raise ValueError("Username cannot be empty")
        
        v = v.strip()
        
        # Check if it looks like an email
        if '@' in v:
            email_regex = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
            if not email_regex.match(v):
                raise ValueError("Invalid email format")
        
        return v
    
    @field_validator("password", mode='before')
    def validate_password(cls, v):
        """Validate password before converting to SecretStr"""
        if not v:
            raise ValueError("Password cannot be empty")
        
        # Convert to string if it's not already
        if not isinstance(v, str):
            v = str(v)
        
        if len(v) < 1:
            raise ValueError("Password cannot be empty")
        
        # Optionally add password strength checks
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters long")
        
        return v

class User(BaseModel):
    name: str
    email: EmailStr
    password: str
    profile_pic: str = None
 

    @field_validator("name")
    def name_must_be_non_empty(cls, v):
        if not v:
            raise ValueError("Name cannot be empty")
        return v

    @field_validator("email")
    def email_must_be_valid_format(cls, v):
        email_regex = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
        if not email_regex.match(v):
            raise ValueError("Invalid email format")
        return v

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
# App/api/v1/UserAuth.py - PROFESSIONAL REFACTORED VERSION

from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from App.schemas.AuthScheema import TokenResponse
from sqlalchemy.ext.asyncio import AsyncSession
from App.core.LoggingInit import get_core_logger
from App.core.Connector import get_db
from App.models.UserAuthModel import User,UpdateUser,LoginUser
from App.schemas.AuthScheema import UserResponse
from App.api.dependencies.auth import (
    authenticate_user,
    create_access_token,
    create_refresh_token,

    get_password_hash,

    validate_password_strength
)
from App.repository.UserRepository import UserRepository

# Initialize logger
logger = get_core_logger(__name__)

# Pydantic Models (Request/Response schemas)
 




# Create router
router = APIRouter(prefix="/basic_auth", tags=["Authentication"])

@router.post(
    "/login",
    status_code=status.HTTP_200_OK,
    summary="User login",
    description="Authenticate user with email and password"
)
async def login(
    form_data: LoginUser,
    db: AsyncSession = Depends(get_db)
):
    """
    Login endpoint supporting OAuth2 password flow.
    Returns access and refresh tokens.
    """
    try:
        # Get password from SecretStr - FIX THIS
        password = form_data.password.get_secret_value()
        
        # Authenticate user - USE username (not name)
        user = await authenticate_user(form_data.username, password, db)
         
        if not user:
            logger.warning(f"Failed login attempt for email: {form_data.username}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Create tokens
        access_token = create_access_token(
            data={
                "sub": user["email"],
                "user_id": user["id"],
                "name": user["name"],
                "role": user["role"]
            }
        )
        
        refresh_token = create_refresh_token(
            data={
                "sub": user["email"],
                "user_id": user["id"]
            }
        )
        
        logger.info(f"User logged in successfully: {user['email']}")
        
        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=3600  # 1 hour
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during login"
        )

# App/api/v1/UserAuth.py - FIXED VERSION
@router.post(
    "/signup",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register new user",
    description="Create a new user account"
)
async def signup(
    user_data: User,
    db: AsyncSession = Depends(get_db)
):
    """Register a new user"""
    try:
        repo = UserRepository(db)
        
        # Check if user exists
        existing_user = await repo.get_by_email(user_data.email)
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
        
        # Create user data dictionary
        user_dict = user_data.model_dump()
        
        # Validate and hash password
        if "password" not in user_dict:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password is required"
            )
        
        # Validate password strength
        if not validate_password_strength(user_dict["password"]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must be at least 8 characters with uppercase, lowercase, digit, and special character"
            )
        
        user_dict["password_hash"] = get_password_hash(user_dict["password"])
        del user_dict["password"]  # Remove plain password
        
        # Set default values
        user_dict["user_role"] = "user"
        user_dict["is_active"] = True
        
        user = await repo.create(user_dict)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create user"
            )
        
        logger.info(f"New user registered: {user.email}")
        
        # FIX: Convert SQLAlchemy model to dictionary before validation
        user_dict_response = {
            "id": user.id,
            "name": user.name,
            "email": user.email,
            "profile_pic": user.profile_pic or None  # Ensure None if empty
        }
        
        # Validate with UserResponse model
        return UserResponse(**user_dict_response)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Signup error: {e}")
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Registration failed: {str(e)}"
        )

import sys
import os
import asyncio
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException

# Add the project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mocking modules that might cause import errors or need hardware
sys.modules['App.core.LoggingInit'] = MagicMock()
sys.modules['App.core.Connector'] = MagicMock()

async def verify_deleted_user_blocked():
    print("Verifying that deleted users are blocked in auth.py...")
    
    # Import the dependency
    from App.api.dependencies.auth import get_current_user
    
    # Mock credentials and DB
    mock_credentials = MagicMock()
    mock_credentials.credentials = "fake_token"
    mock_db = AsyncMock()
    
    # Mock payload decoding
    from datetime import datetime, timedelta, timezone
    future_exp = (datetime.now(timezone.utc) + timedelta(hours=1)).timestamp()
    
    mock_payload = {
        "user_id": 123,
        "type": "access",
        "exp": future_exp
    }
    
    with MagicMock() as mock_jwt:
        import App.api.dependencies.auth as auth_mod
        auth_mod.decode_jwt = MagicMock(return_value=mock_payload)
        
        # Mock Repository and deleted User
        mock_user = MagicMock()
        mock_user.id = 123
        mock_user.email = "deleted@example.com"
        mock_user.deleted = True  # THE FLAG WE ARE TESTING
        mock_user.disabled = False
        mock_user.is_active = True
        
        mock_repo = MagicMock()
        mock_repo.get_by_id = AsyncMock(return_value=mock_user)
        
        with MagicMock() as mock_repo_class:
            auth_mod.UserRepository = MagicMock(return_value=mock_repo)
            
            try:
                await get_current_user(credentials=mock_credentials, db=mock_db)
                print("❌ FAIL: get_current_user allowed a deleted user!")
                return False
            except HTTPException as exc:
                if exc.status_code == 403 and "deleted" in exc.detail.lower():
                    print(f"✅ PASS: get_current_user blocked deleted user with: {exc.detail}")
                else:
                    print(f"❌ FAIL: get_current_user raised wrong error: {exc.status_code} - {exc.detail}")
                    return False
    return True

if __name__ == "__main__":
    success = asyncio.run(verify_deleted_user_blocked())
    if not success:
        sys.exit(1)

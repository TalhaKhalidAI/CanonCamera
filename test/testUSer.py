# test_auth_correct.py
import asyncio
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from App.core.Connector import database
from App.api.dependencies.auth import authenticate_user

async def test_auth():
    """Test authentication directly"""
    try:
        print("🔍 Testing authentication...")
        
        # Connect to database
        await database.connect()
        print("✅ Database connected")
        
        # Create a session using database.session() context manager
        async with database.session() as session:
            # Now test authenticate_user with the session
            result = await authenticate_user("talha", "Talha@6295", session)
            print(f"🔑 Authentication result: {result}")
            
            if result:
                print("✅ SUCCESS! User authenticated")
                print(f"   User ID: {result.get('id')}")
                print(f"   Name: {result.get('name')}")
                print(f"   Email: {result.get('email')}")
                print(f"   Role: {result.get('role')}")
            else:
                print("❌ FAILED! Authentication returned None")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await database.disconnect()
        print("✅ Database disconnected")

if __name__ == "__main__":
    asyncio.run(test_auth())
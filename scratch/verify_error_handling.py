import sys
import os
from unittest.mock import MagicMock
from fastapi import HTTPException

# Add the project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the helper we modified
from App.api.v1.CamControl import _map_hardware_error

def test_map_hardware_error_propagates_http_exception():
    print("Testing _map_hardware_error propagation...")
    exc = HTTPException(status_code=403, detail="Not enough permissions")
    mapped = _map_hardware_error(exc)
    
    assert mapped is exc
    assert mapped.status_code == 403
    assert mapped.detail == "Not enough permissions"
    print("✓ _map_hardware_error propagates HTTPException correctly.")

def test_map_hardware_error_handles_other_exceptions():
    print("Testing _map_hardware_error handling unknown exceptions...")
    exc = Exception("Something went wrong")
    mapped = _map_hardware_error(exc)
    
    assert isinstance(mapped, HTTPException)
    assert mapped.status_code == 500
    assert mapped.detail == "An internal error occurred. Check server logs."
    print("✓ _map_hardware_error converts unknown exceptions to 500.")

if __name__ == "__main__":
    try:
        test_map_hardware_error_propagates_http_exception()
        test_map_hardware_error_handles_other_exceptions()
        print("\nAll tests passed locally!")
    except AssertionError as e:
        print(f"\nTest failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nAn error occurred during testing: {e}")
        sys.exit(1)

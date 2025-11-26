"""
Simple import test to identify the exact import issue
"""

def test_imports():
    print("Testing imports step by step...")
    
    try:
        print("1. Testing basic FastAPI imports...")
        from fastapi import APIRouter, Request, HTTPException
        print("✅ FastAPI imports successful")
    except Exception as e:
        print(f"❌ FastAPI imports failed: {e}")
        return
    
    try:
        print("2. Testing backend package...")
        import backend
        print("✅ Backend package import successful")
    except Exception as e:
        print(f"❌ Backend package import failed: {e}")
        return
    
    try:
        print("3. Testing auth package...")
        import backend.auth
        print("✅ Auth package import successful")
    except Exception as e:
        print(f"❌ Auth package import failed: {e}")
        return
    
    try:
        print("4. Testing auth utils...")
        from backend.auth.service_principal_utils import log_api_request
        print("✅ Auth utils import successful")
    except Exception as e:
        print(f"❌ Auth utils import failed: {e}")
        return
    
    try:
        print("5. Testing auth routes...")
        from backend.auth.routes import router
        print("✅ Auth routes router import successful")
    except Exception as e:
        print(f"❌ Auth routes router import failed: {e}")
        return
    
    try:
        print("6. Testing auth function...")
        from backend.auth.routes import get_authenticated_token
        print("✅ Auth function import successful")
    except Exception as e:
        print(f"❌ Auth function import failed: {e}")
        return
    
    try:
        print("7. Testing FDB routes...")
        from backend.fdb.routes import router as fdb_router
        print("✅ FDB routes import successful")
    except Exception as e:
        print(f"❌ FDB routes import failed: {e}")
        return
    
    print("✅ All imports successful!")

if __name__ == "__main__":
    test_imports()
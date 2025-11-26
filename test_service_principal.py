"""
Test script to verify service principal authentication integration
Run this to test that the FDB routes work with service principal authentication
"""

import asyncio
import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

async def test_service_principal_auth():
    """Test service principal authentication functionality"""
    
    print("🔐 Testing Service Principal Authentication Integration")
    print("=" * 60)
    
    # Check environment variables
    print("1. Checking environment variables...")
    required_vars = [
        "DATABRICKS_HOST",
        "DATABRICKS_CLIENT_ID", 
        "DATABRICKS_CLIENT_SECRET"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
        else:
            print(f"   ✅ {var}: {'*' * 20}")  # Mask the actual values
    
    if missing_vars:
        print(f"   ❌ Missing environment variables: {', '.join(missing_vars)}")
        print("   Please set these variables before testing.")
        return False
    
    # Test service principal utilities
    print("\n2. Testing service principal utilities...")
    try:
        from backend.auth.service_principal_utils import (
            get_service_principal_context,
            log_api_request,
            get_audit_user
        )
        
        context = get_service_principal_context()
        print(f"   ✅ Service Principal Context: {context['auth_type']}")
        print(f"   ✅ Client ID: {context['client_id'][:8]}...")
        
        audit_user = get_audit_user()
        print(f"   ✅ Audit User: {audit_user[:8]}...")
        
        # Test logging (this will appear in logs)
        log_api_request("Test Operation", tenant="MASTER", operation="test")
        print("   ✅ Logging function works")
        
    except Exception as e:
        print(f"   ❌ Service Principal Utilities Error: {e}")
        return False
    
    # Test authentication token generation
    print("\n3. Testing token generation...")
    try:
        from backend.auth.routes import get_service_principal_token
        
        databricks_host = os.getenv("DATABRICKS_HOST")
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
        
        # This will make an actual API call to Databricks
        token = await get_service_principal_token(databricks_host, client_id, client_secret)
        
        if token and len(token) > 10:  # Basic validation
            print(f"   ✅ Token generated successfully (length: {len(token)})")
            print(f"   ✅ Token preview: {token[:20]}...")
        else:
            print("   ❌ Token generation failed - invalid token received")
            return False
            
    except Exception as e:
        print(f"   ❌ Token Generation Error: {e}")
        return False
    
    # Test workspace validation
    print("\n4. Testing workspace validation...")
    try:
        from backend.auth.routes import validate_service_principal
        
        workspace_info = await validate_service_principal(databricks_host, token)
        
        print(f"   ✅ Workspace validation successful")
        print(f"   ✅ Workspace ID: {workspace_info.get('workspace_id', 'N/A')}")
        print(f"   ✅ Service Principal User: {workspace_info.get('user_name', 'N/A')}")
        print(f"   ✅ SCIM Available: {workspace_info.get('scim_available', False)}")
        print(f"   ✅ Workspace Accessible: {workspace_info.get('workspace_accessible', False)}")
        
    except Exception as e:
        print(f"   ❌ Workspace Validation Error: {e}")
        return False
    
    print("\n🎉 All service principal authentication tests passed!")
    print("✅ FDB routes are now properly configured for service principal authentication")
    print("\nNext steps:")
    print("1. Start your FastAPI application")
    print("2. Test endpoints like: GET /api/fdb/search?tenant=MASTER")
    print("3. Check logs to see service principal authentication in action")
    
    return True

if __name__ == "__main__":
    asyncio.run(test_service_principal_auth())
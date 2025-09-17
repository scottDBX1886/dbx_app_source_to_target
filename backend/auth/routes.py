"""
Authentication routes and logic
"""
import os
from fastapi import APIRouter, Request
import httpx
import logging

logger = logging.getLogger(__name__)

# Create router for auth endpoints
router = APIRouter(prefix="/api", tags=["authentication"])

async def get_real_user_info(databricks_host: str, user_token: str, email: str = None):
    """Get real user information from Databricks API with detailed debugging"""
    debug_info = {
        "api_url": f"{databricks_host}/api/2.0/preview/scim/v2/Me",
        "has_token": bool(user_token),
        "token_length": len(user_token) if user_token else 0,
        "host": databricks_host
    }
    
    try:
        headers = {"Authorization": f"Bearer {user_token}", "Content-Type": "application/json"}
        
        async with httpx.AsyncClient() as client:
            # Get current user info
            response = await client.get(
                f"{databricks_host}/api/2.0/preview/scim/v2/Me",
                headers=headers,
                timeout=10.0
            )
            
            debug_info["status_code"] = response.status_code
            debug_info["response_headers"] = dict(response.headers)
            
            if response.status_code == 200:
                user_data = response.json()
                debug_info["response_keys"] = list(user_data.keys())
                
                # Extract real groups from SCIM response
                groups = []
                if "groups" in user_data:
                    groups = [group.get("display", group.get("$ref", "Unknown")) 
                             for group in user_data["groups"]]
                
                return {
                    "success": True,
                    "user_info": {
                        "user_id": user_data.get("id", "unknown"),
                        "email": user_data.get("emails", [{}])[0].get("value", email or "unknown"),
                        "display_name": user_data.get("displayName", "Unknown User"),
                        "user_name": user_data.get("userName", email or "unknown"),
                        "active": user_data.get("active", True),
                        "groups": groups
                    },
                    "debug_info": debug_info
                }
            else:
                debug_info["error"] = f"API returned {response.status_code}"
                debug_info["response_text"] = response.text[:200]
                return {
                    "success": False,
                    "error": f"User API call failed with status {response.status_code}",
                    "debug_info": debug_info
                }
                
    except Exception as e:
        import traceback
        debug_info["exception_type"] = type(e).__name__
        debug_info["exception_message"] = str(e)
        debug_info["traceback"] = traceback.format_exc()[:500]
        
        return {
            "success": False,
            "error": f"API_ERROR_UNKNOWN: {str(e)}",
            "debug_info": debug_info
        }

@router.get("/health/user-info")
async def get_user_info(request: Request):
    """Get user information with authentication status"""
    headers = dict(request.headers)
    
    # Get Databricks forwarded headers
    email = headers.get("x-forwarded-email")
    user_token = headers.get("x-forwarded-access-token") 
    databricks_host = request.url.hostname
    
    # Build full databricks host URL
    if databricks_host:
        databricks_host = f"https://{databricks_host}"
    
    # Simple return for missing auth
    if not user_token or not email:
        return {
            "user_info": {
                "user_id": None,
                "email": email,
                "display_name": email or "Unknown",
                "groups": []
            },
            "authenticated": False,
            "app": "Gainwell Main App"
        }
    
    # Try to get user info from Databricks API
    try:
        headers_api = {"Authorization": f"Bearer {user_token}"}
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{databricks_host}/api/2.0/preview/scim/v2/Me",
                headers=headers_api,
                timeout=10.0
            )
            
            if response.status_code == 200:
                user_data = response.json()
                
                # Extract groups
                groups = []
                if "groups" in user_data:
                    groups = [group.get("display", "Unknown Group") for group in user_data["groups"]]
                
                return {
                    "user_info": {
                        "user_id": user_data.get("id"),
                        "email": user_data.get("emails", [{}])[0].get("value", email),
                        "display_name": user_data.get("displayName", email),
                        "user_name": user_data.get("userName", email),
                        "groups": groups
                    },
                    "authenticated": True,
                    "app": "Gainwell Main App"
                }
            else:
                # API call failed, return basic info
                return {
                    "user_info": {
                        "user_id": None,
                        "email": email,
                        "display_name": email,
                        "groups": ["api-error"]
                    },
                    "authenticated": True,
                    "app": "Gainwell Main App",
                    "error": f"API returned {response.status_code}"
                }
                
    except Exception as e:
        logger.error(f"Auth API error: {e}")
        return {
            "user_info": {
                "user_id": None,
                "email": email,
                "display_name": email,
                "groups": ["exception"]
            },
            "authenticated": True,
            "app": "Gainwell Main App",
            "error": str(e)
        }

@router.get("/debug/headers")
async def debug_headers(request: Request):
    """Debug endpoint to inspect all headers"""
    return {
        "headers": dict(request.headers),
        "client": request.client.host if request.client else None,
        "url": str(request.url)
    }

@router.get("/debug/simple-user")
async def debug_simple_user(request: Request):
    """Simple debug endpoint to check user headers only"""
    headers = dict(request.headers)
    
    return {
        "status": "debug_working",
        "x_forwarded_email": headers.get("x-forwarded-email"),
        "x_forwarded_access_token": "present" if headers.get("x-forwarded-access-token") else "missing",
        "x_forwarded_host": headers.get("x-forwarded-host"),
        "x_databricks_user_id": headers.get("x-databricks-user-id"),
        "user_agent": headers.get("user-agent"),
        "all_x_headers": {k: v for k, v in headers.items() if k.lower().startswith('x-')}
    }

@router.get("/debug/test-api")
async def debug_api_call(request: Request):
    """Debug endpoint to test API connectivity step-by-step"""
    headers = dict(request.headers)
    user_token = headers.get("x-forwarded-access-token")
    databricks_host = headers.get("x-forwarded-host")
    
    if not databricks_host or not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}" if databricks_host else None
    
    result = {
        "email": headers.get("x-forwarded-email"),
        "has_user_token": bool(user_token),
        "token_preview": user_token[:20] + "..." if user_token else None,
        "databricks_host": databricks_host,
        "tests": {}
    }
    
    if not user_token:
        result["error"] = "No user token available"
        return result
    
    # Test various endpoints
    test_endpoints = [
        ("clusters_list", f"{databricks_host}/api/2.0/clusters/list"),
        ("scim_me", f"{databricks_host}/api/2.0/preview/scim/v2/Me"),
        ("scim_users_me", f"{databricks_host}/api/2.0/preview/scim/v2/Users/me")
    ]
    
    for test_name, test_url in test_endpoints:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    test_url,
                    headers={"Authorization": f"Bearer {user_token}"},
                    timeout=5.0
                )
                result["tests"][test_name] = {
                    "status_code": response.status_code,
                    "success": response.status_code == 200,
                    "response_preview": str(response.text[:100])
                }
        except Exception as e:
            result["tests"][test_name] = {
                "error": str(e),
                "exception_type": type(e).__name__
            }
    
    return result

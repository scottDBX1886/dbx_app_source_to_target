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

async def get_scim_debug_info(databricks_host: str, user_token: str, email: str = None):
    """Get SCIM debug information to understand group issues"""
    if not user_token:
        return {"error": "No user token"}
    
    result = {
        "databricks_host": databricks_host,
        "email": email,
        "scim_url": f"{databricks_host}/api/2.0/preview/scim/v2/Me"
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{databricks_host}/api/2.0/preview/scim/v2/Me",
                headers={"Authorization": f"Bearer {user_token}", "Content-Type": "application/json"},
                timeout=10.0
            )
            
            result["status_code"] = response.status_code
            result["response_headers"] = dict(response.headers)
            
            if response.status_code == 200:
                user_data = response.json()
                result["response_keys"] = list(user_data.keys())
                result["full_response"] = user_data
                
                # Check groups specifically
                if "groups" in user_data:
                    result["groups_found"] = True
                    result["groups_count"] = len(user_data["groups"])
                    result["groups_raw"] = user_data["groups"]
                else:
                    result["groups_found"] = False
                    result["groups_count"] = 0
                
            else:
                result["error"] = f"API returned {response.status_code}"
                result["response_text"] = response.text
                
    except Exception as e:
        result["exception"] = str(e)
        result["exception_type"] = type(e).__name__
    
    return result

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
    """Get user information with authentication status - ORIGINAL WORKING VERSION"""
    headers = dict(request.headers)
    
    # Get Databricks forwarded headers
    email = headers.get("x-forwarded-email")
    user_token = headers.get("x-forwarded-access-token") 
    databricks_host = headers.get("x-forwarded-host")
    
    if not databricks_host or not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}" if databricks_host else None
    
    # Debug info
    debug_data = {
        "email": email,
        "has_user_token": bool(user_token),
        "token_preview": user_token[:20] + "..." if user_token else None,
        "databricks_host": databricks_host
    }
    
    if not user_token:
        return {
            "user_info": {
                "user_id": None,
                "email": email,
                "display_name": None,
                "groups": ["no-token"]
            },
            "authenticated": False,
            "app": "Gainwell Main App",
            "debug_info": debug_data
        }
    
    # Get real user data from Databricks - ORIGINAL WORKING APPROACH
    result = await get_real_user_info(databricks_host, user_token, email)
    
    # Also get the SCIM debug info for troubleshooting
    scim_debug = await get_scim_debug_info(databricks_host, user_token, email)
    
    if result["success"]:
        return {
            "user_info": result["user_info"],
            "authenticated": True,
            "app": "Gainwell Main App",
            "has_user_token": True,
            "authorization_type": "hybrid",
            "data_source": "databricks_api",
            "debug_info": result.get("debug_info", {}),
            "scim_debug": scim_debug  # Add SCIM debug info
        }
    else:
        return {
            "user_info": {
                "user_id": None,
                "email": email,
                "display_name": None,
                "groups": ["api-error"]
            },
            "authenticated": False,
            "app": "Gainwell Main App",
            "error": result["error"],
            "debug_info": result.get("debug_info", {}),
            "scim_debug": scim_debug  # Add SCIM debug info even on error
        }

@router.get("/debug/headers")
async def debug_headers(request: Request):
    """Debug endpoint to inspect all headers"""
    return {
        "headers": dict(request.headers),
        "client": request.client.host if request.client else None,
        "url": str(request.url)
    }

@router.get("/debug/scim-raw")
async def debug_scim_raw(request: Request):
    """Debug: Show exactly what SCIM API returns"""
    headers = dict(request.headers)
    
    email = headers.get("x-forwarded-email")
    user_token = headers.get("x-forwarded-access-token") 
    databricks_host = headers.get("x-forwarded-host")
    
    if not databricks_host or not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}" if databricks_host else None
    
    if not user_token:
        return {"error": "No user token"}
    
    result = {
        "databricks_host": databricks_host,
        "email": email,
        "scim_url": f"{databricks_host}/api/2.0/preview/scim/v2/Me"
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{databricks_host}/api/2.0/preview/scim/v2/Me",
                headers={"Authorization": f"Bearer {user_token}", "Content-Type": "application/json"},
                timeout=10.0
            )
            
            result["status_code"] = response.status_code
            result["response_headers"] = dict(response.headers)
            
            if response.status_code == 200:
                user_data = response.json()
                result["response_keys"] = list(user_data.keys())
                result["full_response"] = user_data
                
                # Check groups specifically
                if "groups" in user_data:
                    result["groups_found"] = True
                    result["groups_count"] = len(user_data["groups"])
                    result["groups_raw"] = user_data["groups"]
                else:
                    result["groups_found"] = False
                    result["groups_count"] = 0
                
            else:
                result["error"] = f"API returned {response.status_code}"
                result["response_text"] = response.text
                
    except Exception as e:
        result["exception"] = str(e)
        result["exception_type"] = type(e).__name__
    
    return result

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

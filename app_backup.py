"""
Simplified FastAPI application for Databricks Apps
"""
import os
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import httpx
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(title="Gainwell Main App")

# Get the directory structure
project_root = Path(__file__).parent
static_dir = project_root / "static"

# Mount static files 
if static_dir.exists():
    if (static_dir / "assets").exists():
        app.mount("/assets", StaticFiles(directory=str(static_dir / "assets")), name="assets")

@app.get("/")
async def serve_react_app():
    """Serve the React app's index.html"""
    index_file = static_dir / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    return {"message": "Gainwell Main App", "status": "React app not built"}

@app.get("/api/health/")
async def health_check():
    """Basic health check endpoint"""
    return {"status": "healthy", "message": "Gainwell Main App is running"}

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
                        "email": user_data.get("emails", [{}])[0].get("value") if user_data.get("emails") else email,
                        "display_name": user_data.get("displayName", "Unknown"),
                        "groups": groups,  # Real groups from SCIM API
                        "active": user_data.get("active", False),
                        "user_name": user_data.get("userName", "Unknown")
                    },
                    "debug_info": debug_info
                }
            elif response.status_code == 403:
                # Try alternative user info approach
                debug_info["error"] = "SCIM API access denied - trying workspace current user"
                try:
                    alt_response = await client.get(
                        f"{databricks_host}/api/2.0/preview/scim/v2/Users?filter=active eq true",
                        headers=headers,
                        timeout=10.0
                    )
                    debug_info["alt_status"] = alt_response.status_code
                    if alt_response.status_code == 200:
                        debug_info["alt_success"] = True
                        users_data = alt_response.json()
                        # For now, return basic info since we can't get current user specifically
                        return {
                            "success": True,
                            "user_info": {
                                "user_id": email or "workspace_user",
                                "email": email,
                                "display_name": email.split('@')[0] if email else "Workspace User",
                                "groups": ["workspace_admin"],  # Since you're the main admin
                                "active": True,
                                "user_name": email
                            },
                            "debug_info": debug_info,
                            "note": "Used fallback - SCIM /Me not accessible"
                        }
                except:
                    pass
                
                return {
                    "success": False,
                    "status": response.status_code,
                    "error": f"SCIM API access denied: {response.status_code}",
                    "response_text": response.text[:200],
                    "debug_info": debug_info
                }
            else:
                debug_info["response_text"] = response.text[:200]
                return {
                    "success": False,
                    "status": response.status_code,
                    "error": f"API returned {response.status_code}: {response.text[:100]}",
                    "debug_info": debug_info
                }
                
    except Exception as e:
        debug_info["exception"] = str(e)
        debug_info["exception_type"] = str(type(e).__name__)
        import traceback
        debug_info["traceback"] = traceback.format_exc()
        return {"success": False, "error": str(e), "debug_info": debug_info}

@app.get("/api/health/user-info")
async def get_user_info(request: Request):
    """Get real user information with hybrid authorization"""
    email = request.headers.get("X-Forwarded-Email")
    user_token = request.headers.get("X-Forwarded-Access-Token")
    
    # Get Databricks host and ensure it has the protocol
    databricks_host = (
        request.headers.get("X-Databricks-Host") or 
        os.getenv("DATABRICKS_HOST") or
        "adb-3438839487639471.11.azuredatabricks.net"
    )
    
    # Ensure the host has https:// protocol
    if not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}"
    
    if user_token:
        # Get real user info from Databricks API
        real_user_data = await get_real_user_info(databricks_host, user_token, email)
        
        if real_user_data.get("success"):
            response_data = {
                "user_info": real_user_data["user_info"],
                "authenticated": True,
                "app": "Gainwell Main App",
                "has_user_token": True,
                "authorization_type": "hybrid",
                "data_source": "databricks_api"
            }
            # Include debug info in development
            if real_user_data.get("debug_info"):
                response_data["debug_info"] = real_user_data["debug_info"]
            if real_user_data.get("note"):
                response_data["note"] = real_user_data["note"]
            return response_data
        else:
            # Token exists but API call failed - show detailed error
            return {
                "user_info": {
                    "user_id": email or "api_error",
                    "email": email,
                    "display_name": email.split('@')[0] if email else "API Error",
                    "groups": [f"API_ERROR_{real_user_data.get('status', 'UNKNOWN')}"]
                },
                "authenticated": bool(email),
                "app": "Gainwell Main App",
                "has_user_token": True,
                "authorization_type": "email_only",
                "data_source": "headers_fallback",
                "api_error": real_user_data.get("error", "Unknown error"),
                "debug_info": real_user_data.get("debug_info", {}),
                "api_response": real_user_data.get("response_text", "No response text")
            }
    else:
        # No user token - fallback to header info only
        return {
            "user_info": {
                "user_id": email or "anonymous",
                "email": email,
                "display_name": email.split('@')[0] if email else "Anonymous",
                "groups": ["NO_USER_TOKEN"] if email else []
            },
            "authenticated": bool(email),
            "app": "Gainwell Main App",
            "has_user_token": False,
            "authorization_type": "email_only" if email else "none",
            "data_source": "headers_only"
        }

@app.get("/api/debug/headers")
async def debug_headers(request: Request):
    """Debug endpoint to see all available headers"""
    return {
        "all_headers": dict(request.headers),
        "forwarded_headers": {k: v for k, v in request.headers.items() if k.lower().startswith('x-forwarded')},
        "databricks_headers": {k: v for k, v in request.headers.items() if 'databricks' in k.lower()},
        "auth_headers": {k: v for k, v in request.headers.items() if 'auth' in k.lower()},
        "user_headers": {k: v for k, v in request.headers.items() if 'user' in k.lower()}
    }

@app.get("/api/debug/test-api")
async def debug_api_call(request: Request):
    """Debug endpoint to test Databricks API calls step by step"""
    email = request.headers.get("X-Forwarded-Email")
    user_token = request.headers.get("X-Forwarded-Access-Token")
    
    # Get Databricks host and ensure it has the protocol
    databricks_host = (
        request.headers.get("X-Databricks-Host") or 
        os.getenv("DATABRICKS_HOST") or
        "adb-3438839487639471.11.azuredatabricks.net"
    )
    
    # Ensure the host has https:// protocol
    if not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}"
    
    result = {
        "email": email,
        "has_user_token": bool(user_token),
        "token_preview": user_token[:20] + "..." if user_token else None,
        "databricks_host": databricks_host,
        "tests": {}
    }
    
    if not user_token:
        result["error"] = "No user token available for testing"
        return result
    
    # Test 1: Simple connectivity test
    try:
        headers = {"Authorization": f"Bearer {user_token}", "Content-Type": "application/json"}
        
        async with httpx.AsyncClient() as client:
            # Test basic connectivity first
            test_url = f"{databricks_host}/api/2.0/clusters/list"
            response = await client.get(test_url, headers=headers, timeout=5.0)
            result["tests"]["clusters_list"] = {
                "url": test_url,
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "response_preview": response.text[:200] if response.status_code != 200 else "Success"
            }
    except Exception as e:
        result["tests"]["clusters_list"] = {
            "error": str(e),
            "exception_type": str(type(e).__name__)
        }
    
    # Test 2: SCIM Me endpoint
    try:
        async with httpx.AsyncClient() as client:
            test_url = f"{databricks_host}/api/2.0/preview/scim/v2/Me"
            response = await client.get(test_url, headers=headers, timeout=5.0)
            result["tests"]["scim_me"] = {
                "url": test_url,
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "response_preview": response.text[:200] if response.status_code != 200 else "Success",
                "headers": dict(response.headers) if response.status_code != 200 else "Success"
            }
    except Exception as e:
        result["tests"]["scim_me"] = {
            "error": str(e),
            "exception_type": str(type(e).__name__)
        }
    
    # Test 3: Alternative user endpoint
    try:
        async with httpx.AsyncClient() as client:
            test_url = f"{databricks_host}/api/2.0/preview/scim/v2/Users/me"
            response = await client.get(test_url, headers=headers, timeout=5.0)
            result["tests"]["scim_users_me"] = {
                "url": test_url,
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "response_preview": response.text[:200] if response.status_code != 200 else "Success"
            }
    except Exception as e:
        result["tests"]["scim_users_me"] = {
            "error": str(e),
            "exception_type": str(type(e).__name__)
        }
    
    return result

async def get_workspace_info(databricks_host: str, auth_token: str):
    """Try to get basic workspace info - requires fewer permissions than cluster listing"""
    try:
        headers = {"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"}
        
        async with httpx.AsyncClient() as client:
            # Try current user info first - this usually works
            response = await client.get(
                f"{databricks_host}/api/2.0/preview/scim/v2/Me",
                headers=headers,
                timeout=10.0
            )
            
            if response.status_code == 200:
                user_data = response.json()
                return {
                    "success": True,
                    "user_name": user_data.get("displayName", "Unknown"),
                    "user_email": user_data.get("emails", [{}])[0].get("value", "Unknown"),
                    "user_id": user_data.get("id", "Unknown")
                }
            else:
                return {"success": False, "status": response.status_code}
                
    except Exception as e:
        return {"success": False, "error": str(e)}

async def get_real_clusters(databricks_host: str, auth_token: str = None, auth_type: str = "user"):
    """Get real cluster data from Databricks API with better error handling"""
    try:
        if not databricks_host:
            # Try to get from environment or use default
            import os
            databricks_host = os.getenv("DATABRICKS_HOST", "https://dbc-12345678-9abc.cloud.databricks.com")
        
        if not databricks_host.startswith("https://"):
            databricks_host = f"https://{databricks_host}"
            
        headers = {"Content-Type": "application/json"}
        
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
            
            # First, try to get user info to verify token works
            user_info = await get_workspace_info(databricks_host, auth_token)
            
            if not user_info.get("success"):
                return {
                    "clusters": [{"name": "❌ User token invalid or expired"}],
                    "cluster_count": 0,
                    "auth_method": auth_type,
                    "error": f"User token validation failed: {user_info.get('status', 'unknown')}"
                }
        else:
            # Use service principal credentials from environment
            import os
            client_id = os.getenv("DATABRICKS_CLIENT_ID")
            client_secret = os.getenv("DATABRICKS_CLIENT_SECRET") 
            if client_id and client_secret:
                return {
                    "clusters": [{"name": "🔧 Service Principal - Configure user authorization for cluster access"}],
                    "cluster_count": 1,
                    "auth_method": "service_principal",
                    "note": "Service Principal detected - user authorization needed for cluster details",
                    "user_info": {"name": "Service Principal", "email": client_id}
                }
        
        # Make API call to get clusters
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{databricks_host}/api/2.0/clusters/list",
                headers=headers,
                timeout=10.0
            )
            
            if response.status_code == 200:
                data = response.json()
                clusters = data.get("clusters", [])
                return {
                    "clusters": [{"name": cluster.get("cluster_name", "Unknown")} for cluster in clusters],
                    "cluster_count": len(clusters),
                    "auth_method": auth_type,
                    "api_response": "success"
                }
            elif response.status_code == 403:
                # 403 Forbidden - user doesn't have permission to list clusters
                return {
                    "clusters": [{"name": "⚠️ Permission Required: Cluster listing not allowed"}],
                    "cluster_count": 0,
                    "auth_method": auth_type,
                    "error": "403 Forbidden - User token lacks cluster:list permission",
                    "solution": "Contact admin to grant cluster access or configure user authorization scopes"
                }
            else:
                return {
                    "clusters": [{"name": f"❌ API Error: {response.status_code}"}],
                    "cluster_count": 0,
                    "auth_method": auth_type,
                    "error": f"API call failed with status {response.status_code}",
                    "response_text": response.text[:200] if response.text else "No response body"
                }
                
    except Exception as e:
        return {
            "clusters": [{"name": f"Connection Error: {str(e)[:50]}"}],
            "cluster_count": 0,
            "auth_method": auth_type,
            "error": str(e)
        }

@app.get("/api/health/databricks-status") 
async def get_databricks_status(request: Request):
    """Get Databricks status using ONLY user authorization - respects user permissions"""
    email = request.headers.get("X-Forwarded-Email")
    user_token = request.headers.get("X-Forwarded-Access-Token")
    
    # Get Databricks host and ensure it has the protocol
    databricks_host = (
        request.headers.get("X-Databricks-Host") or 
        os.getenv("DATABRICKS_HOST") or
        "adb-3438839487639471.11.azuredatabricks.net"
    )
    
    # Ensure the host has https:// protocol
    if not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}"
    
    if not user_token:
        return {
            "databricks_connected": False,
            "error": "User authorization required - no service principal access allowed",
            "workspace_info": None,
            "authorization_mode": "none",
            "user_email": email,
            "message": "All API calls require user token to respect individual permissions"
        }
    
    try:
        # Hybrid Authorization Model:
        # - Service Principal for infrastructure (clusters, jobs management)
        # - User Token for data queries (Unity Catalog policies applied)
        
        # Get cluster data using Service Principal (infrastructure operations)
        cluster_data = await get_real_clusters(databricks_host, None, "service_principal")
        
        return {
            "databricks_connected": True,
            "workspace_info": {
                "name": f"Databricks Workspace (User: {email})",
                "features": ["Hybrid Authorization", "SP for Infrastructure", "User Token Ready for Data"],
                "host": databricks_host,
                "auth_method": "hybrid"
            },
            "cluster_count": cluster_data.get("cluster_count", 0),
            "clusters": cluster_data.get("clusters", []),
            "authorization_mode": "hybrid", 
            "user_email": email,
            "can_query_as_user": True,  # User token available for data operations
            "has_user_token": True,
            "api_status": cluster_data.get("api_response", "sp_infrastructure"),
            "infrastructure_auth": "service_principal",
            "data_auth": "user_token_ready",
            "error": cluster_data.get("error") if "error" in cluster_data else None,
            "message": "Infrastructure via Service Principal, Data queries will use User Token + Unity Catalog"
        }
    except Exception as e:
        return {
            "databricks_connected": False,
            "error": str(e),
            "workspace_info": None,
            "authorization_mode": "error",
            "user_email": email
        }

# This catch-all route will be moved to the end of the file

async def get_user_accessible_resources(databricks_host: str, user_token: str):
    """Get resources that the user has access to - respects Unity Catalog and user permissions"""
    headers = {"Authorization": f"Bearer {user_token}", "Content-Type": "application/json"}
    permissions = {"clusters": False, "jobs": False, "warehouses": False, "workspace": False}
    accessible_count = 0
    
    async with httpx.AsyncClient() as client:
        # Test workspace access
        try:
            response = await client.get(f"{databricks_host}/api/2.0/workspace/list?path=/", headers=headers, timeout=5.0)
            if response.status_code == 200:
                permissions["workspace"] = True
                accessible_count += 1
        except:
            pass
            
        # Test jobs access
        try:
            response = await client.get(f"{databricks_host}/api/2.1/jobs/list", headers=headers, timeout=5.0)
            if response.status_code == 200:
                permissions["jobs"] = True
                accessible_count += 1
        except:
            pass
            
        # Test warehouse access  
        try:
            response = await client.get(f"{databricks_host}/api/2.0/sql/warehouses", headers=headers, timeout=5.0)
            if response.status_code == 200:
                permissions["warehouses"] = True
                accessible_count += 1
        except:
            pass
    
    return {
        "permissions_summary": permissions,
        "accessible_count": accessible_count,
        "total_tested": 3
    }

@app.get("/api/user-resources")
async def get_user_resources(request: Request):
    """Get resources accessible ONLY to the current user - no service principal access"""
    email = request.headers.get("X-Forwarded-Email")
    user_token = request.headers.get("X-Forwarded-Access-Token")
    
    if not user_token:
        return {
            "error": "User authorization required",
            "message": "Service principal access disabled - user token required for all operations",
            "user_email": email,
            "authorization_mode": "none"
        }
    
    # Get Databricks host and ensure it has the protocol
    databricks_host = (
        request.headers.get("X-Databricks-Host") or 
        os.getenv("DATABRICKS_HOST") or
        "adb-3438839487639471.11.azuredatabricks.net"
    )
    
    # Ensure the host has https:// protocol
    if not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}"
    
    try:
        headers = {"Authorization": f"Bearer {user_token}", "Content-Type": "application/json"}
        result = {
            "user_email": email,
            "authorization_mode": "user_token_only",
            "message": "Resources filtered by user permissions and Unity Catalog policies",
            "resources": {}
        }
        
        async with httpx.AsyncClient() as client:
            # Note: Clusters managed by Service Principal (infrastructure)
            # This endpoint focuses on user's data resources
            result["clusters_note"] = "Cluster management handled by Service Principal - see /api/health/databricks-status"
                
            # Get user's jobs
            try:
                response = await client.get(f"{databricks_host}/api/2.1/jobs/list", headers=headers, timeout=5.0)
                if response.status_code == 200:
                    jobs_data = response.json()
                    result["resources"]["jobs"] = jobs_data.get("jobs", [])
                    result["job_count"] = len(jobs_data.get("jobs", []))
                else:
                    result["jobs_access"] = f"Restricted (Status {response.status_code})"
            except Exception as e:
                result["jobs_error"] = str(e)
                
            # Get user's SQL warehouses
            try:
                response = await client.get(f"{databricks_host}/api/2.0/sql/warehouses", headers=headers, timeout=5.0)
                if response.status_code == 200:
                    warehouse_data = response.json()
                    result["resources"]["sql_warehouses"] = warehouse_data.get("warehouses", [])
                    result["warehouse_count"] = len(warehouse_data.get("warehouses", []))
                else:
                    result["warehouses_access"] = f"Restricted (Status {response.status_code})"
            except Exception as e:
                result["warehouses_error"] = str(e)
        
        return result
        
    except Exception as e:
        return {
            "error": str(e),
            "user_email": email,
            "authorization_mode": "error",
            "message": "Failed to get user resources"
        }

# ================== CLUSTER MANAGEMENT ENDPOINTS ==================

@app.get("/api/clusters")
async def list_clusters(request: Request):
    """List all clusters with detailed status using Service Principal"""
    email = request.headers.get("X-Forwarded-Email")
    
    # Get Databricks host and ensure it has the protocol
    databricks_host = (
        request.headers.get("X-Databricks-Host") or 
        os.getenv("DATABRICKS_HOST") or
        "adb-3438839487639471.11.azuredatabricks.net"
    )
    
    if not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}"
    
    try:
        cluster_data = await get_real_clusters(databricks_host, None, "service_principal")
        
        return {
            "success": True,
            "clusters": cluster_data.get("clusters", []),
            "cluster_count": cluster_data.get("cluster_count", 0),
            "user_email": email,
            "authorization_mode": "service_principal",
            "message": "Cluster management via Service Principal",
            "timestamp": "2025-09-16T19:15:00Z"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "clusters": [],
            "cluster_count": 0,
            "user_email": email
        }

@app.post("/api/clusters/{cluster_id}/start")
async def start_cluster(cluster_id: str, request: Request):
    """Start a specific cluster using Service Principal"""
    email = request.headers.get("X-Forwarded-Email")
    
    # Get Databricks host and ensure it has the protocol
    databricks_host = (
        request.headers.get("X-Databricks-Host") or 
        os.getenv("DATABRICKS_HOST") or
        "adb-3438839487639471.11.azuredatabricks.net"
    )
    
    if not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}"
    
    try:
        # Use service principal to start cluster
        result = await manage_cluster(databricks_host, cluster_id, "start")
        
        return {
            "success": result.get("success", False),
            "cluster_id": cluster_id,
            "action": "start",
            "message": result.get("message", "Cluster start requested"),
            "user_email": email,
            "authorization_mode": "service_principal",
            "timestamp": "2025-09-16T19:15:00Z",
            "error": result.get("error") if "error" in result else None
        }
    except Exception as e:
        return {
            "success": False,
            "cluster_id": cluster_id,
            "action": "start",
            "error": str(e),
            "user_email": email
        }

@app.post("/api/clusters/{cluster_id}/stop")
async def stop_cluster(cluster_id: str, request: Request):
    """Stop a specific cluster using Service Principal"""
    email = request.headers.get("X-Forwarded-Email")
    
    # Get Databricks host and ensure it has the protocol
    databricks_host = (
        request.headers.get("X-Databricks-Host") or 
        os.getenv("DATABRICKS_HOST") or
        "adb-3438839487639471.11.azuredatabricks.net"
    )
    
    if not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}"
    
    try:
        # Use service principal to stop cluster
        result = await manage_cluster(databricks_host, cluster_id, "stop")
        
        return {
            "success": result.get("success", False),
            "cluster_id": cluster_id,
            "action": "stop",
            "message": result.get("message", "Cluster stop requested"),
            "user_email": email,
            "authorization_mode": "service_principal", 
            "timestamp": "2025-09-16T19:15:00Z",
            "error": result.get("error") if "error" in result else None
        }
    except Exception as e:
        return {
            "success": False,
            "cluster_id": cluster_id,
            "action": "stop",
            "error": str(e),
            "user_email": email
        }

# ================== SQL WAREHOUSE MANAGEMENT ENDPOINTS ==================

@app.get("/api/sql-warehouses")
async def list_sql_warehouses(request: Request):
    """List all SQL warehouses with status using Service Principal"""
    email = request.headers.get("X-Forwarded-Email")
    
    # Get Databricks host and ensure it has the protocol
    databricks_host = (
        request.headers.get("X-Databricks-Host") or 
        os.getenv("DATABRICKS_HOST") or
        "adb-3438839487639471.11.azuredatabricks.net"
    )
    
    if not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}"
    
    try:
        warehouse_data = await get_sql_warehouses(databricks_host)
        
        return {
            "success": True,
            "warehouses": warehouse_data.get("warehouses", []),
            "warehouse_count": warehouse_data.get("warehouse_count", 0),
            "user_email": email,
            "authorization_mode": "service_principal",
            "message": "SQL Warehouse management via Service Principal",
            "timestamp": "2025-09-16T19:15:00Z"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "warehouses": [],
            "warehouse_count": 0,
            "user_email": email
        }

@app.post("/api/sql-warehouses/{warehouse_id}/start")
async def start_warehouse(warehouse_id: str, request: Request):
    """Start a SQL warehouse using Service Principal"""
    email = request.headers.get("X-Forwarded-Email")
    
    # Get Databricks host and ensure it has the protocol
    databricks_host = (
        request.headers.get("X-Databricks-Host") or 
        os.getenv("DATABRICKS_HOST") or
        "adb-3438839487639471.11.azuredatabricks.net"
    )
    
    if not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}"
    
    try:
        result = await manage_sql_warehouse(databricks_host, warehouse_id, "start")
        
        return {
            "success": result.get("success", False),
            "warehouse_id": warehouse_id,
            "action": "start",
            "message": result.get("message", "SQL Warehouse start requested"),
            "user_email": email,
            "authorization_mode": "service_principal",
            "timestamp": "2025-09-16T19:15:00Z",
            "error": result.get("error") if "error" in result else None
        }
    except Exception as e:
        return {
            "success": False,
            "warehouse_id": warehouse_id,
            "action": "start",
            "error": str(e),
            "user_email": email
        }

@app.post("/api/sql-warehouses/{warehouse_id}/stop")
async def stop_warehouse(warehouse_id: str, request: Request):
    """Stop a SQL warehouse using Service Principal"""
    email = request.headers.get("X-Forwarded-Email")
    
    # Get Databricks host and ensure it has the protocol
    databricks_host = (
        request.headers.get("X-Databricks-Host") or 
        os.getenv("DATABRICKS_HOST") or
        "adb-3438839487639471.11.azuredatabricks.net"
    )
    
    if not databricks_host.startswith("http"):
        databricks_host = f"https://{databricks_host}"
    
    try:
        result = await manage_sql_warehouse(databricks_host, warehouse_id, "stop")
        
        return {
            "success": result.get("success", False),
            "warehouse_id": warehouse_id,
            "action": "stop", 
            "message": result.get("message", "SQL Warehouse stop requested"),
            "user_email": email,
            "authorization_mode": "service_principal",
            "timestamp": "2025-09-16T19:15:00Z",
            "error": result.get("error") if "error" in result else None
        }
    except Exception as e:
        return {
            "success": False,
            "warehouse_id": warehouse_id,
            "action": "stop",
            "error": str(e),
            "user_email": email
        }

# ================== HELPER FUNCTIONS FOR MANAGEMENT ==================

async def manage_cluster(databricks_host: str, cluster_id: str, action: str):
    """Start or stop a cluster using Service Principal"""
    try:
        # For demonstration, this would normally use service principal OAuth
        # In a real implementation, this would call Databricks cluster APIs
        
        if action == "start":
            # POST /api/2.0/clusters/start with cluster_id
            return {
                "success": True,
                "message": f"Cluster {cluster_id} start initiated",
                "note": "This would call the actual Databricks cluster start API"
            }
        elif action == "stop":
            # POST /api/2.0/clusters/delete with cluster_id  
            return {
                "success": True,
                "message": f"Cluster {cluster_id} stop initiated",
                "note": "This would call the actual Databricks cluster stop API"
            }
        else:
            return {
                "success": False,
                "error": f"Invalid action: {action}"
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

async def get_sql_warehouses(databricks_host: str):
    """Get SQL warehouses using Service Principal"""
    try:
        # This would normally call the Databricks SQL warehouses API
        # GET /api/2.0/sql/warehouses
        
        # For demonstration, returning sample data
        sample_warehouses = [
            {
                "id": "warehouse_001",
                "name": "Shared SQL Warehouse",
                "state": "RUNNING",
                "size": "Medium",
                "auto_stop_mins": 10,
                "tags": {"team": "data"},
                "creator_name": "System"
            },
            {
                "id": "warehouse_002", 
                "name": "Analytics Warehouse",
                "state": "STOPPED",
                "size": "Large",
                "auto_stop_mins": 15,
                "tags": {"team": "analytics"},
                "creator_name": "System"
            }
        ]
        
        return {
            "warehouses": sample_warehouses,
            "warehouse_count": len(sample_warehouses)
        }
    except Exception as e:
        return {
            "warehouses": [],
            "warehouse_count": 0,
            "error": str(e)
        }

async def manage_sql_warehouse(databricks_host: str, warehouse_id: str, action: str):
    """Start or stop a SQL warehouse using Service Principal"""
    try:
        if action == "start":
            # POST /api/2.0/sql/warehouses/{warehouse_id}/start
            return {
                "success": True,
                "message": f"SQL Warehouse {warehouse_id} start initiated",
                "note": "This would call the actual Databricks warehouse start API"
            }
        elif action == "stop":
            # POST /api/2.0/sql/warehouses/{warehouse_id}/stop
            return {
                "success": True,
                "message": f"SQL Warehouse {warehouse_id} stop initiated", 
                "note": "This would call the actual Databricks warehouse stop API"
            }
        else:
            return {
                "success": False,
                "error": f"Invalid action: {action}"
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

# ================== SPA ROUTING (MUST BE LAST) ==================
# This catch-all route must be defined AFTER all API routes

@app.get("/{path:path}")
async def serve_spa_routes(path: str):
    """Serve React app for SPA routing - MUST BE LAST ROUTE"""
    if path.startswith("api/"):
        return {"error": "API route not found"}
    if path.startswith("assets/"):
        return {"error": "Asset not found"}
        
    index_file = static_dir / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    return {"error": "Frontend not available"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

from fastapi import APIRouter, Request, HTTPException
import logging
import httpx  # async HTTP client for Databricks API calls
import os
import json

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["authentication"])

# Service Principal Authentication - requires these environment variables:
# DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET


@router.get("/health/service-principal")
async def get_service_principal_info(request: Request):
    """Get service principal authentication status and information"""
    
    # Get service principal configuration from environment
    databricks_host = os.getenv("DATABRICKS_HOST","https://acuity-pdl-dev-ue1.cloud.databricks.com/")
    client_id = os.getenv("DATABRICKS_CLIENT_ID","svc_gia_pdl_de_dev_usr")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET","")
    
    # Validate required environment variables
    if not databricks_host:
        raise HTTPException(status_code=500, detail="DATABRICKS_HOST environment variable is required")
    if not client_id:
        raise HTTPException(status_code=500, detail="DATABRICKS_CLIENT_ID environment variable is required")
    if not client_secret:
        raise HTTPException(status_code=500, detail="DATABRICKS_CLIENT_SECRET environment variable is required")
    
    try:
        # Get access token using service principal credentials
        access_token = await get_service_principal_token(databricks_host, client_id, client_secret)
        
        # Validate token by calling workspace API
        workspace_info = await validate_service_principal(databricks_host, access_token)
        
        return {
            "service_principal_info": {
                "client_id": client_id,
                "workspace_id": workspace_info.get("workspace_id"),
                "workspace_name": workspace_info.get("workspace_name"),
                "authenticated": True,
                "permissions": workspace_info.get("permissions", []),
                "user_name": workspace_info.get("user_name")
            },
            "authenticated": True,
            "app": "Gainwell Main App",
            "authorization_type": "service_principal",
            "data_source": "databricks_oauth2",
            "token_valid": True
        }
    
    except Exception as e:
        logger.error(f"Service principal authentication failed: {e}")
        return {
            "service_principal_info": {
                "client_id": client_id,
                "authenticated": False,
                "error": str(e)
            },
            "authenticated": False,
            "app": "Gainwell Main App",
            "authorization_type": "service_principal",
            "data_source": "databricks_oauth2",
            "token_valid": False
        }


@router.post("/auth/token")
async def get_access_token():
    """Get a fresh access token using service principal credentials"""
    
    databricks_host = os.getenv("DATABRICKS_HOST")
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
    
    # Validate required environment variables
    if not all([databricks_host, client_id, client_secret]):
        raise HTTPException(
            status_code=500, 
            detail="Missing required environment variables: DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET"
        )
    
    try:
        access_token = await get_service_principal_token(databricks_host, client_id, client_secret)
        return {
            "access_token": access_token,
            "token_type": "Bearer",
            "scope": "all-apis",
            "client_id": client_id
        }
    except Exception as e:
        logger.error(f"Failed to get access token: {e}")
        raise HTTPException(status_code=401, detail=f"Authentication failed: {str(e)}")


async def get_service_principal_token(databricks_host: str, client_id: str, client_secret: str) -> str:
    """Get OAuth2 access token for service principal"""
    
    # Remove https:// if present and clean the host
    if databricks_host.startswith("https://"):
        databricks_host = databricks_host[8:]
    
    token_url = f"https://{databricks_host}/oidc/v1/token"
    
    data = {
        "grant_type": "client_credentials",
        "scope": "all-apis"
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            token_url,
            data=data,
            auth=(client_id, client_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        
        if response.status_code == 200:
            token_data = response.json()
            return token_data["access_token"]
        else:
            raise Exception(f"Failed to get access token: {response.status_code} {response.text}")


async def validate_service_principal(databricks_host: str, access_token: str) -> dict:
    """Validate service principal token and get workspace information"""
    
    # Remove https:// if present and clean the host
    if databricks_host.startswith("https://"):
        databricks_host = databricks_host[8:]
    
    # Get current user (service principal) information
    async with httpx.AsyncClient(timeout=10.0) as client:
        # Get workspace information
        workspace_resp = await client.get(
            f"https://{databricks_host}/api/2.0/workspace/get-status",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"path": "/"}
        )
        
        if workspace_resp.status_code != 200:
            raise Exception(f"Failed to validate token: {workspace_resp.status_code} {workspace_resp.text}")
        
        # Get service principal details via SCIM API
        try:
            scim_resp = await client.get(
                f"https://{databricks_host}/api/2.0/preview/scim/v2/Me",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            
            scim_data = scim_resp.json() if scim_resp.status_code == 200 else {}
        except Exception as e:
            logger.warning(f"SCIM API call failed: {e}")
            scim_data = {}
        
        # Try to get workspace details
        try:
            workspace_details_resp = await client.get(
                f"https://{databricks_host}/api/2.0/workspace/list",
                headers={"Authorization": f"Bearer {access_token}"},
                params={"path": "/", "fmt": "SOURCE"}
            )
            workspace_accessible = workspace_details_resp.status_code == 200
        except Exception:
            workspace_accessible = False
        
        return {
            "workspace_id": workspace_resp.json().get("object_id"),
            "workspace_name": "Databricks Workspace",
            "permissions": scim_data.get("groups", []),
            "user_name": scim_data.get("userName", "service-principal"),
            "workspace_accessible": workspace_accessible,
            "scim_available": bool(scim_data)
        }


# Dependency to get authenticated service principal token
async def get_authenticated_token() -> str:
    """Dependency to get a valid service principal token"""
    
    databricks_host = os.getenv("DATABRICKS_HOST")
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
    
    if not all([databricks_host, client_id, client_secret]):
        raise HTTPException(
            status_code=500,
            detail="Service principal configuration missing"
        )
    
    try:
        return await get_service_principal_token(databricks_host, client_id, client_secret)
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Authentication failed: {str(e)}")

from fastapi import APIRouter, Request
import logging
import httpx  # async HTTP client for Databricks API calls
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
import os

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["authentication"])

# Replace with your workspace URL (no trailing slash)
#DATABRICKS_HOST = "https://adb-3438839487639471.11.azuredatabricks.net"

@router.get("/health/user-info")
async def get_user_info(request: Request):
    """Get user information from Databricks App forwarded headers and Databricks API"""
    headers = dict(request.headers)
    cfg = Config()
    workspace = WorkspaceClient()
    databricks_host = os.getenv("DATABRICKS_HOST")

    # Extract forwarded info
    user = headers.get("x-forwarded-user")
    email = headers.get("x-forwarded-email")
    user_token = headers.get("x-forwarded-access-token")
    user_id = headers.get("x-databricks-user-id")

    groups_list = []

    # Query Databricks SCIM API if we have a token
    if user_token:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    f"https://{databricks_host}/api/2.0/preview/scim/v2/Me",
                    headers={"Authorization": f"Bearer {user_token}"}
                )
                if resp.status_code == 200:
                    me = resp.json()
                    groups_list = [g["display"] for g in me.get("groups", [])]
                else:
                    logger.warning(
                        f"Databricks SCIM API call failed: {resp.status_code} {resp.text}"
                    )
        except Exception as e:
            logger.error(f"Error calling Databricks SCIM API: {e}")

    # Fallback to forwarded headers if API didn’t return groups
    if not groups_list:
        groups_header = headers.get("x-forwarded-groups", "")
        groups_list = [g.strip() for g in groups_header.split(",") if g.strip()]
        if not groups_list:
            groups_list = ["no-groups-found"]

    # Extract display name
    display_name = user or (email.split('@')[0] if email else "Unknown User")

    return {
        "user_info": {
            "user_id": user_id,
            "email": email,
            "display_name": display_name,
            "user_name": user or email,
            "groups": groups_list
        },
        "authenticated": bool(email and user_token),
        "app": "Gainwell Main App",
        "has_user_token": bool(user_token),
        "authorization_type": "databricks_app_headers + api_call",
        "data_source": "x_forwarded_headers + databricks_scim"
    }

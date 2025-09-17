"""
Authentication routes - Clean header-based approach
"""
from fastapi import APIRouter, Request
import logging

logger = logging.getLogger(__name__)

# Create router for auth endpoints
router = APIRouter(prefix="/api", tags=["authentication"])

@router.get("/health/user-info")
async def get_user_info(request: Request):
    """Get user information from Databricks App forwarded headers"""
    headers = dict(request.headers)
    
    # Get user info from Databricks App forwarded headers
    user = headers.get("x-forwarded-user")
    email = headers.get("x-forwarded-email") 
    groups_header = headers.get("x-forwarded-groups", "")
    user_token = headers.get("x-forwarded-access-token")
    user_id = headers.get("x-databricks-user-id")
    
    # Parse groups from comma-separated string
    groups_list = [g.strip() for g in groups_header.split(",") if g.strip()]
    
    # If no groups found, try other possible header names
    if not groups_list:
        other_headers = [
            "x-databricks-groups",
            "x-forwarded-user-groups", 
            "x-databricks-user-groups",
            "x-forwarded-roles"
        ]
        for header_name in other_headers:
            alt_groups = headers.get(header_name, "")
            if alt_groups:
                groups_list = [g.strip() for g in alt_groups.split(",") if g.strip()]
                break
    
    # Extract display name from email or user
    display_name = user or (email.split('@')[0] if email else "Unknown User")
    
    # Final fallback for groups
    if not groups_list:
        groups_list = ["no-groups-found"]
    
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
        "authorization_type": "databricks_app_headers",
        "data_source": "x_forwarded_headers"
    }

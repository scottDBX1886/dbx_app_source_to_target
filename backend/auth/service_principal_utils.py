"""
Service Principal Authentication Utilities
Shared utilities for logging and authentication context in service principal mode
"""
import os
import logging

logger = logging.getLogger(__name__)


def get_service_principal_context() -> dict:
    """Get service principal context for logging and audit purposes"""
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "svc_gia_pdl_de_dev_usr")
    workspace_host = os.getenv("DATABRICKS_HOST", "https://acuity-pdl-dev-ue1.cloud.databricks.com/")
    
    return {
        "client_id": client_id,
        "workspace_host": workspace_host,
        "auth_type": "service_principal"
    }


def log_api_request(operation: str, **kwargs):
    """
    Standardized logging for API requests using service principal context
    
    Args:
        operation: Description of the operation (e.g., "FDB search", "PDL export")
        **kwargs: Additional parameters to log (tenant, query, etc.)
    """
    sp_context = get_service_principal_context()
    
    log_parts = [f"{operation}:"]
    
    # Add service principal info
    log_parts.append(f"service_principal={sp_context['client_id']}")
    
    # Add any additional parameters
    for key, value in kwargs.items():
        log_parts.append(f"{key}={value}")
    
    logger.info(", ".join(log_parts))


def get_audit_user() -> str:
    """
    Get the user identifier for audit purposes
    In service principal mode, this returns the client_id
    """
    return get_service_principal_context()["client_id"]
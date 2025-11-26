"""
Simplified FDB routes for debugging import issues
This version removes the authentication dependency temporarily
"""
import os
from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fdb", tags=["fdb-search"])

@router.get("/health")
async def fdb_health_check():
    """Simple health check endpoint to test FDB routes"""
    return {
        "status": "healthy",
        "service": "FDB Routes",
        "message": "FDB routes are working"
    }

@router.get("/search")
async def search_fdb_records(
    request: Request,
    tenant: str = Query("MASTER", description="Tenant (MASTER/AK/MO)"),
    query: Optional[str] = Query(None, description="Search query"),
    limit: Optional[int] = Query(10, description="Result limit")
):
    """
    Simplified FDB search endpoint without authentication for debugging
    """
    try:
        logger.info(f"FDB search: tenant={tenant}, query={query}, limit={limit}")
        
        # Return mock data for testing
        mock_records = [
            {
                "ndc": "12345678901",
                "gsn": 12345,
                "brand": "Test Drug A",
                "pkg_size": "100 tablets",
                "hic3": "ABC"
            },
            {
                "ndc": "98765432109", 
                "gsn": 67890,
                "brand": "Test Drug B",
                "pkg_size": "30 capsules",
                "hic3": "XYZ"
            }
        ]
        
        # Apply limit
        if limit and limit > 0:
            mock_records = mock_records[:limit]
        
        return {
            "tenant": tenant,
            "query": query,
            "limit": limit,
            "total_found": len(mock_records),
            "data_source": "Mock Data - Testing",
            "records": mock_records,
            "note": "This is a simplified version for testing imports"
        }
        
    except Exception as e:
        logger.error(f"Error in FDB search: {e}")
        return {
            "tenant": tenant,
            "query": query,
            "error": str(e),
            "error_type": type(e).__name__,
            "total_found": 0,
            "records": []
        }
"""
Weekly Review API routes
Future: Connect to review workflow management
"""
from fastapi import APIRouter, Request, Query
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Create router for weekly review endpoints
router = APIRouter(prefix="/api/weekly", tags=["weekly-review"])

@router.get("/pool/{review_type}")
async def get_weekly_pool(
    request: Request,
    review_type: str,  # "fmt" or "pdl"
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    week_ending: str = Query(..., description="Week ending date (YYYY-MM-DD)")
):
    """
    Get weekly review pool (new NDCs not in master)
    Future: Generate from FDB loads vs existing masters
    
    Example: GET /api/weekly/pool/fmt?tenant=AK&week_ending=2024-12-15
    """
    
    return {
        "message": "Weekly Review Pool API - Ready for real data integration",
        "review_type": review_type,
        "tenant": tenant,
        "week_ending": week_ending,
        "status": "placeholder",
        "future_integration": {
            "pool_generation": "New FDB NDCs not in master tables",
            "matching_logic": "100% match, GSN match, brand match, no match",
            "reviewer_assignment": "Distribute to Reviewer A and B"
        }
    }

@router.post("/assign")
async def assign_reviews(
    request: Request,
    review_type: str = Query(..., description="Review type (fmt/pdl)"),
    tenant: str = Query(..., description="Tenant")
):
    """
    Assign NDCs to reviewers
    Future: Assign pool items to Reviewer A and B
    """
    
    return {
        "message": "Review Assignment API - Ready for real data integration",
        "review_type": review_type,
        "tenant": tenant,
        "status": "placeholder",
        "future_integration": {
            "assignment_logic": "Load balancing between reviewers",
            "mbid_suggestion": "Auto-suggest based on drug classification",
            "workflow": "Track assignment to completion"
        }
    }

@router.post("/approve")
async def approve_weekly_review(
    request: Request,
    review_type: str = Query(..., description="Review type (fmt/pdl)"),
    tenant: str = Query(..., description="Tenant")
):
    """
    Approve and sync weekly review to master tables
    Future: Final approval with conflict detection
    """
    
    return {
        "message": "Weekly Approval API - Ready for real data integration",
        "review_type": review_type,
        "tenant": tenant,  
        "status": "placeholder",
        "future_integration": {
            "conflict_detection": "Overlap checking and validation",
            "master_sync": "Atomic updates to master tables",
            "pos_export": "Generate POS files for PDL reviews",
            "audit_trail": "Complete review history"
        }
    }

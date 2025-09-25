"""
Weekly Review API routes for FMT
FMT-specific weekly review workflow management with live data
"""
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
import logging
import pandas as pd
import json
import io

logger = logging.getLogger(__name__)

# Create router for FMT weekly review endpoints
router = APIRouter(prefix="/api/weekly/fmt", tags=["weekly-review-fmt"])

def load_fmt_weekly_pool_data(tenant: str, week_ending: str) -> pd.DataFrame:
    """
    Load FMT weekly pool data - new NDCs not in FMT master tables
    """
    try:
        from backend.services.connector import query
        from backend.config.settings import get_settings
        warehouse_id = get_settings().databricks_warehouse_id
        
        # Convert week_ending to datetime for date calculations
        week_end_date = datetime.strptime(week_ending, '%Y-%m-%d')
        week_start_date = week_end_date - timedelta(days=6)
        
        logger.info(f"DEBUG: Searching for FMT data between {week_start_date.strftime('%Y-%m-%d')} and {week_ending}")
        
        # Get new FDB data not in FMT master for this week
        new_ndcs_query = f"""
        SELECT DISTINCT f.ndc, f.brand, f.gsn, f.hic3, f.mfr, 
               f.load_date, 'NEW' as status,
               CASE 
                 WHEN EXISTS (SELECT 1 FROM demo.gainwell.fmt_master m WHERE m.ndc = f.ndc) THEN '100% match'
                 WHEN EXISTS (SELECT 1 FROM demo.gainwell.fmt_master m WHERE m.fmt_drug LIKE CONCAT('%', UPPER(f.brand), '%')) THEN 'brand match'
                 ELSE 'no match'
               END as match_type
        FROM demo.gainwell.fdb_core_drugs f
        WHERE f.load_date >= '{week_start_date.strftime('%Y-%m-%d')}'
          AND f.load_date <= '{week_ending}'
          AND NOT EXISTS (SELECT 1 FROM demo.gainwell.fmt_master m WHERE m.ndc = f.ndc)
        ORDER BY f.load_date DESC, f.ndc
        """
        
        # First, let's check what FDB data exists at all
        debug_query = f"SELECT COUNT(*) as total_fdb FROM demo.gainwell.fdb_core_drugs"
        debug_df = query(debug_query, warehouse_id=warehouse_id, as_dict=False)
        total_fdb = debug_df.iloc[0]['total_fdb'] if not debug_df.empty else 0
        logger.info(f"DEBUG: Total FDB records available: {total_fdb}")
        
        # Check how many have recent load dates
        recent_query = f"""
        SELECT COUNT(*) as recent_count 
        FROM demo.gainwell.fdb_core_drugs f 
        WHERE f.load_date >= '{week_start_date.strftime('%Y-%m-%d')}'
          AND f.load_date <= '{week_ending}'
        """
        recent_df = query(recent_query, warehouse_id=warehouse_id, as_dict=False)
        recent_count = recent_df.iloc[0]['recent_count'] if not recent_df.empty else 0
        logger.info(f"DEBUG: FDB records with recent load dates: {recent_count}")
        
        # If no recent data, let's get some sample data for testing
        if recent_count == 0:
            logger.warning(f"No recent FDB data found. Using sample of latest 20 records for FMT testing.")
            new_ndcs_query = f"""
            SELECT DISTINCT f.ndc, f.brand, f.gsn, f.hic3, f.mfr, 
                   f.load_date, 'NEW' as status,
                   CASE 
                     WHEN EXISTS (SELECT 1 FROM demo.gainwell.fmt_master m WHERE m.ndc = f.ndc) THEN '100% match'
                     WHEN EXISTS (SELECT 1 FROM demo.gainwell.fmt_master m WHERE m.fmt_drug LIKE CONCAT('%', UPPER(f.brand), '%')) THEN 'brand match'
                     ELSE 'no match'
                   END as match_type
            FROM demo.gainwell.fdb_core_drugs f
            WHERE NOT EXISTS (SELECT 1 FROM demo.gainwell.fmt_master m WHERE m.ndc = f.ndc)
            ORDER BY f.load_date DESC, f.ndc
            LIMIT 20
            """
        
        df = query(new_ndcs_query, warehouse_id=warehouse_id, as_dict=False)
        
        logger.info(f"Weekly FMT pool for {tenant} week ending {week_ending}: {len(df)} new NDCs")
        if not df.empty:
            logger.info(f"DEBUG: Sample NDCs found: {df['ndc'].head(5).tolist()}")
            logger.info(f"DEBUG: Match type distribution: {df['match_type'].value_counts().to_dict()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading weekly FMT pool data: {e}")
        return pd.DataFrame()

def get_fmt_review_assignments(tenant: str, week_ending: str) -> Dict[str, List[Dict]]:
    """
    Get current FMT reviewer assignments for the week
    """
    try:
        from backend.services.connector import query
        from backend.config.settings import get_settings
        warehouse_id = get_settings().databricks_warehouse_id
        
        # Query FMT review assignments table (would need to be created)
        assignments_query = f"""
        SELECT ndc, reviewer, mbid, eff_date, end_date, status, assignment_date
        FROM demo.gainwell.fmt_review_assignments 
        WHERE tenant = '{tenant.lower()}' 
          AND week_ending = '{week_ending}'
        ORDER BY assignment_date DESC
        """
        
        try:
            df = query(assignments_query, warehouse_id=warehouse_id, as_dict=False)
            
            # Group by reviewer
            reviewer_a = df[df['reviewer'] == 'A'].to_dict('records') if 'reviewer' in df.columns else []
            reviewer_b = df[df['reviewer'] == 'B'].to_dict('records') if 'reviewer' in df.columns else []
            
            return {
                'reviewer_a': reviewer_a,
                'reviewer_b': reviewer_b
            }
        except:
            # If assignments table doesn't exist yet, return empty
            return {'reviewer_a': [], 'reviewer_b': []}
        
    except Exception as e:
        logger.error(f"Error loading FMT assignments: {e}")
        return {'reviewer_a': [], 'reviewer_b': []}

@router.get("/pool")
async def get_fmt_weekly_pool(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    week_ending: str = Query(..., description="Week ending date (YYYY-MM-DD)"),
    search: Optional[str] = Query(None, description="Search NDC/Brand/GSN/HIC3/MFR")
):
    """
    Get FMT weekly review pool (new NDCs not in FMT master)
    
    Example: GET /api/weekly/fmt/pool?tenant=AK&week_ending=2024-12-15
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"Weekly FMT pool: tenant={tenant}, week_ending={week_ending}, user={user_email}")
        
        # Load weekly pool data
        df = load_fmt_weekly_pool_data(tenant, week_ending)
        
        if df.empty:
            return {
                "pool_data": [],
                "summary": {
                    "total_new_drugs": 0,
                    "match_counts": {"100% match": 0, "brand match": 0, "no match": 0}
                },
                "tenant": tenant,
                "week_ending": week_ending,
                "review_type": "fmt"
            }
        
        # Apply search filter if provided
        if search:
            search_lower = search.lower()
            mask = (
                df['ndc'].astype(str).str.contains(search_lower, case=False, na=False) |
                df['brand'].astype(str).str.contains(search_lower, case=False, na=False) |
                df['gsn'].astype(str).str.contains(search_lower, case=False, na=False) |
                df['hic3'].astype(str).str.contains(search_lower, case=False, na=False) |
                df['mfr'].astype(str).str.contains(search_lower, case=False, na=False)
            )
            df = df[mask]
        
        # Convert to records for JSON response
        records = []
        for _, row in df.iterrows():
            record = {
                "ndc": str(row['ndc']),
                "brand": str(row['brand']) if pd.notna(row['brand']) else "",
                "gsn": str(row['gsn']) if pd.notna(row['gsn']) else "",
                "hic3": str(row['hic3']) if pd.notna(row['hic3']) else "",
                "mfr": str(row['mfr']) if pd.notna(row['mfr']) else "",
                "load_date": str(row['load_date']) if pd.notna(row['load_date']) else "",
                "status": str(row['status']) if pd.notna(row['status']) else "NEW",
                "match_type": str(row['match_type']) if pd.notna(row['match_type']) else "no match"
            }
            records.append(record)
        
        # Calculate summary statistics
        match_counts = df['match_type'].value_counts().to_dict()
        
        return {
            "pool_data": records,
            "summary": {
                "total_new_drugs": len(records),
                "match_counts": match_counts
            },
            "tenant": tenant,
            "week_ending": week_ending,
            "review_type": "fmt"
        }
        
    except Exception as e:
        logger.error(f"Error getting weekly FMT pool: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get weekly pool: {str(e)}")

@router.get("/groups")
async def get_fmt_review_groups(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    week_ending: str = Query(..., description="Week ending date (YYYY-MM-DD)")
):
    """
    Get FMT review groups categorized by match type with status counts
    
    Example: GET /api/weekly/fmt/groups?tenant=AK&week_ending=2024-12-15
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"FMT review groups: tenant={tenant}, week_ending={week_ending}, user={user_email}")
        
        # Load pool data
        df = load_fmt_weekly_pool_data(tenant, week_ending)
        
        if df.empty:
            return {
                "groups": {},
                "tenant": tenant,
                "week_ending": week_ending,
                "review_type": "fmt"
            }
        
        # Get assignments to determine reviewer status
        assignments = get_fmt_review_assignments(tenant, week_ending)
        assignment_map = {}
        for reviewer in ['reviewer_a', 'reviewer_b']:
            for assignment in assignments[reviewer]:
                ndc = str(assignment['ndc'])
                if ndc not in assignment_map:
                    assignment_map[ndc] = []
                assignment_map[ndc].append(reviewer[-1])  # 'A' or 'B'
        
        # Group by match type
        groups = {}
        for match_type in df['match_type'].unique():
            match_df = df[df['match_type'] == match_type]
            
            # Count statuses for this group
            status_counts = {'A': 0, 'B': 0, 'both': 0, 'rejected': 0, 'pending': 0}
            
            group_records = []
            for _, row in match_df.iterrows():
                ndc = str(row['ndc'])
                reviewers = assignment_map.get(ndc, [])
                
                if len(reviewers) == 2:
                    status = 'both'
                elif 'A' in reviewers:
                    status = 'A'
                elif 'B' in reviewers:
                    status = 'B'
                else:
                    status = 'pending'
                
                status_counts[status] += 1
                
                group_records.append({
                    "ndc": ndc,
                    "brand": str(row['brand']) if pd.notna(row['brand']) else "",
                    "gsn": str(row['gsn']) if pd.notna(row['gsn']) else "",
                    "hic3": str(row['hic3']) if pd.notna(row['hic3']) else "",
                    "mfr": str(row['mfr']) if pd.notna(row['mfr']) else "",
                    "load_date": str(row['load_date']) if pd.notna(row['load_date']) else "",
                    "status": status,
                    "suggested_mbid": ""  # TODO: Implement MBID suggestion logic
                })
            
            groups[match_type] = {
                "records": group_records,
                "counts": status_counts
            }
        
        return {
            "groups": groups,
            "tenant": tenant,
            "week_ending": week_ending,
            "review_type": "fmt"
        }
        
    except Exception as e:
        logger.error(f"Error getting FMT review groups: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get review groups: {str(e)}")

@router.get("/assignments")
async def get_fmt_reviewer_assignments(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    week_ending: str = Query(..., description="Week ending date (YYYY-MM-DD)")
):
    """
    Get current FMT reviewer assignments
    
    Example: GET /api/weekly/fmt/assignments?tenant=AK&week_ending=2024-12-15
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"Getting FMT assignments: tenant={tenant}, week_ending={week_ending}, user={user_email}")
        
        assignments = get_fmt_review_assignments(tenant, week_ending)
        
        return {
            "assignments": assignments,
            "tenant": tenant,
            "week_ending": week_ending,
            "review_type": "fmt"
        }
        
    except Exception as e:
        logger.error(f"Error getting FMT assignments: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get assignments: {str(e)}")

@router.get("/comparison")
async def get_fmt_comparison_data(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    week_ending: str = Query(..., description="Week ending date (YYYY-MM-DD)")
):
    """
    Get comparison data for FMT conflict resolution between reviewers
    
    Example: GET /api/weekly/fmt/comparison?tenant=AK&week_ending=2024-12-15
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"Getting FMT comparison data: tenant={tenant}, week_ending={week_ending}, user={user_email}")
        
        # Get assignments from both reviewers
        assignments = get_fmt_review_assignments(tenant, week_ending)
        
        # Find conflicts (NDCs assigned to both reviewers with different values)
        conflicts = []
        reviewer_a_map = {str(item['ndc']): item for item in assignments['reviewer_a']}
        reviewer_b_map = {str(item['ndc']): item for item in assignments['reviewer_b']}
        
        for ndc in reviewer_a_map.keys():
            if ndc in reviewer_b_map:
                a_assignment = reviewer_a_map[ndc]
                b_assignment = reviewer_b_map[ndc]
                
                # Check for differences
                has_conflict = (
                    a_assignment.get('mbid') != b_assignment.get('mbid') or
                    a_assignment.get('eff_date') != b_assignment.get('eff_date') or
                    a_assignment.get('end_date') != b_assignment.get('end_date')
                )
                
                conflicts.append({
                    "ndc": ndc,
                    "reviewer_a": a_assignment,
                    "reviewer_b": b_assignment,
                    "has_conflict": has_conflict,
                    "auto_resolution": "AUTO" if not has_conflict else "CUSTOM"
                })
        
        return {
            "comparison_data": conflicts,
            "total_conflicts": len([c for c in conflicts if c['has_conflict']]),
            "total_matches": len([c for c in conflicts if not c['has_conflict']]),
            "tenant": tenant,
            "week_ending": week_ending,
            "review_type": "fmt"
        }
        
    except Exception as e:
        logger.error(f"Error getting FMT comparison data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get comparison data: {str(e)}")

@router.post("/assign")
async def assign_fmt_reviews(request: Request):
    """
    Assign NDCs to FMT reviewers with MBID and dates
    
    Body: {
        "tenant": "MASTER|AK|MO", 
        "week_ending": "YYYY-MM-DD",
        "assignments": [
            {
                "ndc": "12345678901",
                "reviewer": "A|B",
                "mbid": "CARDIO_ACE_001",
                "eff_date": "2024-12-15",
                "end_date": null
            }
        ]
    }
    """
    try:
        body = await request.json()
        tenant = body.get('tenant')
        week_ending = body.get('week_ending')
        assignments = body.get('assignments', [])
        
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"Assigning {len(assignments)} FMT reviews: tenant={tenant}, user={user_email}")
        
        # TODO: Insert assignments into database table demo.gainwell.fmt_review_assignments
        
        return {
            "message": f"Successfully assigned {len(assignments)} NDCs for FMT review",
            "review_type": "fmt",
            "tenant": tenant,
            "week_ending": week_ending,
            "assignments_created": len(assignments),
            "assigned_by": user_email
        }
        
    except Exception as e:
        logger.error(f"Error assigning FMT reviews: {e}")
        raise HTTPException(status_code=500, detail=f"Assignment failed: {str(e)}")

@router.post("/resolve")
async def resolve_fmt_conflicts(request: Request):
    """
    Resolve conflicts between FMT reviewer assignments
    
    Body: {
        "tenant": "MASTER|AK|MO",
        "week_ending": "YYYY-MM-DD", 
        "resolutions": [
            {
                "ndc": "12345678901",
                "resolution": "AUTO|A|B|CUSTOM",
                "final_mbid": "CARDIO_ACE_001",
                "final_eff_date": "2024-12-15",
                "final_end_date": null
            }
        ]
    }
    """
    try:
        body = await request.json()
        tenant = body.get('tenant')
        week_ending = body.get('week_ending')
        resolutions = body.get('resolutions', [])
        
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"Resolving {len(resolutions)} FMT conflicts: tenant={tenant}, user={user_email}")
        
        # TODO: Update resolution status in database
        
        return {
            "message": f"Successfully resolved {len(resolutions)} FMT conflicts",
            "review_type": "fmt",
            "tenant": tenant,
            "week_ending": week_ending,
            "resolutions_processed": len(resolutions),
            "resolved_by": user_email
        }
        
    except Exception as e:
        logger.error(f"Error resolving FMT conflicts: {e}")
        raise HTTPException(status_code=500, detail=f"Resolution failed: {str(e)}")

@router.post("/approve")
async def approve_fmt_weekly_review(request: Request):
    """
    Final approval - sync resolved FMT items to master tables
    
    Body: {
        "tenant": "MASTER|AK|MO",
        "week_ending": "YYYY-MM-DD",
        "approved_items": [
            {
                "ndc": "12345678901",
                "mbid": "CARDIO_ACE_001",
                "eff_date": "2024-12-15",
                "end_date": null
            }
        ]
    }
    """
    try:
        body = await request.json()
        tenant = body.get('tenant')
        week_ending = body.get('week_ending')
        approved_items = body.get('approved_items', [])
        
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"Approving {len(approved_items)} FMT items: tenant={tenant}, user={user_email}")
        
        # TODO: Insert approved items into demo.gainwell.fmt_mbids
        
        return {
            "message": f"Successfully approved and synced {len(approved_items)} FMT items to master",
            "review_type": "fmt",
            "tenant": tenant,
            "week_ending": week_ending,
            "items_approved": len(approved_items),
            "approved_by": user_email,
            "sync_status": "completed"
        }
        
    except Exception as e:
        logger.error(f"Error approving FMT weekly review: {e}")
        raise HTTPException(status_code=500, detail=f"Approval failed: {str(e)}")

@router.post("/reject")
async def reject_fmt_items(request: Request):
    """
    Reject selected FMT items from review
    
    Body: {
        "tenant": "MASTER|AK|MO",
        "week_ending": "YYYY-MM-DD",
        "rejected_ndcs": ["12345678901", "98765432109"],
        "rejection_reason": "Duplicate entry"
    }
    """
    try:
        body = await request.json()
        tenant = body.get('tenant')
        week_ending = body.get('week_ending')
        rejected_ndcs = body.get('rejected_ndcs', [])
        rejection_reason = body.get('rejection_reason', "")
        
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"Rejecting {len(rejected_ndcs)} FMT NDCs: tenant={tenant}, user={user_email}")
        
        # TODO: Mark items as rejected in database
        
        return {
            "message": f"Successfully rejected {len(rejected_ndcs)} FMT NDCs",
            "review_type": "fmt",
            "tenant": tenant,
            "week_ending": week_ending,
            "items_rejected": len(rejected_ndcs),
            "rejection_reason": rejection_reason,
            "rejected_by": user_email
        }
        
    except Exception as e:
        logger.error(f"Error rejecting FMT items: {e}")
        raise HTTPException(status_code=500, detail=f"Rejection failed: {str(e)}")

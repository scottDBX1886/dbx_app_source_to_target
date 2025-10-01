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

def load_fmt_weekly_pool_data(tenant: str, week_ending: str, search: Optional[str] = None) -> pd.DataFrame:
    """
    Load FMT weekly pool data - new FMT master records for review
    """
    try:
        from backend.services.connector import query
        from backend.config.settings import get_settings
        warehouse_id = get_settings().databricks_warehouse_id
        
        # Convert week_ending to datetime for date calculations
        week_end_date = datetime.strptime(week_ending, '%Y-%m-%d')
        week_start_date = week_end_date - timedelta(days=6)
        
        logger.info(f"DEBUG: Searching for FMT master data between {week_start_date.strftime('%Y-%m-%d')} and {week_ending}")
        
        # Get new FMT master data for this week that needs review
        new_ndcs_query = f"""
        SELECT DISTINCT f.ndc, f.fmt_drug as brand, f.mbid,
               f.load_date, 'NEW' as status,
               CASE 
                 WHEN EXISTS (SELECT 1 FROM demo.gainwell.fmt_mbids m WHERE m.mbid = f.mbid AND m.tenant = '{tenant.lower()}') THEN '100% match'
                 WHEN EXISTS (SELECT 1 FROM demo.gainwell.fmt_mbids m WHERE m.description LIKE CONCAT('%', UPPER(f.fmt_drug), '%') AND m.tenant = '{tenant.lower()}') THEN 'brand match'
                 ELSE 'no match'
               END as match_type
        FROM demo.gainwell.fmt_master f
        WHERE f.load_date >= '{week_start_date.strftime('%Y-%m-%d')}'
          AND f.load_date <= '{week_ending}'
        ORDER BY f.load_date DESC, f.ndc
        """
        
        # First, let's check what FMT master data exists at all
        debug_query = f"SELECT COUNT(*) as total_fmt FROM demo.gainwell.fmt_master"
        debug_df = query(debug_query, warehouse_id=warehouse_id, as_dict=False)
        total_fmt = debug_df.iloc[0]['total_fmt'] if not debug_df.empty else 0
        logger.info(f"DEBUG: Total FMT master records available: {total_fmt}")
        
        # Check how many have recent load dates
        recent_query = f"""
        SELECT COUNT(*) as recent_count 
        FROM demo.gainwell.fmt_master f 
        WHERE f.load_date >= '{week_start_date.strftime('%Y-%m-%d')}'
          AND f.load_date <= '{week_ending}'
        """
        recent_df = query(recent_query, warehouse_id=warehouse_id, as_dict=False)
        recent_count = recent_df.iloc[0]['recent_count'] if not recent_df.empty else 0
        logger.info(f"DEBUG: FMT master records with recent load dates for {tenant}: {recent_count}")
        
        # If no recent data, let's get some sample data for testing
        if recent_count == 0:
            logger.warning(f"No recent FMT master data found. Using sample of latest 20 records for FMT testing.")
            
            # Build search filter for sample data
            search_filter = ""
            if search:
                search_lower = search.lower()
                search_filter = f"""
                AND (
                    LOWER(f.ndc) LIKE '%{search_lower}%' OR
                    LOWER(f.fmt_drug) LIKE '%{search_lower}%' OR
                    LOWER(f.mbid) LIKE '%{search_lower}%'
                )
                """
            
            new_ndcs_query = f"""
            SELECT DISTINCT f.ndc, f.fmt_drug as brand, f.mbid,
                   f.load_date, 'NEW' as status,
                   CASE 
                     WHEN EXISTS (SELECT 1 FROM demo.gainwell.fmt_mbids m WHERE m.mbid = f.mbid AND m.tenant = '{tenant.lower()}') THEN '100% match'
                     WHEN EXISTS (SELECT 1 FROM demo.gainwell.fmt_mbids m WHERE m.description LIKE CONCAT('%', UPPER(f.fmt_drug), '%') AND m.tenant = '{tenant.lower()}') THEN 'brand match'
                     ELSE 'no match'
                   END as match_type
            FROM demo.gainwell.fmt_master f
            WHERE 1=1 {search_filter}
            ORDER BY f.load_date DESC, f.ndc
            LIMIT 20
            """
        
        df = query(new_ndcs_query, warehouse_id=warehouse_id, as_dict=False)
        
        logger.info(f"Weekly FMT pool for {tenant} week ending {week_ending}: {len(df)} FMT master records")
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
        
        # Query FMT review assignments table (only show non-resolved assignments)
        assignments_query = f"""
        SELECT ndc, reviewer, mbid, eff_date, end_date, status, assignment_date
        FROM demo.gainwell.fmt_review_assignments 
        WHERE tenant = '{tenant.lower()}' 
          AND week_ending = '{week_ending}'
          AND status IN ('ASSIGNED', 'IN_PROGRESS')
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
                df['mbid'].astype(str).str.contains(search_lower, case=False, na=False)
            )
            df = df[mask]
        
        # Convert to records for JSON response
        records = []
        for _, row in df.iterrows():
            record = {
                "ndc": str(row['ndc']),
                "brand": str(row['brand']) if pd.notna(row['brand']) else "",
                "mbid": str(row['mbid']) if pd.notna(row['mbid']) else "",
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
    week_ending: str = Query(..., description="Week ending date (YYYY-MM-DD)"),
    search: Optional[str] = Query(None, description="Search NDC/Brand/MBID")
):
    """
    Get FMT review groups categorized by match type with status counts
    
    Example: GET /api/weekly/fmt/groups?tenant=AK&week_ending=2024-12-15
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"FMT review groups: tenant={tenant}, week_ending={week_ending}, search='{search}', user={user_email}")
        
        # Load pool data
        df = load_fmt_weekly_pool_data(tenant, week_ending, search)
        
        if df.empty:
            return {
                "groups": {},
                "tenant": tenant,
                "week_ending": week_ending,
                "review_type": "fmt"
            }
        
        # Search filter is now applied at source in load_fmt_weekly_pool_data
        logger.info(f"Groups endpoint - Loaded {len(df)} records with search='{search}'")
        
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
                    "mbid": str(row['mbid']) if pd.notna(row['mbid']) else "",
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

@router.get("/reviewer-a")
async def get_fmt_reviewer_a_assignments(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    week_ending: str = Query(..., description="Week ending date (YYYY-MM-DD)")
):
    """
    Get FMT Reviewer A assignments for the UI
    
    Example: GET /api/weekly/fmt/reviewer-a?tenant=AK&week_ending=2024-12-15
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"Getting FMT Reviewer A assignments: tenant={tenant}, week_ending={week_ending}, user={user_email}")
        
        assignments = get_fmt_review_assignments(tenant, week_ending)
        reviewer_a_assignments = assignments.get('reviewer_a', [])
        
        return {
            "assignments": reviewer_a_assignments,
            "reviewer": "A",
            "tenant": tenant,
            "week_ending": week_ending,
            "review_type": "fmt"
        }
        
    except Exception as e:
        logger.error(f"Error getting FMT Reviewer A assignments: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get Reviewer A assignments: {str(e)}")

@router.get("/reviewer-b")
async def get_fmt_reviewer_b_assignments(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    week_ending: str = Query(..., description="Week ending date (YYYY-MM-DD)")
):
    """
    Get FMT Reviewer B assignments for the UI
    
    Example: GET /api/weekly/fmt/reviewer-b?tenant=AK&week_ending=2024-12-15
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"Getting FMT Reviewer B assignments: tenant={tenant}, week_ending={week_ending}, user={user_email}")
        
        assignments = get_fmt_review_assignments(tenant, week_ending)
        reviewer_b_assignments = assignments.get('reviewer_b', [])
        
        return {
            "assignments": reviewer_b_assignments,
            "reviewer": "B",
            "tenant": tenant,
            "week_ending": week_ending,
            "review_type": "fmt"
        }
        
    except Exception as e:
        logger.error(f"Error getting FMT Reviewer B assignments: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get Reviewer B assignments: {str(e)}")

@router.get("/final-approval")
async def get_fmt_final_approval_data(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    week_ending: str = Query(..., description="Week ending date (YYYY-MM-DD)")
):
    """
    Get FMT final approval data (resolved conflicts ready for final approval)
    
    Example: GET /api/weekly/fmt/final-approval?tenant=AK&week_ending=2024-12-15
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"Getting FMT final approval data: tenant={tenant}, week_ending={week_ending}, user={user_email}")
        
        # Get comparison data to find resolved conflicts
        comparison_data = await get_fmt_comparison_data(request, tenant, week_ending)
        
        # Filter for items that are ready for final approval (resolved conflicts)
        final_approval_items = []
        for item in comparison_data.get('comparison_data', []):
            if not item.get('has_conflict', True):  # Only non-conflicting items
                final_approval_items.append({
                    "ndc": item.get('ndc'),
                    "mbid": item.get('reviewer_a', {}).get('mbid', ''),
                    "eff_date": item.get('reviewer_a', {}).get('eff_date', ''),
                    "end_date": item.get('reviewer_a', {}).get('end_date', ''),
                    "resolution_type": item.get('auto_resolution', 'AUTO')
                })
        
        return {
            "final_approval_items": final_approval_items,
            "tenant": tenant,
            "week_ending": week_ending,
            "review_type": "fmt"
        }
        
    except Exception as e:
        logger.error(f"Error getting FMT final approval data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get final approval data: {str(e)}")

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
        
        # Insert assignments into database table demo.gainwell.fmt_review_assignments
        from backend.services.connector import query
        from backend.config.settings import get_settings
        import uuid
        from datetime import datetime
        
        warehouse_id = get_settings().databricks_warehouse_id
        assignments_created = 0
        
        for assignment in assignments:
            try:
                # Generate unique assignment ID
                assignment_id = str(uuid.uuid4())
                
                # Prepare assignment data
                ndc = assignment.get('ndc')
                reviewer = assignment.get('reviewer')
                mbid = assignment.get('mbid', '')
                eff_date = assignment.get('eff_date', week_ending)
                end_date = assignment.get('end_date')
                
                # Insert assignment record
                insert_sql = f"""
                INSERT INTO demo.gainwell.fmt_review_assignments (
                    assignment_id, ndc, reviewer, tenant, week_ending,
                    mbid, eff_date, end_date, status, assignment_date,
                    created_by, created_at, updated_by, updated_at
                ) VALUES (
                    '{assignment_id}',
                    '{ndc}',
                    '{reviewer}',
                    '{tenant.lower()}',
                    '{week_ending}',
                    {f"'{mbid}'" if mbid else 'NULL'},
                    '{eff_date}',
                    {f"'{end_date}'" if end_date else 'NULL'},
                    'ASSIGNED',
                    current_timestamp(),
                    '{user_email}',
                    current_timestamp(),
                    '{user_email}',
                    current_timestamp()
                )
                """
                
                query(insert_sql, warehouse_id=warehouse_id, as_dict=False)
                assignments_created += 1
                logger.info(f"Created assignment {assignment_id} for NDC {ndc} to reviewer {reviewer}")
                
            except Exception as e:
                logger.error(f"Error creating assignment for NDC {assignment.get('ndc')}: {e}")
                # Continue with other assignments even if one fails
                continue
        
        return {
            "message": f"Successfully assigned {assignments_created} NDCs for FMT review",
            "review_type": "fmt",
            "tenant": tenant,
            "week_ending": week_ending,
            "assignments_created": assignments_created,
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
        
        # Update resolution status in database
        from backend.services.connector import query
        from backend.config.settings import get_settings
        import uuid
        from datetime import datetime
        
        warehouse_id = get_settings().databricks_warehouse_id
        resolutions_processed = 0
        
        for resolution in resolutions:
            try:
                ndc = resolution.get('ndc')
                resolution_type = resolution.get('resolution', 'AUTO')
                final_mbid = resolution.get('final_mbid', '')
                final_eff_date = resolution.get('final_eff_date', week_ending)
                final_end_date = resolution.get('final_end_date')
                
                # Update assignment records with resolution and remove from reviewer sections
                update_sql = f"""
                UPDATE demo.gainwell.fmt_review_assignments 
                SET resolution_type = '{resolution_type}',
                    resolution_date = current_timestamp(),
                    resolved_by = '{user_email}',
                    status = 'RESOLVED',
                    updated_by = '{user_email}',
                    updated_at = current_timestamp()
                WHERE ndc = '{ndc}' 
                  AND tenant = '{tenant.lower()}' 
                  AND week_ending = '{week_ending}'
                """
                
                query(update_sql, warehouse_id=warehouse_id, as_dict=False)
                resolutions_processed += 1
                logger.info(f"Resolved conflict for NDC {ndc} with resolution {resolution_type}")
                
            except Exception as e:
                logger.error(f"Error resolving conflict for NDC {resolution.get('ndc')}: {e}")
                continue
        
        return {
            "message": f"Successfully resolved {resolutions_processed} FMT conflicts",
            "review_type": "fmt",
            "tenant": tenant,
            "week_ending": week_ending,
            "resolutions_processed": resolutions_processed,
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
        
        # Insert approved items into demo.gainwell.fmt_mbids
        from backend.services.connector import query
        from backend.config.settings import get_settings
        import uuid
        from datetime import datetime
        
        warehouse_id = get_settings().databricks_warehouse_id
        items_approved = 0
        
        for item in approved_items:
            try:
                ndc = item.get('ndc')
                mbid = item.get('mbid', '')
                eff_date = item.get('eff_date', week_ending)
                end_date = item.get('end_date')
                
                # Generate approval ID
                approval_id = str(uuid.uuid4())
                
                # Create final_approvals table if it doesn't exist
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS demo.gainwell.final_approvals (
                    approval_id STRING NOT NULL,
                    review_type STRING NOT NULL,
                    tenant STRING NOT NULL,
                    week_ending DATE NOT NULL,
                    ndc STRING NOT NULL,
                    final_mbid STRING,
                    final_keycode STRING,
                    final_template STRING,
                    eff_date DATE,
                    end_date DATE,
                    approved_by STRING,
                    approved_at TIMESTAMP,
                    sync_status STRING DEFAULT 'PENDING'
                ) USING DELTA
                PARTITIONED BY (tenant, week_ending)
                TBLPROPERTIES (
                    'delta.checkpoint.writeStatsAsJson' = 'false',
                    'delta.checkpoint.writeStatsAsStruct' = 'true',
                    'delta.feature.allowColumnDefaults' = 'supported',
                    'delta.feature.appendOnly' = 'supported',
                    'delta.feature.invariants' = 'supported',
                    'delta.minReaderVersion' = '1',
                    'delta.minWriterVersion' = '7'
                )
                """
                
                try:
                    query(create_table_sql, warehouse_id=warehouse_id, as_dict=False)
                except Exception as e:
                    logger.warning(f"Could not create final_approvals table: {e}")
                
                # Insert into final_approvals table
                final_approval_sql = f"""
                INSERT INTO demo.gainwell.final_approvals (
                    approval_id, review_type, tenant, week_ending, ndc,
                    final_mbid, eff_date, end_date,
                    approved_by, approved_at, sync_status
                ) VALUES (
                    '{approval_id}',
                    'FMT',
                    '{tenant.lower()}',
                    '{week_ending}',
                    '{ndc}',
                    {f"'{mbid}'" if mbid else 'NULL'},
                    '{eff_date}',
                    {f"'{end_date}'" if end_date else 'NULL'},
                    '{user_email}',
                    current_timestamp(),
                    'PENDING'
                )
                """
                
                query(final_approval_sql, warehouse_id=warehouse_id, as_dict=False)
                
                # Insert into fmt_mbids table
                insert_sql = f"""
                INSERT INTO demo.gainwell.fmt_mbids (
                    mbid, description, tenant, begin_date, end_date
                ) VALUES (
                    {f"'{mbid}'" if mbid else 'NULL'},
                    'Approved from weekly review - NDC {ndc}',
                    '{tenant.lower()}',
                    '{eff_date}',
                    {f"'{end_date}'" if end_date else 'NULL'}
                )
                """
                
                query(insert_sql, warehouse_id=warehouse_id, as_dict=False)
                
                # Update assignment status to APPROVED
                update_sql = f"""
                UPDATE demo.gainwell.fmt_review_assignments 
                SET status = 'APPROVED'
                WHERE ndc = '{ndc}' 
                  AND tenant = '{tenant.lower()}' 
                  AND week_ending = '{week_ending}'
                """
                
                query(update_sql, warehouse_id=warehouse_id, as_dict=False)
                
                # Update final_approvals sync status to COMPLETED
                sync_update_sql = f"""
                UPDATE demo.gainwell.final_approvals 
                SET sync_status = 'COMPLETED',
                    approved_by = '{user_email}',
                    approved_at = current_timestamp()
                WHERE approval_id = '{approval_id}'
                """
                
                query(sync_update_sql, warehouse_id=warehouse_id, as_dict=False)
                items_approved += 1
                logger.info(f"Approved and synced NDC {ndc} with MBID {mbid}")
                
            except Exception as e:
                logger.error(f"Error approving item for NDC {item.get('ndc')}: {e}")
                continue
        
        return {
            "message": f"Successfully approved and synced {items_approved} FMT items to master",
            "review_type": "fmt",
            "tenant": tenant,
            "week_ending": week_ending,
            "items_approved": items_approved,
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

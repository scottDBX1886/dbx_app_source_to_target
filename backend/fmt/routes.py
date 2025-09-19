import os
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional
import logging
import pandas as pd
import json
import io
from backend.services.connector import query, insert_data
from backend.config.settings import Settings, get_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fmt", tags=["fmt-master"])


def load_fmt_data(tenant: str = "MASTER") -> pd.DataFrame:
    """
    Load FMT Master data with tenant-specific filtering using sample data.
    
    TODO: Replace with actual database connection when ready.
    Currently using sample CSV files from generate_fmt_data.py
    
    For production:
    - Connect to your database/data lake/API
    - Use: warehouse_id = get_settings().databricks_warehouse_id
    - Query: df_fmt = query(f"SELECT * FROM demo.gainwell.fmt_master", warehouse_id=warehouse_id, as_dict=False)
    - Query: df_mbids = query(f"SELECT * FROM demo.gainwell.fmt_mbids WHERE tenant = '{tenant}' OR tenant = 'MASTER'", warehouse_id=warehouse_id, as_dict=False)
    """
    try:
        # Load core FMT data from sample files with absolute path
        import os
        base_dir = os.getcwd()
        fmt_file = os.path.join(base_dir, "sample_fmt_data", "fmt_master.csv")
        mbids_file = os.path.join(base_dir, "sample_fmt_data", "fmt_mbids.csv")
        
        logger.info(f"Looking for FMT files at: {fmt_file}, {mbids_file}")
        
        if not os.path.exists(fmt_file):
            raise FileNotFoundError(f"FMT master file not found: {fmt_file}")
        if not os.path.exists(mbids_file):
            raise FileNotFoundError(f"MBID file not found: {mbids_file}")
        
        df_fmt = pd.read_csv(fmt_file)
        df_mbids = pd.read_csv(mbids_file)
        
        # Filter MBIDs for tenant (include MASTER + specific tenant)
        df_mbids_filtered = df_mbids[
            (df_mbids['tenant'] == 'MASTER') | (df_mbids['tenant'] == tenant)
        ]
        
        # Enhanced debugging
        logger.info(f"=== FMT TENANT: {tenant} ===")
        logger.info(f"FMT Master records: {len(df_fmt)}")
        logger.info(f"MBID records for {tenant}: {len(df_mbids_filtered)}")
        logger.info(f"FMT NDC sample: {df_fmt['ndc'].head().tolist()}")
        
        # Join FMT data with MBID descriptions
        if not df_mbids_filtered.empty:
            df = pd.merge(df_fmt, df_mbids_filtered, on='mbid', how='left', suffixes=('', '_mbid'))
        else:
            df = df_fmt
        
        logger.info(f"Final FMT dataset: {len(df)} records")
        return df
        
    except Exception as e:
        logger.error(f"Error loading FMT data for tenant {tenant}: {e}")
        # Return empty DataFrame with expected columns to prevent crashes
        empty_df = pd.DataFrame(columns=[
            "ndc", "fmt_drug", "mbid", "status", "start_date", "end_date", "load_date",
            "effective_date", "expiration_date", "created_by", "updated_by", "notes", "review_status",
            "description", "tenant_mbid", "begin_date"
        ])
        
        raise HTTPException(
            status_code=500,
            detail="No FMT data available - please check data files in sample_fmt_data/ or integrate your database connection."
        )


def search_dataframe(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """
    Search dataframe across multiple FMT fields
    """
    if not query or df.empty:
        return df
    
    query_lower = query.lower()
    
    # Start with False mask
    mask = pd.Series([False] * len(df), index=df.index)
    
    # Search across available FMT-specific fields
    if 'ndc' in df.columns:
        mask = mask | df['ndc'].astype(str).str.contains(query_lower, case=False, na=False)
    if 'fmt_drug' in df.columns:
        mask = mask | df['fmt_drug'].str.contains(query_lower, case=False, na=False)
    if 'mbid' in df.columns:
        mask = mask | df['mbid'].astype(str).str.contains(query_lower, case=False, na=False)
    if 'status' in df.columns:
        mask = mask | df['status'].str.contains(query_lower, case=False, na=False)
    if 'description' in df.columns:
        mask = mask | df['description'].str.contains(query_lower, case=False, na=False)
    
    return df[mask]


@router.get("/search")
async def search_fmt_records(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    query: Optional[str] = Query(None, description="Search query"),
    status: Optional[str] = Query(None, description="Status filter (PDL/Approved/Review/Restricted)"),
    limit: Optional[int] = Query(100, description="Maximum records to return")
):
    """
    Search FMT master records for tenant
    
    Example: GET /api/fmt/search?tenant=AK&query=amoxicillin&limit=50
    """
    try:
        # Get user info from headers
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"FMT search: tenant={tenant}, query={query}, status={status}, limit={limit}, user={user_email}")
        
        # Load data for the specified tenant
        df = load_fmt_data(tenant)
        
        if df.empty:
            return {
                "tenant": tenant,
                "query": query,
                "error": "No data available - data loading not implemented",
                "total_found": 0,
                "records": []
            }
        
        logger.info(f"Loaded {len(df)} FMT records for tenant {tenant}")
        
        # Apply search filter
        if query:
            df = search_dataframe(df, query)
            logger.info(f"After search filter: {len(df)} records")
        
        # Apply status filter
        if status:
            df = df[df['status'].str.contains(status, case=False, na=False)]
            logger.info(f"After status filter: {len(df)} records")
        
        # Apply limit
        if limit and limit > 0:
            df = df.head(limit)
        
        # Convert to JSON-serializable format - main FMT table fields
        records = []
        for _, row in df.iterrows():
            try:
                record = {
                    "ndc": str(row['ndc']),
                    "fmt_drug": str(row['fmt_drug']) if pd.notna(row['fmt_drug']) else "",
                    "mbid": str(row['mbid']) if pd.notna(row['mbid']) else "",
                    "status": str(row['status']) if pd.notna(row['status']) else "",
                    "start_date": str(row['start_date']) if pd.notna(row['start_date']) else "",
                    "end_date": str(row['end_date']) if pd.notna(row['end_date']) else ""
                }
                records.append(record)
                
            except Exception as row_error:
                logger.error(f"Error processing row {row.name}: {row_error}")
                continue
        
        return {
            "tenant": tenant,
            "query": query,
            "status": status,
            "total_found": len(records),
            "records": records
        }
        
    except Exception as e:
        logger.error(f"Error in FMT search: {e}")
        return {
            "tenant": tenant,
            "query": query,
            "error": f"Search failed: {str(e)}",
            "total_found": 0,
            "records": []
        }


@router.get("/details/{ndc}")
async def get_fmt_details(
    ndc: str,
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)")
):
    """
    Get detailed FMT information for an NDC (for drawer popup)
    
    Example: GET /api/fmt/details/00011122233?tenant=AK
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"FMT details: ndc={ndc}, tenant={tenant}, user={user_email}")
        
        # Load data for the specified tenant
        df = load_fmt_data(tenant)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No FMT data available")
        
        # Find the specific NDC
        record = df[df['ndc'] == ndc]
        if record.empty:
            raise HTTPException(status_code=404, detail=f"NDC {ndc} not found in FMT master")
        
        # Get the first matching record (should be unique)
        row = record.iloc[0]
        
        # Build detailed response with safe field access
        def get_field(field_name, default=""):
            """Safely get field value from row"""
            if field_name in row.index and pd.notna(row[field_name]):
                return str(row[field_name])
            return default

        details = {
            "ndc": ndc,
            "core_info": {
                "ndc": get_field('ndc'),
                "fmt_drug": get_field('fmt_drug'),
                "status": get_field('status'),
                "load_date": get_field('load_date')
            },
            "mbid_info": {
                "mbid": get_field('mbid'),
                "description": get_field('description'),
                "tenant": get_field('tenant'),
                "begin_date": get_field('begin_date')
            },
            "date_info": {
                "start_date": get_field('start_date'),
                "end_date": get_field('end_date'),
                "effective_date": get_field('effective_date'),
                "expiration_date": get_field('expiration_date')
            },
            "audit_info": {
                "created_by": get_field('created_by'),
                "updated_by": get_field('updated_by'),
                "review_status": get_field('review_status'),
                "notes": get_field('notes')
            }
        }
        
        # Convert any numpy booleans to Python booleans for JSON serialization
        def convert_numpy_types(obj):
            if isinstance(obj, dict):
                return {k: convert_numpy_types(v) for k, v in obj.items()}
            elif hasattr(obj, 'item'):  # numpy scalar
                return obj.item()
            else:
                return obj
        
        details = convert_numpy_types(details)
        
        return details
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting FMT details for NDC {ndc}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get details: {str(e)}")


@router.get("/export")
async def export_fmt_data(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    format: str = Query("csv", description="Export format (csv/json)"),
    query: Optional[str] = Query(None, description="Optional search filter"),
    status: Optional[str] = Query(None, description="Optional status filter"),
    limit: Optional[int] = Query(None, description="Optional record limit")
):
    """
    Export FMT data in CSV or JSON format
    
    Example: GET /api/fmt/export?tenant=MO&format=csv&query=insulin&limit=1000
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"FMT export: tenant={tenant}, format={format}, query={query}, status={status}, user={user_email}")
        
        # Load data
        df = load_fmt_data(tenant)
        
        if df.empty:
            return {
                "tenant": tenant,
                "error": "No data available for export",
                "record_count": 0
            }
        
        # Apply filters
        if query:
            df = search_dataframe(df, query)
        if status:
            df = df[df['status'].str.contains(status, case=False, na=False)]
        if limit and limit > 0:
            df = df.head(limit)
        
        # Generate filename
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        query_suffix = f"_{query}" if query else ""
        filename = f"fmt_export_{tenant.lower()}{query_suffix}_{timestamp}.{format}"
        
        if format.lower() == "json":
            # JSON export
            records = []
            for _, row in df.iterrows():
                record = {col: str(val) if pd.notna(val) else "" for col, val in row.items()}
                records.append(record)
            
            json_data = json.dumps({
                "tenant": tenant,
                "export_timestamp": timestamp,
                "record_count": len(records),
                "query": query,
                "status": status,
                "records": records
            }, indent=2)
            
            return StreamingResponse(
                io.StringIO(json_data),
                media_type="application/json",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
        else:
            # CSV export (default)
            # Select main columns for export
            export_columns = ['ndc', 'fmt_drug', 'mbid', 'status', 'start_date', 'end_date', 'load_date']
            df_export = df[export_columns].copy()
            
            csv_data = df_export.to_csv(index=False)
            
            return StreamingResponse(
                io.StringIO(csv_data),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
    
    except Exception as e:
        logger.error(f"Error exporting FMT data: {e}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
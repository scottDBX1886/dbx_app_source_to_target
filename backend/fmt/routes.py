import os
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional
import logging
# Removed pandas import - using pure Spark DataFrames
import json
import io
from backend.services.connector import query, insert_data
from backend.config.settings import Settings, get_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fmt", tags=["fmt-master"])


def load_fmt_data(tenant: str = "MASTER") -> pd.DataFrame:
    """
    Load FMT Master data with tenant-specific filtering using sample data.

    
    For production:
    - Connect to your database/data lake/API
    - Use: warehouse_id = get_settings().databricks_warehouse_id
    - Query: df_fmt = query(f"SELECT * FROM demo.gainwell.fmt_master", warehouse_id=warehouse_id, as_dict=False)
    - Query: df_mbids = query(f"SELECT * FROM demo.gainwell.fmt_mbids WHERE tenant = '{tenant}' OR tenant = 'MASTER'", warehouse_id=warehouse_id, as_dict=False)
    """
    try:
        
        warehouse_id = get_settings().databricks_warehouse_id
        
        df_fmt = query(f"SELECT * FROM demo.gainwell.fmt_master", warehouse_id=warehouse_id, as_dict=False)
        df_mbids = query(f"SELECT * FROM demo.gainwell.fmt_mbids WHERE tenant = '{tenant}'", warehouse_id=warehouse_id, as_dict=False)
        
        # Enhanced debugging
        logger.info(f"=== FMT TENANT: {tenant} ===")
        logger.info(f"FMT Master records: {df_fmt.count()}")
        logger.info(f"MBID records for {tenant}: {df_mbids.count()}")
        
        # Sample NDCs for logging
        try:
            ndc_sample = [row.ndc for row in df_fmt.select('ndc').limit(5).collect()]
            logger.info(f"FMT NDC sample: {ndc_sample}")
        except:
            logger.info(f"FMT NDC sample: No data")
        
        # Join DataFrames if mbids available
        if df_mbids.count() > 0:
            df = df_fmt.join(df_mbids, on='mbid', how='inner')
        else:
            df = df_fmt
        
        logger.info(f"Final FMT dataset: {df.count()} records")
        return df
        
    except Exception as e:
        logger.error(f"Error loading FMT data for tenant {tenant}: {e}")
        # Log error and raise exception - Spark DataFrames handle this differently
        
        raise HTTPException(
            status_code=500,
            detail="No FMT data available - please check data files in sample_fmt_data/ or integrate your database connection."
        )


def search_dataframe(df, query: str):
    """
    Search Spark DataFrame across multiple FMT fields
    """
    if not query or df.count() == 0:
        return df
    
    from pyspark.sql.functions import col, lower, when, isnull
    
    query_lower = query.lower()
    
    # Build search condition across available FMT-specific fields
    search_condition = None
    
    # Check available columns and build search conditions
    df_columns = df.columns
    
    if 'ndc' in df_columns:
        condition = lower(col('ndc').cast('string')).contains(query_lower)
        search_condition = condition if search_condition is None else search_condition | condition
    
    if 'fmt_drug' in df_columns:
        condition = lower(col('fmt_drug')).contains(query_lower)
        search_condition = condition if search_condition is None else search_condition | condition
    
    if 'mbid' in df_columns:
        condition = lower(col('mbid').cast('string')).contains(query_lower)
        search_condition = condition if search_condition is None else search_condition | condition
        
    if 'status' in df_columns:
        condition = lower(col('status')).contains(query_lower)
        search_condition = condition if search_condition is None else search_condition | condition
        
    if 'description' in df_columns:
        condition = lower(col('description')).contains(query_lower)
        search_condition = condition if search_condition is None else search_condition | condition
    
    # Apply search condition if any was built
    if search_condition is not None:
        return df.filter(search_condition)
    else:
        return df


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
        
        if df.count() == 0:
            return {
                "tenant": tenant,
                "query": query,
                "error": "No data available - data loading not implemented",
                "total_found": 0,
                "records": []
            }
        
        logger.info(f"Loaded {df.count()} FMT records for tenant {tenant}")
        
        # Apply search filter
        if query:
            df = search_dataframe(df, query)
            logger.info(f"After search filter: {df.count()} records")
        
        # Apply status filter
        if status:
            from pyspark.sql.functions import col, lower
            df = df.filter(lower(col('status')).contains(status.lower()))
            logger.info(f"After status filter: {df.count()} records")
        
        # Apply limit
        if limit and limit > 0:
            df = df.limit(limit)
        
        # Convert to JSON-serializable format - main FMT table fields
        records = []
        for row in df.collect():
            try:
                def safe_str(value):
                    return str(value) if value is not None else ""
                
                record = {
                    "ndc": safe_str(row['ndc']),
                    "fmt_drug": safe_str(row['fmt_drug']),
                    "mbid": safe_str(row['mbid']),
                    "status": safe_str(row['status']),
                    "start_date": safe_str(row['start_date']),
                    "end_date": safe_str(row['end_date'])
                }
                records.append(record)
                
            except Exception as row_error:
                logger.error(f"Error processing row: {row_error}")
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
        
        if df.count() == 0:
            raise HTTPException(status_code=404, detail="No FMT data available")
        
        # Find the specific NDC (convert to string for comparison)
        logger.info(f"Searching for NDC {ndc} in {df.count()} records")
        
        # Sample NDCs for logging
        try:
            ndc_sample = [row.ndc for row in df.select('ndc').limit(5).collect()]
            logger.info(f"Available NDC sample: {ndc_sample}")
        except:
            logger.info(f"Available NDC sample: No data")
        
        from pyspark.sql.functions import col
        
        # Find matching records
        record = df.filter(col('ndc').cast('string') == str(ndc))
        if record.count() == 0:
            logger.warning(f"NDC {ndc} not found. Trying alternative search...")
            # Try without string conversion in case of type mismatch
            record = df.filter(col('ndc') == ndc)
            
        if record.count() == 0:
            raise HTTPException(status_code=404, detail=f"NDC {ndc} not found in FMT master. Available: {df.count()} records")
        
        # Get the first matching record (should be unique)
        row = record.collect()[0]
        
        # Build detailed response with safe field access
        def get_field(field_name, default=""):
            """Safely get field value from Spark Row"""
            try:
                value = row[field_name]
                return str(value) if value is not None else default
            except:
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
        
        if df.count() == 0:
            return {
                "tenant": tenant,
                "error": "No data available for export",
                "record_count": 0
            }
        
        # Apply filters
        if query:
            df = search_dataframe(df, query)
        if status:
            from pyspark.sql.functions import col, lower
            df = df.filter(lower(col('status')).contains(status.lower()))
        if limit and limit > 0:
            df = df.limit(limit)
        
        # Generate filename
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        query_suffix = f"_{query}" if query else ""
        filename = f"fmt_export_{tenant.lower()}{query_suffix}_{timestamp}.{format}"
        
        if format.lower() == "json":
            # JSON export
            records = []
            for row in df.collect():
                record = {col: str(row[col]) if row[col] is not None else "" for col in df.columns}
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
            available_columns = [col for col in export_columns if col in df.columns]
            df_export = df.select(available_columns)
            
            # Convert to pandas for CSV output (temporary conversion just for export)
            import pandas as pd
            csv_data = df_export.toPandas().to_csv(index=False)
            
            return StreamingResponse(
                io.StringIO(csv_data),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
    
    except Exception as e:
        logger.error(f"Error exporting FMT data: {e}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
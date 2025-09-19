import os
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional
import logging
import pandas as pd
import json
import io

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pdl", tags=["pdl-coding"])

def load_pdl_data(tenant: str = "MASTER") -> pd.DataFrame:
    """
    Load PDL data with tenant-specific filtering.
    TODO: Replace with real database queries via services.connector
    """
    try:
        # TODO: Replace with database queries
        # from backend.services.connector import query
        # from backend.config.settings import get_settings
        # warehouse_id = get_settings().databricks_warehouse_id
        # df_pdl = query(f"SELECT * FROM demo.gainwell.pdl_master", warehouse_id=warehouse_id, as_dict=False)
        # df_keycodes = query(f"SELECT * FROM demo.gainwell.pdl_keycodes WHERE tenant = '{tenant}' OR tenant = 'MASTER'", warehouse_id=warehouse_id, as_dict=False)
        
        # For now, use sample data
        pdl_master_path = os.path.join(os.getcwd(), "sample_pdl_data", "pdl_master.csv")
        keycodes_path = os.path.join(os.getcwd(), "sample_pdl_data", "pdl_keycodes.csv")
        
        if os.path.exists(pdl_master_path) and os.path.exists(keycodes_path):
            df_pdl = pd.read_csv(pdl_master_path)
            df_keycodes = pd.read_csv(keycodes_path)
            
            # Filter keycodes by tenant
            df_keycodes_filtered = df_keycodes[
                (df_keycodes['tenant'] == tenant) | (df_keycodes['tenant'] == 'MASTER')
            ]
            
            logger.info(f"=== PDL TENANT: {tenant} ===")
            logger.info(f"PDL Master records: {len(df_pdl)}")
            logger.info(f"Keycode records for {tenant}: {len(df_keycodes_filtered)}")
            
            if not df_keycodes_filtered.empty:
                df = pd.merge(df_pdl, df_keycodes_filtered, on='ndc', how='left', suffixes=('', '_keycode'))
            else:
                df = df_pdl
            
            logger.info(f"Final PDL dataset: {len(df)} records")
            return df
        else:
            logger.warning(f"PDL sample data files not found")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error loading PDL data for tenant {tenant}: {e}")
        return pd.DataFrame()

def search_dataframe(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """
    Search dataframe across multiple PDL fields
    """
    if not query or df.empty:
        return df
    
    query_lower = query.lower()
    
    # Create mask for searchable columns
    mask = pd.Series([False] * len(df))
    
    if 'ndc' in df.columns:
        mask |= df['ndc'].astype(str).str.contains(query_lower, case=False, na=False)
    if 'pdl_drug' in df.columns:
        mask |= df['pdl_drug'].str.contains(query_lower, case=False, na=False)
    if 'key_code' in df.columns:
        mask |= df['key_code'].astype(str).str.contains(query_lower, case=False, na=False)
    if 'status' in df.columns:
        mask |= df['status'].str.contains(query_lower, case=False, na=False)
    if 'template' in df.columns:
        mask |= df['template'].str.contains(query_lower, case=False, na=False)
    
    return df[mask]

@router.get("/search")
async def search_pdl_records(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    query: Optional[str] = Query(None, description="Search query"),
    status: Optional[str] = Query(None, description="Status filter (Active/Inactive/Pending)"),
    limit: Optional[int] = Query(100, description="Maximum records to return")
):
    """
    Search PDL coding records for tenant
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"PDL search: tenant={tenant}, query={query}, status={status}, limit={limit}, user={user_email}")
        
        df = load_pdl_data(tenant)
        
        if df.empty:
            return {
                "tenant": tenant,
                "query": query,
                "error": "No PDL data available - data loading not implemented",
                "total_found": 0,
                "records": []
            }
        
        logger.info(f"Loaded {len(df)} PDL records for tenant {tenant}")
        
        if query:
            df = search_dataframe(df, query)
            logger.info(f"After search filter: {len(df)} records")
        
        if status:
            df = df[df['status'].str.contains(status, case=False, na=False)]
            logger.info(f"After status filter: {len(df)} records")
        
        if limit and limit > 0:
            df = df.head(limit)
        
        records = []
        for _, row in df.iterrows():
            try:
                record = {
                    "ndc": str(row['ndc']),
                    "pdl_drug": str(row.get('pdl_drug', '')) if pd.notna(row.get('pdl_drug', '')) else "",
                    "key_code": str(row.get('key_code', '')) if pd.notna(row.get('key_code', '')) else "",
                    "status": str(row.get('status', '')) if pd.notna(row.get('status', '')) else "",
                    "template": str(row.get('template', '')) if pd.notna(row.get('template', '')) else "",
                    "effective_date": str(row.get('effective_date', '')) if pd.notna(row.get('effective_date', '')) else ""
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
        logger.error(f"Error in PDL search: {e}")
        return {
            "tenant": tenant,
            "query": query,
            "error": f"Search failed: {str(e)}",
            "total_found": 0,
            "records": []
        }

@router.get("/details/{ndc}")
async def get_pdl_details(
    ndc: str,
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)")
):
    """
    Get detailed PDL information for an NDC (for drawer popup)
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"PDL details: ndc={ndc}, tenant={tenant}, user={user_email}")
        
        df = load_pdl_data(tenant)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No PDL data available")
        
        # Try different matching strategies
        record = df[df['ndc'].astype(str) == str(ndc)]
        if record.empty:
            # Fallback search
            record = df[df['ndc'].astype(str).str.contains(str(ndc), case=False, na=False)]
        
        if record.empty:
            logger.warning(f"NDC {ndc} not found in PDL data. Available NDCs: {df['ndc'].head().tolist()}")
            raise HTTPException(status_code=404, detail=f"NDC {ndc} not found in PDL coding")
        
        row = record.iloc[0]
        
        details = {
            "ndc": ndc,
            "core_info": {
                "ndc": str(row['ndc']),
                "pdl_drug": str(row.get('pdl_drug', '')) if pd.notna(row.get('pdl_drug', '')) else "",
                "status": str(row.get('status', '')) if pd.notna(row.get('status', '')) else "",
                "load_date": str(row.get('load_date', '')) if pd.notna(row.get('load_date', '')) else ""
            },
            "keycode_info": {
                "key_code": str(row.get('key_code', '')) if pd.notna(row.get('key_code', '')) else "",
                "template": str(row.get('template', '')) if pd.notna(row.get('template', '')) else "",
                "tenant": str(row.get('tenant_keycode', '')) if pd.notna(row.get('tenant_keycode', '')) else "",
                "generation_date": str(row.get('generation_date', '')) if pd.notna(row.get('generation_date', '')) else ""
            },
            "date_info": {
                "effective_date": str(row.get('effective_date', '')) if pd.notna(row.get('effective_date', '')) else "",
                "expiration_date": str(row.get('expiration_date', '')) if pd.notna(row.get('expiration_date', '')) else "",
                "last_updated": str(row.get('last_updated', '')) if pd.notna(row.get('last_updated', '')) else ""
            },
            "audit_info": {
                "created_by": str(row.get('created_by', '')) if pd.notna(row.get('created_by', '')) else "",
                "updated_by": str(row.get('updated_by', '')) if pd.notna(row.get('updated_by', '')) else "",
                "notes": str(row.get('notes', '')) if pd.notna(row.get('notes', '')) else "",
                "pos_export_status": str(row.get('pos_export_status', '')) if pd.notna(row.get('pos_export_status', '')) else ""
            }
        }
        
        # Convert numpy types to native Python types
        def convert_numpy_types(obj):
            if isinstance(obj, dict):
                return {k: convert_numpy_types(v) for k, v in obj.items()}
            elif hasattr(obj, 'item'):
                return obj.item()
            else:
                return obj
        
        details = convert_numpy_types(details)
        
        return details
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting PDL details for NDC {ndc}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get details: {str(e)}")

@router.get("/export")
async def export_pdl_data(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    format: str = Query("csv", description="Export format (csv/json)"),
    query: Optional[str] = Query(None, description="Optional search filter"),
    status: Optional[str] = Query(None, description="Optional status filter"),
    limit: Optional[int] = Query(None, description="Optional record limit")
):
    """
    Export PDL data in CSV or JSON format
    """
    try:
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"PDL export: tenant={tenant}, format={format}, query={query}, status={status}, user={user_email}")
        
        df = load_pdl_data(tenant)
        
        if df.empty:
            return {
                "tenant": tenant,
                "error": "No data available for export",
                "record_count": 0
            }
        
        if query:
            df = search_dataframe(df, query)
        if status:
            df = df[df['status'].str.contains(status, case=False, na=False)]
        if limit and limit > 0:
            df = df.head(limit)
        
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        query_suffix = f"_{query}" if query else ""
        filename = f"pdl_export_{tenant.lower()}{query_suffix}_{timestamp}.{format}"
        
        if format.lower() == "json":
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
            export_columns = ['ndc', 'pdl_drug', 'key_code', 'status', 'template', 'effective_date']
            available_columns = [col for col in export_columns if col in df.columns]
            df_export = df[available_columns].copy()
            
            csv_data = df_export.to_csv(index=False)
            
            return StreamingResponse(
                io.StringIO(csv_data),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
    
    except Exception as e:
        logger.error(f"Error exporting PDL data: {e}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

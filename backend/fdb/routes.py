import os
from fastapi import APIRouter, Request, Query, HTTPException, Depends
from fastapi.responses import StreamingResponse
from typing import Optional
import logging
import pandas as pd
import json
import io
from backend.services.connector import query, insert_data
from backend.config.settings import Settings, get_settings
from backend.auth.service_principal_utils import log_api_request, get_service_principal_context

# Configure logger for this module
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Local authentication dependency to avoid circular imports
async def get_authenticated_token() -> str:
    """Local wrapper for authentication to avoid circular imports"""
    from backend.auth.routes import get_authenticated_token as auth_token
    return await auth_token()

router = APIRouter(prefix="/api/fdb", tags=["fdb-search"])

async def load_fdb_data(tenant: str = "MASTER", access_token: str = None) -> pd.DataFrame:
    """
    Load FDB data from your data source
    """

    """
    def read_table(table_name, conn):
    with conn.cursor() as cursor:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()
    """

    try:
        warehouse_id = get_settings().databricks_warehouse_id
        
        # Debug logging
        logger.info(f"Using warehouse_id: {warehouse_id}")
        logger.info(f"Access token provided: {'Yes' if access_token else 'No'}")
        if access_token:
            logger.info(f"Access token length: {len(access_token)}")
        
        # First, let's test what catalogs are accessible
        try:
            logger.info("Testing accessible catalogs...")
            catalogs_result = query("SHOW CATALOGS", warehouse_id=warehouse_id, as_dict=True, access_token=access_token)
            logger.info(f"Available catalogs: {[cat.get('catalog', cat) for cat in catalogs_result]}")
        except Exception as catalog_error:
            logger.warning(f"Could not list catalogs: {catalog_error}")
        
        table_name = "samples.accuweather.forecast_daily_calendar_imperial"
        logger.info(f"Querying table: {table_name}")
        
        # Try the primary table first
        try:
            df_core = query(f"SELECT '123' as ndc,'0856789' as gsn,'DR REDDYS' as brand,10 as pkg_size,'BCA' as hic3 FROM {table_name};", warehouse_id=warehouse_id, as_dict=False, access_token=access_token)
        except Exception as primary_error:
            logger.error(f"Primary table failed: {primary_error}")
            
            # Try alternative catalog names if the service principal has access
            alternative_tables = [
                "pdl_de_dev.pdl_de_brnz.fdb_new_drugs_to_hist_vw",
                "pdl_de_dev.pdl_data_temp.fdb_new_drugs_to_hist_vw",
                "pdl_dev.pdl_data_temp.fdb_new_drugs_to_hist_vw"
            ]
            
            df_core = None
            for alt_table in alternative_tables:
                try:
                    logger.info(f"Trying alternative table: {alt_table}")
                    df_core = query(f"SELECT ndc,gsn,brand_name as brand,pkg_size,hic3 FROM {alt_table};", warehouse_id=warehouse_id, as_dict=False, access_token=access_token)
                    logger.info(f"Success with table: {alt_table}")
                    break
                except Exception as alt_error:
                    logger.warning(f"Alternative table {alt_table} failed: {alt_error}")
                    continue
            
            if df_core is None:
                raise Exception("No accessible FDB tables found with current service principal permissions")
        
        #df_formulary = query(f"SELECT ndc FROM pdl_dev.pdl_ref_brnz.fdb_new_drugs_to_hist_vw", warehouse_id=warehouse_id,as_dict=False)
        #logger.info(f"df_core preview:\n{df_core.head().to_string()} for tenant {tenant}")
        #logger.info(f"df_formulary preview:\n{df_formulary.head().to_string()} for tenant {tenant}")

        df = df_core
        
        logger.info(f"Loaded {len(df)} FDB records for tenant {tenant}")
        return df
        
    except Exception as e:
        logger.error(f"Error loading FDB data for tenant {tenant}: {e}")
        # Return empty DataFrame with expected columns to prevent crashes
        return pd.DataFrame(columns=[
            "ndc", "gsn", "brand", "pkg_size", "hic3"
        ])

def search_dataframe(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """
    Search dataframe across multiple text fields
    """
    if not query or df.empty:
        return df
    
    query_lower = query.lower()
    
    # Search across multiple fields
    mask = (
        df['ndc'].astype(str).str.contains(query_lower, case=False, na=False) |
        df['brand'].str.contains(query_lower, case=False, na=False) |
        df['hic3'].astype(str).str.contains(query_lower, case=False, na=False)
    )
    
    return df[mask]

@router.get("/search")
async def search_fdb_records(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    query: Optional[str] = Query(None, description="Search query"),
    limit: Optional[int] = Query(100, description="Result limit"),
    token: str = Depends(get_authenticated_token)
):
    """
    Search FDB records for a specific tenant
    
    Example: GET /api/fdb/search?tenant=AK&query=amoxicillin&limit=50
    Requires service principal authentication.
    """
    try:
        # Log request using standardized service principal utilities
        log_api_request("FDB search", tenant=tenant, query=query, limit=limit)
        
        # Load data for the specified tenant
        df = await load_fdb_data(tenant, token)
        
        if df.empty:
            return {
                "tenant": tenant,
                "query": query,
                "error": "No data available - data loading not implemented",
                "total_found": 0,
                "records": []
            }
        
        logger.info(f"Loaded {len(df)} records for tenant {tenant}")
        
        # Apply search filter
        if query:
            df = search_dataframe(df, query)
            logger.info(f"After search filter: {len(df)} records")
        
        # Apply limit
        if limit and limit > 0:
            df = df.head(limit)
        
        # Convert to JSON-serializable format - main FDB table fields
        records = []
        for _, row in df.iterrows():
            try:
                record = {
                    "ndc": str(row['ndc']),
                    "gsn": int(row['gsn']) if pd.notna(row['gsn']) else None,
                    "brand": str(row['brand']) if pd.notna(row['brand']) else "",
                    "pkg_size": str(row['pkg_size']) if pd.notna(row['pkg_size']) else "",
                    "hic3": str(row['hic3']) if pd.notna(row['hic3']) else ""
                }
                records.append(record)
                
            except Exception as row_error:
                logger.warning(f"Error processing row: {row_error}")
                continue
        
        return {
            "tenant": tenant,
            "query": query,
            "limit": limit,
            "total_found": len(records),
            "data_source": "Live Data",  # TODO: Update with actual source name
            "service_principal": get_service_principal_context()["client_id"],
            "records": records
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

@router.get("/details/{ndc}")
async def get_fdb_details(
    request: Request,
    ndc: str,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    token: str = Depends(get_authenticated_token)
):
    """
    Get detailed FDB record for specific NDC
    
    Example: GET /api/fdb/details/00003012345?tenant=AK
    Requires service principal authentication.
    """
    try:
        # Log request using standardized service principal utilities
        log_api_request("FDB details", ndc=ndc, tenant=tenant)
        
        # Load data
        df = await load_fdb_data(tenant, token)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="Data loading not implemented")
        
        # Find the specific NDC
        record = df[df['ndc'].astype(str) == ndc]
        if record.empty:
            raise HTTPException(status_code=404, detail=f"NDC {ndc} not found for tenant {tenant}")
        
        record = record.iloc[0]  # Get first match
        
        # Build detailed response matching prototype drawer structure
        details = {
            "ndc": ndc,
            "tenant": tenant,
            "data_source": "Live Data",  # TODO: Update with actual source name
            "service_principal": get_service_principal_context()["client_id"],
            "Core": {
                "NDC": str(record['ndc']),
                "GSN": int(record['gsn']) if pd.notna(record['gsn']) else None,
                "Brand": str(record['brand']) if pd.notna(record['brand']) else "",
                "Generic": str(record['generic']) if pd.notna(record['generic']) else "",
                "Rx/OTC": str(record['rx_otc']) if pd.notna(record['rx_otc']) else "",
                "Load Date": str(record['load_date']) if pd.notna(record['load_date']) else ""
            },
            "Classification": {
                "HIC3": str(record['hic3']) if pd.notna(record['hic3']) else "",
                "HICL": str(record['hicl']) if pd.notna(record['hicl']) else "",
                "DCC": str(record['dcc']) if pd.notna(record['dcc']) else "",
                "GSN Desc": str(record['gsn_desc']) if pd.notna(record['gsn_desc']) else ""
            },
            "Pricing & Flags": {
                "Federally Rebateable": bool(record['rebate']) if pd.notna(record['rebate']) else False,
                "MFT Obsolete": bool(record['obsolete']) if pd.notna(record['obsolete']) else False,
                "MFR": str(record['mfr']) if pd.notna(record['mfr']) else ""
            },
            "Packaging & Origin": {
                "Package Size": str(record['pkg_size']) if pd.notna(record['pkg_size']) else "",
                "Form": str(record['pkg_form']) if pd.notna(record['pkg_form']) else "",
                "Origin": str(record['pkg_origin']) if pd.notna(record['pkg_origin']) else ""
            }
        }
        
        # Add formulary info if available (tenant-specific data)
        if 'formulary_status' in record and pd.notna(record['formulary_status']):
            details["Formulary"] = {
                "Status": str(record['formulary_status']),
                "Tier": int(record['tier']) if pd.notna(record['tier']) else None,
                "Prior Auth Required": bool(record['pa_required']) if pd.notna(record['pa_required']) else False,
                "Quantity Limits": str(record['ql_limits']) if pd.notna(record['ql_limits']) and record['ql_limits'] else "None"
            }
        
        return details
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting FDB details for NDC {ndc}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get details: {str(e)}")

@router.get("/export")
async def export_fdb_data(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    format: str = Query("csv", description="Export format (csv/json)"),
    query: Optional[str] = Query(None, description="Optional search filter"),
    limit: Optional[int] = Query(None, description="Optional record limit"),
    token: str = Depends(get_authenticated_token)
):
    """
    Export FDB data for tenant
    
    Example: GET /api/fdb/export?tenant=MO&format=csv&query=insulin&limit=1000
    Requires service principal authentication.
    """
    try:
        # Log request using standardized service principal utilities
        log_api_request("FDB export", tenant=tenant, format=format, query=query, limit=limit)
        
        # Load data for the specified tenant
        df = await load_fdb_data(tenant, token)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data available - data loading not implemented")
        
        # Apply search filter if provided
        if query:
            df = search_dataframe(df, query)
        
        # Apply limit if provided
        if limit and limit > 0:
            df = df.head(limit)
        
        # Generate filename
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        query_suffix = f"_{query}" if query else ""
        filename = f"fdb_export_{tenant.lower()}{query_suffix}_{timestamp}.{format}"
        
        if format.lower() == "csv":
            # Generate CSV
            output = io.StringIO()
            df.to_csv(output, index=False)
            csv_content = output.getvalue()
            
            return StreamingResponse(
                io.BytesIO(csv_content.encode()),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
            
        elif format.lower() == "json":
            # Generate JSON
            records = df.to_dict('records')
            
            # Clean up NaN values for JSON serialization
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
            
            export_data = {
                "export_info": {
                    "tenant": tenant,
                    "query": query,
                    "total_records": len(records),
                    "export_timestamp": timestamp,
                    "data_source": "Live Data",  # TODO: Update with actual source name
                    "exported_by": get_service_principal_context()["client_id"]
                },
                "records": records
            }
            
            json_content = json.dumps(export_data, indent=2)
            
            return StreamingResponse(
                io.BytesIO(json_content.encode()),
                media_type="application/json",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
        else:
            raise HTTPException(status_code=400, detail="Unsupported format. Use 'csv' or 'json'")
            
    except Exception as e:
        logger.error(f"Error in FDB export: {e}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
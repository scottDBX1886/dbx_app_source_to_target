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

router = APIRouter(prefix="/api/fdb", tags=["fdb-search"])

def load_fdb_data(tenant: str = "MASTER") -> pd.DataFrame:
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
        
        df_core_spark = query(f"SELECT * FROM demo.gainwell.fdb_core_drugs", warehouse_id=warehouse_id,as_dict=False)
        df_formulary_spark = query(f"SELECT * FROM demo.gainwell.fdb_formulary_{tenant.lower()}", warehouse_id=warehouse_id,as_dict=False)
        
        # Convert Spark DataFrames to pandas DataFrames for processing
        df_core = df_core_spark.toPandas()
        df_formulary = df_formulary_spark.toPandas()
        
        logger.info(f"df_core preview:\n{df_core.head().to_string() if not df_core.empty else 'No data'} for tenant {tenant}")
        logger.info(f"df_formulary preview:\n{df_formulary.head().to_string() if not df_formulary.empty else 'No data'} for tenant {tenant}")

        df = pd.merge(df_core, df_formulary, on='ndc', how='inner')
        
        logger.info(f"Loaded {len(df)} FDB records for tenant {tenant}")
        return df
        
    except Exception as e:
        logger.error(f"Error loading FDB data for tenant {tenant}: {e}")
        # Return empty DataFrame with expected columns to prevent crashes
        return pd.DataFrame(columns=[
            "ndc", "gsn", "brand", "generic", "rx_otc", "pkg_size", "hic3", "hicl", "dcc", "mfr", "load_date",
            "obsolete", "rebate", "pkg_origin", "gsn_desc", "pkg_form",
            "formulary_status", "tier", "pa_required", "ql_limits"
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
        df['generic'].str.contains(query_lower, case=False, na=False) |
        df['mfr'].str.contains(query_lower, case=False, na=False) |
        df['hic3'].astype(str).str.contains(query_lower, case=False, na=False) |
        df['dcc'].str.contains(query_lower, case=False, na=False)
    )
    
    return df[mask]

@router.get("/search")
async def search_fdb_records(
    request: Request,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)"),
    query: Optional[str] = Query(None, description="Search query"),
    limit: Optional[int] = Query(100, description="Result limit")
):
    """
    Search FDB records for a specific tenant
    
    Example: GET /api/fdb/search?tenant=AK&query=amoxicillin&limit=50
    """
    try:
        # Get user info from headers
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"FDB search: tenant={tenant}, query={query}, limit={limit}, user={user_email}")
        
        # Load data for the specified tenant
        df = load_fdb_data(tenant)
        
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
                    "generic": str(row['generic']) if pd.notna(row['generic']) else "",
                    "rx_otc": str(row['rx_otc']) if pd.notna(row['rx_otc']) else "",
                    "pkg_size": str(row['pkg_size']) if pd.notna(row['pkg_size']) else "",
                    "hic3": str(row['hic3']) if pd.notna(row['hic3']) else "",
                    "hicl": str(row['hicl']) if pd.notna(row['hicl']) else "",
                    "dcc": str(row['dcc']) if pd.notna(row['dcc']) else "",
                    "mfr": str(row['mfr']) if pd.notna(row['mfr']) else "",
                    "load_date": str(row['load_date']) if pd.notna(row['load_date']) else ""
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
            "user_email": user_email,
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
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)")
):
    """
    Get detailed FDB record for specific NDC
    
    Example: GET /api/fdb/details/00003012345?tenant=AK
    """
    try:
        # Get user info from headers
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"FDB details: NDC={ndc}, tenant={tenant}, user={user_email}")
        
        # Load data
        df = load_fdb_data(tenant)
        
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
            "user_email": user_email,
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
    limit: Optional[int] = Query(None, description="Optional record limit")
):
    """
    Export FDB data for tenant
    
    Example: GET /api/fdb/export?tenant=MO&format=csv&query=insulin&limit=1000
    """
    try:
        # Get user info from headers
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"FDB export: tenant={tenant}, format={format}, user={user_email}")
        
        # Load data for the specified tenant
        df = load_fdb_data(tenant)
        
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
                    "exported_by": user_email
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
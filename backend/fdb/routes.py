"""
FDB Search API routes
Live integration with FDB data from Databricks Volume
"""
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional, List, Dict, Any
import logging
import pandas as pd
import json
import io
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Create router for FDB endpoints
router = APIRouter(prefix="/api/fdb", tags=["fdb-search"])

@router.get("/debug")
async def debug_volume_access():
    """Debug endpoint to check volume access and file availability"""
    import os
    
    debug_info = {
        "volume_paths_checked": {},
        "current_working_directory": os.getcwd(),
        "environment_variables": {
            "PATH": os.getenv("PATH", ""),
            "PYTHONPATH": os.getenv("PYTHONPATH", ""),
        },
        "pandas_available": False,
        "test_results": {}
    }
    
    # Check if pandas is available
    try:
        import pandas as pd
        debug_info["pandas_available"] = True
        debug_info["pandas_version"] = pd.__version__
    except ImportError as e:
        debug_info["pandas_import_error"] = str(e)
    
    # Check different volume path formats
    paths_to_check = [
        "/Volumes/demo/gainwell/fdb_data",
        "/dbfs/Volumes/demo/gainwell/fdb_data", 
        "fdb_core_drugs.csv"  # Check if files are in current directory
    ]
    
    for path in paths_to_check:
        debug_info["volume_paths_checked"][path] = {
            "exists": os.path.exists(path),
            "is_dir": os.path.isdir(path) if os.path.exists(path) else None,
            "is_file": os.path.isfile(path) if os.path.exists(path) else None,
        }
        
        # If it's a directory, list contents
        if os.path.exists(path) and os.path.isdir(path):
            try:
                debug_info["volume_paths_checked"][path]["contents"] = os.listdir(path)[:10]  # First 10 files
            except Exception as e:
                debug_info["volume_paths_checked"][path]["list_error"] = str(e)
    
    # Test pandas file reading if available
    if debug_info["pandas_available"]:
        test_file_path = None
        for path in ["/Volumes/demo/gainwell/fdb_data/fdb_core_drugs.csv", "/dbfs/Volumes/demo/gainwell/fdb_data/fdb_core_drugs.csv"]:
            if os.path.exists(path):
                test_file_path = path
                break
        
        if test_file_path:
            try:
                import pandas as pd
                df = pd.read_csv(test_file_path, nrows=5)
                debug_info["test_results"]["pandas_read_success"] = True
                debug_info["test_results"]["sample_columns"] = list(df.columns)
                debug_info["test_results"]["sample_row_count"] = len(df)
                debug_info["test_results"]["test_file_used"] = test_file_path
            except Exception as e:
                debug_info["test_results"]["pandas_read_error"] = str(e)
                debug_info["test_results"]["test_file_used"] = test_file_path
        else:
            debug_info["test_results"]["no_test_file_found"] = "Could not find fdb_core_drugs.csv in any expected location"
    
    return debug_info

# Volume configuration - Update these paths for your environment
VOLUME_BASE_PATH = "/Volumes/demo/gainwell/fdb_data"
FALLBACK_VOLUME_PATH = "/dbfs/Volumes/demo/gainwell/fdb_data"  # Alternative path format

def get_volume_path(filename: str) -> str:
    """Get the correct volume path for a file"""
    primary_path = f"{VOLUME_BASE_PATH}/{filename}"
    fallback_path = f"{FALLBACK_VOLUME_PATH}/{filename}"
    
    # Try primary path first
    if os.path.exists(primary_path):
        return primary_path
    elif os.path.exists(fallback_path):
        return fallback_path
    else:
        # Return primary path and let pandas handle the error
        return primary_path

def load_fdb_data(tenant: str = "MASTER") -> pd.DataFrame:
    """
    Load core FDB data with optional tenant-specific formulary data
    """
    try:
        # Check if pandas is available
        try:
            import pandas as pd
        except ImportError as e:
            logger.error(f"Pandas not available: {e}")
            raise HTTPException(
                status_code=500, 
                detail=f"Pandas library not installed. Please add pandas to requirements.txt. Error: {str(e)}"
            )
        
        # Load core drug data
        core_path = get_volume_path("fdb_core_drugs.csv")
        logger.info(f"Attempting to load core FDB data from: {core_path}")
        
        if not os.path.exists(core_path):
            logger.error(f"Core FDB file not found at: {core_path}")
            raise HTTPException(
                status_code=500, 
                detail=f"FDB core data file not found at {core_path}. Please ensure data is uploaded to the volume."
            )
        
        try:
            df_core = pd.read_csv(core_path)
            logger.info(f"Successfully loaded {len(df_core)} core records")
        except Exception as e:
            logger.error(f"Error reading core FDB file {core_path}: {e}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to read core FDB file: {str(e)}"
            )
        
        # Add tenant-specific formulary data if available
        tenant_lower = tenant.lower()
        formulary_path = get_volume_path(f"fdb_formulary_{tenant_lower}.csv")
        
        try:
            if os.path.exists(formulary_path):
                df_formulary = pd.read_csv(formulary_path)
                # Merge formulary data
                df_merged = pd.merge(df_core, df_formulary, on='ndc', how='left')
                logger.info(f"Loaded {len(df_merged)} records with {tenant} formulary data")
                return df_merged
            else:
                logger.warning(f"Formulary file not found for tenant {tenant} at {formulary_path}, using core data only")
        except Exception as e:
            logger.warning(f"Error loading formulary data for {tenant}: {e}, using core data only")
        
        return df_core
            
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading FDB data: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error loading FDB data: {str(e)}")

def search_dataframe(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """
    Search dataframe across multiple text fields
    """
    if not query:
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
        # Load data for the specified tenant
        df = load_fdb_data(tenant)
        
        # Apply search filter
        if query:
            df = search_dataframe(df, query)
        
        # Apply limit
        if limit and limit > 0:
            df = df.head(limit)
        
        # Convert to JSON-serializable format
        records = []
        for _, row in df.iterrows():
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
                "obsolete": bool(row['obsolete']) if pd.notna(row['obsolete']) else False,
                "rebate": bool(row['rebate']) if pd.notna(row['rebate']) else False,
                "load_date": str(row['load_date']) if pd.notna(row['load_date']) else "",
                "pkg_origin": str(row['pkg_origin']) if pd.notna(row['pkg_origin']) else "",
                "gsn_desc": str(row['gsn_desc']) if pd.notna(row['gsn_desc']) else "",
                "pkg_form": str(row['pkg_form']) if pd.notna(row['pkg_form']) else "",
            }
            
            # Add formulary data if available
            if 'formulary_status' in row and pd.notna(row['formulary_status']):
                record.update({
                    "formulary_status": str(row['formulary_status']),
                    "tier": int(row['tier']) if pd.notna(row['tier']) else None,
                    "pa_required": bool(row['pa_required']) if pd.notna(row['pa_required']) else False,
                    "ql_limits": str(row['ql_limits']) if pd.notna(row['ql_limits']) else None,
                })
            
            records.append(record)
        
        return {
            "tenant": tenant,
            "query": query,
            "limit": limit,
            "total_found": len(records),
            "data_source": f"Databricks Volume: {VOLUME_BASE_PATH}",
            "records": records
        }
        
    except Exception as e:
        logger.error(f"Error in FDB search: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@router.get("/details/{ndc}")
async def get_fdb_details(
    request: Request,
    ndc: str,
    tenant: str = Query(..., description="Tenant (MASTER/AK/MO)")
):
    """
    Get detailed FDB record for specific NDC with aggregated data
    
    Example: GET /api/fdb/details/00011122233?tenant=AK
    """
    try:
        # Load core data
        df = load_fdb_data(tenant)
        
        # Find the specific NDC
        record = df[df['ndc'].astype(str) == ndc]
        if record.empty:
            raise HTTPException(status_code=404, detail=f"NDC {ndc} not found for tenant {tenant}")
        
        record = record.iloc[0]  # Get first (should be only) match
        
        # Try to load additional data files for enrichment
        additional_data = {}
        
        # Try to load pricing data
        try:
            pricing_path = get_volume_path("fdb_pricing.csv")
            df_pricing = pd.read_csv(pricing_path)
            pricing_record = df_pricing[df_pricing['ndc'].astype(str) == ndc]
            if not pricing_record.empty:
                pricing = pricing_record.iloc[0]
                additional_data["pricing"] = {
                    "awp": float(pricing['awp']) if pd.notna(pricing['awp']) else None,
                    "wac": float(pricing['wac']) if pd.notna(pricing['wac']) else None,
                    "nadac": float(pricing['nadac']) if pd.notna(pricing['nadac']) else None,
                    "federal_rebate": float(pricing['federal_rebate']) if pd.notna(pricing['federal_rebate']) else None,
                    "state_rebate": float(pricing['state_rebate']) if pd.notna(pricing['state_rebate']) else None,
                }
        except Exception:
            logger.warning(f"Could not load pricing data for NDC {ndc}")
        
        # Try to load regional data
        try:
            regional_path = get_volume_path(f"fdb_regional_{tenant.lower()}.csv")
            df_regional = pd.read_csv(regional_path)
            regional_record = df_regional[df_regional['ndc'].astype(str) == ndc]
            if not regional_record.empty:
                regional = regional_record.iloc[0]
                additional_data["regional"] = {
                    "regional_code": str(regional['regional_code']) if pd.notna(regional['regional_code']) else None,
                    "preference_score": int(regional['preference_score']) if pd.notna(regional['preference_score']) else None,
                    "local_mfr": str(regional['local_mfr']) if pd.notna(regional['local_mfr']) else None,
                    "distribution_notes": str(regional['distribution_notes']) if pd.notna(regional['distribution_notes']) else None,
                }
        except Exception:
            logger.warning(f"Could not load regional data for NDC {ndc}")
        
        # Build detailed response
        details = {
            "ndc": ndc,
            "tenant": tenant,
            "data_source": f"Databricks Volume: {VOLUME_BASE_PATH}",
            "sections": {
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
                    "Federally Rebateable": "Yes" if bool(record['rebate']) else "No",
                    "MFT Obsolete": "Yes" if bool(record['obsolete']) else "No",
                    "MFR": str(record['mfr']) if pd.notna(record['mfr']) else ""
                },
                "Packaging & Origin": {
                    "Package Size": str(record['pkg_size']) if pd.notna(record['pkg_size']) else "",
                    "Form": str(record['pkg_form']) if pd.notna(record['pkg_form']) else "",
                    "Origin": str(record['pkg_origin']) if pd.notna(record['pkg_origin']) else ""
                }
            }
        }
        
        # Add formulary info if available
        if 'formulary_status' in record and pd.notna(record['formulary_status']):
            details["sections"]["Formulary"] = {
                "Status": str(record['formulary_status']),
                "Tier": int(record['tier']) if pd.notna(record['tier']) else None,
                "Prior Auth Required": "Yes" if bool(record['pa_required']) else "No",
                "Quantity Limits": str(record['ql_limits']) if pd.notna(record['ql_limits']) else "None"
            }
        
        # Add additional data sections
        if "pricing" in additional_data:
            details["sections"]["Pricing Details"] = additional_data["pricing"]
            
        if "regional" in additional_data:
            details["sections"]["Regional Info"] = additional_data["regional"]
        
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
        # Load data for the specified tenant
        df = load_fdb_data(tenant)
        
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
                    "data_source": f"Databricks Volume: {VOLUME_BASE_PATH}"
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

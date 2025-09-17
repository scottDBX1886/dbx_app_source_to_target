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
async def debug_volume_access(request: Request):
    """Debug endpoint to check volume access and authentication context"""
    try:
        import os
        
        debug_info = {
            "status": "debug_endpoint_working",
            "current_working_directory": os.getcwd(),
            "pandas_available": False,
            "spark_available": False,
            "basic_imports": {},
            "authentication_context": {},
            "environment_info": {}
        }
        
        # Check authentication headers (from user forwarding)
        debug_info["authentication_context"] = {
            "x_forwarded_email": request.headers.get("X-Forwarded-Email"),
            "x_forwarded_access_token": bool(request.headers.get("X-Forwarded-Access-Token")),
            "x_databricks_user_id": request.headers.get("X-Databricks-User-Id"),
            "authorization": bool(request.headers.get("Authorization")),
            "user_agent": request.headers.get("User-Agent"),
        }
        
        # Check request context for workspace URL determination
        debug_info["request_context"] = {
            "host": request.headers.get("host"),
            "x_forwarded_proto": request.headers.get("x-forwarded-proto"),
            "x_forwarded_host": request.headers.get("x-forwarded-host"),
            "referer": request.headers.get("referer"),
            "origin": request.headers.get("origin"),
        }
        
        # Derived workspace URL
        host = request.headers.get("host", "").lower()
        if "dbc-" in host or "azuredatabricks.net" in host:
            derived_workspace_url = f"https://{host}"
        else:
            derived_workspace_url = request.headers.get("x-forwarded-proto", "https") + "://" + host
        debug_info["derived_workspace_url"] = derived_workspace_url
        
        # Check environment variables
        debug_info["environment_info"] = {
            "databricks_runtime_version": os.getenv("DATABRICKS_RUNTIME_VERSION"),
            "spark_home": os.getenv("SPARK_HOME"),
            "java_home": os.getenv("JAVA_HOME"),
            "python_path": os.getenv("PYTHONPATH"),
            "path": os.getenv("PATH", "")[:200]  # First 200 chars
        }
        
        # Check basic imports
        try:
            import pandas as pd
            debug_info["pandas_available"] = True
            debug_info["pandas_version"] = pd.__version__
            debug_info["basic_imports"]["pandas"] = "SUCCESS"
        except Exception as e:
            debug_info["basic_imports"]["pandas"] = f"FAILED: {str(e)}"
        
        try:
            from pyspark.sql import SparkSession
            debug_info["spark_available"] = True
            debug_info["basic_imports"]["pyspark"] = "SUCCESS"
            
            # Try to create spark session with authentication context
            spark = SparkSession.builder.appName("FDBDebug").getOrCreate()
            debug_info["basic_imports"]["spark_session"] = "SUCCESS"
            debug_info["spark_session_info"] = {
                "spark_version": spark.version,
                "spark_context": str(spark.sparkContext),
                "sql_context": str(spark.sql)
            }
            
        except Exception as e:
            debug_info["basic_imports"]["pyspark"] = f"FAILED: {str(e)}"
        
        return debug_info
        
    except Exception as e:
        return {
            "status": "debug_endpoint_error", 
            "error": str(e),
            "error_type": type(e).__name__
        }

@router.get("/debug-simple")  
async def debug_simple():
    """Ultra simple debug endpoint"""
    try:
        return {"status": "alive", "message": "FDB routes working"}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@router.get("/debug-volume-permissions")
async def debug_volume_permissions(request: Request):
    """Test volume access permissions"""
    try:
        import os
        
        volume_info = {
            "volume_path": "/Volumes/demo/gainwell/fdb_data",
            "tests": {},
            "authentication_used": "service_principal"
        }
        
        # Check if we have user forwarding headers
        user_token = request.headers.get("X-Forwarded-Access-Token")
        user_email = request.headers.get("X-Forwarded-Email")
        
        if user_token:
            volume_info["authentication_used"] = "user_forwarding"
            volume_info["user_email"] = user_email
        
        # Test basic file system access
        volume_info["tests"]["volume_exists"] = os.path.exists("/Volumes")
        volume_info["tests"]["demo_catalog_exists"] = os.path.exists("/Volumes/demo")
        volume_info["tests"]["gainwell_schema_exists"] = os.path.exists("/Volumes/demo/gainwell")
        volume_info["tests"]["fdb_data_volume_exists"] = os.path.exists("/Volumes/demo/gainwell/fdb_data")
        
        # Try to list contents if volume exists
        if os.path.exists("/Volumes/demo/gainwell/fdb_data"):
            try:
                contents = os.listdir("/Volumes/demo/gainwell/fdb_data")
                volume_info["tests"]["volume_contents"] = contents[:10]  # First 10 files
            except Exception as e:
                volume_info["tests"]["list_error"] = str(e)
        
        # Test specific file access
        core_file = "/Volumes/demo/gainwell/fdb_data/fdb_core_drugs.csv"
        volume_info["tests"]["core_file_exists"] = os.path.exists(core_file)
        
        if os.path.exists(core_file):
            try:
                # Test file size and readability
                stat = os.stat(core_file)
                volume_info["tests"]["core_file_size"] = stat.st_size
                volume_info["tests"]["core_file_readable"] = os.access(core_file, os.R_OK)
            except Exception as e:
                volume_info["tests"]["core_file_stat_error"] = str(e)
        
        return volume_info
        
    except Exception as e:
        return {
            "status": "volume_debug_error",
            "error": str(e),
            "error_type": type(e).__name__
        }

@router.get("/debug-sdk-access")
async def debug_sdk_access(request: Request):
    """Test Databricks SDK access with detailed error handling"""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config
        import io
        import pandas as pd
        
        # Get user token
        user_token = request.headers.get("X-Forwarded-Access-Token")
        if not user_token:
            return {
                "status": "error",
                "message": "No user token available",
                "user_token_present": False
            }
        
        debug_info = {
            "user_token_present": True,
            "sdk_test_results": {}
        }
        
        # Test Databricks SDK
        test_file_path = "/Volumes/demo/gainwell/fdb_data/fdb_core_drugs.csv"
        
        try:
            # Initialize WorkspaceClient with user token, avoiding OAuth env var conflicts
            import os
            
            # Extract host from the environment 
            databricks_host = os.environ.get("DATABRICKS_HOST")
            if not databricks_host:
                databricks_host = "https://adb-3438839487639471.11.azuredatabricks.net"  # From error message
            
            debug_info["sdk_test_results"]["databricks_host"] = databricks_host
            
            # Create config with explicit host and token only, ignoring OAuth env vars
            config = Config(
                host=databricks_host,
                token=user_token,
                # Explicitly disable other auth methods
                username=None,
                password=None,
                client_id=None,
                client_secret=None
            )
            w = WorkspaceClient(config=config)
            
            debug_info["sdk_test_results"]["workspace_client_created"] = True
            debug_info["sdk_test_results"]["test_file_path"] = test_file_path
            
            # Try to download the file
            file_content = w.files.download(test_file_path)
            
            debug_info["sdk_test_results"]["download_success"] = True
            debug_info["sdk_test_results"]["content_length"] = len(file_content)
            
            # Try to parse as CSV
            df = pd.read_csv(io.BytesIO(file_content))
            debug_info["sdk_test_results"]["csv_parse_success"] = True
            debug_info["sdk_test_results"]["csv_rows"] = len(df)
            debug_info["sdk_test_results"]["csv_columns"] = list(df.columns)
            debug_info["sdk_test_results"]["first_few_rows"] = df.head(3).to_dict('records')
            
        except Exception as e:
            debug_info["sdk_test_results"]["exception"] = str(e)
            debug_info["sdk_test_results"]["exception_type"] = type(e).__name__
        
        return debug_info
        
    except Exception as e:
        return {
            "status": "debug_error",
            "error": str(e),
            "error_type": type(e).__name__
        }

# Volume configuration
VOLUME_BASE_PATH = "/Volumes/demo/gainwell/fdb_data"

async def load_fdb_data_via_sdk(tenant: str = "MASTER", user_token: str = None, request: Request = None) -> pd.DataFrame:
    """
    Load core FDB data with tenant-specific formulary filtering using Databricks SDK
    """
    try:
        import pandas as pd
        import io
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config
        
        if not user_token:
            raise HTTPException(
                status_code=401, 
                detail="User token required for volume access via Databricks SDK"
            )
        
        logger.info("Initializing Databricks WorkspaceClient with user token")
        
        # Initialize WorkspaceClient with user token, avoiding OAuth env var conflicts
        # Clear OAuth-related environment variables to prevent SDK confusion
        import os
        
        # Extract host from the environment or derive from request context
        databricks_host = os.environ.get("DATABRICKS_HOST")
        if not databricks_host:
            # If no host in environment, we'll need to derive it somehow
            # For now, let's try to get it from the app context
            databricks_host = "https://adb-3438839487639471.11.azuredatabricks.net"  # From error message
        
        # Create config with explicit host and token only, ignoring OAuth env vars
        config = Config(
            host=databricks_host,
            token=user_token,
            # Explicitly disable other auth methods
            username=None,
            password=None,
            client_id=None,
            client_secret=None
        )
        w = WorkspaceClient(config=config)
        
        # Load core drug data from volume using Databricks SDK Files API
        core_file_path = "/Volumes/demo/gainwell/fdb_data/fdb_core_drugs.csv"
        logger.info(f"Loading core FDB data from volume via SDK: {core_file_path}")
        
        try:
            # Use WorkspaceClient Files API to read the CSV
            file_content = w.files.download(core_file_path)
            df_core = pd.read_csv(io.BytesIO(file_content))
            logger.info(f"Successfully loaded {len(df_core)} core records from volume via SDK")
        except Exception as e:
            logger.error(f"Failed to load core file via SDK: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to access core file via SDK: {str(e)}"
            )
        
        # Load tenant-specific formulary data to create different datasets per tenant
        if tenant != "MASTER":
            tenant_lower = tenant.lower()
            formulary_file_path = f"/Volumes/demo/gainwell/fdb_data/fdb_formulary_{tenant_lower}.csv"
            
            logger.info(f"Loading formulary data for {tenant} from: {formulary_file_path}")
            
            try:
                formulary_content = w.files.download(formulary_file_path)
                df_formulary = pd.read_csv(io.BytesIO(formulary_content))
                logger.info(f"Loaded {len(df_formulary)} formulary records for {tenant}")
                
                # INNER JOIN to get only drugs that have formulary entries for this tenant
                df_merged = pd.merge(df_core, df_formulary, on='ndc', how='inner')
                logger.info(f"After tenant filtering: {len(df_merged)} records for {tenant} (was {len(df_core)} core records)")
                
                return df_merged
            except Exception as e:
                logger.warning(f"Could not load formulary data for {tenant}: {e}")
                # Fall back to core data for this tenant
                return df_core
        
        # For MASTER, return all core data
        logger.info(f"Using all core data for MASTER tenant: {len(df_core)} records")
        return df_core
            
    except Exception as e:
        logger.error(f"Failed to load FDB data from volume via SDK: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Volume data access via SDK failed: {str(e)}"
        )

def search_dataframe(df, query: str):
    """
    Search dataframe across multiple text fields
    """
    import pandas as pd
    
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
    Search FDB records for a specific tenant using Databricks Files API
    
    Example: GET /api/fdb/search?tenant=AK&query=amoxicillin&limit=50
    """
    try:
        import pandas as pd
        logger.info(f"FDB search request: tenant={tenant}, query={query}, limit={limit}")
        
        # Get user token from forwarded headers
        user_token = request.headers.get("X-Forwarded-Access-Token")
        if not user_token:
            raise HTTPException(
                status_code=401, 
                detail="User access token required for volume access"
            )
        
        # Load data for the specified tenant using Files API
        df = await load_fdb_data_via_sdk(tenant, user_token, request)
        logger.info(f"Loaded {len(df)} records for tenant {tenant}")
        
        # Apply search filter
        if query:
            df = search_dataframe(df, query)
            logger.info(f"After search filter: {len(df)} records")
        
        # Apply limit
        if limit and limit > 0:
            df = df.head(limit)
        
        # Convert to JSON-serializable format
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
                
            except Exception as row_error:
                logger.warning(f"Error processing row: {row_error}")
                continue
        
        logger.info(f"Returning {len(records)} records")
        return {
            "tenant": tenant,
            "query": query,
            "limit": limit,
            "total_found": len(records),
            "data_source": f"Databricks SDK: {VOLUME_BASE_PATH}",
            "authentication": "databricks_sdk",
            "records": records
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in FDB search: {e}")
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
    Get detailed FDB record for specific NDC with aggregated data
    
    Example: GET /api/fdb/details/00011122233?tenant=AK
    """
    try:
        import pandas as pd
        
        # Get user token from forwarded headers
        user_token = request.headers.get("X-Forwarded-Access-Token")
        if not user_token:
            raise HTTPException(
                status_code=401, 
                detail="User access token required for volume access"
            )
        
        # Load core data using Files API
        df = await load_fdb_data_via_sdk(tenant, user_token, request)
        
        # Find the specific NDC
        record = df[df['ndc'].astype(str) == ndc]
        if record.empty:
            raise HTTPException(status_code=404, detail=f"NDC {ndc} not found for tenant {tenant}")
        
        record = record.iloc[0]  # Get first (should be only) match
        
        # Try to load additional data files for enrichment using Databricks SDK
        additional_data = {}
        
        import io
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config
        
        # Initialize WorkspaceClient with user token, avoiding OAuth env var conflicts
        import os
        
        # Extract host from the environment or derive from request context
        databricks_host = os.environ.get("DATABRICKS_HOST")
        if not databricks_host:
            # Try to get from request context (when running in Databricks App)  
            if request:
                host = request.headers.get("host", "").lower()
                if "azuredatabricks.net" in host or "dbc-" in host:
                    databricks_host = f"https://{host}"
            
            # Fallback to the host we detected from the error message if still not found
            if not databricks_host:
                databricks_host = "https://adb-3438839487639471.11.azuredatabricks.net"
        
        # Create config with explicit host and token only, ignoring OAuth env vars
        config = Config(
            host=databricks_host,
            token=user_token,
            # Explicitly disable other auth methods
            username=None,
            password=None,
            client_id=None,
            client_secret=None
        )
        w = WorkspaceClient(config=config)
        
        # Try to load pricing data
        try:
            pricing_file_path = f"{VOLUME_BASE_PATH}/fdb_pricing.csv"
            pricing_content = w.files.download(pricing_file_path)
            df_pricing = pd.read_csv(io.BytesIO(pricing_content))
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
        except Exception as e:
            logger.warning(f"Could not load pricing data for NDC {ndc}: {e}")
        
        # Try to load regional data
        try:
            regional_file_path = f"{VOLUME_BASE_PATH}/fdb_regional_{tenant.lower()}.csv"
            regional_content = w.files.download(regional_file_path)
            df_regional = pd.read_csv(io.BytesIO(regional_content))
            regional_record = df_regional[df_regional['ndc'].astype(str) == ndc]
            if not regional_record.empty:
                regional = regional_record.iloc[0]
                additional_data["regional"] = {
                    "regional_code": str(regional['regional_code']) if pd.notna(regional['regional_code']) else None,
                    "preference_score": int(regional['preference_score']) if pd.notna(regional['preference_score']) else None,
                    "local_mfr": str(regional['local_mfr']) if pd.notna(regional['local_mfr']) else None,
                    "distribution_notes": str(regional['distribution_notes']) if pd.notna(regional['distribution_notes']) else None,
                }
        except Exception as e:
            logger.warning(f"Could not load regional data for NDC {ndc}: {e}")
        
        # Build detailed response
        details = {
            "ndc": ndc,
            "tenant": tenant,
            "data_source": f"Databricks SDK: {VOLUME_BASE_PATH}",
            "authentication": "databricks_sdk",
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
        import pandas as pd
        import json
        import io
        
        # Get user token from forwarded headers
        user_token = request.headers.get("X-Forwarded-Access-Token")
        if not user_token:
            raise HTTPException(
                status_code=401, 
                detail="User access token required for volume access"
            )
        
        # Load data for the specified tenant using Files API
        df = await load_fdb_data_via_sdk(tenant, user_token, request)
        
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
                    "data_source": f"Databricks SDK: {VOLUME_BASE_PATH}",
                    "authentication": "user_forwarding"
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

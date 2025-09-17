import os
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional
import logging
import pandas as pd
import json
import io

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fdb", tags=["fdb-search"])

# Sample FDB data matching the prototype format: NDC, GSN, Brand, Generic, Rx/OTC, Package Size, HIC3, HICL, DCC, MFR, Load Date
SAMPLE_FDB_DATA = [
    {
        "ndc": "00003012345",
        "gsn": 12345,
        "brand": "Tylenol",
        "generic": "Acetaminophen",
        "rx_otc": "OTC",
        "pkg_size": "100 TAB",
        "hic3": "A01",
        "hicl": "A01AA",
        "dcc": "02",
        "mfr": "Johnson & Johnson",
        "load_date": "2024-01-01",
        # Additional fields for popup details
        "obsolete": False,
        "rebate": False,
        "pkg_origin": "US",
        "gsn_desc": "Acetaminophen 325mg Tablet",
        "pkg_form": "TAB"
    },
    {
        "ndc": "00003054321",
        "gsn": 54321,
        "brand": "Advil",
        "generic": "Ibuprofen",
        "rx_otc": "OTC",
        "pkg_size": "50 TAB",
        "hic3": "A02",
        "hicl": "A02BB",
        "dcc": "03",
        "mfr": "Pfizer",
        "load_date": "2024-01-01",
        # Additional fields for popup details
        "obsolete": False,
        "rebate": True,
        "pkg_origin": "US",
        "gsn_desc": "Ibuprofen 200mg Tablet",
        "pkg_form": "TAB"
    },
    {
        "ndc": "00003098765",
        "gsn": 98765,
        "brand": "Amoxil",
        "generic": "Amoxicillin",
        "rx_otc": "RX",
        "pkg_size": "30 CAP",
        "hic3": "B01",
        "hicl": "B01CA",
        "dcc": "01",
        "mfr": "GlaxoSmithKline",
        "load_date": "2024-01-01",
        # Additional fields for popup details
        "obsolete": False,
        "rebate": True,
        "pkg_origin": "US",
        "gsn_desc": "Amoxicillin 500mg Capsule",
        "pkg_form": "CAP"
    },
    {
        "ndc": "00378123456",
        "gsn": 13579,
        "brand": "Lipitor",
        "generic": "Atorvastatin",
        "rx_otc": "RX",
        "pkg_size": "90 TAB",
        "hic3": "C10",
        "hicl": "C10AA",
        "dcc": "05",
        "mfr": "Mylan",
        "load_date": "2024-01-15",
        # Additional fields for popup details
        "obsolete": False,
        "rebate": True,
        "pkg_origin": "US",
        "gsn_desc": "Atorvastatin 20mg Tablet",
        "pkg_form": "TAB"
    },
    {
        "ndc": "00378987654",
        "gsn": 24680,
        "brand": "Synthroid",
        "generic": "Levothyroxine",
        "rx_otc": "RX",
        "pkg_size": "100 TAB",
        "hic3": "H03",
        "hicl": "H03AA",
        "dcc": "06",
        "mfr": "AbbVie",
        "load_date": "2024-02-01",
        # Additional fields for popup details
        "obsolete": False,
        "rebate": False,
        "pkg_origin": "US",
        "gsn_desc": "Levothyroxine 50mcg Tablet",
        "pkg_form": "TAB"
    }
]

# Sample tenant-specific formulary data  
TENANT_FORMULARY_DATA = {
    "AK": [
        {"ndc": "00003012345", "formulary_status": "Preferred", "tier": 1, "pa_required": False, "ql_limits": None},
        {"ndc": "00003054321", "formulary_status": "Non-Preferred", "tier": 2, "pa_required": True, "ql_limits": "30 per 30 days"},
        {"ndc": "00378123456", "formulary_status": "Preferred", "tier": 1, "pa_required": False, "ql_limits": None}
    ],
    "MO": [
        {"ndc": "00003012345", "formulary_status": "Preferred", "tier": 1, "pa_required": False, "ql_limits": None},
        {"ndc": "00003098765", "formulary_status": "Preferred", "tier": 1, "pa_required": False, "ql_limits": None},
        {"ndc": "00378987654", "formulary_status": "Non-Preferred", "tier": 3, "pa_required": True, "ql_limits": "90 per 90 days"}
    ]
}

def get_sample_fdb_data(tenant: str = "MASTER") -> pd.DataFrame:
    """
    Get sample FDB data with tenant-specific formulary filtering
    """
    try:
        # Start with core sample data
        df_core = pd.DataFrame(SAMPLE_FDB_DATA)
        
        # Add tenant-specific formulary data if not MASTER
        if tenant != "MASTER" and tenant in TENANT_FORMULARY_DATA:
            formulary_data = TENANT_FORMULARY_DATA[tenant]
            df_formulary = pd.DataFrame(formulary_data)
            
            # Inner join to get only drugs in this tenant's formulary
            df_merged = pd.merge(df_core, df_formulary, on='ndc', how='inner')
            logger.info(f"Tenant {tenant}: {len(df_merged)} records (filtered from {len(df_core)} core records)")
            return df_merged
        
        # For MASTER or unknown tenants, return all core data
        logger.info(f"Tenant {tenant}: {len(df_core)} records (all core data)")
        return df_core
        
    except Exception as e:
        logger.error(f"Error getting sample FDB data: {e}")
        # Return empty DataFrame on error
        return pd.DataFrame()

def search_dataframe(df, query: str):
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
    Search FDB records for a specific tenant using sample data
    
    Example: GET /api/fdb/search?tenant=AK&query=amoxicillin&limit=50
    """
    try:
        logger.info(f"FDB search request: tenant={tenant}, query={query}, limit={limit}")
        
        # Get user info from headers for logging
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"Search requested by user: {user_email}")
        
        # Load sample data for the specified tenant
        df = get_sample_fdb_data(tenant)
        
        if df.empty:
            return {
                "tenant": tenant,
                "query": query,
                "error": "No data available",
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
        
        # Convert to JSON-serializable format - only main FDB table fields
        # Matching prototype format: NDC, GSN, Brand, Generic, Rx/OTC, Package Size, HIC3, HICL, DCC, MFR, Load Date
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
        
        logger.info(f"Returning {len(records)} records")
        return {
            "tenant": tenant,
            "query": query,
            "limit": limit,
            "total_found": len(records),
            "data_source": "Sample Data",
            "user_email": user_email,
            "records": records
        }
        
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
    Get detailed FDB record for specific NDC
    
    Example: GET /api/fdb/details/00003012345?tenant=AK
    """
    try:
        # Get user info from headers for logging
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        logger.info(f"Details requested by user: {user_email} for NDC: {ndc}")
        
        # Load sample data
        df = get_sample_fdb_data(tenant)
        
        # Find the specific NDC
        record = df[df['ndc'].astype(str) == ndc]
        if record.empty:
            raise HTTPException(status_code=404, detail=f"NDC {ndc} not found for tenant {tenant}")
        
        record = record.iloc[0]  # Get first (should be only) match
        
        # Build detailed response matching prototype drawer structure
        # Sections: Core, Classification, Pricing & Flags, Packaging & Origin
        details = {
            "ndc": ndc,
            "tenant": tenant,
            "data_source": "Sample Data",
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
        
        # Add formulary info if available
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
    Export FDB sample data for tenant
    
    Example: GET /api/fdb/export?tenant=MO&format=csv&query=insulin&limit=1000
    """
    try:
        # Get user info from headers for logging
        user_email = request.headers.get("X-Forwarded-Email", "unknown")
        
        # Load sample data for the specified tenant
        df = get_sample_fdb_data(tenant)
        
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
                    "data_source": "Sample Data",
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
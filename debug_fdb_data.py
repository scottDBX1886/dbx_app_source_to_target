#!/usr/bin/env python3
"""
Debug script to check FDB data in the database
"""

import os
import sys
from datetime import datetime, timedelta

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def check_fdb_data():
    """Check what FDB data is available"""
    try:
        from backend.services.connector import query
        from backend.config.settings import get_settings
        
        warehouse_id = get_settings().databricks_warehouse_id
        
        print("=== FDB Core Drugs Table Analysis ===")
        
        # Check if table exists and has data
        check_query = """
        SELECT 
            COUNT(*) as total_records,
            MIN(load_date) as earliest_date,
            MAX(load_date) as latest_date,
            COUNT(DISTINCT ndc) as unique_ndcs
        FROM demo.gainwell.fdb_core_drugs
        """
        
        result = query(check_query, warehouse_id=warehouse_id, as_dict=False)
        if not result.empty:
            print(f"Total records: {result.iloc[0]['total_records']}")
            print(f"Date range: {result.iloc[0]['earliest_date']} to {result.iloc[0]['latest_date']}")
            print(f"Unique NDCs: {result.iloc[0]['unique_ndcs']}")
        else:
            print("❌ No data found in fdb_core_drugs table")
            return
        
        # Check recent data
        recent_query = """
        SELECT ndc, brand, load_date 
        FROM demo.gainwell.fdb_core_drugs 
        ORDER BY load_date DESC 
        LIMIT 10
        """
        
        recent_result = query(recent_query, warehouse_id=warehouse_id, as_dict=False)
        print(f"\n=== Recent 10 Records ===")
        if not recent_result.empty:
            for _, row in recent_result.iterrows():
                print(f"NDC: {row['ndc']}, Brand: {row['brand']}, Load Date: {row['load_date']}")
        else:
            print("No recent records found")
        
        # Check current week data
        today = datetime.now()
        week_ending = today.strftime('%Y-%m-%d')
        week_start = (today - timedelta(days=6)).strftime('%Y-%m-%d')
        
        print(f"\n=== Current Week Data ({week_start} to {week_ending}) ===")
        week_query = f"""
        SELECT COUNT(*) as week_count
        FROM demo.gainwell.fdb_core_drugs f 
        WHERE f.load_date >= '{week_start}'
          AND f.load_date <= '{week_ending}'
        """
        
        week_result = query(week_query, warehouse_id=warehouse_id, as_dict=False)
        if not week_result.empty:
            week_count = week_result.iloc[0]['week_count']
            print(f"Records in current week: {week_count}")
            
            if week_count == 0:
                print("⚠️  No records in current week - will use fallback logic")
            else:
                print("✅ Found records in current week")
        else:
            print("❌ Error checking week data")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_fdb_data()

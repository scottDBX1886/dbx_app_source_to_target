#!/usr/bin/env python3
"""
Create Final Approvals Table in Databricks
Executes the DDL to create the final_approvals table for tracking approval decisions.
"""

import os
import sys
from databricks import sql
from databricks.sdk import WorkspaceClient

def create_final_approvals_table():
    """Create the final_approvals table in Databricks"""
    
    # Read the DDL file
    ddl_file = os.path.join(os.path.dirname(__file__), 'create_final_approvals_table.sql')
    
    try:
        with open(ddl_file, 'r') as f:
            ddl_content = f.read()
        
        print("📋 Final Approvals Table DDL:")
        print("=" * 50)
        print(ddl_content)
        print("=" * 50)
        
        # Initialize Databricks client
        w = WorkspaceClient()
        
        # Get warehouse ID from environment or use default
        warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID')
        if not warehouse_id:
            print("❌ Error: DATABRICKS_WAREHOUSE_ID environment variable not set")
            return False
        
        print(f"🔗 Connecting to warehouse: {warehouse_id}")
        
        # Execute the DDL
        with sql.connect(
            server_hostname=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
            http_path=os.getenv('DATABRICKS_HTTP_PATH'),
            access_token=os.getenv('DATABRICKS_TOKEN')
        ) as connection:
            with connection.cursor() as cursor:
                print("🚀 Executing DDL...")
                cursor.execute(ddl_content)
                print("✅ Final approvals table created successfully!")
                
                # Verify table creation
                cursor.execute("SHOW TABLES IN demo.gainwell LIKE 'final_approvals'")
                result = cursor.fetchall()
                if result:
                    print("✅ Table verification: final_approvals table exists")
                else:
                    print("⚠️  Table verification: final_approvals table not found")
                
                return True
                
    except FileNotFoundError:
        print(f"❌ Error: DDL file not found: {ddl_file}")
        return False
    except Exception as e:
        print(f"❌ Error creating final approvals table: {e}")
        return False

if __name__ == "__main__":
    print("🏗️  Creating Final Approvals Table in Databricks")
    print("=" * 60)
    
    success = create_final_approvals_table()
    
    if success:
        print("\n🎉 Final approvals table creation completed successfully!")
        print("📊 The table is now ready to track approval decisions.")
    else:
        print("\n❌ Final approvals table creation failed!")
        sys.exit(1)

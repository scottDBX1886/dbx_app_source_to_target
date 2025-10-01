#!/usr/bin/env python3
"""
Simple Assignment Tables Creation Script
Creates FMT and PDL review assignment tables using Databricks SQL connector
"""

import os
from backend.services.connector import query
from backend.config.settings import get_settings
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_assignment_tables():
    """Create FMT and PDL assignment tables in Databricks"""
    
    settings = get_settings()
    warehouse_id = settings.databricks_warehouse_id
    
    if not warehouse_id:
        logger.error("DATABRICKS_WAREHOUSE_ID not configured")
        return False
    
    # FMT Review Assignments Table
    fmt_table_sql = """
    CREATE TABLE IF NOT EXISTS demo.gainwell.fmt_review_assignments (
        assignment_id STRING NOT NULL,
        ndc STRING NOT NULL,
        reviewer STRING NOT NULL,
        tenant STRING NOT NULL,
        week_ending DATE NOT NULL,
        mbid STRING,
        eff_date DATE,
        end_date DATE,
        status STRING NOT NULL DEFAULT 'ASSIGNED',
        assignment_date TIMESTAMP NOT NULL,
        created_by STRING,
        created_at TIMESTAMP NOT NULL,
        updated_by STRING,
        updated_at TIMESTAMP NOT NULL,
        notes STRING,
        rejection_reason STRING
    ) 
    USING DELTA
    PARTITIONED BY (tenant, week_ending)
    """
    
    # PDL Review Assignments Table
    pdl_table_sql = """
    CREATE TABLE IF NOT EXISTS demo.gainwell.pdl_review_assignments (
        assignment_id STRING NOT NULL,
        ndc STRING NOT NULL,
        reviewer STRING NOT NULL,
        tenant STRING NOT NULL,
        week_ending DATE NOT NULL,
        keycode STRING,
        template STRING,
        eff_date DATE,
        status STRING NOT NULL DEFAULT 'ASSIGNED',
        assignment_date TIMESTAMP NOT NULL,
        created_by STRING,
        created_at TIMESTAMP NOT NULL,
        updated_by STRING,
        updated_at TIMESTAMP NOT NULL,
        notes STRING,
        rejection_reason STRING
    ) 
    USING DELTA
    PARTITIONED BY (tenant, week_ending)
    """
    
    try:
        logger.info("Creating FMT review assignments table...")
        query(fmt_table_sql, warehouse_id=warehouse_id, as_dict=False)
        logger.info("✅ FMT table created successfully")
        
        logger.info("Creating PDL review assignments table...")
        query(pdl_table_sql, warehouse_id=warehouse_id, as_dict=False)
        logger.info("✅ PDL table created successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating assignment tables: {e}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting assignment tables creation...")
    
    success = create_assignment_tables()
    if success:
        logger.info("🎉 Assignment tables setup complete!")
        logger.info("📋 Tables Created:")
        logger.info("  • demo.gainwell.fmt_review_assignments")
        logger.info("  • demo.gainwell.pdl_review_assignments")
    else:
        logger.error("❌ Failed to create assignment tables")


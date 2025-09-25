#!/usr/bin/env python3
"""
Databricks Table Creation Script for Assignment Tables
Creates FMT and PDL review assignment tables using Databricks SQL connector

Usage:
  python create_assignment_tables_databricks.py
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
        -- Primary Key (using UUID for distributed systems)
        assignment_id STRING NOT NULL COMMENT 'Unique assignment identifier (UUID)',
        
        -- Core Assignment Fields
        ndc STRING NOT NULL COMMENT 'National Drug Code (11-digit identifier)',
        reviewer STRING NOT NULL COMMENT 'Reviewer identifier (A or B)',
        tenant STRING NOT NULL COMMENT 'Tenant identifier (MASTER, AK, MO, etc.)',
        week_ending DATE NOT NULL COMMENT 'Week ending date for this review cycle',
        
        -- FMT Specific Fields
        mbid STRING COMMENT 'Master Benefit ID assigned by reviewer',
        eff_date DATE COMMENT 'Effective date for the assignment',
        end_date DATE COMMENT 'End date for the assignment (nullable)',
        
        -- Status and Workflow
        status STRING NOT NULL DEFAULT 'ASSIGNED' COMMENT 'Assignment status (ASSIGNED, REVIEWED, APPROVED, REJECTED, CONFLICTED)',
        assignment_date TIMESTAMP NOT NULL COMMENT 'When the assignment was created',
        
        -- Audit Fields
        created_by STRING COMMENT 'User who created the assignment',
        created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
        updated_by STRING COMMENT 'User who last updated the assignment',
        updated_at TIMESTAMP NOT NULL COMMENT 'Record last update timestamp',
        
        -- Resolution Fields (for conflict resolution)
        resolution_type STRING COMMENT 'Resolution method (AUTO, A, B, CUSTOM)',
        resolution_date TIMESTAMP COMMENT 'When conflicts were resolved',
        resolved_by STRING COMMENT 'User who resolved conflicts',
        
        -- Notes and Comments
        notes STRING COMMENT 'Optional notes about the assignment',
        rejection_reason STRING COMMENT 'Reason if assignment was rejected'
    ) 
    USING DELTA
    PARTITIONED BY (tenant, week_ending)
    COMMENT 'Weekly FMT review assignments for dual reviewer workflow'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.enableChangeDataFeed' = 'true',
        'delta.columnMapping.mode' = 'name'
    )
    """
    
    # PDL Review Assignments Table
    pdl_table_sql = """
    CREATE TABLE IF NOT EXISTS demo.gainwell.pdl_review_assignments (
        -- Primary Key (using UUID for distributed systems)
        assignment_id STRING NOT NULL COMMENT 'Unique assignment identifier (UUID)',
        
        -- Core Assignment Fields
        ndc STRING NOT NULL COMMENT 'National Drug Code (11-digit identifier)',
        reviewer STRING NOT NULL COMMENT 'Reviewer identifier (A or B)',
        tenant STRING NOT NULL COMMENT 'Tenant identifier (MASTER, AK, MO, etc.)',
        week_ending DATE NOT NULL COMMENT 'Week ending date for this review cycle',
        
        -- PDL Specific Fields
        keycode STRING COMMENT 'PDL key code assigned by reviewer',
        template STRING COMMENT 'PDL template type (PRIOR_AUTH_REQUIRED, etc.)',
        eff_date DATE COMMENT 'Effective date for the key code assignment',
        
        -- Status and Workflow
        status STRING NOT NULL DEFAULT 'ASSIGNED' COMMENT 'Assignment status (ASSIGNED, REVIEWED, APPROVED, REJECTED, CONFLICTED)',
        assignment_date TIMESTAMP NOT NULL COMMENT 'When the assignment was created',
        
        -- Audit Fields
        created_by STRING COMMENT 'User who created the assignment',
        created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
        updated_by STRING COMMENT 'User who last updated the assignment',
        updated_at TIMESTAMP NOT NULL COMMENT 'Record last update timestamp',
        
        -- Resolution Fields (for conflict resolution)
        resolution_type STRING COMMENT 'Resolution method (AUTO, A, B, CUSTOM)',
        resolution_date TIMESTAMP COMMENT 'When conflicts were resolved',
        resolved_by STRING COMMENT 'User who resolved conflicts',
        
        -- POS Export Fields (PDL generates POS exports)
        pos_export_included BOOLEAN DEFAULT FALSE COMMENT 'Whether included in POS export file',
        pos_export_date TIMESTAMP COMMENT 'When included in POS export',
        pos_export_filename STRING COMMENT 'POS export file name',
        
        -- Notes and Comments
        notes STRING COMMENT 'Optional notes about the assignment',
        rejection_reason STRING COMMENT 'Reason if assignment was rejected'
    ) 
    USING DELTA
    PARTITIONED BY (tenant, week_ending)
    COMMENT 'Weekly PDL review assignments for dual reviewer workflow with POS export'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.enableChangeDataFeed' = 'true',
        'delta.columnMapping.mode' = 'name'
    )
    """
    
    # Sample data for testing
    fmt_sample_data_sql = """
    INSERT INTO demo.gainwell.fmt_review_assignments 
    (assignment_id, ndc, reviewer, tenant, week_ending, mbid, eff_date, end_date, status, assignment_date, created_by, created_at, updated_at, notes)
    VALUES 
    (uuid(), '12345678901', 'A', 'AK', '2024-12-15', 'CARDIO_ACE_001', '2024-12-15', NULL, 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Auto-assigned based on match type'),
    (uuid(), '98765432109', 'B', 'AK', '2024-12-15', 'DIABETES_MET_002', '2024-12-15', '2025-12-15', 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Brand match assignment'),
    (uuid(), '11122233344', 'A', 'MO', '2024-12-15', NULL, NULL, NULL, 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Pending review - no match found')
    """
    
    pdl_sample_data_sql = """
    INSERT INTO demo.gainwell.pdl_review_assignments 
    (assignment_id, ndc, reviewer, tenant, week_ending, keycode, template, eff_date, status, assignment_date, created_by, created_at, updated_at, notes)
    VALUES 
    (uuid(), '22233344455', 'A', 'AK', '2024-12-15', 'PA', 'PRIOR_AUTH_REQUIRED', '2024-12-15', 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Prior auth required for new brand'),
    (uuid(), '77766655544', 'B', 'AK', '2024-12-15', 'QL', 'QUANTITY_LIMIT', '2024-12-15', 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Quantity limits apply'),
    (uuid(), '55544433322', 'A', 'MO', '2024-12-15', NULL, 'PENDING_REVIEW', NULL, 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Under review for appropriate coding')
    """
    
    try:
        logger.info("Creating FMT review assignments table...")
        query(fmt_table_sql, warehouse_id=warehouse_id, as_dict=False)
        logger.info("✅ FMT review assignments table created successfully")
        
        logger.info("Creating PDL review assignments table...")
        query(pdl_table_sql, warehouse_id=warehouse_id, as_dict=False)
        logger.info("✅ PDL review assignments table created successfully")
        
        logger.info("Inserting sample FMT data...")
        query(fmt_sample_data_sql, warehouse_id=warehouse_id, as_dict=False)
        logger.info("✅ Sample FMT data inserted successfully")
        
        logger.info("Inserting sample PDL data...")
        query(pdl_sample_data_sql, warehouse_id=warehouse_id, as_dict=False)
        logger.info("✅ Sample PDL data inserted successfully")
        
        # Verify tables were created
        logger.info("Verifying table creation...")
        
        fmt_count = query("SELECT COUNT(*) as count FROM demo.gainwell.fmt_review_assignments", warehouse_id=warehouse_id, as_dict=False)
        pdl_count = query("SELECT COUNT(*) as count FROM demo.gainwell.pdl_review_assignments", warehouse_id=warehouse_id, as_dict=False)
        
        logger.info(f"✅ FMT assignments table: {fmt_count.iloc[0]['count']} records")
        logger.info(f"✅ PDL assignments table: {pdl_count.iloc[0]['count']} records")
        
        logger.info("🎉 Assignment tables created successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating assignment tables: {e}")
        return False

def create_views():
    """Create supporting views for reporting"""
    
    settings = get_settings()
    warehouse_id = settings.databricks_warehouse_id
    
    # Progress view for dashboards
    progress_view_sql = """
    CREATE OR REPLACE VIEW demo.gainwell.v_weekly_review_progress AS
    WITH fmt_progress AS (
      SELECT 
          'FMT' as review_type,
          tenant,
          week_ending,
          COUNT(*) as total_assignments,
          SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) as approved_count,
          SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) as rejected_count,
          SUM(CASE WHEN status = 'CONFLICTED' THEN 1 ELSE 0 END) as conflict_count,
          SUM(CASE WHEN status IN ('APPROVED', 'REJECTED') THEN 1 ELSE 0 END) as completed_count,
          MIN(created_at) as first_assignment_date,
          MAX(updated_at) as last_update_date
      FROM demo.gainwell.fmt_review_assignments
      GROUP BY tenant, week_ending
    ),
    pdl_progress AS (
      SELECT 
          'PDL' as review_type,
          tenant,
          week_ending,
          COUNT(*) as total_assignments,
          SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) as approved_count,
          SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) as rejected_count,
          SUM(CASE WHEN status = 'CONFLICTED' THEN 1 ELSE 0 END) as conflict_count,
          SUM(CASE WHEN status IN ('APPROVED', 'REJECTED') THEN 1 ELSE 0 END) as completed_count,
          MIN(created_at) as first_assignment_date,
          MAX(updated_at) as last_update_date
      FROM demo.gainwell.pdl_review_assignments
      GROUP BY tenant, week_ending
    )
    SELECT 
        review_type,
        tenant,
        week_ending,
        total_assignments,
        approved_count,
        rejected_count,
        conflict_count,
        completed_count,
        ROUND((completed_count * 100.0) / NULLIF(total_assignments, 0), 2) as completion_percentage,
        ROUND((approved_count * 100.0) / NULLIF(total_assignments, 0), 2) as approval_percentage,
        first_assignment_date,
        last_update_date,
        datediff(day, first_assignment_date, last_update_date) as days_in_review
    FROM fmt_progress
    UNION ALL
    SELECT 
        review_type,
        tenant,
        week_ending,
        total_assignments,
        approved_count,
        rejected_count,
        conflict_count,
        completed_count,
        ROUND((completed_count * 100.0) / NULLIF(total_assignments, 0), 2) as completion_percentage,
        ROUND((approved_count * 100.0) / NULLIF(total_assignments, 0), 2) as approval_percentage,
        first_assignment_date,
        last_update_date,
        datediff(day, first_assignment_date, last_update_date) as days_in_review
    FROM pdl_progress
    ORDER BY review_type, tenant, week_ending
    """
    
    try:
        logger.info("Creating progress view...")
        query(progress_view_sql, warehouse_id=warehouse_id, as_dict=False)
        logger.info("✅ Progress view created successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating views: {e}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting assignment tables creation...")
    
    success = create_assignment_tables()
    if success:
        logger.info("📊 Creating supporting views...")
        create_views()
        
        logger.info("\n" + "="*60)
        logger.info("🎉 Assignment tables setup complete!")
        logger.info("="*60)
        logger.info("\n📋 Tables Created:")
        logger.info("  • demo.gainwell.fmt_review_assignments")
        logger.info("  • demo.gainwell.pdl_review_assignments")
        logger.info("\n📊 Views Created:")
        logger.info("  • demo.gainwell.v_weekly_review_progress")
        logger.info("\n🔧 Next Steps:")
        logger.info("  1. Update backend TODO comments to use these tables")
        logger.info("  2. Test assignment functionality in the web app")
        logger.info("  3. Configure table optimizations as needed")
        
    else:
        logger.error("❌ Failed to create assignment tables")

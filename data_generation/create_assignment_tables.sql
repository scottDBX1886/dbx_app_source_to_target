-- =========================================================================
-- FMT Review Assignments Table (Databricks/Delta Lake Optimized)
-- Stores reviewer assignments for weekly FMT review process
-- =========================================================================

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
CLUSTER BY (ndc, reviewer)
COMMENT 'Weekly FMT review assignments for dual reviewer workflow'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- =========================================================================
-- PDL Review Assignments Table (Databricks/Delta Lake Optimized)
-- Stores reviewer assignments for weekly PDL coding review process
-- =========================================================================

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
CLUSTER BY (ndc, reviewer)
COMMENT 'Weekly PDL review assignments for dual reviewer workflow with POS export'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- =========================================================================
-- Databricks Performance Optimization
-- =========================================================================

-- Optimize tables for common query patterns
OPTIMIZE demo.gainwell.fmt_review_assignments;
OPTIMIZE demo.gainwell.pdl_review_assignments;

-- Z-Order by frequently filtered columns
ALTER TABLE demo.gainwell.fmt_review_assignments 
SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '10');

ALTER TABLE demo.gainwell.pdl_review_assignments 
SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '10');

-- Create Liquid Clustering for better query performance (Databricks Runtime 13.3+)
-- Note: Uncomment if using Databricks Runtime 13.3+ with Liquid Clustering enabled
-- ALTER TABLE demo.gainwell.fmt_review_assignments CLUSTER BY (tenant, week_ending, ndc, reviewer);
-- ALTER TABLE demo.gainwell.pdl_review_assignments CLUSTER BY (tenant, week_ending, ndc, reviewer);

-- =========================================================================
-- Sample Data for Testing (Using UUIDs for Databricks)
-- =========================================================================

-- Sample FMT assignments with UUIDs and proper timestamps
INSERT INTO demo.gainwell.fmt_review_assignments 
(assignment_id, ndc, reviewer, tenant, week_ending, mbid, eff_date, end_date, status, assignment_date, created_by, created_at, updated_at, notes)
VALUES 
(uuid(), '12345678901', 'A', 'AK', '2024-12-15', 'CARDIO_ACE_001', '2024-12-15', NULL, 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Auto-assigned based on match type'),
(uuid(), '98765432109', 'B', 'AK', '2024-12-15', 'DIABETES_MET_002', '2024-12-15', '2025-12-15', 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Brand match assignment'),
(uuid(), '11122233344', 'A', 'MO', '2024-12-15', NULL, NULL, NULL, 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Pending review - no match found');

-- Sample PDL assignments with UUIDs and proper timestamps
INSERT INTO demo.gainwell.pdl_review_assignments 
(assignment_id, ndc, reviewer, tenant, week_ending, keycode, template, eff_date, status, assignment_date, created_by, created_at, updated_at, notes)
VALUES 
(uuid(), '22233344455', 'A', 'AK', '2024-12-15', 'PA', 'PRIOR_AUTH_REQUIRED', '2024-12-15', 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Prior auth required for new brand'),
(uuid(), '77766655544', 'B', 'AK', '2024-12-15', 'QL', 'QUANTITY_LIMIT', '2024-12-15', 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Quantity limits apply'),
(uuid(), '55544433322', 'A', 'MO', '2024-12-15', NULL, 'PENDING_REVIEW', NULL, 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Under review for appropriate coding');

-- =========================================================================
-- Views for Reporting and Analytics
-- =========================================================================

-- FMT Assignment Summary View
CREATE OR REPLACE VIEW demo.gainwell.v_fmt_assignment_summary AS
SELECT 
    tenant,
    week_ending,
    reviewer,
    status,
    COUNT(*) as assignment_count,
    COUNT(CASE WHEN mbid IS NOT NULL THEN 1 END) as completed_assignments,
    COUNT(CASE WHEN mbid IS NULL THEN 1 END) as pending_assignments,
    MIN(assignment_date) as first_assignment,
    MAX(assignment_date) as last_assignment
FROM demo.gainwell.fmt_review_assignments
GROUP BY tenant, week_ending, reviewer, status
ORDER BY tenant, week_ending, reviewer;

-- PDL Assignment Summary View  
CREATE OR REPLACE VIEW demo.gainwell.v_pdl_assignment_summary AS
SELECT 
    tenant,
    week_ending,
    reviewer,
    status,
    COUNT(*) as assignment_count,
    COUNT(CASE WHEN keycode IS NOT NULL THEN 1 END) as completed_assignments,
    COUNT(CASE WHEN keycode IS NULL THEN 1 END) as pending_assignments,
    COUNT(CASE WHEN pos_export_included = TRUE THEN 1 END) as pos_exported,
    MIN(assignment_date) as first_assignment,
    MAX(assignment_date) as last_assignment
FROM demo.gainwell.pdl_review_assignments
GROUP BY tenant, week_ending, reviewer, status
ORDER BY tenant, week_ending, reviewer;

-- Weekly Review Progress View (Combined FMT + PDL) - Databricks Optimized
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
ORDER BY review_type, tenant, week_ending;

-- =========================================================================
-- Databricks-Specific Materialized View for Performance
-- =========================================================================

-- Create a materialized view for dashboard queries (requires Databricks SQL Pro/Serverless)
-- CREATE MATERIALIZED VIEW IF NOT EXISTS demo.gainwell.mv_weekly_review_dashboard AS
-- SELECT 
--     review_type,
--     tenant,
--     week_ending,
--     total_assignments,
--     completion_percentage,
--     approval_percentage,
--     last_update_date
-- FROM demo.gainwell.v_weekly_review_progress
-- WHERE week_ending >= date_sub(current_date(), 30);  -- Last 30 days only

-- =========================================================================
-- Data Quality and Monitoring Queries
-- =========================================================================

-- Query to check for data quality issues
CREATE OR REPLACE VIEW demo.gainwell.v_assignment_data_quality AS
SELECT 
    'FMT' as review_type,
    tenant,
    week_ending,
    COUNT(*) as total_records,
    SUM(CASE WHEN assignment_id IS NULL THEN 1 ELSE 0 END) as missing_ids,
    SUM(CASE WHEN ndc IS NULL OR length(trim(ndc)) = 0 THEN 1 ELSE 0 END) as missing_ndcs,
    SUM(CASE WHEN reviewer NOT IN ('A', 'B') THEN 1 ELSE 0 END) as invalid_reviewers,
    SUM(CASE WHEN status NOT IN ('ASSIGNED', 'REVIEWED', 'APPROVED', 'REJECTED', 'CONFLICTED') THEN 1 ELSE 0 END) as invalid_status,
    SUM(CASE WHEN created_at > current_timestamp() THEN 1 ELSE 0 END) as future_dates,
    current_timestamp() as check_date
FROM demo.gainwell.fmt_review_assignments
GROUP BY tenant, week_ending

UNION ALL

SELECT 
    'PDL' as review_type,
    tenant,
    week_ending,
    COUNT(*) as total_records,
    SUM(CASE WHEN assignment_id IS NULL THEN 1 ELSE 0 END) as missing_ids,
    SUM(CASE WHEN ndc IS NULL OR length(trim(ndc)) = 0 THEN 1 ELSE 0 END) as missing_ndcs,
    SUM(CASE WHEN reviewer NOT IN ('A', 'B') THEN 1 ELSE 0 END) as invalid_reviewers,
    SUM(CASE WHEN status NOT IN ('ASSIGNED', 'REVIEWED', 'APPROVED', 'REJECTED', 'CONFLICTED') THEN 1 ELSE 0 END) as invalid_status,
    SUM(CASE WHEN created_at > current_timestamp() THEN 1 ELSE 0 END) as future_dates,
    current_timestamp() as check_date
FROM demo.gainwell.pdl_review_assignments
GROUP BY tenant, week_ending;

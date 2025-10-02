# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment Tables Creation for Weekly Review Process
# MAGIC 
# MAGIC This notebook creates the assignment tables needed for the FMT and PDL weekly review workflow:
# MAGIC - `demo.gainwell.fmt_review_assignments` - FMT reviewer assignments
# MAGIC - `demo.gainwell.pdl_review_assignments` - PDL reviewer assignments
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Catalog `demo` and schema `gainwell` must exist
# MAGIC - User must have CREATE TABLE permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create FMT Review Assignments Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo.gainwell.fmt_review_assignments (
# MAGIC     -- Primary Key (using UUID for distributed systems)
# MAGIC     assignment_id STRING NOT NULL COMMENT 'Unique assignment identifier (UUID)',
# MAGIC     
# MAGIC     -- Core Assignment Fields
# MAGIC     ndc STRING NOT NULL COMMENT 'National Drug Code (11-digit identifier)',
# MAGIC     reviewer STRING NOT NULL COMMENT 'Reviewer identifier (A or B)',
# MAGIC     tenant STRING NOT NULL COMMENT 'Tenant identifier (MASTER, AK, MO, etc.)',
# MAGIC     week_ending DATE NOT NULL COMMENT 'Week ending date for this review cycle',
# MAGIC     
# MAGIC     -- FMT Specific Fields
# MAGIC     mbid STRING COMMENT 'Master Benefit ID assigned by reviewer',
# MAGIC     eff_date DATE COMMENT 'Effective date for the assignment',
# MAGIC     end_date DATE COMMENT 'End date for the assignment (nullable)',
# MAGIC     
# MAGIC     -- Status and Workflow
# MAGIC     status STRING NOT NULL DEFAULT 'ASSIGNED' COMMENT 'Assignment status (ASSIGNED, REVIEWED, APPROVED, REJECTED, CONFLICTED)',
# MAGIC     assignment_date TIMESTAMP NOT NULL COMMENT 'When the assignment was created',
# MAGIC     
# MAGIC     -- Audit Fields
# MAGIC     created_by STRING COMMENT 'User who created the assignment',
# MAGIC     created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC     updated_by STRING COMMENT 'User who last updated the assignment',
# MAGIC     updated_at TIMESTAMP NOT NULL COMMENT 'Record last update timestamp',
# MAGIC     
# MAGIC     -- Resolution Fields (for conflict resolution)
# MAGIC     resolution_type STRING COMMENT 'Resolution method (AUTO, A, B, CUSTOM)',
# MAGIC     resolution_date TIMESTAMP COMMENT 'When conflicts were resolved',
# MAGIC     resolved_by STRING COMMENT 'User who resolved conflicts',
# MAGIC     
# MAGIC     -- Notes and Comments
# MAGIC     notes STRING COMMENT 'Optional notes about the assignment',
# MAGIC     rejection_reason STRING COMMENT 'Reason if assignment was rejected'
# MAGIC ) 
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (tenant, week_ending)
# MAGIC COMMENT 'Weekly FMT review assignments for dual reviewer workflow'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create PDL Review Assignments Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo.gainwell.pdl_review_assignments (
# MAGIC     -- Primary Key (using UUID for distributed systems)
# MAGIC     assignment_id STRING NOT NULL COMMENT 'Unique assignment identifier (UUID)',
# MAGIC     
# MAGIC     -- Core Assignment Fields
# MAGIC     ndc STRING NOT NULL COMMENT 'National Drug Code (11-digit identifier)',
# MAGIC     reviewer STRING NOT NULL COMMENT 'Reviewer identifier (A or B)',
# MAGIC     tenant STRING NOT NULL COMMENT 'Tenant identifier (MASTER, AK, MO, etc.)',
# MAGIC     week_ending DATE NOT NULL COMMENT 'Week ending date for this review cycle',
# MAGIC     
# MAGIC     -- PDL Specific Fields
# MAGIC     keycode STRING COMMENT 'PDL key code assigned by reviewer',
# MAGIC     template STRING COMMENT 'PDL template type (PRIOR_AUTH_REQUIRED, etc.)',
# MAGIC     eff_date DATE COMMENT 'Effective date for the key code assignment',
# MAGIC     
# MAGIC     -- Status and Workflow
# MAGIC     status STRING NOT NULL DEFAULT 'ASSIGNED' COMMENT 'Assignment status (ASSIGNED, REVIEWED, APPROVED, REJECTED, CONFLICTED)',
# MAGIC     assignment_date TIMESTAMP NOT NULL COMMENT 'When the assignment was created',
# MAGIC     
# MAGIC     -- Audit Fields
# MAGIC     created_by STRING COMMENT 'User who created the assignment',
# MAGIC     created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
# MAGIC     updated_by STRING COMMENT 'User who last updated the assignment',
# MAGIC     updated_at TIMESTAMP NOT NULL COMMENT 'Record last update timestamp',
# MAGIC     
# MAGIC     -- Resolution Fields (for conflict resolution)
# MAGIC     resolution_type STRING COMMENT 'Resolution method (AUTO, A, B, CUSTOM)',
# MAGIC     resolution_date TIMESTAMP COMMENT 'When conflicts were resolved',
# MAGIC     resolved_by STRING COMMENT 'User who resolved conflicts',
# MAGIC     
# MAGIC     -- POS Export Fields (PDL generates POS exports)
# MAGIC     pos_export_included BOOLEAN DEFAULT FALSE COMMENT 'Whether included in POS export file',
# MAGIC     pos_export_date TIMESTAMP COMMENT 'When included in POS export',
# MAGIC     pos_export_filename STRING COMMENT 'POS export file name',
# MAGIC     
# MAGIC     -- Notes and Comments
# MAGIC     notes STRING COMMENT 'Optional notes about the assignment',
# MAGIC     rejection_reason STRING COMMENT 'Reason if assignment was rejected'
# MAGIC ) 
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (tenant, week_ending)
# MAGIC COMMENT 'Weekly PDL review assignments for dual reviewer workflow with POS export'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Insert Sample Data for Testing

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample FMT assignments with UUIDs and proper timestamps
# MAGIC INSERT INTO demo.gainwell.fmt_review_assignments 
# MAGIC (assignment_id, ndc, reviewer, tenant, week_ending, mbid, eff_date, end_date, status, assignment_date, created_by, created_at, updated_at, notes)
# MAGIC VALUES 
# MAGIC (uuid(), '12345678901', 'A', 'AK', '2024-12-15', 'CARDIO_ACE_001', '2024-12-15', NULL, 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Auto-assigned based on match type'),
# MAGIC (uuid(), '98765432109', 'B', 'AK', '2024-12-15', 'DIABETES_MET_002', '2024-12-15', '2025-12-15', 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Brand match assignment'),
# MAGIC (uuid(), '11122233344', 'A', 'MO', '2024-12-15', NULL, NULL, NULL, 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Pending review - no match found'),
# MAGIC (uuid(), '55566677788', 'B', 'MASTER', '2024-12-15', 'PAIN_NSAID_003', '2024-12-15', '2024-12-31', 'REVIEWED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'NSAID with temporary approval')

# COMMAND ----------

# MAGIC %sql  
# MAGIC -- Sample PDL assignments with UUIDs and proper timestamps
# MAGIC INSERT INTO demo.gainwell.pdl_review_assignments 
# MAGIC (assignment_id, ndc, reviewer, tenant, week_ending, keycode, template, eff_date, status, assignment_date, created_by, created_at, updated_at, notes)
# MAGIC VALUES 
# MAGIC (uuid(), '22233344455', 'A', 'AK', '2024-12-15', 'PA', 'PRIOR_AUTH_REQUIRED', '2024-12-15', 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Prior auth required for new brand'),
# MAGIC (uuid(), '77766655544', 'B', 'AK', '2024-12-15', 'QL', 'QUANTITY_LIMIT', '2024-12-15', 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Quantity limits apply'),
# MAGIC (uuid(), '55544433322', 'A', 'MO', '2024-12-15', NULL, 'PENDING_REVIEW', NULL, 'ASSIGNED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Under review for appropriate coding'),
# MAGIC (uuid(), '99988877766', 'B', 'MASTER', '2024-12-15', 'ST', 'STEP_THERAPY', '2024-12-15', 'APPROVED', current_timestamp(), 'system', current_timestamp(), current_timestamp(), 'Step therapy protocol required')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Reporting Views

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FMT Assignment Summary View
# MAGIC CREATE OR REPLACE VIEW demo.gainwell.v_fmt_assignment_summary AS
# MAGIC SELECT 
# MAGIC     tenant,
# MAGIC     week_ending,
# MAGIC     reviewer,
# MAGIC     status,
# MAGIC     COUNT(*) as assignment_count,
# MAGIC     COUNT(CASE WHEN mbid IS NOT NULL THEN 1 END) as completed_assignments,
# MAGIC     COUNT(CASE WHEN mbid IS NULL THEN 1 END) as pending_assignments,
# MAGIC     MIN(assignment_date) as first_assignment,
# MAGIC     MAX(assignment_date) as last_assignment
# MAGIC FROM demo.gainwell.fmt_review_assignments
# MAGIC GROUP BY tenant, week_ending, reviewer, status
# MAGIC ORDER BY tenant, week_ending, reviewer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PDL Assignment Summary View  
# MAGIC CREATE OR REPLACE VIEW demo.gainwell.v_pdl_assignment_summary AS
# MAGIC SELECT 
# MAGIC     tenant,
# MAGIC     week_ending,
# MAGIC     reviewer,
# MAGIC     status,
# MAGIC     COUNT(*) as assignment_count,
# MAGIC     COUNT(CASE WHEN keycode IS NOT NULL THEN 1 END) as completed_assignments,
# MAGIC     COUNT(CASE WHEN keycode IS NULL THEN 1 END) as pending_assignments,
# MAGIC     COUNT(CASE WHEN pos_export_included = TRUE THEN 1 END) as pos_exported,
# MAGIC     MIN(assignment_date) as first_assignment,
# MAGIC     MAX(assignment_date) as last_assignment
# MAGIC FROM demo.gainwell.pdl_review_assignments
# MAGIC GROUP BY tenant, week_ending, reviewer, status
# MAGIC ORDER BY tenant, week_ending, reviewer

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Combined Progress View

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Weekly Review Progress View (Combined FMT + PDL) - Databricks Optimized
# MAGIC CREATE OR REPLACE VIEW demo.gainwell.v_weekly_review_progress AS
# MAGIC WITH fmt_progress AS (
# MAGIC   SELECT 
# MAGIC       'FMT' as review_type,
# MAGIC       tenant,
# MAGIC       week_ending,
# MAGIC       COUNT(*) as total_assignments,
# MAGIC       SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) as approved_count,
# MAGIC       SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) as rejected_count,
# MAGIC       SUM(CASE WHEN status = 'CONFLICTED' THEN 1 ELSE 0 END) as conflict_count,
# MAGIC       SUM(CASE WHEN status IN ('APPROVED', 'REJECTED') THEN 1 ELSE 0 END) as completed_count,
# MAGIC       MIN(created_at) as first_assignment_date,
# MAGIC       MAX(updated_at) as last_update_date
# MAGIC   FROM demo.gainwell.fmt_review_assignments
# MAGIC   GROUP BY tenant, week_ending
# MAGIC ),
# MAGIC pdl_progress AS (
# MAGIC   SELECT 
# MAGIC       'PDL' as review_type,
# MAGIC       tenant,
# MAGIC       week_ending,
# MAGIC       COUNT(*) as total_assignments,
# MAGIC       SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) as approved_count,
# MAGIC       SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) as rejected_count,
# MAGIC       SUM(CASE WHEN status = 'CONFLICTED' THEN 1 ELSE 0 END) as conflict_count,
# MAGIC       SUM(CASE WHEN status IN ('APPROVED', 'REJECTED') THEN 1 ELSE 0 END) as completed_count,
# MAGIC       MIN(created_at) as first_assignment_date,
# MAGIC       MAX(updated_at) as last_update_date
# MAGIC   FROM demo.gainwell.pdl_review_assignments
# MAGIC   GROUP BY tenant, week_ending
# MAGIC )
# MAGIC SELECT 
# MAGIC     review_type,
# MAGIC     tenant,
# MAGIC     week_ending,
# MAGIC     total_assignments,
# MAGIC     approved_count,
# MAGIC     rejected_count,
# MAGIC     conflict_count,
# MAGIC     completed_count,
# MAGIC     ROUND((completed_count * 100.0) / NULLIF(total_assignments, 0), 2) as completion_percentage,
# MAGIC     ROUND((approved_count * 100.0) / NULLIF(total_assignments, 0), 2) as approval_percentage,
# MAGIC     first_assignment_date,
# MAGIC     last_update_date,
# MAGIC     datediff(day, first_assignment_date, last_update_date) as days_in_review
# MAGIC FROM fmt_progress
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     review_type,
# MAGIC     tenant,
# MAGIC     week_ending,
# MAGIC     total_assignments,
# MAGIC     approved_count,
# MAGIC     rejected_count,
# MAGIC     conflict_count,
# MAGIC     completed_count,
# MAGIC     ROUND((completed_count * 100.0) / NULLIF(total_assignments, 0), 2) as completion_percentage,
# MAGIC     ROUND((approved_count * 100.0) / NULLIF(total_assignments, 0), 2) as approval_percentage,
# MAGIC     first_assignment_date,
# MAGIC     last_update_date,
# MAGIC     datediff(day, first_assignment_date, last_update_date) as days_in_review
# MAGIC FROM pdl_progress
# MAGIC ORDER BY review_type, tenant, week_ending

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Table Creation and Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check FMT table
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_fmt_assignments,
# MAGIC     COUNT(DISTINCT tenant) as tenants,
# MAGIC     COUNT(DISTINCT reviewer) as reviewers,
# MAGIC     COUNT(DISTINCT status) as statuses
# MAGIC FROM demo.gainwell.fmt_review_assignments

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check PDL table
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_pdl_assignments,
# MAGIC     COUNT(DISTINCT tenant) as tenants,
# MAGIC     COUNT(DISTINCT reviewer) as reviewers,
# MAGIC     COUNT(DISTINCT status) as statuses
# MAGIC FROM demo.gainwell.pdl_review_assignments

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample data from both tables
# MAGIC SELECT 'FMT' as type, tenant, reviewer, status, assignment_date, notes 
# MAGIC FROM demo.gainwell.fmt_review_assignments 
# MAGIC UNION ALL
# MAGIC SELECT 'PDL' as type, tenant, reviewer, status, assignment_date, notes 
# MAGIC FROM demo.gainwell.pdl_review_assignments
# MAGIC ORDER BY type, tenant, reviewer

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Performance Optimization

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize tables for better query performance
# MAGIC OPTIMIZE demo.gainwell.fmt_review_assignments

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE demo.gainwell.pdl_review_assignments

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Test Progress View

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the progress view
# MAGIC SELECT * FROM demo.gainwell.v_weekly_review_progress
# MAGIC ORDER BY review_type, tenant, week_ending

# COMMAND ----------

# MAGIC %md
# MAGIC # ✅ Assignment Tables Setup Complete!
# MAGIC 
# MAGIC ## Created Tables:
# MAGIC - **`demo.gainwell.fmt_review_assignments`** - FMT reviewer assignments with MBID tracking
# MAGIC - **`demo.gainwell.pdl_review_assignments`** - PDL reviewer assignments with keycode/template tracking
# MAGIC 
# MAGIC ## Created Views:
# MAGIC - **`demo.gainwell.v_fmt_assignment_summary`** - FMT assignment summary by tenant/week/reviewer
# MAGIC - **`demo.gainwell.v_pdl_assignment_summary`** - PDL assignment summary by tenant/week/reviewer  
# MAGIC - **`demo.gainwell.v_weekly_review_progress`** - Combined progress view for dashboards
# MAGIC 
# MAGIC ## Key Features:
# MAGIC - **Delta Lake format** with auto-optimization
# MAGIC - **Partitioned by tenant and week_ending** for query performance
# MAGIC - **Change Data Feed enabled** for audit trails
# MAGIC - **UUID primary keys** for distributed systems
# MAGIC - **Sample data included** for testing
# MAGIC 
# MAGIC ## Next Steps:
# MAGIC 1. Update backend API endpoints to use these tables
# MAGIC 2. Test assignment functionality in the web application
# MAGIC 3. Configure additional table properties as needed
# MAGIC 4. Set up data retention and archival policies

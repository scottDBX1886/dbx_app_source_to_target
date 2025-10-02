-- =========================================================================
-- Final Approvals Table (Databricks/Delta Lake Optimized)
-- Stores final approval decisions for weekly review process
-- =========================================================================

CREATE TABLE IF NOT EXISTS demo.gainwell.final_approvals (
    -- Primary Key
    approval_id STRING NOT NULL COMMENT 'Unique approval identifier (UUID)',
    
    -- Core Fields
    review_type STRING NOT NULL COMMENT 'Review type (FMT or PDL)',
    tenant STRING NOT NULL COMMENT 'Tenant identifier (MASTER, AK, MO, etc.)',
    week_ending DATE NOT NULL COMMENT 'Week ending date for this review cycle',
    ndc STRING NOT NULL COMMENT 'National Drug Code (11-digit identifier)',
    
    -- FMT Specific Fields
    mbid STRING COMMENT 'Master Benefit ID (for FMT)',
    eff_date DATE COMMENT 'Effective date',
    end_date DATE COMMENT 'End date (nullable)',
    
    -- PDL Specific Fields  
    keycode STRING COMMENT 'PDL key code (for PDL)',
    template STRING COMMENT 'PDL template type (for PDL)',
    
    -- Approval Fields
    approved_by STRING NOT NULL COMMENT 'User who approved',
    approved_at TIMESTAMP NOT NULL COMMENT 'When approved',
    sync_status STRING NOT NULL DEFAULT 'PENDING' COMMENT 'Sync status (PENDING, COMPLETED, FAILED)',
    
    -- Audit Fields
    created_by STRING COMMENT 'User who created the approval',
    created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
    updated_by STRING COMMENT 'User who last updated the approval',
    updated_at TIMESTAMP NOT NULL COMMENT 'Record last update timestamp',
    
    -- Notes
    notes STRING COMMENT 'Optional notes about the approval'
) 
USING DELTA
PARTITIONED BY (tenant, week_ending)
CLUSTER BY (review_type, ndc)
COMMENT 'Final approval decisions for weekly review process'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.feature.allowColumnDefaults' = 'supported'
);

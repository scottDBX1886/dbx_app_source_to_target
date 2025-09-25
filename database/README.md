# Database

This directory contains database setup files and DDL statements for the Gainwell Main App.

## Files

### Assignment Tables DDL
- **`create_assignment_tables.sql`** - Complete SQL DDL for FMT and PDL assignment tables
  - Databricks/Delta Lake optimized
  - Includes partitioning, clustering, and performance optimizations
  - Contains sample data and reporting views
  - Full documentation with comments

### Execution Scripts
- **`create_assignment_tables_databricks.py`** - Python script to execute DDL via Databricks connector
  - Uses backend services for database connection
  - Includes error handling and verification
  - Logging and status reporting

- **`Create_Assignment_Tables.py`** - Databricks notebook format
  - Cell-by-cell execution in Databricks workspace
  - Interactive documentation with markdown cells
  - Step-by-step table creation and verification

## Tables Created

### FMT Review Assignments
- **`demo.gainwell.fmt_review_assignments`**
- Stores FMT reviewer assignments with MBID tracking
- Partitioned by tenant and week_ending
- Supports dual reviewer workflow and conflict resolution

### PDL Review Assignments  
- **`demo.gainwell.pdl_review_assignments`**
- Stores PDL reviewer assignments with keycode/template tracking
- Includes POS export functionality
- Partitioned by tenant and week_ending

## Usage

### Option 1: Direct SQL
Run `create_assignment_tables.sql` in Databricks SQL Editor

### Option 2: Python Script
```bash
python create_assignment_tables_databricks.py
```

### Option 3: Databricks Notebook
Import `Create_Assignment_Tables.py` into Databricks workspace and run

## Prerequisites
- Databricks workspace access
- `demo.gainwell` schema exists
- CREATE TABLE permissions
- For Python script: Backend services configured

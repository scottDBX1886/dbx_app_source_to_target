# Data Generation

This directory contains scripts for generating sample/test data for the Gainwell Main App.

## Files

### FDB Data Generation
- **`generate_fdb_data.py`** - Generates comprehensive FDB sample data
  - Core drugs database with realistic NDCs, brands, manufacturers
  - Tenant-specific formulary data (MASTER, AK, MO)
  - Regional pricing and therapeutic class data
  - Monthly update files for testing data changes
  - Ensures 100% joinability between datasets

### FMT Data Generation
- **`generate_fmt_data.py`** - Basic FMT sample data generator (deprecated)
- **`generate_fmt_tenant_data.py`** - Advanced FMT data generator
  - Tenant-specific formulary management data
  - MBIDs (Master Benefit IDs) with realistic assignments
  - Status distributions reflecting real-world scenarios
  - Overlaps with FDB data for proper joins

### PDL Data Generation
- **`generate_pdl_data.py`** - Basic PDL sample data generator (deprecated)
- **`generate_pdl_sample_data.py`** - Alternative PDL data generator
- **`generate_pdl_tenant_data.py`** - Advanced PDL data generator
  - Tenant-specific key codes and templates
  - Realistic PDL coding scenarios (PA, QL, ST, etc.)
  - Template assignments for different drug categories
  - Cross-tenant variations for testing

### Verification & Analysis
- **`verify_tenant_data.py`** - Data quality and relationship verification
  - Validates cross-tenant data differences
  - Checks joinability between FDB, FMT, and PDL datasets
  - Analyzes status distributions and template usage
  - Generates data quality reports

## Generated Data Structure

### Output Directories
- `sample_fdb_data/` - FDB core and formulary data
- `sample_fmt_data/` - FMT master and MBID data  
- `sample_pdl_data/` - PDL master and keycode data

### Data Relationships
```
FDB Core Drugs (NDCs)
├── FMT Master (subset of NDCs)
│   └── FMT MBIDs (tenant-specific)
└── PDL Master (subset of NDCs)
    └── PDL Keycodes (tenant-specific)
```

## Usage

### Generate All Data Sets
```bash
# Generate FDB data (foundation)
python generate_fdb_data.py

# Generate FMT tenant-specific data
python generate_fmt_tenant_data.py

# Generate PDL tenant-specific data  
python generate_pdl_tenant_data.py

# Verify data quality and relationships
python verify_tenant_data.py
```

### Data Characteristics
- **FDB**: ~2000 core drugs, 5 manufacturers, 5 therapeutic classes
- **FMT**: ~586 formulary entries, 26 MBIDs across tenants
- **PDL**: ~350 coded drugs, 1196 keycodes across tenants
- **Tenants**: MASTER, AK (Alaska), MO (Missouri)

## Dependencies
- `pandas` - Data manipulation
- `faker` - Realistic fake data generation
- `random` - Statistical distributions
- `datetime` - Date/time handling

## Data Quality Features
- **Referential integrity** - All NDCs exist in FDB core
- **Tenant variations** - Different policies per tenant
- **Realistic distributions** - Status codes, dates, assignments
- **Joinable datasets** - Guaranteed foreign key relationships

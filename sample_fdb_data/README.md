# FDB Sample Dataset for Formulary Management Tool

## 📋 Overview

This directory contains **25 realistic pharmaceutical data files** simulating the structure of First DataBank (FDB) data across multiple tenants and categories. The dataset includes ~2,000 total drug records across various file types.

## 🗂️ File Structure

### **Core Data Files**
| File | Records | Description |
|------|---------|-------------|
| `fdb_core_drugs.csv` | 500 | Primary drug information (NDC, brand, generic, manufacturer, etc.) |
| `fdb_pricing.csv` | 300 | Pricing data (AWP, WAC, NADAC, rebates) |
| `dataset_metadata.json` | - | Dataset information and file descriptions |

### **Tenant-Specific Files**
| File Pattern | Records Each | Description |
|-------------|--------------|-------------|
| `fdb_formulary_{tenant}.csv` | 150 | Formulary status, tiers, prior auth requirements |
| `fdb_regional_{tenant}.csv` | 100 | Regional preferences and distribution notes |

**Available Tenants:** `master`, `ak`, `mo`

### **Therapeutic Class Files**
| File | Records | Therapeutic Area |
|------|---------|------------------|
| `fdb_therapeutic_cardiovascular.csv` | 80 | Heart/blood pressure medications |
| `fdb_therapeutic_diabetes.csv` | 80 | Diabetes management drugs |
| `fdb_therapeutic_oncology.csv` | 80 | Cancer treatment medications |
| `fdb_therapeutic_neurology.csv` | 80 | Neurological condition drugs |
| `fdb_therapeutic_respiratory.csv` | 80 | Respiratory/lung medications |

### **Manufacturer-Specific Files**
| File | Records | Major Pharmaceutical Company |
|------|---------|------------------------------|
| `fdb_manufacturer_pfizer.csv` | 60 | Pfizer products and manufacturing info |
| `fdb_manufacturer_johnson_and_johnson.csv` | 60 | J&J products and manufacturing info |
| `fdb_manufacturer_merck.csv` | 60 | Merck products and manufacturing info |
| `fdb_manufacturer_abbvie.csv` | 60 | AbbVie products and manufacturing info |
| `fdb_manufacturer_bristol-myers_squibb.csv` | 60 | BMS products and manufacturing info |

### **Historical Update Files**
| File Pattern | Records Each | Description |
|-------------|--------------|-------------|
| `fdb_updates_2024-{month}.csv` | 40 | Monthly change tracking (Jan-Jun 2024) |

---

## 🔍 Key Data Fields

### **Core Drug Information (`fdb_core_drugs.csv`)**
```csv
ndc,gsn,brand,generic,rx_otc,pkg_size,hic3,hicl,dcc,mfr,obsolete,rebate,load_date,pkg_origin,gsn_desc,pkg_form
```

- **NDC**: 11-digit National Drug Code
- **GSN**: Generic Sequence Number (10000+)
- **Brand/Generic**: Drug names
- **HIC3/HICL/DCC**: Drug classification codes
- **Obsolete/Rebate**: Boolean flags
- **Load Date**: When data was loaded (past year)

### **Pricing Information (`fdb_pricing.csv`)**
```csv
ndc,awp,wac,nadac,federal_rebate,state_rebate,effective_date
```

- **AWP**: Average Wholesale Price ($10.50 - $2,500)
- **WAC**: Wholesale Acquisition Cost (~85% of AWP)
- **NADAC**: National Average Drug Acquisition Cost (~40% of AWP)
- **Federal/State Rebates**: Percentage rebates (10-23% federal, 5-15% state)

### **Formulary Status (`fdb_formulary_{tenant}.csv`)**
```csv
ndc,formulary_status,tier,pa_required,ql_limits,effective_date
```

- **Formulary Status**: Preferred, Non-Preferred, Not Covered, Prior Auth
- **Tier**: 1-4 (lower = preferred)
- **PA Required**: Prior authorization needed
- **QL Limits**: Quantity limits (e.g., "30/30 days", "1/day")

---

## 🏢 Tenant Differences

### **MASTER (Mother Tenant)**
- **Coverage**: National pharmaceutical database
- **Records**: ~500 core drugs + pricing
- **Regional Code**: US-NATIONAL

### **AK (Alaska Child Tenant)**
- **Coverage**: Alaska region with cold-weather preferences
- **Records**: Inherits MASTER + Alaska-specific preferences
- **Regional Codes**: US-AK, US-NORTHWEST
- **Special**: "Alaska Pharmaceuticals" local manufacturer preferences

### **MO (Missouri Child Tenant)**
- **Coverage**: Missouri/Midwest region
- **Records**: Inherits MASTER + Missouri-specific preferences  
- **Regional Codes**: US-MO, US-MIDWEST
- **Special**: "MO Pharmaceuticals" local manufacturer preferences

---

## 🚀 Usage in Databricks

### **1. Upload to Databricks Volume**
```bash
# Create volume in Databricks workspace
databricks fs mkdirs /Volumes/catalog/schema/fdb_data/

# Upload all CSV files
databricks fs cp sample_fdb_data/*.csv /Volumes/catalog/schema/fdb_data/ --recursive
```

### **2. Query Examples**
```sql
-- Read core drug data
SELECT * FROM csv.`/Volumes/catalog/schema/fdb_data/fdb_core_drugs.csv`

-- Join core data with pricing
SELECT c.ndc, c.brand, c.generic, p.awp, p.wac 
FROM csv.`/Volumes/catalog/schema/fdb_data/fdb_core_drugs.csv` c
JOIN csv.`/Volumes/catalog/schema/fdb_data/fdb_pricing.csv` p ON c.ndc = p.ndc

-- Tenant-specific formulary lookup
SELECT * FROM csv.`/Volumes/catalog/schema/fdb_data/fdb_formulary_ak.csv`
WHERE formulary_status = 'Preferred'
```

### **3. Python API Integration**
```python
# Read from volume in Python
import pandas as pd

# Core drugs
df_core = pd.read_csv("/Volumes/catalog/schema/fdb_data/fdb_core_drugs.csv")

# Tenant-specific formulary
df_formulary_ak = pd.read_csv("/Volumes/catalog/schema/fdb_data/fdb_formulary_ak.csv")

# Search functionality
def search_drugs(query, tenant="master"):
    df = pd.read_csv(f"/Volumes/catalog/schema/fdb_data/fdb_core_drugs.csv")
    if tenant != "master":
        df_formulary = pd.read_csv(f"/Volumes/catalog/schema/fdb_data/fdb_formulary_{tenant}.csv")
        df = df.merge(df_formulary, on='ndc', how='inner')
    
    # Search across multiple fields
    mask = (df['brand'].str.contains(query, case=False) | 
            df['generic'].str.contains(query, case=False) |
            df['ndc'].str.contains(query))
    
    return df[mask]
```

---

## 📊 Data Statistics

- **Total Files**: 25
- **Total Records**: ~2,000 drug records
- **File Size**: ~270 KB total
- **Date Range**: 2024-2025 (realistic load dates)
- **Drug Classes**: 35+ therapeutic categories
- **Manufacturers**: 50+ pharmaceutical companies
- **Tenants**: 3 (MASTER + 2 children)

---

## 🔧 Integration with FMT Backend

The generated files are ready for integration with the FDB Search backend (`backend/fdb/routes.py`). Replace the hardcoded data arrays with volume-based queries to provide real search, filtering, and export functionality.

**Next Steps:**
1. Upload files to Databricks volume
2. Update `backend/fdb/routes.py` to query from volume
3. Test search and export functionality
4. Deploy updated app

---

## 📝 Notes

- **Realistic but synthetic**: All data is computer-generated for testing
- **Production ready structure**: Mimics real FDB file organization
- **Scalable**: Easy to add more files/tenants/therapeutic areas
- **Version controlled**: Included in git repository for reproducibility

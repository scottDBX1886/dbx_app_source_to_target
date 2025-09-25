#!/usr/bin/env python3
"""
PDL Data Generator with FDB NDC Integration
Creates PDL master and keycode data, ensuring NDCs align with FDB core drugs for joinability.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# Output directory
OUTPUT_DIR = Path("sample_pdl_data")
OUTPUT_DIR.mkdir(exist_ok=True)

# Load FDB NDCs to ensure joinability
FDB_CORE_DRUGS_PATH = Path("sample_fdb_data/fdb_core_drugs.csv")
if not FDB_CORE_DRUGS_PATH.exists():
    print(f"Error: FDB core drugs file not found at {FDB_CORE_DRUGS_PATH}. Please run generate_fdb_data.py first.")
    exit(1)

df_fdb_core = pd.read_csv(FDB_CORE_DRUGS_PATH)
FDB_NDCS = df_fdb_core['ndc'].astype(str).tolist()
print(f"Loaded {len(FDB_NDCS)} NDCs from FDB data")

# PDL-specific data
PDL_DRUGS = [
    "METFORMIN", "LISINOPRIL", "AMLODIPINE", "METOPROLOL", "OMEPRAZOLE", "SIMVASTATIN", 
    "LOSARTAN", "FUROSEMIDE", "HYDROCHLOROTHIAZIDE", "GABAPENTIN", "SERTRALINE", "TRAMADOL",
    "ATORVASTATIN", "LEVOTHYROXINE", "ALBUTEROL", "PREDNISONE", "AZITHROMYCIN", "AMOXICILLIN"
]

PDL_STATUSES = ["Active", "Inactive", "Pending", "Under Review"]

# PDL Key Code Templates
TEMPLATES = [
    {"name": "default", "pattern": "GSN|brand6|rx_otc|pkg6"},
    {"name": "generic", "pattern": "GSN|brand6|rx_otc|generic6"},
    {"name": "manufacturer", "pattern": "GSN|brand6|rx_otc|mfr6"},
    {"name": "custom_ak", "pattern": "AK|GSN|brand4|rx"},
    {"name": "custom_mo", "pattern": "MO|GSN|brand4|pkg4"}
]

TENANTS = ["MASTER", "AK", "MO"]

def generate_date(start_days_ago=365, end_days_ago=0):
    """Generate random date within range"""
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    random_date = start + (end - start) * random.random()
    return random_date.strftime('%Y-%m-%d')

def generate_key_code(ndc, template_pattern):
    """Generate a key code based on template pattern"""
    # This is a simplified key code generation
    # In real implementation, would use FDB data fields to generate actual codes
    parts = template_pattern.split("|")
    code_parts = []
    
    for part in parts:
        if part == "GSN":
            code_parts.append(f"GSN{random.randint(100000, 999999)}")
        elif part.startswith("brand"):
            num = int(part.replace("brand", "")) if part != "brand" else 6
            code_parts.append(f"BR{str(random.randint(10**(num-3), 10**(num-2)-1)).zfill(num-2)}")
        elif part == "rx_otc":
            code_parts.append(random.choice(["RX", "OTC"]))
        elif part.startswith("pkg"):
            num = int(part.replace("pkg", "")) if part != "pkg" else 4
            code_parts.append(f"PK{str(random.randint(10**(num-3), 10**(num-2)-1)).zfill(num-2)}")
        elif part.startswith("generic"):
            num = int(part.replace("generic", "")) if part != "generic" else 6
            code_parts.append(f"GN{str(random.randint(10**(num-3), 10**(num-2)-1)).zfill(num-2)}")
        elif part.startswith("mfr"):
            num = int(part.replace("mfr", "")) if part != "mfr" else 6
            code_parts.append(f"MF{str(random.randint(10**(num-3), 10**(num-2)-1)).zfill(num-2)}")
        elif part in ["AK", "MO"]:
            code_parts.append(part)
        else:
            code_parts.append(part.upper())
    
    return "-".join(code_parts)

def create_pdl_master_data(num_records=150):
    """Create PDL master data using NDCs from FDB"""
    print(f"Generating {num_records} PDL master records...")
    
    records = []
    # Ensure we only pick NDCs that exist in FDB_NDCS
    selected_fdb_ndcs = random.sample(FDB_NDCS, min(num_records, len(FDB_NDCS)))
    
    for i, ndc in enumerate(selected_fdb_ndcs):
        pdl_drug = random.choice(PDL_DRUGS)
        status = random.choice(PDL_STATUSES)
        
        record = {
            'ndc': ndc,
            'pdl_drug': pdl_drug,
            'status': status,
            'load_date': generate_date(30, 0),
            'effective_date': generate_date(180, 90),
            'expiration_date': random.choice([None, generate_date(80, 0)]),  # Some might not expire
            'last_updated': generate_date(10, 0),
            'created_by': random.choice(['admin', 'pdl_manager', 'system', 'analyst']),
            'updated_by': random.choice(['admin', 'pdl_manager', 'system', 'analyst']),
            'notes': random.choice(['', 'Auto-generated', 'Manual review needed', 'Updated per formulary', 'New PDL entry']),
            'pos_export_status': random.choice(['Exported', 'Pending', 'Failed', 'Not Required'])
        }
        records.append(record)
    
    return records

def create_pdl_keycodes_data(pdl_master_records):
    """Create PDL keycode data for the master records"""
    print(f"Generating keycode records for {len(pdl_master_records)} PDL records...")
    
    records = []
    for master_record in pdl_master_records:
        ndc = master_record['ndc']
        
        # Generate keycodes for each tenant (some NDCs might have multiple tenant-specific codes)
        for tenant in TENANTS:
            # Not all NDCs need keycodes for all tenants
            if random.random() < 0.7:  # 70% chance of having a keycode for this tenant
                template = random.choice(TEMPLATES)
                
                # For tenant-specific templates, prefer those templates
                if tenant == "AK" and template['name'] == 'custom_ak':
                    pass  # Use this template
                elif tenant == "MO" and template['name'] == 'custom_mo':
                    pass  # Use this template
                elif tenant == "MASTER":
                    # MASTER uses standard templates
                    template = random.choice([t for t in TEMPLATES if not t['name'].startswith('custom_')])
                
                key_code = generate_key_code(ndc, template['pattern'])
                
                record = {
                    'ndc': ndc,
                    'key_code': key_code,
                    'template': template['name'],
                    'tenant': tenant,
                    'generation_date': generate_date(20, 0),
                    'status': random.choice(['Active', 'Generated', 'Validated']),
                    'created_by': random.choice(['system', 'pdl_generator', 'admin']),
                    'notes': f"Generated using {template['name']} template"
                }
                records.append(record)
    
    return records

def save_csv(filename, records, fieldnames):
    """Save records to CSV file"""
    filepath = OUTPUT_DIR / filename
    print(f"Saving {filepath} with {len(records)} records")
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

def main():
    print("=== Generating PDL Data with FDB NDC Integration ===")
    
    # 1. Create PDL Master data
    pdl_master_records = create_pdl_master_data(150)
    pdl_master_fieldnames = [
        'ndc', 'pdl_drug', 'status', 'load_date', 'effective_date', 'expiration_date',
        'last_updated', 'created_by', 'updated_by', 'notes', 'pos_export_status'
    ]
    save_csv('pdl_master.csv', pdl_master_records, pdl_master_fieldnames)
    
    # 2. Create PDL Keycode data
    pdl_keycodes_records = create_pdl_keycodes_data(pdl_master_records)
    pdl_keycodes_fieldnames = [
        'ndc', 'key_code', 'template', 'tenant', 'generation_date', 'status', 'created_by', 'notes'
    ]
    save_csv('pdl_keycodes.csv', pdl_keycodes_records, pdl_keycodes_fieldnames)
    
    print("\n=== VERIFICATION ===")
    pdl_master_ndcs = {r['ndc'] for r in pdl_master_records}
    print(f"PDL Master records: {len(pdl_master_records)}")
    print(f"Keycode records: {len(pdl_keycodes_records)}")
    
    overlap = set(FDB_NDCS).intersection(pdl_master_ndcs)
    print(f"NDC overlap with FDB: {len(overlap)}/{len(pdl_master_ndcs)} ({len(overlap)/len(pdl_master_ndcs)*100:.0f}%)")
    
    # Check status distribution
    status_counts = pd.DataFrame(pdl_master_records)['status'].value_counts().to_dict()
    print(f"Status distribution: {status_counts}")
    
    # Check template usage
    template_counts = pd.DataFrame(pdl_keycodes_records)['template'].value_counts().to_dict()
    print(f"Template usage: {template_counts}")
    
    # Check tenant distribution
    tenant_counts = pd.DataFrame(pdl_keycodes_records)['tenant'].value_counts().to_dict()
    print(f"Tenant distribution: {tenant_counts}")
    
    print(f"\n✅ PDL data generation complete!")
    print(f"Files created in {OUTPUT_DIR}/:")
    print(f"  - pdl_master.csv (main PDL data)")
    print(f"  - pdl_keycodes.csv (generated key codes)")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Enhanced PDL Data Generator with Realistic Tenant-Specific Key Code Generation
Creates PDL master and keycode data showing realistic differences between MASTER, AK, and MO tenants.
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
print(f"Loaded {len(FDB_NDCS)} NDCs from FDB data for tenant-specific PDL generation")

# PDL-specific data with realistic drug categories
PDL_DRUGS = [
    "METFORMIN", "LISINOPRIL", "AMLODIPINE", "METOPROLOL", "OMEPRAZOLE", "SIMVASTATIN", 
    "LOSARTAN", "ATORVASTATIN", "LEVOTHYROXINE", "ALBUTEROL", "SERTRALINE", "GABAPENTIN",
    "PREDNISONE", "AZITHROMYCIN", "AMOXICILLIN", "TRAMADOL", "HYDROCHLOROTHIAZIDE", "FUROSEMIDE",
    "IBUPROFEN", "ACETAMINOPHEN", "WARFARIN", "CLOPIDOGREL", "PANTOPRAZOLE", "ESCITALOPRAM"
]

PDL_STATUSES = ["Active", "Inactive", "Pending", "Under Review"]

# Tenant-specific key code templates (realistic differences)
TENANT_TEMPLATES = {
    "MASTER": [
        {"name": "master_standard", "pattern": "GSN|BRAND6|RX|PKG4", "weight": 0.4},
        {"name": "master_generic", "pattern": "GSN|GEN6|RX|PKG4", "weight": 0.3},
        {"name": "master_mfr", "pattern": "GSN|BRAND4|MFR4|RX", "weight": 0.3}
    ],
    "AK": [
        {"name": "ak_rural", "pattern": "AK|GSN|BRAND4|RX|RURAL", "weight": 0.5},
        {"name": "ak_standard", "pattern": "AK|GSN|BRAND6|RX", "weight": 0.3},
        {"name": "ak_extended", "pattern": "AK|GSN|BRAND4|PKG6|EXT", "weight": 0.2}
    ],
    "MO": [
        {"name": "mo_medicaid", "pattern": "MO|GSN|BRAND6|MCAID|PKG3", "weight": 0.4},
        {"name": "mo_community", "pattern": "MO|GSN|BRAND4|COMM|RX", "weight": 0.35},
        {"name": "mo_standard", "pattern": "MO|GSN|BRAND6|RX", "weight": 0.25}
    ]
}

def generate_date(start_days_ago=365, end_days_ago=0):
    """Generate random date within range"""
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    random_date = start + (end - start) * random.random()
    return random_date.strftime('%Y-%m-%d')

def generate_realistic_key_code(ndc, template_pattern, drug_name=""):
    """Generate a realistic key code based on template pattern and drug info"""
    parts = template_pattern.split("|")
    code_parts = []
    
    # Create more realistic mappings based on actual drug
    gsn_map = {
        "METFORMIN": "024578", "LISINOPRIL": "030123", "AMLODIPINE": "058234",
        "METOPROLOL": "018432", "OMEPRAZOLE": "023987", "SIMVASTATIN": "019876"
    }
    
    brand_map = {
        "METFORMIN": "GLUCOPH", "LISINOPRIL": "ZESTRL", "AMLODIPINE": "NORVAS",
        "METOPROLOL": "LOPRES", "OMEPRAZOLE": "PRILOS", "SIMVASTATIN": "ZOCOR"
    }
    
    for part in parts:
        if part == "GSN":
            # Use realistic GSN or generate one
            gsn = gsn_map.get(drug_name, f"{random.randint(100000, 999999):06d}")
            code_parts.append(gsn)
        elif part.startswith("BRAND"):
            num = int(part.replace("BRAND", "")) if len(part) > 5 else 6
            brand = brand_map.get(drug_name, f"BR{random.randint(1000, 9999):04d}")[:num]
            code_parts.append(brand.ljust(num, 'X'))
        elif part == "RX":
            code_parts.append("RX")
        elif part == "OTC":
            code_parts.append("OTC")
        elif part.startswith("PKG"):
            num = int(part.replace("PKG", "")) if len(part) > 3 else 4
            code_parts.append(f"P{random.randint(10**(num-2), 10**(num-1)-1):0{num-1}d}")
        elif part.startswith("GEN"):
            num = int(part.replace("GEN", "")) if len(part) > 3 else 6
            generic = f"GN{random.randint(1000, 9999):04d}"[:num]
            code_parts.append(generic.ljust(num, 'X'))
        elif part.startswith("MFR"):
            num = int(part.replace("MFR", "")) if len(part) > 3 else 4
            code_parts.append(f"MF{random.randint(100, 999):03d}")
        elif part in ["AK", "MO"]:
            code_parts.append(part)
        elif part == "RURAL":
            code_parts.append("RUR")
        elif part == "MCAID":
            code_parts.append("MCD")
        elif part == "COMM":
            code_parts.append("COM")
        elif part == "EXT":
            code_parts.append("EXT")
        else:
            code_parts.append(part)
    
    return "-".join(code_parts)

def select_template_for_tenant(tenant):
    """Select appropriate template based on tenant and weights"""
    templates = TENANT_TEMPLATES[tenant]
    template_names = [t["name"] for t in templates]
    weights = [t["weight"] for t in templates]
    selected_name = random.choices(template_names, weights=weights)[0]
    return next(t for t in templates if t["name"] == selected_name)

def create_tenant_specific_pdl_data(num_base_records=180):
    """Create realistic tenant-specific PDL data with different key code policies"""
    print(f"Generating tenant-specific PDL master records with realistic key code differences...")
    
    records = []
    base_ndcs = random.sample(FDB_NDCS, min(num_base_records, len(FDB_NDCS)))
    
    # Tenant-specific status preferences for PDL
    tenant_status_preferences = {
        "MASTER": {"Active": 0.7, "Inactive": 0.1, "Pending": 0.15, "Under Review": 0.05},
        "AK": {"Active": 0.6, "Inactive": 0.15, "Pending": 0.15, "Under Review": 0.1},  # More conservative
        "MO": {"Active": 0.75, "Inactive": 0.1, "Pending": 0.1, "Under Review": 0.05}   # More aggressive
    }
    
    def get_weighted_status(tenant):
        """Get status based on tenant preferences"""
        prefs = tenant_status_preferences[tenant]
        return random.choices(list(prefs.keys()), weights=list(prefs.values()))[0]
    
    for ndc in base_ndcs:
        pdl_drug = random.choice(PDL_DRUGS)
        
        # MASTER record (baseline)
        master_record = {
            'ndc': ndc,
            'pdl_drug': pdl_drug,
            'status': get_weighted_status("MASTER"),
            'load_date': generate_date(300, 150),
            'effective_date': generate_date(300, 150),
            'expiration_date': None,  # MASTER rarely expires
            'last_updated': generate_date(30, 0),
            'created_by': 'pdl_admin',
            'updated_by': 'pdl_admin',
            'notes': 'MASTER PDL baseline entry',
            'pos_export_status': random.choice(['Exported', 'Pending', 'Not Required'])
        }
        records.append(master_record)
        
        # AK record (70% chance, often with different focus)
        if random.random() < 0.70:
            ak_status = get_weighted_status("AK")
            # AK might have expiration dates for certain statuses
            ak_exp_date = generate_date(180, 30) if ak_status in ['Pending', 'Under Review'] else None
            
            ak_record = {
                'ndc': ndc,
                'pdl_drug': pdl_drug,
                'status': ak_status,
                'load_date': generate_date(200, 80),
                'effective_date': generate_date(200, 80),
                'expiration_date': ak_exp_date,
                'last_updated': generate_date(15, 0),
                'created_by': random.choice(['ak_pdl_coordinator', 'ak_rural_specialist']),
                'updated_by': random.choice(['ak_pdl_coordinator', 'ak_rural_specialist']),
                'notes': f'AK rural healthcare PDL - {ak_status.lower()} for remote access',
                'pos_export_status': random.choice(['Exported', 'Pending'])
            }
            records.append(ak_record)
        
        # MO record (85% chance, aggressive PDL management)
        if random.random() < 0.85:
            mo_status = get_weighted_status("MO")
            
            mo_record = {
                'ndc': ndc,
                'pdl_drug': pdl_drug,
                'status': mo_status,
                'load_date': generate_date(150, 50),
                'effective_date': generate_date(150, 50),
                'expiration_date': None,  # MO typically keeps active longer
                'last_updated': generate_date(10, 0),
                'created_by': random.choice(['mo_pdl_manager', 'mo_medicaid_coordinator']),
                'updated_by': random.choice(['mo_pdl_manager', 'mo_medicaid_coordinator']),
                'notes': f'MO Medicaid PDL - {mo_status.lower()} per state formulary',
                'pos_export_status': random.choice(['Exported', 'Not Required'])
            }
            records.append(mo_record)
    
    # Add tenant-specific only entries
    additional_ndcs = random.sample([n for n in FDB_NDCS if n not in base_ndcs], 80)
    
    # AK-specific entries (focus on rural/cold weather needs)
    for ndc in additional_ndcs[:30]:
        ak_drug = random.choice(["ALBUTEROL", "PREDNISONE", "TRAMADOL", "ACETAMINOPHEN"])
        ak_record = {
            'ndc': ndc,
            'pdl_drug': ak_drug,
            'status': random.choice(['Active', 'Pending']),
            'load_date': generate_date(90, 20),
            'effective_date': generate_date(90, 20),
            'expiration_date': generate_date(120, 30) if random.random() < 0.4 else None,
            'last_updated': generate_date(5, 0),
            'created_by': 'ak_rural_program',
            'updated_by': 'ak_rural_program',
            'notes': 'AK-specific PDL for rural healthcare initiative',
            'pos_export_status': 'Pending'
        }
        records.append(ak_record)
    
    # MO-specific entries (state program focus)
    for ndc in additional_ndcs[30:60]:
        mo_drug = random.choice(["METFORMIN", "LISINOPRIL", "SERTRALINE", "OMEPRAZOLE"])
        mo_record = {
            'ndc': ndc,
            'pdl_drug': mo_drug,
            'status': random.choice(['Active', 'Active', 'Pending']),  # MO tends to approve
            'load_date': generate_date(60, 10),
            'effective_date': generate_date(60, 10),
            'expiration_date': None,
            'last_updated': generate_date(3, 0),
            'created_by': 'mo_medicaid_plus',
            'updated_by': 'mo_medicaid_plus',
            'notes': 'MO Medicaid Plus program - enhanced PDL coverage',
            'pos_export_status': 'Exported'
        }
        records.append(mo_record)
    
    return records

def create_tenant_specific_keycodes(pdl_master_records):
    """Generate tenant-specific key codes based on PDL master records"""
    print(f"Generating tenant-specific key codes for {len(pdl_master_records)} PDL records...")
    
    records = []
    
    # Group records by NDC to handle tenant-specific key code generation
    ndc_groups = {}
    for record in pdl_master_records:
        ndc = record['ndc']
        if ndc not in ndc_groups:
            ndc_groups[ndc] = []
        ndc_groups[ndc].append(record)
    
    for ndc, master_records in ndc_groups.items():
        # Generate key codes for each tenant that has this NDC
        tenants_with_ndc = []
        for record in master_records:
            # Infer tenant from created_by field
            if 'ak_' in record['created_by']:
                tenants_with_ndc.append('AK')
            elif 'mo_' in record['created_by']:
                tenants_with_ndc.append('MO')
            else:
                tenants_with_ndc.append('MASTER')
        
        # Remove duplicates but keep order
        tenants_with_ndc = list(dict.fromkeys(tenants_with_ndc))
        
        for tenant in tenants_with_ndc:
            # Each tenant might generate different key codes for the same NDC
            template = select_template_for_tenant(tenant)
            
            # Get drug name from one of the master records
            drug_name = master_records[0]['pdl_drug']
            
            key_code = generate_realistic_key_code(ndc, template['pattern'], drug_name)
            
            keycode_record = {
                'ndc': ndc,
                'key_code': key_code,
                'template': template['name'],
                'tenant': tenant,
                'generation_date': generate_date(60, 0),
                'status': random.choice(['Active', 'Generated', 'Validated']),
                'created_by': f"{tenant.lower()}_pdl_system",
                'notes': f"Generated using {template['name']} template for {tenant} tenant"
            }
            records.append(keycode_record)
            
            # Some NDCs might have multiple key codes per tenant (different versions)
            if random.random() < 0.15:  # 15% chance of having an alternate key code
                alt_template = select_template_for_tenant(tenant)
                if alt_template['name'] != template['name']:  # Make sure it's different
                    alt_key_code = generate_realistic_key_code(ndc, alt_template['pattern'], drug_name)
                    alt_keycode_record = {
                        'ndc': ndc,
                        'key_code': alt_key_code,
                        'template': alt_template['name'],
                        'tenant': tenant,
                        'generation_date': generate_date(30, 0),
                        'status': 'Generated',  # Usually newer version
                        'created_by': f"{tenant.lower()}_pdl_system",
                        'notes': f"Alternative {alt_template['name']} key code for {tenant} tenant"
                    }
                    records.append(alt_keycode_record)
    
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
    print("=== Generating Tenant-Specific PDL Data with Realistic Key Code Differences ===")
    
    # 1. Create tenant-specific PDL Master data
    pdl_master_records = create_tenant_specific_pdl_data(180)
    pdl_master_fieldnames = [
        'ndc', 'pdl_drug', 'status', 'load_date', 'effective_date', 'expiration_date',
        'last_updated', 'created_by', 'updated_by', 'notes', 'pos_export_status'
    ]
    save_csv('pdl_master.csv', pdl_master_records, pdl_master_fieldnames)
    
    # 2. Create tenant-specific keycode data
    pdl_keycodes_records = create_tenant_specific_keycodes(pdl_master_records)
    pdl_keycodes_fieldnames = [
        'ndc', 'key_code', 'template', 'tenant', 'generation_date', 'status', 'created_by', 'notes'
    ]
    save_csv('pdl_keycodes.csv', pdl_keycodes_records, pdl_keycodes_fieldnames)
    
    print("\n=== TENANT ANALYSIS ===")
    df_master = pd.DataFrame(pdl_master_records)
    df_keycodes = pd.DataFrame(pdl_keycodes_records)
    
    # Analyze PDL master by tenant (inferred from created_by)
    tenant_inference = []
    for record in pdl_master_records:
        if 'ak_' in record['created_by']:
            tenant_inference.append('AK')
        elif 'mo_' in record['created_by']:
            tenant_inference.append('MO')
        else:
            tenant_inference.append('MASTER')
    
    tenant_counts = pd.Series(tenant_inference).value_counts()
    print(f"PDL Master records by tenant: {tenant_counts.to_dict()}")
    
    # Analyze key codes by tenant
    keycode_tenant_counts = df_keycodes['tenant'].value_counts()
    print(f"Key code records by tenant: {keycode_tenant_counts.to_dict()}")
    
    # Template usage by tenant
    for tenant in ['MASTER', 'AK', 'MO']:
        tenant_keycodes = df_keycodes[df_keycodes['tenant'] == tenant]
        if not tenant_keycodes.empty:
            template_usage = tenant_keycodes['template'].value_counts().to_dict()
            print(f"{tenant} template usage: {template_usage}")
    
    # Status distribution by tenant
    for tenant in ['MASTER', 'AK', 'MO']:
        tenant_records = [r for r in pdl_master_records if tenant.lower() in r['created_by']]
        if tenant_records:
            status_dist = pd.DataFrame(tenant_records)['status'].value_counts().to_dict()
            print(f"{tenant} status distribution: {status_dist}")
    
    # Check NDC overlap with FDB
    pdl_ndcs = {r['ndc'] for r in pdl_master_records}
    overlap = set(FDB_NDCS).intersection(pdl_ndcs)
    print(f"NDC overlap with FDB: {len(overlap)}/{len(pdl_ndcs)} ({len(overlap)/len(pdl_ndcs)*100:.0f}%)")
    
    print(f"\n✅ Enhanced PDL data generation complete!")
    print(f"Files created in {OUTPUT_DIR}/:")
    print(f"  - pdl_master.csv ({len(pdl_master_records)} records with tenant differences)")
    print(f"  - pdl_keycodes.csv ({len(pdl_keycodes_records)} key codes with tenant-specific templates)")
    print(f"\nRealistic tenant key code scenarios:")
    print(f"  • MASTER: Standard GSN-based patterns for baseline formulary")
    print(f"  • AK: Rural-focused codes with extended packaging considerations")  
    print(f"  • MO: Medicaid-specific codes with community care integration")

if __name__ == "__main__":
    main()

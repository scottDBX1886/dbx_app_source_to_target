#!/usr/bin/env python3
"""
Enhanced FMT Data Generator with Realistic Tenant-Specific Data
Creates FMT master and MBID data showing realistic differences between MASTER, AK, and MO tenants.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# Output directory
OUTPUT_DIR = Path("sample_fmt_data")
OUTPUT_DIR.mkdir(exist_ok=True)

# Load FDB NDCs to ensure joinability
FDB_CORE_DRUGS_PATH = Path("sample_fdb_data/fdb_core_drugs.csv")
if not FDB_CORE_DRUGS_PATH.exists():
    print(f"Error: FDB core drugs file not found at {FDB_CORE_DRUGS_PATH}. Please run generate_fdb_data.py first.")
    exit(1)

df_fdb_core = pd.read_csv(FDB_CORE_DRUGS_PATH)
FDB_NDCS = df_fdb_core['ndc'].astype(str).tolist()
print(f"Loaded {len(FDB_NDCS)} NDCs from FDB data for tenant-specific FMT generation")

# FMT-specific data with realistic drug categories
FMT_DRUGS = [
    "METFORMIN", "LISINOPRIL", "AMLODIPINE", "METOPROLOL", "OMEPRAZOLE", "SIMVASTATIN", 
    "LOSARTAN", "ATORVASTATIN", "LEVOTHYROXINE", "ALBUTEROL", "SERTRALINE", "GABAPENTIN",
    "PREDNISONE", "AZITHROMYCIN", "AMOXICILLIN", "TRAMADOL", "HYDROCHLOROTHIAZIDE", "FUROSEMIDE"
]

FMT_STATUSES = ["PDL", "Approved", "Review", "Restricted"]

def generate_date(start_days_ago=365, end_days_ago=0):
    """Generate random date within range"""
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    random_date = start + (end - start) * random.random()
    return random_date.strftime('%Y-%m-%d')

def create_tenant_specific_fmt_data(num_base_records=200):
    """Create realistic tenant-specific FMT data with different policies per tenant"""
    print(f"Generating tenant-specific FMT master records with realistic differences...")
    
    records = []
    base_ndcs = random.sample(FDB_NDCS, min(num_base_records, len(FDB_NDCS)))
    
    # Enhanced MBID mapping for more realistic scenarios
    mbid_mapping = {
        "METFORMIN": {"MASTER": "DIAB_METFORM_001", "AK": "AK_DIAB_001", "MO": "MO_DIAB_001"},
        "LISINOPRIL": {"MASTER": "CARDIO_ACE_001", "AK": "AK_CARDIO_001", "MO": "CARDIO_ACE_001"},
        "AMLODIPINE": {"MASTER": "CARDIO_CCB_001", "AK": "AK_CARDIO_002", "MO": "MO_CARDIO_001"},
        "METOPROLOL": {"MASTER": "CARDIO_BB_001", "AK": "AK_CARDIO_001", "MO": "MO_CARDIO_002"},
        "OMEPRAZOLE": {"MASTER": "GI_PPI_001", "AK": "AK_GI_001", "MO": "GI_PPI_001"},
        "SIMVASTATIN": {"MASTER": "CARDIO_STATIN_001", "AK": "AK_CARDIO_003", "MO": "MO_CARDIO_003"},
        "LOSARTAN": {"MASTER": "CARDIO_ARB_001", "AK": "AK_CARDIO_001", "MO": "MO_CARDIO_001"},
        "ATORVASTATIN": {"MASTER": "CARDIO_STATIN_002", "AK": "AK_CARDIO_003", "MO": "MO_CARDIO_003"},
        "LEVOTHYROXINE": {"MASTER": "ENDO_THYROID_001", "AK": "AK_ENDO_001", "MO": "ENDO_THYROID_001"},
        "ALBUTEROL": {"MASTER": "RESP_BRONCH_001", "AK": "AK_RESP_001", "MO": "RESP_BRONCH_001"},
        "SERTRALINE": {"MASTER": "PSYCH_SSRI_001", "AK": "AK_PSYCH_001", "MO": "MO_PSYCH_001"},
        "GABAPENTIN": {"MASTER": "NEURO_ANTI_001", "AK": "AK_PAIN_001", "MO": "NEURO_ANTI_001"},
    }
    
    # Tenant-specific status preferences (realistic policy differences)
    tenant_status_preferences = {
        "MASTER": {"PDL": 0.4, "Approved": 0.4, "Review": 0.15, "Restricted": 0.05},
        "AK": {"PDL": 0.3, "Approved": 0.3, "Review": 0.25, "Restricted": 0.15},  # More restrictive
        "MO": {"PDL": 0.45, "Approved": 0.35, "Review": 0.15, "Restricted": 0.05}  # More permissive
    }
    
    def get_weighted_status(tenant):
        """Get status based on tenant preferences"""
        prefs = tenant_status_preferences[tenant]
        return random.choices(list(prefs.keys()), weights=list(prefs.values()))[0]
    
    def get_mbid_for_drug_tenant(drug, tenant):
        """Get appropriate MBID for drug/tenant combo"""
        if drug in mbid_mapping and tenant in mbid_mapping[drug]:
            return mbid_mapping[drug][tenant]
        # Fallback to generic MBID pattern
        return f"{tenant}_GEN_{random.randint(100, 999):03d}"
    
    for ndc in base_ndcs:
        fmt_drug = random.choice(FMT_DRUGS)
        
        # MASTER record (baseline)
        master_status = get_weighted_status("MASTER")
        master_record = {
            'ndc': ndc,
            'fmt_drug': fmt_drug,
            'mbid': get_mbid_for_drug_tenant(fmt_drug, "MASTER"),
            'status': master_status,
            'start_date': generate_date(400, 200),
            'end_date': None,  # MASTER typically doesn't expire
            'load_date': generate_date(30, 0),
            'effective_date': generate_date(400, 200),
            'expiration_date': None,
            'created_by': 'system_admin',
            'updated_by': 'system_admin',
            'review_status': 'Approved',
            'notes': 'MASTER baseline formulary entry'
        }
        records.append(master_record)
        
        # AK record (75% chance, often different from MASTER)
        if random.random() < 0.75:
            ak_status = get_weighted_status("AK")
            # AK might have stricter end dates for certain statuses
            ak_end_date = generate_date(60, 0) if ak_status == "Restricted" else None
            
            ak_record = {
                'ndc': ndc,
                'fmt_drug': fmt_drug,
                'mbid': get_mbid_for_drug_tenant(fmt_drug, "AK"),
                'status': ak_status,
                'start_date': generate_date(300, 100),
                'end_date': ak_end_date,
                'load_date': generate_date(20, 0),
                'effective_date': generate_date(300, 100),
                'expiration_date': ak_end_date,
                'created_by': random.choice(['ak_pharmacist', 'ak_manager', 'ak_reviewer']),
                'updated_by': random.choice(['ak_pharmacist', 'ak_manager', 'ak_reviewer']),
                'review_status': 'Approved' if ak_status in ['PDL', 'Approved'] else random.choice(['Pending', 'In Review']),
                'notes': f'AK state policy - {ak_status.lower()} per remote access guidelines'
            }
            records.append(ak_record)
        
        # MO record (80% chance, different policies than AK)
        if random.random() < 0.80:
            mo_status = get_weighted_status("MO")
            
            mo_record = {
                'ndc': ndc,
                'fmt_drug': fmt_drug,
                'mbid': get_mbid_for_drug_tenant(fmt_drug, "MO"),
                'status': mo_status,
                'start_date': generate_date(250, 80),
                'end_date': None,  # MO rarely uses end dates
                'load_date': generate_date(15, 0),
                'effective_date': generate_date(250, 80),
                'expiration_date': None,
                'created_by': random.choice(['mo_analyst', 'mo_pharmacist', 'mo_director']),
                'updated_by': random.choice(['mo_analyst', 'mo_pharmacist', 'mo_director']),
                'review_status': 'Approved' if mo_status in ['PDL', 'Approved'] else 'Pending',
                'notes': f'MO Medicaid formulary - {mo_status.lower()} status'
            }
            records.append(mo_record)
    
    # Add tenant-specific only entries (realistic scenario)
    additional_ndcs = random.sample([n for n in FDB_NDCS if n not in base_ndcs], 100)
    
    # AK-specific entries (cold weather, rural access considerations)
    for ndc in additional_ndcs[:40]:
        ak_drug = random.choice(["ALBUTEROL", "PREDNISONE", "GABAPENTIN", "TRAMADOL"])  # Common in rural/cold areas
        ak_record = {
            'ndc': ndc,
            'fmt_drug': ak_drug,
            'mbid': get_mbid_for_drug_tenant(ak_drug, "AK"),
            'status': random.choice(['Review', 'Approved']),  # New entries under review
            'start_date': generate_date(120, 30),
            'end_date': None,
            'load_date': generate_date(10, 0),
            'effective_date': generate_date(120, 30),
            'expiration_date': generate_date(180, 60) if random.random() < 0.3 else None,
            'created_by': 'ak_rural_program',
            'updated_by': 'ak_rural_program',
            'review_status': 'In Review',
            'notes': 'AK-specific addition for rural healthcare access'
        }
        records.append(ak_record)
    
    # MO-specific entries (urban health, state-specific programs)
    for ndc in additional_ndcs[40:80]:
        mo_drug = random.choice(["METFORMIN", "LISINOPRIL", "SERTRALINE", "OMEPRAZOLE"])  # Common chronic care
        mo_record = {
            'ndc': ndc,
            'fmt_drug': mo_drug,
            'mbid': get_mbid_for_drug_tenant(mo_drug, "MO"),
            'status': random.choice(['PDL', 'Approved']),  # MO tends to approve state programs
            'start_date': generate_date(90, 15),
            'end_date': None,
            'load_date': generate_date(5, 0),
            'effective_date': generate_date(90, 15),
            'expiration_date': None,
            'created_by': 'mo_state_program',
            'updated_by': 'mo_state_program',
            'review_status': 'Approved',
            'notes': 'MO Medicaid Plus program - enhanced coverage'
        }
        records.append(mo_record)
    
    return records

def create_enhanced_mbid_data():
    """Create enhanced MBID reference data with realistic tenant differences"""
    print("Generating enhanced MBID records with tenant-specific policies...")
    
    mbids = [
        # MASTER MBIDs (baseline categories)
        {"mbid": "CARDIO_ACE_001", "description": "Cardiovascular - ACE Inhibitors", "tenant": "MASTER"},
        {"mbid": "CARDIO_CCB_001", "description": "Cardiovascular - Calcium Channel Blockers", "tenant": "MASTER"},
        {"mbid": "CARDIO_BB_001", "description": "Cardiovascular - Beta Blockers", "tenant": "MASTER"},
        {"mbid": "CARDIO_ARB_001", "description": "Cardiovascular - ARBs", "tenant": "MASTER"},
        {"mbid": "CARDIO_STATIN_001", "description": "Cardiovascular - Statins (Simvastatin)", "tenant": "MASTER"},
        {"mbid": "CARDIO_STATIN_002", "description": "Cardiovascular - Statins (Atorvastatin)", "tenant": "MASTER"},
        {"mbid": "DIAB_METFORM_001", "description": "Diabetes - Metformin", "tenant": "MASTER"},
        {"mbid": "GI_PPI_001", "description": "GI - Proton Pump Inhibitors", "tenant": "MASTER"},
        {"mbid": "ENDO_THYROID_001", "description": "Endocrine - Thyroid Hormones", "tenant": "MASTER"},
        {"mbid": "RESP_BRONCH_001", "description": "Respiratory - Bronchodilators", "tenant": "MASTER"},
        {"mbid": "PSYCH_SSRI_001", "description": "Psychiatric - SSRIs", "tenant": "MASTER"},
        {"mbid": "NEURO_ANTI_001", "description": "Neurologic - Anticonvulsants", "tenant": "MASTER"},
        
        # AK-specific MBIDs (cold weather, rural access focus)
        {"mbid": "AK_CARDIO_001", "description": "AK Cardio - Cold Weather Formulations", "tenant": "AK"},
        {"mbid": "AK_CARDIO_002", "description": "AK Cardio - Extended Release (Rural)", "tenant": "AK"},
        {"mbid": "AK_CARDIO_003", "description": "AK Cardio - High Altitude Considerations", "tenant": "AK"},
        {"mbid": "AK_RESP_001", "description": "AK Respiratory - Cold Climate Approved", "tenant": "AK"},
        {"mbid": "AK_PAIN_001", "description": "AK Pain Management - Rural Access Program", "tenant": "AK"},
        {"mbid": "AK_DIAB_001", "description": "AK Diabetes - Remote Monitoring Compatible", "tenant": "AK"},
        {"mbid": "AK_GI_001", "description": "AK GI - Extended Supply Program", "tenant": "AK"},
        {"mbid": "AK_ENDO_001", "description": "AK Endocrine - Telehealth Compatible", "tenant": "AK"},
        {"mbid": "AK_PSYCH_001", "description": "AK Psychiatric - Behavioral Health Focus", "tenant": "AK"},
        
        # MO-specific MBIDs (urban health, state programs)
        {"mbid": "MO_CARDIO_001", "description": "MO Cardiovascular - Urban Health Initiative", "tenant": "MO"},
        {"mbid": "MO_CARDIO_002", "description": "MO Cardio - Community Care Network", "tenant": "MO"},
        {"mbid": "MO_CARDIO_003", "description": "MO Cardio - State Formulary Plus", "tenant": "MO"},
        {"mbid": "MO_DIAB_001", "description": "MO Diabetes - Enhanced Coverage Program", "tenant": "MO"},
        {"mbid": "MO_PSYCH_001", "description": "MO Psychiatric - Community Mental Health", "tenant": "MO"}
    ]
    
    records = []
    for mbid_info in mbids:
        record = {
            'mbid': mbid_info['mbid'],
            'description': mbid_info['description'],
            'tenant': mbid_info['tenant'],
            'begin_date': generate_date(800, 400),
            'end_date': None,
            'created_by': 'system' if mbid_info['tenant'] == 'MASTER' else f"{mbid_info['tenant'].lower()}_admin",
            'status': 'Active'
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
    print("=== Generating Tenant-Specific FMT Data with Realistic Differences ===")
    
    # 1. Create tenant-specific FMT Master data
    fmt_master_records = create_tenant_specific_fmt_data(200)
    fmt_master_fieldnames = [
        'ndc', 'fmt_drug', 'mbid', 'status', 'start_date', 'end_date', 'load_date',
        'effective_date', 'expiration_date', 'created_by', 'updated_by', 'review_status', 'notes'
    ]
    save_csv('fmt_master.csv', fmt_master_records, fmt_master_fieldnames)
    
    # 2. Create enhanced MBID data
    fmt_mbids_records = create_enhanced_mbid_data()
    fmt_mbids_fieldnames = [
        'mbid', 'description', 'tenant', 'begin_date', 'end_date', 'created_by', 'status'
    ]
    save_csv('fmt_mbids.csv', fmt_mbids_records, fmt_mbids_fieldnames)
    
    print("\n=== TENANT ANALYSIS ===")
    df_master = pd.DataFrame(fmt_master_records)
    
    # Analyze by tenant
    tenant_counts = df_master['mbid'].str.extract(r'^(MASTER|AK|MO|[^_]+)')[0].value_counts()
    print(f"Records by tenant pattern: {tenant_counts.to_dict()}")
    
    # Status distribution by tenant inference
    for tenant in ['MASTER', 'AK', 'MO']:
        tenant_records = [r for r in fmt_master_records if tenant in r['mbid']]
        if tenant_records:
            status_dist = pd.DataFrame(tenant_records)['status'].value_counts().to_dict()
            print(f"{tenant} status distribution: {status_dist}")
    
    # Check NDC overlap with FDB
    fmt_ndcs = {r['ndc'] for r in fmt_master_records}
    overlap = set(FDB_NDCS).intersection(fmt_ndcs)
    print(f"NDC overlap with FDB: {len(overlap)}/{len(fmt_ndcs)} ({len(overlap)/len(fmt_ndcs)*100:.0f}%)")
    
    print(f"\n✅ Enhanced FMT data generation complete!")
    print(f"Files created in {OUTPUT_DIR}/:")
    print(f"  - fmt_master.csv ({len(fmt_master_records)} records with tenant differences)")
    print(f"  - fmt_mbids.csv ({len(fmt_mbids_records)} MBIDs with tenant-specific policies)")
    print(f"\nRealistic tenant scenarios:")
    print(f"  • MASTER: Baseline formulary with broad approvals")
    print(f"  • AK: Rural/cold weather focus, more restrictive policies")  
    print(f"  • MO: Urban health focus, state program enhancements")

if __name__ == "__main__":
    main()

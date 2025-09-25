#!/usr/bin/env python3
"""
FMT Data Generator with Matching NDCs to FDB
Ensures FMT master data has NDCs that exist in FDB data for proper integration
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# Output directory
OUTPUT_DIR = Path("sample_fmt_data")
OUTPUT_DIR.mkdir(exist_ok=True)

# FDB data path (to get matching NDCs)
FDB_DATA_PATH = Path("sample_fdb_data/fdb_core_drugs.csv")

# FMT Drug Names (based on prototype)
FMT_DRUGS = [
    "AMOXICILLIN", "ZYRTEC", "LIPITOR", "CRESTOR", "HUMALOG", "NOVOLOG", 
    "ADVIL", "MOTRIN", "ALLEREASE", "SYNTHROID", "METFORMIN", "XARELTO",
    "ELIQUIS", "KEYTRUDA", "OZEMPIC", "JARDIANCE", "TRULICITY", "ENBREL",
    "HUMIRA", "LANTUS", "NEXIUM", "ADVAIR"
]

# MBID definitions (Master Business Identifier)
MBIDS = [
    {"mbid": "AK123456", "description": "Antihistamines", "tenant": "MASTER", "begin_date": "2024-01-01"},
    {"mbid": "CV999001", "description": "Cardio - Statins", "tenant": "MASTER", "begin_date": "2024-01-01"},
    {"mbid": "IMM000777", "description": "Immunology - Insulins", "tenant": "MASTER", "begin_date": "2024-01-01"},
    {"mbid": "PAIN12345", "description": "Pain Management - NSAIDs", "tenant": "MASTER", "begin_date": "2024-01-01"},
    {"mbid": "ENDO55555", "description": "Endocrinology - Diabetes", "tenant": "MASTER", "begin_date": "2024-01-01"},
    {"mbid": "ONCO77777", "description": "Oncology - Immunotherapy", "tenant": "MASTER", "begin_date": "2024-01-01"},
    # Child tenant MBIDs
    {"mbid": "AK123456_a", "description": "AK Antihistamines - sub", "tenant": "AK", "begin_date": "2024-02-01"},
    {"mbid": "MO888888", "description": "MO Specific Formulary", "tenant": "MO", "begin_date": "2024-02-15"},
]

# FMT Statuses
FMT_STATUSES = ["PDL", "Approved", "Review", "Restricted"]

def generate_date(start_days_ago=365, end_days_ago=0):
    """Generate random date within range"""
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    random_date = start + (end - start) * random.random()
    return random_date.strftime('%Y-%m-%d')

def load_fdb_ndcs():
    """Load NDCs from existing FDB data"""
    if not FDB_DATA_PATH.exists():
        print(f"WARNING: FDB data not found at {FDB_DATA_PATH}")
        print("Please run generate_fdb_data.py first to create FDB data")
        return []
    
    df = pd.read_csv(FDB_DATA_PATH)
    ndcs = df['ndc'].tolist()
    print(f"Loaded {len(ndcs)} NDCs from FDB data")
    return ndcs

def map_drug_to_mbid(fmt_drug):
    """Map FMT drug name to appropriate MBID"""
    drug_upper = fmt_drug.upper()
    
    if drug_upper in ['ZYRTEC', 'ALLEREASE']:
        return random.choice(['AK123456', 'AK123456_a'])
    elif drug_upper in ['LIPITOR', 'CRESTOR']:
        return 'CV999001'
    elif drug_upper in ['HUMALOG', 'NOVOLOG', 'LANTUS', 'OZEMPIC', 'JARDIANCE', 'TRULICITY']:
        return random.choice(['IMM000777', 'ENDO55555'])
    elif drug_upper in ['ADVIL', 'MOTRIN']:
        return 'PAIN12345'
    elif drug_upper in ['KEYTRUDA']:
        return 'ONCO77777'
    elif drug_upper in ['METFORMIN', 'SYNTHROID']:
        return 'ENDO55555'
    else:
        return random.choice(['AK123456', 'CV999001', 'IMM000777', 'PAIN12345'])

def generate_fmt_master_data(fdb_ndcs, num_records=200):
    """Generate FMT master data using NDCs from FDB"""
    print(f"Generating {num_records} FMT master records...")
    
    if not fdb_ndcs:
        print("No FDB NDCs available - creating placeholder data")
        fdb_ndcs = [f"{random.randint(1000000000, 9999999999)}" for _ in range(100)]
    
    # Select random subset of FDB NDCs for FMT
    selected_ndcs = random.sample(fdb_ndcs, min(num_records, len(fdb_ndcs)))
    
    records = []
    for i, ndc in enumerate(selected_ndcs):
        fmt_drug = random.choice(FMT_DRUGS)
        mbid = map_drug_to_mbid(fmt_drug)
        status = random.choice(FMT_STATUSES)
        
        # Generate date ranges
        start_date = generate_date(180, 30)
        # 70% chance of open-ended (no end date)
        end_date = "" if random.random() < 0.7 else generate_date(30, 0)
        
        record = {
            'ndc': ndc,
            'fmt_drug': fmt_drug,
            'mbid': mbid,
            'status': status,
            'start_date': start_date,
            'end_date': end_date,
            'load_date': generate_date(7, 0),
            'effective_date': start_date,
            'expiration_date': end_date,
            'created_by': random.choice(['admin', 'pharmacist', 'reviewer', 'manager']),
            'updated_by': random.choice(['admin', 'pharmacist', 'reviewer', 'manager']),
            'review_status': random.choice(['Pending', 'Approved', 'Rejected', '']),
            'notes': random.choice(['', 'Special handling required', 'Prior auth needed', 'Formulary exception', 'Tier adjustment'])
        }
        records.append(record)
    
    return records

def generate_mbid_data():
    """Generate MBID reference data"""
    print(f"Generating {len(MBIDS)} MBID records...")
    
    records = []
    for mbid_info in MBIDS:
        record = {
            'mbid': mbid_info['mbid'],
            'description': mbid_info['description'],
            'tenant': mbid_info['tenant'],
            'begin_date': mbid_info['begin_date'],
            'end_date': '',  # Most MBIDs are open-ended
            'created_by': 'system',
            'status': 'Active'
        }
        records.append(record)
    
    return records

def save_csv(filename, records, fieldnames):
    """Save records to CSV"""
    filepath = OUTPUT_DIR / filename
    print(f"Saving {filepath} with {len(records)} records")
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

def main():
    print("=== Generating FMT Data with FDB NDC Integration ===\n")
    
    # Load NDCs from existing FDB data
    fdb_ndcs = load_fdb_ndcs()
    
    # Generate FMT Master data
    fmt_records = generate_fmt_master_data(fdb_ndcs, 200)
    fmt_fieldnames = [
        'ndc', 'fmt_drug', 'mbid', 'status', 'start_date', 'end_date', 'load_date',
        'effective_date', 'expiration_date', 'created_by', 'updated_by', 'review_status', 'notes'
    ]
    save_csv('fmt_master.csv', fmt_records, fmt_fieldnames)
    
    # Generate MBID reference data
    mbid_records = generate_mbid_data()
    mbid_fieldnames = ['mbid', 'description', 'tenant', 'begin_date', 'end_date', 'created_by', 'status']
    save_csv('fmt_mbids.csv', mbid_records, mbid_fieldnames)
    
    print(f"\n=== VERIFICATION ===")
    print(f"FMT Master records: {len(fmt_records)}")
    print(f"MBID records: {len(mbid_records)}")
    
    if fdb_ndcs:
        # Verify NDC overlap with FDB
        fmt_ndcs = {r['ndc'] for r in fmt_records}
        fdb_ndc_set = set(fdb_ndcs)
        overlap = fmt_ndcs.intersection(fdb_ndc_set)
        print(f"NDC overlap with FDB: {len(overlap)}/{len(fmt_ndcs)} ({len(overlap)/len(fmt_ndcs)*100:.0f}%)")
    
    # Show status distribution
    status_counts = {}
    for record in fmt_records:
        status = record['status']
        status_counts[status] = status_counts.get(status, 0) + 1
    print(f"Status distribution: {status_counts}")
    
    # Show MBID usage
    mbid_counts = {}
    for record in fmt_records:
        mbid = record['mbid']
        mbid_counts[mbid] = mbid_counts.get(mbid, 0) + 1
    print(f"MBID usage: {dict(list(mbid_counts.items())[:5])}...")
    
    print(f"\n✅ FMT data generation complete!")
    print(f"Files created in {OUTPUT_DIR}/:")
    print(f"  - fmt_master.csv (main FMT data)")
    print(f"  - fmt_mbids.csv (MBID reference)")

if __name__ == "__main__":
    main()

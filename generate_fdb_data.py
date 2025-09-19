#!/usr/bin/env python3
"""
FDB Sample Data Generator - FIXED VERSION
Creates datasets with matching NDCs that can be properly joined
"""
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

# Output directory
OUTPUT_DIR = Path("sample_fdb_data")
OUTPUT_DIR.mkdir(exist_ok=True)

# Core drug information
DRUGS = [
    {"brand": "Lipitor", "generic": "atorvastatin", "mfr": "Pfizer", "hic3": "002", "hicl": "HMG-CoA Reductase Inhibitors", "dcc": "CARDIO"},
    {"brand": "Humira", "generic": "adalimumab", "mfr": "AbbVie", "hic3": "011", "hicl": "TNF Blockers", "dcc": "IMMUNO"},
    {"brand": "Advair", "generic": "fluticasone-salmeterol", "mfr": "GlaxoSmithKline", "hic3": "080", "hicl": "Corticosteroids", "dcc": "RESPIR"},
    {"brand": "Nexium", "generic": "esomeprazole", "mfr": "AstraZeneca", "hic3": "030", "hicl": "Proton Pump Inhibitors", "dcc": "GI"},
    {"brand": "Crestor", "generic": "rosuvastatin", "mfr": "AstraZeneca", "hic3": "002", "hicl": "HMG-CoA Reductase Inhibitors", "dcc": "CARDIO"},
    {"brand": "Lantus", "generic": "insulin glargine", "mfr": "Sanofi", "hic3": "080", "hicl": "Insulin", "dcc": "ENDO"},
    {"brand": "Enbrel", "generic": "etanercept", "mfr": "Amgen", "hic3": "025", "hicl": "TNF Blockers", "dcc": "IMMUNO"},
    {"brand": "Metformin", "generic": "metformin", "mfr": "Teva", "hic3": "040", "hicl": "Biguanides", "dcc": "ENDO"},
    {"brand": "Synthroid", "generic": "levothyroxine", "mfr": "AbbVie", "hic3": "060", "hicl": "Thyroid Hormones", "dcc": "ENDO"},
    {"brand": "Xarelto", "generic": "rivaroxaban", "mfr": "Bayer", "hic3": "015", "hicl": "Anticoagulants", "dcc": "CARDIO"},
    {"brand": "Eliquis", "generic": "apixaban", "mfr": "Bristol-Myers Squibb", "hic3": "015", "hicl": "Anticoagulants", "dcc": "CARDIO"},
    {"brand": "Ozempic", "generic": "semaglutide", "mfr": "Novo Nordisk", "hic3": "041", "hicl": "GLP-1 Agonists", "dcc": "ENDO"},
    {"brand": "Jardiance", "generic": "empagliflozin", "mfr": "Boehringer Ingelheim", "hic3": "042", "hicl": "SGLT2 Inhibitors", "dcc": "ENDO"},
    {"brand": "Keytruda", "generic": "pembrolizumab", "mfr": "Merck", "hic3": "090", "hicl": "Immunotherapy", "dcc": "ONCO"},
    {"brand": "Trulicity", "generic": "dulaglutide", "mfr": "Eli Lilly", "hic3": "041", "hicl": "GLP-1 Agonists", "dcc": "ENDO"},
]

# Generate a FIXED list of NDCs that will be used across ALL tables
def generate_master_ndc_list(count=500):
    """Generate a master list of NDCs to be used consistently"""
    print(f"Generating master list of {count} NDCs...")
    ndcs = []
    for i in range(count):
        # Generate realistic-looking 10-digit NDCs
        ndc = f"{random.randint(1000, 9999)}{random.randint(100000, 999999)}"
        ndcs.append(ndc)
    return ndcs

def generate_date(start_days_ago=365, end_days_ago=0):
    """Generate random date within range"""
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    random_date = start + (end - start) * random.random()
    return random_date.strftime('%Y-%m-%d')

def create_core_data(master_ndcs):
    """Create core FDB data using the master NDC list"""
    print(f"Creating core data with {len(master_ndcs)} records...")
    
    records = []
    for i, ndc in enumerate(master_ndcs):
        drug = random.choice(DRUGS)
        
        record = {
            'ndc': ndc,  # Use the master NDC
            'gsn': 10000 + i,
            'brand': drug['brand'],
            'generic': drug['generic'],
            'rx_otc': random.choice(['RX', 'OTC']),
            'pkg_size': random.choice(['30', '60', '90', '120', '1ml', '5ml']),
            'hic3': drug['hic3'],
            'hicl': drug['hicl'],
            'dcc': drug['dcc'],
            'mfr': drug['mfr'],
            'obsolete': random.choice([True, False]),
            'rebate': random.choice([True, False]),
            'load_date': generate_date(180, 0),
            'pkg_origin': random.choice(['US', 'CA', 'DE', 'FR', 'UK']),
            'gsn_desc': drug['hicl'],
            'pkg_form': random.choice(['Tablet', 'Capsule', 'Injection', 'Cream', 'Solution'])
        }
        records.append(record)
    
    return records

def create_formulary_data(master_ndcs, tenant, num_records=150):
    """Create formulary data using NDCs from the master list"""
    print(f"Creating formulary data for {tenant} with {num_records} records...")
    
    # Select a random subset of NDCs from the master list
    selected_ndcs = random.sample(master_ndcs, min(num_records, len(master_ndcs)))
    
    # Different formulary preferences by tenant
    if tenant == "master":
        status_choices = ["Preferred", "Non-Preferred", "Prior Auth", "Not Covered"]
        status_weights = [0.3, 0.3, 0.25, 0.15]
        tier_weights = [0.2, 0.3, 0.3, 0.2]
    elif tenant == "ak":
        status_choices = ["Preferred", "Non-Preferred", "Prior Auth", "Not Covered"]
        status_weights = [0.4, 0.35, 0.15, 0.1]  # More generous
        tier_weights = [0.3, 0.4, 0.2, 0.1]
    elif tenant == "mo":
        status_choices = ["Preferred", "Non-Preferred", "Prior Auth", "Not Covered"]
        status_weights = [0.35, 0.3, 0.2, 0.15]  # Balanced
        tier_weights = [0.25, 0.35, 0.25, 0.15]
    else:
        status_choices = ["Preferred", "Non-Preferred", "Prior Auth", "Not Covered"]
        status_weights = [0.25, 0.25, 0.25, 0.25]
        tier_weights = [0.25, 0.25, 0.25, 0.25]
    
    records = []
    for ndc in selected_ndcs:
        record = {
            'ndc': ndc,  # Use the exact same NDC from master list
            'formulary_status': random.choices(status_choices, weights=status_weights)[0],
            'tier': random.choices([1, 2, 3, 4], weights=tier_weights)[0],
            'pa_required': random.choice([True, False]),
            'ql_limits': random.choice(['30/30 days', '60/30 days', '90/30 days', '1/day', '']),
            'effective_date': generate_date(365, 0)
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
    print("=== Generating FDB Data with Matching NDCs ===\n")
    
    # STEP 1: Generate master list of NDCs (used by ALL tables)
    master_ndcs = generate_master_ndc_list(500)
    
    # STEP 2: Create core data using master NDCs
    core_records = create_core_data(master_ndcs)
    core_fieldnames = [
        'ndc', 'gsn', 'brand', 'generic', 'rx_otc', 'pkg_size', 'hic3', 'hicl', 'dcc', 'mfr',
        'obsolete', 'rebate', 'load_date', 'pkg_origin', 'gsn_desc', 'pkg_form'
    ]
    save_csv('fdb_core_drugs.csv', core_records, core_fieldnames)
    
    # STEP 3: Create formulary data for each tenant using master NDCs
    formulary_fieldnames = ['ndc', 'formulary_status', 'tier', 'pa_required', 'ql_limits', 'effective_date']
    
    tenants = ['master', 'ak', 'mo']
    for tenant in tenants:
        formulary_records = create_formulary_data(master_ndcs, tenant, 150)
        save_csv(f'fdb_formulary_{tenant}.csv', formulary_records, formulary_fieldnames)
    
    print(f"\n=== VERIFICATION ===")
    print(f"Master NDCs generated: {len(master_ndcs)}")
    
    # Verify joins will work
    core_ndcs = set(record['ndc'] for record in core_records)
    print(f"Core data NDCs: {len(core_ndcs)}")
    
    for tenant in tenants:
        filepath = OUTPUT_DIR / f'fdb_formulary_{tenant}.csv'
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            formulary_ndcs = {row['ndc'] for row in reader}
            overlap = core_ndcs.intersection(formulary_ndcs)
            print(f"{tenant.upper()} formulary: {len(formulary_ndcs)} NDCs, {len(overlap)} match core ({len(overlap)/len(formulary_ndcs)*100:.0f}% join success)")
    
    print(f"\n✅ Data generation complete! All datasets can now be properly joined.")

if __name__ == "__main__":
    main()
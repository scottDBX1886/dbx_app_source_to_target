#!/usr/bin/env python3
"""
Generate sample PDL data for database upload.
Creates tenant-specific keycodes that overlap with FDB NDCs for proper joins.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
random.seed(42)
np.random.seed(42)

def load_fdb_ndcs():
    """Load NDCs from existing FDB data to ensure joinability."""
    try:
        fdb_core_path = os.path.join("sample_fdb_data", "fdb_core_drugs.csv")
        if os.path.exists(fdb_core_path):
            fdb_df = pd.read_csv(fdb_core_path)
            return fdb_df['ndc'].unique().tolist()
        else:
            print(f"Warning: FDB data not found at {fdb_core_path}")
            # Return some default NDCs if FDB data not available
            return [
                2099767005, 6399802123, 7781544230, 6839045633, 5473301234,
                5546702543, 1680902345, 4387600123, 6836702321, 4976300987
            ]
    except Exception as e:
        print(f"Error loading FDB NDCs: {e}")
        return []

def generate_pdl_master():
    """Generate PDL Master data."""
    
    # Load FDB NDCs to ensure joinability
    fdb_ndcs = load_fdb_ndcs()
    
    # Use a subset of FDB NDCs for PDL (about 70% overlap)
    num_pdl_records = min(500, int(len(fdb_ndcs) * 0.7))
    selected_ndcs = random.sample(fdb_ndcs, num_pdl_records)
    
    pdl_drugs = [
        'LISINOPRIL', 'METFORMIN', 'ATORVASTATIN', 'AMLODIPINE', 'METOPROLOL',
        'OMEPRAZOLE', 'SIMVASTATIN', 'LOSARTAN', 'GABAPENTIN', 'HYDROCHLOROTHIAZIDE',
        'LEVOTHYROXINE', 'ALBUTEROL', 'FUROSEMIDE', 'PREDNISONE', 'TRAMADOL',
        'IBUPROFEN', 'ACETAMINOPHEN', 'ASPIRIN', 'WARFARIN', 'CLOPIDOGREL',
        'SERTRALINE', 'FLUOXETINE', 'CITALOPRAM', 'DULOXETINE', 'VENLAFAXINE',
        'LORAZEPAM', 'ALPRAZOLAM', 'ZOLPIDEM', 'TRAZODONE', 'QUETIAPINE',
        'RISPERIDONE', 'OLANZAPINE', 'ARIPIPRAZOLE', 'LITHIUM', 'CARBAMAZEPINE'
    ]
    
    statuses = ['Active', 'Inactive', 'Under Review', 'Pending']
    manufacturers = [
        'Generic Pharmaceuticals', 'Teva', 'Sandoz', 'Mylan', 'Actavis',
        'Pfizer', 'Novartis', 'Roche', 'GSK', 'Merck', 'Bristol-Myers',
        'AbbVie', 'Johnson & Johnson', 'Amgen', 'Gilead'
    ]
    
    records = []
    base_date = datetime.now() - timedelta(days=365)
    
    for i, ndc in enumerate(selected_ndcs):
        
        # Generate dates
        load_date = base_date + timedelta(days=random.randint(0, 300))
        effective_date = load_date - timedelta(days=random.randint(0, 30))
        
        # Some records have expiration dates
        has_expiration = random.random() < 0.3
        expiration_date = effective_date + timedelta(days=random.randint(365, 730)) if has_expiration else None
        
        record = {
            'ndc': ndc,
            'pdl_drug': random.choice(pdl_drugs),
            'manufacturer': random.choice(manufacturers),
            'dosage_form': random.choice(['Tablet', 'Capsule', 'Solution', 'Injection', 'Cream', 'Suspension']),
            'strength': f"{random.randint(1, 100)} {random.choice(['mg', 'mcg', 'mL', 'g', 'units'])}",
            'package_size': f"{random.randint(30, 500)} {random.choice(['tablets', 'capsules', 'mL', 'units'])}",
            'status': random.choice(statuses),
            'load_date': load_date.strftime('%Y-%m-%d'),
            'effective_date': effective_date.strftime('%Y-%m-%d'),
            'expiration_date': expiration_date.strftime('%Y-%m-%d') if expiration_date else None,
            'created_by': random.choice(['system_admin', 'data_loader', 'pdl_manager', 'clinical_reviewer']),
            'updated_by': random.choice(['system_admin', 'data_loader', 'pdl_manager', 'clinical_reviewer']),
            'review_status': random.choice(['Approved', 'Pending', 'Under Review', 'Rejected']),
            'notes': f"PDL baseline entry - {random.choice(['standard coverage', 'tier 1 preferred', 'tier 2 non-preferred', 'specialty drug', 'prior auth required'])}"
        }
        
        records.append(record)
    
    return pd.DataFrame(records)

def generate_pdl_keycodes():
    """Generate PDL Keycodes for different tenants."""
    
    # Load FDB NDCs and PDL master data
    fdb_ndcs = load_fdb_ndcs()
    
    # Use subset of NDCs that would be in PDL master
    num_pdl_records = min(500, int(len(fdb_ndcs) * 0.7))
    pdl_ndcs = random.sample(fdb_ndcs, num_pdl_records)
    
    tenants = ['master', 'ak', 'mo']
    templates = [
        'FORMULARY_TIER_1', 'FORMULARY_TIER_2', 'FORMULARY_TIER_3',
        'NON_FORMULARY', 'PRIOR_AUTH_REQUIRED', 'STEP_THERAPY',
        'QUANTITY_LIMITS', 'SPECIALTY_DRUG', 'GENERIC_REQUIRED',
        'BRAND_PREFERRED', 'THERAPEUTIC_INTERCHANGE', 'COVERAGE_GAP'
    ]
    
    keycodes = [
        'PA', 'ST', 'QL', 'GPI', 'BvG', 'TI', 'SP', 'NF', 'T1', 'T2', 'T3',
        'CVR', 'EXC', 'LIM', 'MAN', 'OTC', 'RES', 'SUB', 'UNL', 'VAC'
    ]
    
    statuses = ['Active', 'Inactive', 'Pending', 'Under Review']
    
    records = []
    base_date = datetime.now() - timedelta(days=365)
    
    for tenant in tenants:
        # Different tenants have different coverage patterns
        if tenant == 'master':
            # Master has baseline coverage for most drugs
            coverage_rate = 0.85
            preferred_templates = ['FORMULARY_TIER_1', 'FORMULARY_TIER_2', 'GENERIC_REQUIRED']
        elif tenant == 'ak':
            # Alaska has more restrictive coverage
            coverage_rate = 0.65
            preferred_templates = ['FORMULARY_TIER_2', 'PRIOR_AUTH_REQUIRED', 'STEP_THERAPY']
        else:  # mo
            # Missouri has moderate coverage
            coverage_rate = 0.75
            preferred_templates = ['FORMULARY_TIER_1', 'FORMULARY_TIER_3', 'QUANTITY_LIMITS']
        
        # Select NDCs for this tenant
        num_covered = int(len(pdl_ndcs) * coverage_rate)
        tenant_ndcs = random.sample(pdl_ndcs, num_covered)
        
        for ndc in tenant_ndcs:
            # Each NDC might have multiple keycodes
            num_keycodes = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1], k=1)[0]
            
            for _ in range(num_keycodes):
                effective_date = base_date + timedelta(days=random.randint(0, 300))
                
                record = {
                    'ndc': ndc,
                    'keycode': random.choice(keycodes),
                    'template': random.choice(preferred_templates),
                    'tenant': tenant,
                    'status': random.choice(statuses),
                    'effective_date': effective_date.strftime('%Y-%m-%d'),
                    'created_by': f"{tenant}_admin",
                    'notes': f"Tenant-specific keycode assignment for {tenant.upper()}"
                }
                
                records.append(record)
    
    return pd.DataFrame(records)

def main():
    """Generate and save PDL sample data."""
    
    print("🔸 Generating PDL Sample Data 🔸")
    
    # Create output directory
    os.makedirs("sample_pdl_data", exist_ok=True)
    
    # Generate PDL Master data
    print("Generating PDL Master data...")
    pdl_master_df = generate_pdl_master()
    pdl_master_path = os.path.join("sample_pdl_data", "pdl_master.csv")
    pdl_master_df.to_csv(pdl_master_path, index=False)
    print(f"✅ PDL Master: {len(pdl_master_df)} records saved to {pdl_master_path}")
    
    # Generate PDL Keycodes data
    print("Generating PDL Keycodes data...")
    pdl_keycodes_df = generate_pdl_keycodes()
    pdl_keycodes_path = os.path.join("sample_pdl_data", "pdl_keycodes.csv")
    pdl_keycodes_df.to_csv(pdl_keycodes_path, index=False)
    print(f"✅ PDL Keycodes: {len(pdl_keycodes_df)} records saved to {pdl_keycodes_path}")
    
    # Show sample data
    print("\n📊 Sample PDL Master Data:")
    print(pdl_master_df.head(3).to_string(index=False))
    
    print("\n📊 Sample PDL Keycodes Data:")
    print(pdl_keycodes_df.head(5).to_string(index=False))
    
    # Show tenant distribution
    print("\n📈 Keycode Distribution by Tenant:")
    tenant_counts = pdl_keycodes_df['tenant'].value_counts()
    for tenant, count in tenant_counts.items():
        print(f"  {tenant.upper()}: {count} keycodes")
    
    # Show template usage
    print("\n📈 Template Usage:")
    template_counts = pdl_keycodes_df['template'].value_counts().head(5)
    for template, count in template_counts.items():
        print(f"  {template}: {count}")
    
    # Verify joinability with FDB
    fdb_ndcs = set(load_fdb_ndcs())
    pdl_ndcs = set(pdl_master_df['ndc'].unique())
    join_overlap = len(pdl_ndcs & fdb_ndcs)
    print(f"\n🔗 NDC Join Analysis:")
    print(f"  FDB NDCs: {len(fdb_ndcs)}")
    print(f"  PDL NDCs: {len(pdl_ndcs)}")
    print(f"  Overlap: {join_overlap} ({join_overlap/len(pdl_ndcs)*100:.1f}% of PDL)")
    
    print(f"\n🎉 PDL sample data generation complete!")
    print(f"📁 Files ready for database upload in: sample_pdl_data/")

if __name__ == "__main__":
    main()

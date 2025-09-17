#!/usr/bin/env python3
"""
FDB Sample Data Generator
Creates 20+ realistic pharmaceutical data files for different tenants
"""
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
import uuid

# Output directory
OUTPUT_DIR = Path("sample_fdb_data")
OUTPUT_DIR.mkdir(exist_ok=True)

# Realistic pharmaceutical data
BRAND_NAMES = [
    "Lipitor", "Humira", "Advair", "Nexium", "Crestor", "Lantus", "Rituxan", "Enbrel", 
    "Copaxone", "Neulasta", "Lucentis", "Avastin", "Herceptin", "Remicade", "Synagis",
    "Gleevec", "Taxotere", "Procrit", "Epogen", "Aranesp", "Zyprexa", "Seroquel",
    "Risperdal", "Abilify", "Geodon", "Xanax", "Ativan", "Valium", "Ambien", "Lunesta",
    "Viagra", "Cialis", "Levitra", "Flomax", "Avodart", "Proscar", "Cardura", "Hytrin",
    "Prinivil", "Zestril", "Vasotec", "Capoten", "Altace", "Mavik", "Univasc", "Accupril",
    "Norvasc", "Procardia", "Cardizem", "Verapamil", "Diltiazem", "Adalat", "DynaCirc",
    "Zocor", "Pravachol", "Lescol", "Mevacor", "Vytorin", "Zetia", "WelChol", "Questran",
    "Glucophage", "Amaryl", "Glucotrol", "Diabeta", "Micronase", "Prandin", "Starlix",
    "Actos", "Avandia", "Januvia", "Onglyza", "Tradjenta", "Victoza", "Byetta", "Symlin"
]

GENERIC_NAMES = [
    "atorvastatin", "adalimumab", "fluticasone-salmeterol", "esomeprazole", "rosuvastatin",
    "insulin glargine", "rituximab", "etanercept", "glatiramer", "pegfilgrastim",
    "ranibizumab", "bevacizumab", "trastuzumab", "infliximab", "palivizumab",
    "imatinib", "docetaxel", "epoetin alfa", "epoetin alfa", "darbepoetin alfa",
    "olanzapine", "quetiapine", "risperidone", "aripiprazole", "ziprasidone",
    "alprazolam", "lorazepam", "diazepam", "zolpidem", "eszopiclone",
    "sildenafil", "tadalafil", "vardenafil", "tamsulosin", "dutasteride",
    "finasteride", "doxazosin", "terazosin", "lisinopril", "lisinopril",
    "enalapril", "captopril", "ramipril", "trandolapril", "moexipril", "quinapril",
    "amlodipine", "nifedipine", "diltiazem", "verapamil", "diltiazem", "nifedipine",
    "simvastatin", "pravastatin", "fluvastatin", "lovastatin", "ezetimibe-simvastatin",
    "ezetimibe", "colesevelam", "cholestyramine", "metformin", "glimepiride",
    "glipizide", "glyburide", "glyburide", "repaglinide", "nateglinide",
    "pioglitazone", "rosiglitazone", "sitagliptin", "saxagliptin", "linagliptin",
    "liraglutide", "exenatide", "pramlintide"
]

MANUFACTURERS = [
    "Pfizer", "AbbVie", "GlaxoSmithKline", "AstraZeneca", "Crestor Inc", "Sanofi",
    "Genentech", "Amgen", "Teva", "Biogen", "Lucentis Pharma", "Genentech",
    "Roche", "Johnson & Johnson", "MedImmune", "Novartis", "Sanofi", "Amgen",
    "Janssen", "Eli Lilly", "AstraZeneca", "Janssen", "Otsuka", "Pfizer",
    "Pfizer", "Pfizer", "Roche", "Sanofi", "Lunesta Inc", "Pfizer",
    "Eli Lilly", "Bayer", "Boehringer Ingelheim", "GSK", "Merck", "Abbott",
    "Pfizer", "Pfizer", "Bristol-Myers Squibb", "Bristol-Myers Squibb",
    "Bristol-Myers Squibb", "Bristol-Myers Squibb", "Aventis", "Abbott", "Abbott",
    "Pfizer", "Pfizer", "Forest", "Verapamil Inc", "Forest", "Pfizer", "Sandoz",
    "Merck", "Bristol-Myers Squibb", "Novartis", "Merck", "Merck", "Merck",
    "Daiichi Sankyo", "WelChol Inc", "Bristol-Myers Squibb", "Takeda", "GSK",
    "Merck", "Takeda", "Boehringer Ingelheim", "Novo Nordisk", "AstraZeneca", "Symlin Inc"
]

DRUG_CLASSES = [
    "HMG-CoA Reductase Inhibitors", "TNF Blockers", "Corticosteroids", "Proton Pump Inhibitors",
    "HMG-CoA Reductase Inhibitors", "Insulin", "Monoclonal Antibodies", "TNF Blockers",
    "Multiple Sclerosis Agents", "Colony Stimulating Factors", "VEGF Inhibitors",
    "Monoclonal Antibodies", "HER2 Receptor Antagonists", "TNF Blockers", "Monoclonal Antibodies",
    "Tyrosine Kinase Inhibitors", "Taxanes", "Erythropoiesis Stimulating Agents", 
    "Erythropoiesis Stimulating Agents", "Erythropoiesis Stimulating Agents",
    "Atypical Antipsychotics", "Atypical Antipsychotics", "Atypical Antipsychotics",
    "Atypical Antipsychotics", "Atypical Antipsychotics", "Benzodiazepines",
    "Benzodiazepines", "Benzodiazepines", "Hypnotics", "Hypnotics",
    "PDE5 Inhibitors", "PDE5 Inhibitors", "PDE5 Inhibitors", "Alpha Blockers", "5-Alpha Reductase Inhibitors"
]

HIC3_CODES = ["001", "002", "003", "010", "011", "020", "025", "030", "040", "050", "060", "070", "080", "090", "100"]
DCC_CODES = ["CV", "ENDO", "NEURO", "GI", "RESP", "ONCO", "RHEU", "URO", "PSYCH", "INFECT", "DERM", "OPHTH"]

def generate_ndc():
    """Generate realistic NDC-11 format"""
    labeler = f"{random.randint(10000, 99999):05d}"
    product = f"{random.randint(100, 999):03d}"
    package = f"{random.randint(10, 99):02d}"
    return f"{labeler}{product}{package}"

def generate_dates():
    """Generate realistic load dates over past year"""
    base_date = datetime.now() - timedelta(days=365)
    return base_date + timedelta(days=random.randint(0, 365))

def create_core_fdb_file():
    """Core FDB drug information"""
    filename = OUTPUT_DIR / "fdb_core_drugs.csv"
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'ndc', 'gsn', 'brand', 'generic', 'rx_otc', 'pkg_size', 
            'hic3', 'hicl', 'dcc', 'mfr', 'obsolete', 'rebate',
            'load_date', 'pkg_origin', 'gsn_desc', 'pkg_form'
        ])
        
        for i in range(500):
            brand_idx = i % len(BRAND_NAMES)
            drug_class = DRUG_CLASSES[brand_idx % len(DRUG_CLASSES)]
            writer.writerow([
                generate_ndc(),
                10000 + i,
                BRAND_NAMES[brand_idx],
                GENERIC_NAMES[brand_idx % len(GENERIC_NAMES)],
                random.choice(['RX', 'OTC']),
                random.choice(['30', '60', '90', '100', '120', '5ml', '10ml', '1']),
                random.choice(HIC3_CODES),
                drug_class,
                random.choice(DCC_CODES),
                MANUFACTURERS[brand_idx % len(MANUFACTURERS)],
                random.choice([True, False]),
                random.choice([True, False]),
                generate_dates().strftime('%Y-%m-%d'),
                random.choice(['US', 'CA', 'DE', 'UK', 'FR']),
                drug_class,
                random.choice(['Tablet', 'Capsule', 'Injection', 'Cream', 'Solution', 'Suspension'])
            ])

def create_pricing_file():
    """FDB pricing information"""
    filename = OUTPUT_DIR / "fdb_pricing.csv"
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['ndc', 'awp', 'wac', 'nadac', 'federal_rebate', 'state_rebate', 'effective_date'])
        
        for i in range(300):
            awp = round(random.uniform(10.50, 2500.00), 2)
            wac = round(awp * 0.85, 2)  # WAC typically ~85% of AWP
            nadac = round(awp * 0.40, 2)  # NADAC typically much lower
            
            writer.writerow([
                generate_ndc(),
                awp,
                wac, 
                nadac,
                round(random.uniform(0.10, 0.23), 4),  # 10-23% federal rebate
                round(random.uniform(0.05, 0.15), 4),  # 5-15% state rebate
                generate_dates().strftime('%Y-%m-%d')
            ])

def create_tenant_specific_files():
    """Create tenant-specific FDB files"""
    tenants = ['MASTER', 'AK', 'MO']
    
    for tenant in tenants:
        # Formulary file
        filename = OUTPUT_DIR / f"fdb_formulary_{tenant.lower()}.csv"
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['ndc', 'formulary_status', 'tier', 'pa_required', 'ql_limits', 'effective_date'])
            
            for i in range(150):
                writer.writerow([
                    generate_ndc(),
                    random.choice(['Preferred', 'Non-Preferred', 'Not Covered', 'Prior Auth']),
                    random.choice([1, 2, 3, 4]),
                    random.choice([True, False]),
                    random.choice(['30/30 days', '60/30 days', '90/30 days', None, '1/day']),
                    generate_dates().strftime('%Y-%m-%d')
                ])
        
        # Regional preferences
        filename = OUTPUT_DIR / f"fdb_regional_{tenant.lower()}.csv"
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['ndc', 'regional_code', 'preference_score', 'local_mfr', 'distribution_notes'])
            
            regional_preferences = {
                'MASTER': ['US-NATIONAL'],
                'AK': ['US-AK', 'US-NORTHWEST'], 
                'MO': ['US-MO', 'US-MIDWEST']
            }
            
            for i in range(100):
                writer.writerow([
                    generate_ndc(),
                    random.choice(regional_preferences[tenant]),
                    random.randint(1, 10),
                    f"{tenant} Pharmaceuticals" if tenant != 'MASTER' else random.choice(MANUFACTURERS[:10]),
                    f"Distributed in {tenant} region" if tenant != 'MASTER' else "National distribution"
                ])

def create_therapeutic_class_files():
    """Create files organized by therapeutic class"""
    classes = ['cardiovascular', 'diabetes', 'oncology', 'neurology', 'respiratory']
    
    for drug_class in classes:
        filename = OUTPUT_DIR / f"fdb_therapeutic_{drug_class}.csv"
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['ndc', 'therapeutic_class', 'subclass', 'mechanism', 'contraindications'])
            
            for i in range(80):
                mechanisms = {
                    'cardiovascular': ['ACE Inhibitor', 'Beta Blocker', 'Calcium Channel Blocker', 'ARB', 'Diuretic'],
                    'diabetes': ['Metformin', 'Insulin', 'GLP-1 Agonist', 'DPP-4 Inhibitor', 'SGLT2 Inhibitor'],
                    'oncology': ['Chemotherapy', 'Targeted Therapy', 'Immunotherapy', 'Hormone Therapy'],
                    'neurology': ['Anticonvulsant', 'Dopamine Agonist', 'Cholinesterase Inhibitor'],
                    'respiratory': ['Beta Agonist', 'Corticosteroid', 'Anticholinergic', 'Leukotriene Modifier']
                }
                
                writer.writerow([
                    generate_ndc(),
                    drug_class.title(),
                    f"{drug_class.title()} Subclass {random.randint(1,5)}",
                    random.choice(mechanisms.get(drug_class, ['Unknown'])),
                    f"Standard {drug_class} contraindications apply"
                ])

def create_manufacturer_files():
    """Create manufacturer-specific files"""
    major_mfrs = ['Pfizer', 'Johnson & Johnson', 'Merck', 'AbbVie', 'Bristol-Myers Squibb']
    
    for mfr in major_mfrs:
        filename = OUTPUT_DIR / f"fdb_manufacturer_{mfr.replace(' ', '_').replace('&', 'and').lower()}.csv"
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['ndc', 'manufacturer', 'manufacturing_site', 'lot_info', 'expiry_tracking'])
            
            for i in range(60):
                writer.writerow([
                    generate_ndc(),
                    mfr,
                    f"{mfr} {random.choice(['Plant A', 'Plant B', 'Facility 1', 'Facility 2'])}",
                    f"LOT{random.randint(100000, 999999)}",
                    f"{random.randint(12, 36)} months"
                ])

def create_update_history_files():
    """Create files showing historical updates"""
    months = ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06']
    
    for month in months:
        filename = OUTPUT_DIR / f"fdb_updates_{month}.csv"
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['ndc', 'change_type', 'old_value', 'new_value', 'change_date', 'reason'])
            
            change_types = ['Price Update', 'Formulary Change', 'Manufacturer Change', 'Discontinuation', 'New Product']
            
            for i in range(40):
                change_type = random.choice(change_types)
                writer.writerow([
                    generate_ndc(),
                    change_type,
                    f"Old {change_type} value",
                    f"New {change_type} value", 
                    f"{month}-{random.randint(1, 28):02d}",
                    f"Routine {change_type.lower()}"
                ])

def create_metadata_file():
    """Create metadata about the dataset"""
    metadata = {
        "dataset_info": {
            "name": "FDB Sample Dataset",
            "version": "1.0", 
            "created_date": datetime.now().isoformat(),
            "total_files": 20,
            "description": "Sample pharmaceutical data for Formulary Management Tool testing"
        },
        "tenants": {
            "MASTER": {
                "description": "Master/Mother tenant with base pharmaceutical data",
                "record_count_estimate": 500,
                "coverage": "National"
            },
            "AK": {
                "description": "Alaska child tenant with regional preferences", 
                "record_count_estimate": 250,
                "coverage": "Alaska region",
                "inherits_from": "MASTER"
            },
            "MO": {
                "description": "Missouri child tenant with regional preferences",
                "record_count_estimate": 250, 
                "coverage": "Missouri region",
                "inherits_from": "MASTER"
            }
        },
        "file_descriptions": {
            "fdb_core_drugs.csv": "Primary drug information (NDC, brand, generic, etc.)",
            "fdb_pricing.csv": "Drug pricing information (AWP, WAC, NADAC)",
            "fdb_formulary_*.csv": "Tenant-specific formulary status",
            "fdb_regional_*.csv": "Regional distribution preferences",
            "fdb_therapeutic_*.csv": "Therapeutic class organization",
            "fdb_manufacturer_*.csv": "Manufacturer-specific information",
            "fdb_updates_*.csv": "Historical change tracking"
        }
    }
    
    with open(OUTPUT_DIR / "dataset_metadata.json", 'w') as f:
        json.dump(metadata, f, indent=2)

def main():
    """Generate all sample FDB files"""
    print("🔧 Generating FDB sample dataset...")
    
    # Core files
    print("  📋 Creating core drug file...")
    create_core_fdb_file()
    
    print("  💰 Creating pricing file...")
    create_pricing_file()
    
    print("  🏢 Creating tenant-specific files...")
    create_tenant_specific_files()
    
    print("  🩺 Creating therapeutic class files...")
    create_therapeutic_class_files()
    
    print("  🏭 Creating manufacturer files...")
    create_manufacturer_files()
    
    print("  📅 Creating update history files...")
    create_update_history_files()
    
    print("  📄 Creating metadata file...")
    create_metadata_file()
    
    # Count files
    file_count = len(list(OUTPUT_DIR.glob("*.csv"))) + len(list(OUTPUT_DIR.glob("*.json")))
    print(f"\n✅ Generated {file_count} files in {OUTPUT_DIR}/")
    
    print("\n📂 Files created:")
    for file_path in sorted(OUTPUT_DIR.iterdir()):
        file_size = file_path.stat().st_size
        print(f"  {file_path.name} ({file_size:,} bytes)")

if __name__ == "__main__":
    main()

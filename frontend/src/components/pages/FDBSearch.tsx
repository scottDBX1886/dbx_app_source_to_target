/**
 * FDB Search Page
 * Search and view FDB records across ~20 files
 */
import React, { useState, useEffect, useContext } from 'react';
import { TenantContext } from '../../App';

// Tenant-specific FDB datasets
const fdbDatasets = {
  MASTER: [
    {
      ndc: "00011122233", 
      gsn: 10001, 
      brand: "Amoxicillin", 
      generic: "Amoxicillin", 
      rx_otc: "RX", 
      pkg_size: "30",  
      hic3: "123", 
      hicl: "AMOX", 
      dcc: "ABX",  
      mfr: "Alpha", 
      obsolete: false, 
      rebate: true,  
      load_date: "2024-12-01", 
      pkg_origin: "US", 
      gsn_desc: "Penicillins", 
      pkg_form: "Capsule"
    },
    {
      ndc: "99988877766", 
      gsn: 10002, 
      brand: "Zyrtec", 
      generic: "Cetirizine", 
      rx_otc: "OTC", 
      pkg_size: "60",  
      hic3: "456", 
      hicl: "ANTH", 
      dcc: "ALL",  
      mfr: "ZenPharm", 
      obsolete: false, 
      rebate: false, 
      load_date: "2024-12-02", 
      pkg_origin: "CA", 
      gsn_desc: "Antihistamines", 
      pkg_form: "Tablet"
    },
    {
      ndc: "55544433322", 
      gsn: 20001, 
      brand: "Lipitor", 
      generic: "Atorvastatin", 
      rx_otc: "RX", 
      pkg_size: "30",  
      hic3: "789", 
      hicl: "STAT", 
      dcc: "CV",   
      mfr: "Beta",  
      obsolete: false, 
      rebate: true,  
      load_date: "2024-12-03", 
      pkg_origin: "US", 
      gsn_desc: "Statins", 
      pkg_form: "Tablet"
    },
    {
      ndc: "12312312312", 
      gsn: 30001, 
      brand: "Humalog", 
      generic: "Insulin Lispro", 
      rx_otc: "RX", 
      pkg_size: "5", 
      hic3: "999", 
      hicl: "INS",  
      dcc: "IMM",  
      mfr: "Gamma", 
      obsolete: false, 
      rebate: true,  
      load_date: "2024-12-04", 
      pkg_origin: "DE", 
      gsn_desc: "Insulins", 
      pkg_form: "Vial"
    },
    {
      ndc: "11122233344", 
      gsn: 40001, 
      brand: "Advil", 
      generic: "Ibuprofen", 
      rx_otc: "OTC", 
      pkg_size: "100", 
      hic3: "555", 
      hicl: "NSAID", 
      dcc: "PAIN", 
      mfr: "Omega", 
      obsolete: false, 
      rebate: false, 
      load_date: "2024-12-05", 
      pkg_origin: "US", 
      gsn_desc: "NSAIDs", 
      pkg_form: "Tablet"
    }
  ],
  AK: [
    {
      ndc: "00011122233", 
      gsn: 10001, 
      brand: "Amoxicillin", 
      generic: "Amoxicillin", 
      rx_otc: "RX", 
      pkg_size: "30",  
      hic3: "123", 
      hicl: "AMOX", 
      dcc: "ABX",  
      mfr: "Alpha", 
      obsolete: false, 
      rebate: true,  
      load_date: "2024-12-01", 
      pkg_origin: "US", 
      gsn_desc: "Penicillins", 
      pkg_form: "Capsule"
    },
    {
      ndc: "99988877766", 
      gsn: 10002, 
      brand: "Zyrtec", 
      generic: "Cetirizine", 
      rx_otc: "OTC", 
      pkg_size: "60",  
      hic3: "456", 
      hicl: "ANTH", 
      dcc: "ALL",  
      mfr: "ZenPharm", 
      obsolete: false, 
      rebate: false, 
      load_date: "2024-12-02", 
      pkg_origin: "AK", 
      gsn_desc: "Antihistamines", 
      pkg_form: "Tablet"
    },
    {
      ndc: "AK001234567", 
      gsn: 10003, 
      brand: "Claritin-AK", 
      generic: "Loratadine", 
      rx_otc: "OTC", 
      pkg_size: "30",  
      hic3: "456", 
      hicl: "ANTH", 
      dcc: "ALL",  
      mfr: "Alaska Pharma", 
      obsolete: false, 
      rebate: false, 
      load_date: "2024-12-06", 
      pkg_origin: "AK", 
      gsn_desc: "Antihistamines", 
      pkg_form: "Tablet"
    },
    {
      ndc: "AK987654321", 
      gsn: 50001, 
      brand: "Arctic-Cough", 
      generic: "Dextromethorphan", 
      rx_otc: "OTC", 
      pkg_size: "120ml", 
      hic3: "678", 
      hicl: "COUGH", 
      dcc: "RESP", 
      mfr: "Alaska Pharma", 
      obsolete: false, 
      rebate: false, 
      load_date: "2024-12-07", 
      pkg_origin: "AK", 
      gsn_desc: "Cough Suppressants", 
      pkg_form: "Syrup"
    }
  ],
  MO: [
    {
      ndc: "55544433322", 
      gsn: 20001, 
      brand: "Lipitor", 
      generic: "Atorvastatin", 
      rx_otc: "RX", 
      pkg_size: "30",  
      hic3: "789", 
      hicl: "STAT", 
      dcc: "CV",   
      mfr: "Beta",  
      obsolete: false, 
      rebate: true,  
      load_date: "2024-12-03", 
      pkg_origin: "MO", 
      gsn_desc: "Statins", 
      pkg_form: "Tablet"
    },
    {
      ndc: "12312312312", 
      gsn: 30001, 
      brand: "Humalog", 
      generic: "Insulin Lispro", 
      rx_otc: "RX", 
      pkg_size: "5", 
      hic3: "999", 
      hicl: "INS",  
      dcc: "IMM",  
      mfr: "Gamma", 
      obsolete: false, 
      rebate: true,  
      load_date: "2024-12-04", 
      pkg_origin: "MO", 
      gsn_desc: "Insulins", 
      pkg_form: "Vial"
    },
    {
      ndc: "MO123456789", 
      gsn: 20002, 
      brand: "Show-Me Statin", 
      generic: "Simvastatin", 
      rx_otc: "RX", 
      pkg_size: "30",  
      hic3: "789", 
      hicl: "STAT", 
      dcc: "CV",   
      mfr: "Missouri Med", 
      obsolete: false, 
      rebate: true,  
      load_date: "2024-12-08", 
      pkg_origin: "MO", 
      gsn_desc: "Statins", 
      pkg_form: "Tablet"
    },
    {
      ndc: "MO987654321", 
      gsn: 60001, 
      brand: "Gateway Glucose", 
      generic: "Metformin", 
      rx_otc: "RX", 
      pkg_size: "60", 
      hic3: "890", 
      hicl: "DIAB", 
      dcc: "ENDO", 
      mfr: "Missouri Med", 
      obsolete: false, 
      rebate: true, 
      load_date: "2024-12-09", 
      pkg_origin: "MO", 
      gsn_desc: "Diabetes Medications", 
      pkg_form: "Tablet"
    },
    {
      ndc: "MO555666777", 
      gsn: 70001, 
      brand: "Ozark Oxy", 
      generic: "Oxycodone", 
      rx_otc: "RX", 
      pkg_size: "30", 
      hic3: "234", 
      hicl: "NARC", 
      dcc: "PAIN", 
      mfr: "Missouri Med", 
      obsolete: false, 
      rebate: false, 
      load_date: "2024-12-10", 
      pkg_origin: "MO", 
      gsn_desc: "Opioid Analgesics", 
      pkg_form: "Tablet"
    }
  ]
};


export function FDBSearch() {
  const { tenant } = useContext(TenantContext);
  const [searchQuery, setSearchQuery] = useState('');
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [selectedNdc, setSelectedNdc] = useState<any>(null);
  
  // Get current tenant's data
  const currentData = fdbDatasets[tenant as keyof typeof fdbDatasets] || fdbDatasets.MASTER;
  const [filteredData, setFilteredData] = useState(currentData);

  // Update filtered data when tenant changes
  useEffect(() => {
    const newData = fdbDatasets[tenant as keyof typeof fdbDatasets] || fdbDatasets.MASTER;
    setFilteredData(newData);
    setSearchQuery(''); // Clear search when tenant changes
  }, [tenant]);

  // Filter data based on search query
  const handleSearch = () => {
    if (!searchQuery.trim()) {
      setFilteredData(currentData);
    } else {
      const filtered = currentData.filter(record => 
        record.ndc.toLowerCase().includes(searchQuery.toLowerCase()) ||
        record.brand.toLowerCase().includes(searchQuery.toLowerCase()) ||
        record.generic.toLowerCase().includes(searchQuery.toLowerCase()) ||
        record.hic3.toString().includes(searchQuery) ||
        record.mfr.toLowerCase().includes(searchQuery.toLowerCase())
      );
      setFilteredData(filtered);
    }
  };

  // Handle Enter key in search input
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  // Open NDC drawer
  const openDrawer = (ndc: string) => {
    const record = currentData.find(r => r.ndc === ndc);
    if (!record) {
      alert("Record not found");
      return;
    }
    setSelectedNdc(record);
    setDrawerOpen(true);
  };

  // Close drawer
  const closeDrawer = () => {
    setDrawerOpen(false);
    setSelectedNdc(null);
  };

  // Handle ESC key to close drawer
  useEffect(() => {
    const handleEsc = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        closeDrawer();
      }
    };
    document.addEventListener('keydown', handleEsc);
    return () => document.removeEventListener('keydown', handleEsc);
  }, []);

  // Prepare drawer data sections
  const getDrawerData = (record: any) => ({
    "Core": {
      "NDC": record.ndc,
      "GSN": record.gsn,
      "Brand": record.brand,
      "Generic": record.generic,
      "Rx/OTC": record.rx_otc,
      "Load Date": record.load_date
    },
    "Classification": {
      "HIC3": record.hic3,
      "HICL": record.hicl,
      "DCC": record.dcc,
      "GSN Desc": record.gsn_desc
    },
    "Pricing & Flags": {
      "Federally Rebateable": record.rebate ? "Yes" : "No",
      "MFT Obsolete": record.obsolete ? "Yes" : "No",
      "MFR": record.mfr
    },
    "Packaging & Origin": {
      "Package Size": record.pkg_size,
      "Form": record.pkg_form,
      "Origin": record.pkg_origin
    }
  });

  // Copy drawer data to clipboard
  const copyJson = async () => {
    if (!selectedNdc) return;
    try {
      const data = getDrawerData(selectedNdc);
      await navigator.clipboard.writeText(JSON.stringify(data, null, 2));
      alert("Copied JSON");
    } catch (e) {
      alert("Clipboard failed");
    }
  };

  // Download CSV
  const downloadCsv = () => {
    if (!selectedNdc) return;
    const data = getDrawerData(selectedNdc);
    const flatData = [];
    
    for (const [section, kv] of Object.entries(data)) {
      for (const [key, value] of Object.entries(kv)) {
        flatData.push({ section, field: key, value: String(value) });
      }
    }
    
    const csvContent = [
      'section,field,value',
      ...flatData.map(row => `"${row.section}","${row.field}","${row.value}"`)
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `ndc_${selectedNdc.ndc}_summary.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="fdb-search-page">
      <div className="page-header">
        <h1>🔍 FDB Search</h1>
        <p className="muted">Search and view FDB records across ~20 files. View-only; NDC opens details drawer.</p>
      </div>

      {/* Search Panel */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">Search FDB</h2>
        </div>
        
        <div className="row">
          <input 
            type="text" 
            placeholder="Search NDC, Brand, Generic, HIC3, MFR" 
            style={{ minWidth: '320px', flex: 1 }}
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={handleKeyDown}
          />
          <button className="btn primary" onClick={handleSearch}>
            Search
          </button>
          <span className="hint">View-only; NDC opens details drawer</span>
        </div>
        
        <div className="divider"></div>
        
        <div className="scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>NDC</th>
                <th>GSN</th>
                <th>Brand</th>
                <th>Generic</th>
                <th>Rx/OTC</th>
                <th>Package Size</th>
                <th>HIC3</th>
                <th>HICL</th>
                <th>DCC</th>
                <th>MFR</th>
                <th>Load Date</th>
              </tr>
            </thead>
            <tbody>
              {filteredData.map((record) => (
                <tr key={record.ndc}>
                  <td>
                    <code>
                      <a 
                        href="#" 
                        className="ndc-link" 
                        onClick={(e) => {
                          e.preventDefault();
                          openDrawer(record.ndc);
                        }}
                      >
                        {record.ndc}
                      </a>
                    </code>
                  </td>
                  <td>{record.gsn}</td>
                  <td>{record.brand}</td>
                  <td>{record.generic}</td>
                  <td>{record.rx_otc}</td>
                  <td>{record.pkg_size}</td>
                  <td>{record.hic3}</td>
                  <td>{record.hicl}</td>
                  <td>{record.dcc}</td>
                  <td>{record.mfr}</td>
                  <td>{record.load_date}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>


      {/* NDC Details Drawer */}
      <div className={`drawer-backdrop ${drawerOpen ? 'show' : ''}`} onClick={closeDrawer}></div>
      <aside className={`drawer ${drawerOpen ? 'open' : ''}`} role="dialog" aria-modal="true">
        <div className="drawer-header">
          <div style={{ flex: '1 1 auto' }}>
            <h2>NDC Details</h2>
            <div className="muted">Aggregated from "~20 FDB files" • View-only</div>
          </div>
          <button className="btn ghost" onClick={closeDrawer} aria-label="Close drawer">
            ✕
          </button>
        </div>
        
        <div className="drawer-body">
          {selectedNdc && (
            <>
              {Object.entries(getDrawerData(selectedNdc)).map(([section, kv]) => (
                <div key={section} className="sec">
                  <h4>{section}</h4>
                  <div className="kv">
                    {Object.entries(kv).map(([key, value]) => (
                      <React.Fragment key={key}>
                        <div className="muted">{key}</div>
                        <div className="code">{String(value)}</div>
                      </React.Fragment>
                    ))}
                  </div>
                </div>
              ))}
              
              <div className="divider"></div>
              
              <div className="row right">
                <button className="btn" onClick={copyJson}>
                  Copy JSON
                </button>
                <button className="btn" onClick={downloadCsv}>
                  Download CSV
                </button>
              </div>
            </>
          )}
        </div>
      </aside>
    </div>
  );
}

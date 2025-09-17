/**
 * PDL Coding Page
 * PDL Key Code management and templates
 */
import React, { useState, useEffect, useContext } from 'react';
import { TenantContext } from '../../App';

// Tenant-specific PDL Coding datasets  
const pdlCodingDatasets = {
  MASTER: [
    { ndc: "00011122233", fmt_drug: "AMOXICILLIN", key_code: "10001|AMOXIC|RX|30", status: "PDL", start: "2024-01-01", end: null },
    { ndc: "99988877766", fmt_drug: "ZYRTEC", key_code: "10002|ZYRTEC|OTC|60", status: "PDL", start: "2024-03-01", end: null },
    { ndc: "55544433322", fmt_drug: "LIPITOR", key_code: "20001|LIPITO|RX|30", status: "PDL", start: "2024-06-01", end: null },
    { ndc: "12312312312", fmt_drug: "HUMALOG", key_code: "30001|HUMALO|RX|5", status: "Approved", start: "2024-02-01", end: null },
    { ndc: "11122233344", fmt_drug: "ADVIL", key_code: "40001|ADVIL|OTC|100", status: "Review", start: "2024-07-01", end: null }
  ],
  AK: [
    { ndc: "00011122233", fmt_drug: "AMOXICILLIN", key_code: "10001|AMOXIC|RX|30", status: "PDL", start: "2024-01-01", end: null },
    { ndc: "99988877766", fmt_drug: "ZYRTEC", key_code: "10002|ZYRTEC|OTC|60", status: "PDL", start: "2024-03-01", end: null },
    { ndc: "AK001234567", fmt_drug: "CLARITIN-AK", key_code: "10003|CLARIT|OTC|30", status: "PDL", start: "2024-06-01", end: null },
    { ndc: "AK987654321", fmt_drug: "ARCTIC-COUGH", key_code: "50001|ARCTIC|OTC|120ML", status: "Approved", start: "2024-07-01", end: null }
  ],
  MO: [
    { ndc: "55544433322", fmt_drug: "LIPITOR", key_code: "20001|LIPITO|RX|30", status: "PDL", start: "2024-06-01", end: null },
    { ndc: "12312312312", fmt_drug: "HUMALOG", key_code: "30001|HUMALO|RX|5", status: "Approved", start: "2024-02-01", end: null },
    { ndc: "MO123456789", fmt_drug: "SHOW-ME STATIN", key_code: "20002|SHOWME|RX|30", status: "PDL", start: "2024-08-01", end: null },
    { ndc: "MO987654321", fmt_drug: "GATEWAY GLUCOSE", key_code: "60001|GATEWA|RX|60", status: "Review", start: "2024-09-01", end: null },
    { ndc: "MO555666777", fmt_drug: "OZARK OXY", key_code: "70001|OZARKO|RX|30", status: "Restricted", start: "2024-10-01", end: null }
  ]
};

export function PDLCoding() {
  const { tenant } = useContext(TenantContext);
  const [searchQuery, setSearchQuery] = useState('');
  
  // Get current tenant's data
  const currentData = pdlCodingDatasets[tenant as keyof typeof pdlCodingDatasets] || pdlCodingDatasets.MASTER;
  const [filteredData, setFilteredData] = useState(currentData);

  // Update filtered data when tenant changes
  useEffect(() => {
    const newData = pdlCodingDatasets[tenant as keyof typeof pdlCodingDatasets] || pdlCodingDatasets.MASTER;
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
        record.fmt_drug.toLowerCase().includes(searchQuery.toLowerCase()) ||
        record.key_code.toLowerCase().includes(searchQuery.toLowerCase()) ||
        record.status.toLowerCase().includes(searchQuery.toLowerCase())
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

  // Download CSV function
  const downloadCsv = () => {
    const csvContent = [
      'NDC,FMT Drug,PDL Key Code,Status,Start,End',
      ...filteredData.map(row => 
        `"${row.ndc}","${row.fmt_drug}","${row.key_code}","${row.status}","${row.start}","${row.end || ''}"`)
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `pdl_coding_${tenant}_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  };

  // Get status styling
  const getStatusClass = (status: string) => {
    switch (status.toLowerCase()) {
      case 'pdl': return 'status success';
      case 'approved': return 'status success';
      case 'review': return 'status warning';
      case 'restricted': return 'status error';
      default: return 'status';
    }
  };

  return (
    <div className="pdl-coding-page">
      <div className="page-header">
        <h1>🏷️ PDL Coding</h1>
        <p className="muted">PDL Key Code management with configurable templates and automated generation.</p>
      </div>

      {/* PDL Records */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">PDL Master Records</h2>
        </div>
        
        <div className="row">
          <input 
            type="text" 
            placeholder="Search NDC / FMT Drug / Key Code / Status" 
            style={{ minWidth: '320px', flex: 1 }}
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={handleKeyDown}
          />
          <button className="btn primary" onClick={handleSearch}>
            Search
          </button>
          <button className="btn" onClick={downloadCsv}>
            Download CSV
          </button>
        </div>
        
        <div className="divider"></div>
        
        <div className="scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>NDC</th>
                <th>FMT Drug</th>
                <th>PDL Key Code</th>
                <th>Status</th>
                <th>Start</th>
                <th>End</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {filteredData.map((record) => (
                <tr key={record.ndc}>
                  <td><code>{record.ndc}</code></td>
                  <td>{record.fmt_drug}</td>
                  <td><code>{record.key_code}</code></td>
                  <td><span className={getStatusClass(record.status)}>{record.status}</span></td>
                  <td>{record.start}</td>
                  <td>{record.end || '-'}</td>
                  <td>
                    <button className="btn ghost" style={{ padding: '4px 8px', fontSize: '12px' }}>
                      Edit
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Key Code Templates */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🔧 PDL Key Code Templates</h2>
        </div>
        
        <div className="alert info mb-2">
          <strong>Config-Driven Templates</strong>
          <div className="muted mt-1">
            PDL Key Codes are generated using configurable templates with override hierarchy.
          </div>
        </div>
        
        <div className="template-examples">
          <h3>Template Examples</h3>
          <div className="kv">
            <div className="muted">Default Template</div>
            <div><code>GSN + first 6 of Brand + RX/OTC + Package size</code></div>
            
            <div className="muted">Alternative Template 1</div>
            <div><code>GSN + first 6 of Brand + RX/OTC + first 6 of Generic</code></div>
            
            <div className="muted">Alternative Template 2</div>
            <div><code>GSN + first 6 of Brand + RX/OTC + first 6 of MFR</code></div>
            
            <div className="muted">Example Output</div>
            <div><code>10001|AMOXIC|RX|30</code> (GSN=10001, Brand=AMOXICILLIN, RX/OTC=RX, Size=30)</div>
          </div>
        </div>
        
        <div className="divider"></div>
        
        <h3>Override Hierarchy</h3>
        <div className="override-hierarchy">
          <div className="hierarchy-item">
            <span className="status success">1</span>
            <strong>NDC</strong> - Specific NDC override
          </div>
          <div className="hierarchy-item">
            <span className="status success">2</span>
            <strong>GSN/GCN</strong> - Generic sequence number
          </div>
          <div className="hierarchy-item">
            <span className="status success">3</span>
            <strong>HICL</strong> - Hierarchic ingredient classification
          </div>
          <div className="hierarchy-item">
            <span className="status success">4</span>
            <strong>GC3/HIC3 (Primary)</strong> - Generic code 3 / HIC level 3
          </div>
          <div className="hierarchy-item">
            <span className="status success">5</span>
            <strong>DCC</strong> - Drug category code
          </div>
        </div>
      </div>

      {/* Exclusions */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🚫 Exclusions & Filters</h2>
        </div>
        
        <div className="exclusion-rules">
          <h3>Automatic Exclusions</h3>
          <ul>
            <li><strong>Federally Rebateable:</strong> Excluded from PDL coding</li>
            <li><strong>MFT Obsolete:</strong> Obsolete manufacturer records excluded</li>
            <li><strong>Specific NDCs:</strong> Manually excluded NDC list</li>
            <li><strong>Future-Dated Rows:</strong> Records with future effective dates</li>
          </ul>
          
          <h3>Export Configuration</h3>
          <div className="kv">
            <div className="muted">POS Export Format</div>
            <div>NDC, Action (ADD/REMOVE), Effective Date, Status (PDL)</div>
            
            <div className="muted">Export Filename</div>
            <div><code>pos_export_YYYY-MM-DD.csv</code></div>
            
            <div className="muted">Automated Scheduling</div>
            <div>Weekly extracts from Delta OLAP</div>
          </div>
        </div>
      </div>

      {/* API Integration */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🔗 API Integration</h2>
        </div>
        
        <div className="alert info">
          <strong>Future API Wiring</strong>
          <div className="muted mt-1">
            This page will connect to Lakebase OLTP for PDL coding operations and Delta OLAP for analytics.
          </div>
        </div>
        
        <div className="kv">
          <div className="muted">Endpoint</div>
          <div><code>GET /api/master/pdl?tenant=&lt;tenant&gt;</code></div>
          
          <div className="muted">Operations</div>
          <div>
            <ul>
              <li>Key code generation using configurable templates</li>
              <li>Exclusion rule application</li>
              <li>POS export file generation</li>
              <li>Weekly extract scheduling</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

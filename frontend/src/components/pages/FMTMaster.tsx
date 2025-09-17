/**
 * FMT Master Page
 * Master formulary management with MBID assignment
 */
import React, { useState, useEffect, useContext } from 'react';
import { TenantContext } from '../../App';

// Tenant-specific FMT Master datasets
const fmtMasterDatasets = {
  MASTER: [
    { ndc: "00011122233", fmt_drug: "AMOXICILLIN", mbid: "AK123456", status: "PDL", start: "2024-01-01", end: null },
    { ndc: "99988877766", fmt_drug: "ZYRTEC", mbid: "AK123456", status: "PDL", start: "2024-03-01", end: null },
    { ndc: "55544433322", fmt_drug: "LIPITOR", mbid: "CV999001", status: "PDL", start: "2024-06-01", end: null },
    { ndc: "12312312312", fmt_drug: "HUMALOG", mbid: "IMM000777", status: "Approved", start: "2024-02-01", end: null },
    { ndc: "11122233344", fmt_drug: "ADVIL", mbid: null, status: "Review", start: "2024-07-01", end: null }
  ],
  AK: [
    { ndc: "00011122233", fmt_drug: "AMOXICILLIN", mbid: "AK123456", status: "PDL", start: "2024-01-01", end: null },
    { ndc: "99988877766", fmt_drug: "ZYRTEC", mbid: "AK123456", status: "PDL", start: "2024-03-01", end: null },
    { ndc: "AK001234567", fmt_drug: "CLARITIN-AK", mbid: "AK123456_a", status: "PDL", start: "2024-06-01", end: null },
    { ndc: "AK987654321", fmt_drug: "ARCTIC-COUGH", mbid: "AK567890", status: "Approved", start: "2024-07-01", end: null }
  ],
  MO: [
    { ndc: "55544433322", fmt_drug: "LIPITOR", mbid: "CV999001", status: "PDL", start: "2024-06-01", end: null },
    { ndc: "12312312312", fmt_drug: "HUMALOG", mbid: "IMM000777", status: "Approved", start: "2024-02-01", end: null },
    { ndc: "MO123456789", fmt_drug: "SHOW-ME STATIN", mbid: "MO999001", status: "PDL", start: "2024-08-01", end: null },
    { ndc: "MO987654321", fmt_drug: "GATEWAY GLUCOSE", mbid: "MO888001", status: "Review", start: "2024-09-01", end: null },
    { ndc: "MO555666777", fmt_drug: "OZARK OXY", mbid: "MO777001", status: "Restricted", start: "2024-10-01", end: null }
  ]
};

export function FMTMaster() {
  const { tenant } = useContext(TenantContext);
  const [searchQuery, setSearchQuery] = useState('');
  
  // Get current tenant's data
  const currentData = fmtMasterDatasets[tenant as keyof typeof fmtMasterDatasets] || fmtMasterDatasets.MASTER;
  const [filteredData, setFilteredData] = useState(currentData);

  // Update filtered data when tenant changes
  useEffect(() => {
    const newData = fmtMasterDatasets[tenant as keyof typeof fmtMasterDatasets] || fmtMasterDatasets.MASTER;
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
        (record.mbid && record.mbid.toLowerCase().includes(searchQuery.toLowerCase())) ||
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
      'NDC,FMT Drug,MBID,Status,Start,End',
      ...filteredData.map(row => 
        `"${row.ndc}","${row.fmt_drug}","${row.mbid || ''}","${row.status}","${row.start}","${row.end || ''}"`)
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `fmt_master_${tenant}_${new Date().toISOString().split('T')[0]}.csv`;
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
    <div className="fmt-master-page">
      <div className="page-header">
        <h1>📋 FMT Master</h1>
        <p className="muted">Master formulary management with MBID assignment and status tracking.</p>
      </div>

      {/* Search & Actions */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">FMT Master Records</h2>
        </div>
        
        <div className="row">
          <input 
            type="text" 
            placeholder="Search NDC / FMT Drug / MBID / Status" 
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
                <th>MBID</th>
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
                  <td><code>{record.mbid || '-'}</code></td>
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

      {/* MBID Management */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🏷️ MBID Management</h2>
        </div>
        
        <div className="alert info mb-2">
          <strong>MBID Structure (Master/Child Model)</strong>
          <div className="muted mt-1">
            MBIDs are created only in MASTER with begin/end dates. Child tenants (AK, MO) reuse and may sub-scope.
          </div>
        </div>
        
        <div className="kv">
          <div className="muted">MBID Format</div>
          <div><code>SS######</code> (state abbrev + 6 digits), optional sub-suffix: _a, _b</div>
          
          <div className="muted">Relationship</div>
          <div>1:1 between MBID and MBID description</div>
          
          <div className="muted">Validation Rules</div>
          <div>
            <ul>
              <li>If one FMT Drug Name falls in two MBIDs → warn/confirm</li>
              <li>If NDCs for same FMT Drug across two MBIDs differ in status → block approve</li>
            </ul>
          </div>
          
          <div className="muted">Available MBIDs</div>
          <div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
              <span className="pill">AK123456 - Antihistamines</span>
              <span className="pill">CV999001 - Cardio – Statins</span>
              <span className="pill">IMM000777 - Immunology – Insulins</span>
              <span className="pill">AK123456_a - AK Antihistamines – sub</span>
            </div>
          </div>
        </div>
      </div>

      {/* API Integration Info */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🔗 API Integration</h2>
        </div>
        
        <div className="alert info">
          <strong>Future API Wiring</strong>
          <div className="muted mt-1">
            This page will connect to Lakebase OLTP for operational FMT master data management.
          </div>
        </div>
        
        <div className="kv">
          <div className="muted">Endpoint</div>
          <div><code>GET /api/master/fmt?tenant=&lt;tenant&gt;</code></div>
          
          <div className="muted">Data Store</div>
          <div>Lakebase OLTP (operational rows) + Delta OLAP (analytics & weekly extracts)</div>
          
          <div className="muted">Operations</div>
          <div>
            <ul>
              <li>MBID assignment and validation</li>
              <li>Status management (PDL, Review, Approved, Rejected)</li>
              <li>Date range validation and conflict detection</li>
              <li>Bulk operations and CSV import/export</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

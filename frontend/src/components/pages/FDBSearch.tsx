/**
 * FDB Search Page - Live data from Databricks Volume
 * Search and view FDB records across ~20 files
 */
import React, { useState, useEffect, useContext } from 'react';
import { TenantContext } from '../../App';
import { fdbApi, type FDBRecord, type FDBDetailsResponse } from '../../services/fdb-api';

export function FDBSearch() {
  const { tenant } = useContext(TenantContext);
  const [searchQuery, setSearchQuery] = useState('');
  const [records, setRecords] = useState<FDBRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  // Popup state (matching FMT Master style)
  const [showPopup, setShowPopup] = useState(false);
  const [selectedRecord, setSelectedRecord] = useState<FDBDetailsResponse | null>(null);

  // Load initial data when tenant changes
  useEffect(() => {
    loadRecords();
  }, [tenant]);

  const loadRecords = async (query?: string) => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await fdbApi.searchRecords(tenant, query, 100);
      setRecords(response.records);
      
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load data');
      console.error('Error loading FDB records:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = () => {
    loadRecords(searchQuery.trim() || undefined);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const openPopup = async (ndc: string) => {
    try {
      setLoading(true);
      const details = await fdbApi.getRecordDetails(ndc, tenant);
      setSelectedRecord(details);
      setShowPopup(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load record details');
      console.error('Error loading record details:', err);
    } finally {
      setLoading(false);
    }
  };

  const closePopup = () => {
    setShowPopup(false);
    setSelectedRecord(null);
  };

  // Handle ESC key to close popup
  useEffect(() => {
    const handleEsc = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        closePopup();
      }
    };
    if (showPopup) {
      document.addEventListener('keydown', handleEsc);
      return () => document.removeEventListener('keydown', handleEsc);
    }
  }, [showPopup]);


  // Copy popup data to clipboard
  const copyJson = async () => {
    if (!selectedRecord) return;
    try {
      const { ndc, tenant, data_source, user_email, ...sections } = selectedRecord;
      await navigator.clipboard.writeText(JSON.stringify(sections, null, 2));
      alert("Copied JSON");
    } catch (e) {
      alert("Clipboard failed");
    }
  };

  const downloadCsvFromPopup = async () => {
    if (!selectedRecord) return;
    
    const { ndc, tenant, data_source, user_email, ...sections } = selectedRecord;
    
    const flatData = [];
    for (const [section, kv] of Object.entries(sections)) {
      if (typeof kv === 'object' && kv !== null) {
        for (const [key, value] of Object.entries(kv)) {
          flatData.push({ section, field: key, value: String(value) });
        }
      }
    }

    const headers = "Section,Field,Value\n";
    const csvRows = flatData.map(row => `"${row.section}","${row.field}","${row.value}"`);
    const csvContent = headers + csvRows.join("\n");

    const blob = new Blob([csvContent], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `ndc_${selectedRecord.ndc}_details.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="fdb-search-page">
      <div className="page-header">
        <h1>🔍 FDB Search</h1>
        <p className="muted">Live data from Databricks Volume - Search ~20 FDB files across tenants</p>
      </div>

      {/* Main Search Panel */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">FDB Records - {tenant} Tenant</h2>
        </div>
        
        <div className="row">
          <input 
            type="text" 
            placeholder="Search NDC / Brand / Generic / Manufacturer" 
            style={{ minWidth: '320px', flex: 1 }}
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            disabled={loading}
          />
          <button className="btn primary" onClick={handleSearch} disabled={loading}>
            {loading ? 'Searching...' : 'Search'}
          </button>
          <span className="hint">View-only; NDC opens details drawer</span>
        </div>
        
        {/* Error Display */}
        {error && (
          <div className="alert error" style={{ marginTop: '16px' }}>
            <strong>Error:</strong> {error}
          </div>
        )}
        
        <div className="divider"></div>
        
        <div className="scroll">
          {loading ? (
            <div style={{ textAlign: 'center', padding: '40px' }}>
              Loading FDB data from volume...
            </div>
          ) : records.length === 0 ? (
            <div style={{ textAlign: 'center', padding: '40px', color: 'var(--color-text-muted)' }}>
              {searchQuery ? 'No records found for your search.' : 'Enter a search term to find FDB records.'}
            </div>
          ) : (
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
                {records.map((record) => (
                  <tr key={record.ndc}>
                    <td>
                      <code>
                        <a 
                          href="#" 
                          className="ndc-link"
                          onClick={(e) => { e.preventDefault(); openPopup(record.ndc); }}
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
          )}
        </div>
      </div>

      {/* Data Source Info */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🔗 Data Source</h2>
        </div>
        
        <div className="alert info">
          <strong>Live Volume Integration</strong>
          <div className="muted mt-1">
            Data is loaded live from Databricks Volume with tenant-specific filtering.
          </div>
        </div>
        
        <div className="kv">
          <div className="muted">Volume Path</div>
          <div><code>/Volumes/demo/gainwell/fdb_data/</code></div>
          
          <div className="muted">Tenant Filtering</div>
          <div>
            {tenant === 'MASTER' 
              ? 'All core drug records (500+ entries)'
              : `Filtered by ${tenant} formulary entries for tenant-specific data`
            }
          </div>
          
          <div className="muted">Files Used</div>
          <div>
            <ul>
              <li>fdb_core_drugs.csv - Primary drug information</li>
              {tenant !== 'MASTER' && <li>fdb_formulary_{tenant.toLowerCase()}.csv - Tenant formulary</li>}
              <li>fdb_pricing.csv - Pricing details (when available)</li>
              <li>fdb_regional_{tenant.toLowerCase()}.csv - Regional preferences</li>
            </ul>
          </div>
        </div>
      </div>

      {/* NDC Details Popup */}
      {showPopup && selectedRecord && (
        <div className="popup-overlay" onClick={closePopup}>
          <div className="popup-window" onClick={(e) => e.stopPropagation()}>
            <div className="popup-header">
              <h3>NDC Details: {selectedRecord.ndc}</h3>
              <div className="popup-actions">
                <button className="btn ghost" onClick={copyJson}>Copy JSON</button>
                <button className="btn ghost" onClick={downloadCsvFromPopup}>Download CSV</button>
                <button className="btn-close" onClick={closePopup}>×</button>
              </div>
            </div>
            <div className="popup-content scroll">
              <div className="muted mb-2">Aggregated from "~20 FDB files" • View-only</div>
              {Object.entries(selectedRecord)
                .filter(([key]) => !['ndc', 'tenant', 'data_source', 'user_email'].includes(key))
                .map(([sectionName, sectionData]) => (
                  <div key={sectionName} className="sec">
                    <h4>{sectionName}</h4>
                    <div className="kv">
                      {typeof sectionData === 'object' && sectionData !== null &&
                        Object.entries(sectionData).map(([key, value]) => (
                          <React.Fragment key={key}>
                            <div className="muted">{key}</div>
                            <div className="code">{String(value)}</div>
                          </React.Fragment>
                        ))
                      }
                    </div>
                  </div>
                ))
              }
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
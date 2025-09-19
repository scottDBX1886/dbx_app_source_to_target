/**
 * FMT Master Page - Matches FDB design with drawer functionality
 * Formulary Management Tool with MBID assignment and status tracking
 */
import React, { useState, useEffect, useContext } from 'react';
import { TenantContext } from '../../App';
import { fmtApi, type FMTRecord, type FMTDetailsResponse } from '../../services/fmt-api';

export function FMTMaster() {
  const { tenant } = useContext(TenantContext);
  const [searchQuery, setSearchQuery] = useState('');
  const [records, setRecords] = useState<FMTRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Drawer state
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [detailsData, setDetailsData] = useState<FMTDetailsResponse | null>(null);
  const [detailsLoading, setDetailsLoading] = useState(false);

  // Load initial data when component mounts or tenant changes
  useEffect(() => {
    loadData();
  }, [tenant]);

  const loadData = async (query?: string) => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await fmtApi.searchRecords(tenant, query, undefined, 100);
      if (response.error) {
        setError(response.error);
        setRecords([]);
      } else {
        setRecords(response.records);
      }
    } catch (err) {
      console.error('Error loading FMT data:', err);
      setError(err instanceof Error ? err.message : 'Failed to load data');
      setRecords([]);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = () => {
    loadData(searchQuery);
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const handleExport = async (format: 'csv' | 'json') => {
    try {
      const blob = await fmtApi.exportData(tenant, format, searchQuery);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `fmt_export_${tenant.toLowerCase()}_${new Date().toISOString().slice(0, 10)}.${format}`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error('Export failed:', err);
      setError(err instanceof Error ? err.message : 'Export failed');
    }
  };

  const openDrawer = async (ndc: string) => {
    setDrawerOpen(true);
    setDetailsLoading(true);
    setDetailsData(null);

    try {
      const details = await fmtApi.getDetails(ndc, tenant);
      setDetailsData(details);
    } catch (err) {
      console.error('Error loading NDC details:', err);
      setError(err instanceof Error ? err.message : 'Failed to load details');
    } finally {
      setDetailsLoading(false);
    }
  };

  const closeDrawer = () => {
    setDrawerOpen(false);
    setDetailsData(null);
  };

  const renderDetailsDrawer = () => {
    if (!drawerOpen) return null;

    return (
      <>
        {/* Backdrop */}
        <div 
          className="drawer-backdrop show" 
          onClick={closeDrawer}
          aria-hidden="false"
        />
        
        {/* Drawer */}
        <aside className={`drawer ${drawerOpen ? 'open' : ''}`} role="dialog" aria-modal="true" aria-labelledby="drawerTitle">
          <div style={{
            position: 'sticky',
            top: 0,
            zIndex: 2,
            background: 'linear-gradient(180deg,#0b1670,#0b1455)',
            borderBottom: '1px solid #2a3c74',
            padding: '10px 14px',
            display: 'flex',
            alignItems: 'center',
            gap: '10px'
          }}>
            <div style={{ flex: '1 1 auto' }}>
              <h2 id="drawerTitle" style={{ margin: 0, fontSize: '18px' }}>FMT Details</h2>
              <div className="muted">Master formulary record with MBID • View-only</div>
            </div>
            <button className="btn ghost" onClick={closeDrawer} aria-label="Close drawer">✕</button>
          </div>
          
          <div style={{ padding: '14px' }}>
            {detailsLoading ? (
              <div>Loading details...</div>
            ) : detailsData ? (
              <>
                {/* Core Information */}
                <div className="sec">
                  <h4>Core Information</h4>
                  <div className="kv">
                    <div className="muted">NDC</div>
                    <div>{detailsData.core_info.ndc}</div>
                    <div className="muted">FMT Drug</div>
                    <div>{detailsData.core_info.fmt_drug}</div>
                    <div className="muted">Status</div>
                    <div>{detailsData.core_info.status}</div>
                    <div className="muted">Load Date</div>
                    <div>{detailsData.core_info.load_date}</div>
                  </div>
                </div>

                {/* MBID Information */}
                <div className="sec">
                  <h4>MBID Information</h4>
                  <div className="kv">
                    <div className="muted">MBID</div>
                    <div>{detailsData.mbid_info.mbid || 'Not assigned'}</div>
                    <div className="muted">Description</div>
                    <div>{detailsData.mbid_info.description || '-'}</div>
                    <div className="muted">Tenant</div>
                    <div>{detailsData.mbid_info.tenant || '-'}</div>
                    <div className="muted">Begin Date</div>
                    <div>{detailsData.mbid_info.begin_date || '-'}</div>
                  </div>
                </div>

                {/* Date Information */}
                <div className="sec">
                  <h4>Date Information</h4>
                  <div className="kv">
                    <div className="muted">Start Date</div>
                    <div>{detailsData.date_info.start_date}</div>
                    <div className="muted">End Date</div>
                    <div>{detailsData.date_info.end_date || 'Open-ended'}</div>
                    <div className="muted">Effective Date</div>
                    <div>{detailsData.date_info.effective_date || '-'}</div>
                    <div className="muted">Expiration Date</div>
                    <div>{detailsData.date_info.expiration_date || '-'}</div>
                  </div>
                </div>

                {/* Audit Information */}
                <div className="sec">
                  <h4>Audit Information</h4>
                  <div className="kv">
                    <div className="muted">Created By</div>
                    <div>{detailsData.audit_info.created_by || '-'}</div>
                    <div className="muted">Updated By</div>
                    <div>{detailsData.audit_info.updated_by || '-'}</div>
                    <div className="muted">Review Status</div>
                    <div>{detailsData.audit_info.review_status || '-'}</div>
                    <div className="muted">Notes</div>
                    <div>{detailsData.audit_info.notes || '-'}</div>
                  </div>
                </div>
              </>
            ) : (
              <div>No details available</div>
            )}
            
            <div className="divider"></div>
            <div className="row right">
              <button 
                className="btn" 
                onClick={() => handleExport('json')}
              >
                Copy JSON
              </button>
              <button 
                className="btn" 
                onClick={() => handleExport('csv')}
              >
                Download CSV
              </button>
            </div>
          </div>
        </aside>
      </>
    );
  };

  return (
    <div className="page-container">
      <div className="panel" aria-labelledby="fmtLabel">
        <div className="row stack">
          <div className="row" style={{ flex: '1 1 auto' }}>
            <label id="fmtLabel" className="sr-only">Search FMT Master</label>
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyPress={handleKeyPress}
              placeholder="Search NDC, FMT Drug, MBID, Status"
              aria-label="Search FMT Master"
              style={{ minWidth: '320px' }}
            />
            <button onClick={handleSearch} className="btn primary" disabled={loading}>
              {loading ? 'Searching...' : 'Search'}
            </button>
            <span className="hint">View-only; NDC opens details drawer</span>
          </div>
        </div>
        
        {error && (
          <div style={{ 
            color: 'var(--err)', 
            padding: '8px', 
            background: 'rgba(255, 107, 107, 0.1)', 
            borderRadius: '4px', 
            margin: '8px 0' 
          }}>
            {error}
          </div>
        )}

        <div className="divider"></div>
        
        <div className="scroll">
          <table className="table">
            <thead>
              <tr>
                <th>NDC</th>
                <th>FMT Drug</th>
                <th>MBID</th>
                <th>Status</th>
                <th>Start</th>
                <th>End</th>
              </tr>
            </thead>
            <tbody>
              {records.map((record) => (
                <tr key={record.ndc}>
                  <td className="code">
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
                  </td>
                  <td>{record.fmt_drug}</td>
                  <td className="code">{record.mbid || '—'}</td>
                  <td>
                    <span className={`status ${record.status.toLowerCase()}`}>
                      {record.status}
                    </span>
                  </td>
                  <td>{record.start_date}</td>
                  <td>{record.end_date || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {renderDetailsDrawer()}
    </div>
  );
}
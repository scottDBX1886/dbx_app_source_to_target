import React, { useState, useEffect, useContext } from 'react';
import { TenantContext } from '../../App';
import { pdlApi, type PDLRecord, type PDLDetailsResponse } from '../../services/pdl-api';

export function PDLCoding() {
  const { tenant } = useContext(TenantContext);
  const [searchQuery, setSearchQuery] = useState('');
  const [records, setRecords] = useState<PDLRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Drawer state
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [detailsData, setDetailsData] = useState<PDLDetailsResponse | null>(null);
  const [detailsLoading, setDetailsLoading] = useState(false);

  // Load initial data when component mounts or tenant changes
  useEffect(() => {
    loadData();
  }, [tenant]);

  const loadData = async (query?: string, status?: string) => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await pdlApi.searchRecords(tenant, query, status, 100);
      if (response.error) {
        setError(response.error);
        setRecords([]);
      } else {
        setRecords(response.records);
      }
    } catch (err) {
      console.error('Error loading PDL data:', err);
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
      const blob = await pdlApi.exportData(tenant, format, searchQuery);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `pdl_export_${tenant.toLowerCase()}_${new Date().toISOString().slice(0, 10)}.${format}`;
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
      const details = await pdlApi.getDetails(ndc, tenant);
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

    const { ndc, core_info, keycode_info, date_info, audit_info } = detailsData || {};

    return (
      <>
        <div className={`drawer-backdrop ${drawerOpen ? 'show' : ''}`} onClick={closeDrawer} aria-hidden={!drawerOpen}></div>
        <aside id="drawer" className={`drawer ${drawerOpen ? 'open' : ''}`} role="dialog" aria-modal="true" aria-labelledby="drawerTitle">
          <div style={{ position: 'sticky', top: 0, zIndex: 2, background: 'linear-gradient(180deg,#0b1670,#0b1455)', borderBottom: '1px solid #2a3c74', padding: '10px 14px', display: 'flex', alignItems: 'center', gap: '10px' }}>
            <div style={{ flex: '1 1 auto' }}>
              <h2 id="drawerTitle" style={{ margin: 0, fontSize: '18px' }}>NDC Details: {ndc}</h2>
              <div className="muted">PDL Coding Data • View-only</div>
            </div>
            <button className="btn ghost" onClick={closeDrawer} aria-label="Close drawer">✕</button>
          </div>
          <div style={{ padding: '14px' }}>
            {detailsLoading ? (
              <div style={{ textAlign: 'center', padding: '40px' }}>Loading details...</div>
            ) : error ? (
              <div className="alert error"><strong>Error:</strong> {error}</div>
            ) : detailsData ? (
              <>
                <div className="sec">
                  <h4>Core Information</h4>
                  <div className="kv">
                    <div><span className="muted">NDC</span></div><div><code className="code">{core_info?.ndc}</code></div>
                    <div><span className="muted">PDL Drug</span></div><div><code className="code">{core_info?.pdl_drug}</code></div>
                    <div><span className="muted">Status</span></div><div><span className={`status ${core_info?.status?.toLowerCase()}`}>{core_info?.status}</span></div>
                    <div><span className="muted">Load Date</span></div><div><code className="code">{core_info?.load_date}</code></div>
                  </div>
                </div>
                <div className="sec">
                  <h4>Key Code Information</h4>
                  <div className="kv">
                    <div><span className="muted">Key Code</span></div><div><code className="code">{keycode_info?.key_code || '—'}</code></div>
                    <div><span className="muted">Template</span></div><div><code className="code">{keycode_info?.template || '—'}</code></div>
                    <div><span className="muted">Tenant</span></div><div><code className="code">{keycode_info?.tenant || '—'}</code></div>
                    <div><span className="muted">Generation Date</span></div><div><code className="code">{keycode_info?.generation_date || '—'}</code></div>
                  </div>
                </div>
                <div className="sec">
                  <h4>Date Information</h4>
                  <div className="kv">
                    <div><span className="muted">Effective Date</span></div><div><code className="code">{date_info?.effective_date || '—'}</code></div>
                    <div><span className="muted">Expiration Date</span></div><div><code className="code">{date_info?.expiration_date || '—'}</code></div>
                    <div><span className="muted">Last Updated</span></div><div><code className="code">{date_info?.last_updated || '—'}</code></div>
                  </div>
                </div>
                <div className="sec">
                  <h4>Audit Information</h4>
                  <div className="kv">
                    <div><span className="muted">Created By</span></div><div><code className="code">{audit_info?.created_by || '—'}</code></div>
                    <div><span className="muted">Updated By</span></div><div><code className="code">{audit_info?.updated_by || '—'}</code></div>
                    <div><span className="muted">Notes</span></div><div><code className="code">{audit_info?.notes || '—'}</code></div>
                    <div><span className="muted">POS Export Status</span></div><div><code className="code">{audit_info?.pos_export_status || '—'}</code></div>
                  </div>
                </div>
                <div className="divider"></div>
                <div className="row right">
                  <button className="btn" onClick={() => handleExport('json')}>Copy JSON</button>
                  <button className="btn" onClick={() => handleExport('csv')}>Download CSV</button>
                </div>
              </>
            ) : (
              <div style={{ textAlign: 'center', padding: '40px' }}>No details available.</div>
            )}
          </div>
        </aside>
      </>
    );
  };

  return (
    <div className="page-container">
      <div className="panel" aria-labelledby="pdlLabel">
        <div className="panel-header">
          <h2 className="panel-title">PDL Coding - {tenant} Tenant</h2>
        </div>
        
        <div className="row">
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="Search NDC / PDL Drug / Key Code / Status / Template"
            aria-label="Search PDL Coding"
            style={{ minWidth: '320px', flex: 1 }}
            disabled={loading}
          />
          <button onClick={handleSearch} className="btn primary" disabled={loading}>
            {loading ? 'Searching...' : 'Search'}
          </button>
          <span className="hint">View-only; NDC opens details drawer</span>
        </div>
        
        {error && (
          <div className="alert error" style={{ marginTop: '16px' }}>
            <strong>Error:</strong> {error}
          </div>
        )}

        <div className="divider"></div>
        
        <div className="scroll">
          {loading ? (
            <div style={{ textAlign: 'center', padding: '40px' }}>
              Loading PDL data...
            </div>
          ) : records.length === 0 ? (
            <div style={{ textAlign: 'center', padding: '40px', color: 'var(--color-text-muted)' }}>
              {searchQuery ? 'No records found for your search.' : 'Enter a search term to find PDL records.'}
            </div>
          ) : (
            <table className="data-table">
              <thead>
                <tr>
                  <th>NDC</th>
                  <th>PDL Drug</th>
                  <th>Key Code</th>
                  <th>Status</th>
                  <th>Template</th>
                  <th>Effective</th>
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
                          onClick={(e) => {
                            e.preventDefault();
                            openDrawer(record.ndc);
                          }}
                        >
                          {record.ndc}
                        </a>
                      </code>
                    </td>
                    <td>{record.pdl_drug}</td>
                    <td><code>{record.key_code || '—'}</code></td>
                    <td>
                      <span className={`status ${record.status.toLowerCase().replace(' ', '-')}`}>
                        {record.status}
                      </span>
                    </td>
                    <td><code>{record.template || '—'}</code></td>
                    <td>{record.effective_date}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {renderDetailsDrawer()}
    </div>
  );
}
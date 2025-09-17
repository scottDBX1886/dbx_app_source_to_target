/**
 * Config Page
 * System configuration management for templates, exclusions, and settings
 */
import React, { useState, useEffect, useContext } from 'react';
import { TenantContext } from '../../App';

// Tenant-specific configuration datasets
const configDatasets = {
  MASTER: [
    { name: "keycode.template.default", value: "GSN|brand6|rx_otc|pkg6", desc: "Default PDL keycode template" },
    { name: "export.exclude.future", value: "true", desc: "Exclude future-dated rows from exports" },
    { name: "mbid.validation.strict", value: "true", desc: "Enable strict MBID validation rules" },
    { name: "review.auto.promote", value: "false", desc: "Auto-promote 100% matches to final review" },
    { name: "export.format.pos", value: "NDC,ACTION,EFF_DATE,STATUS", desc: "POS export file column format" },
    { name: "tenant.inheritance.enabled", value: "true", desc: "Allow child tenants to inherit MASTER records" }
  ],
  AK: [
    { name: "keycode.template.default", value: "GSN|brand6|rx_otc|pkg6", desc: "Default PDL keycode template (inherited)" },
    { name: "export.exclude.future", value: "true", desc: "Exclude future-dated rows from exports (inherited)" },
    { name: "ak.arctic.drug.preference", value: "true", desc: "Prefer Arctic-branded drugs in AK" },
    { name: "ak.manufacturer.local", value: "Alaska Pharma", desc: "Primary local manufacturer for AK" },
    { name: "review.auto.promote", value: "true", desc: "Auto-promote matches for efficiency" },
    { name: "export.format.pos", value: "NDC,ACTION,EFF_DATE,STATUS,AK_ORIGIN", desc: "AK-specific POS export format" }
  ],
  MO: [
    { name: "keycode.template.default", value: "GSN|brand6|rx_otc|pkg6", desc: "Default PDL keycode template (inherited)" },
    { name: "export.exclude.future", value: "false", desc: "Include future-dated rows for planning" },
    { name: "mo.show.me.branding", value: "true", desc: "Use 'Show-Me' branding for MO drugs" },
    { name: "mo.manufacturer.local", value: "Missouri Med", desc: "Primary local manufacturer for MO" },
    { name: "mo.controlled.substances", value: "restricted", desc: "Extra restrictions on controlled substances" },
    { name: "review.auto.promote", value: "false", desc: "Manual review required for all drugs" },
    { name: "export.format.pos", value: "NDC,ACTION,EFF_DATE,STATUS,CONTROL_CLASS", desc: "MO-specific POS export with control class" }
  ]
};

export function Config() {
  const { tenant } = useContext(TenantContext);
  const [searchQuery, setSearchQuery] = useState('');
  
  // Get current tenant's data
  const currentData = configDatasets[tenant as keyof typeof configDatasets] || configDatasets.MASTER;
  const [filteredData, setFilteredData] = useState(currentData);

  // Update filtered data when tenant changes
  useEffect(() => {
    const newData = configDatasets[tenant as keyof typeof configDatasets] || configDatasets.MASTER;
    setFilteredData(newData);
    setSearchQuery(''); // Clear search when tenant changes
  }, [tenant]);

  // Filter data based on search query
  const handleSearch = () => {
    if (!searchQuery.trim()) {
      setFilteredData(currentData);
    } else {
      const filtered = currentData.filter(record => 
        record.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        record.value.toLowerCase().includes(searchQuery.toLowerCase()) ||
        record.desc.toLowerCase().includes(searchQuery.toLowerCase())
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
      'Key,Value,Description',
      ...filteredData.map(row => 
        `"${row.name}","${row.value}","${row.desc}"`)
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `config_values_${tenant}_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="config-page">
      <div className="page-header">
        <h1>⚙️ Configuration</h1>
        <p className="muted">System configuration management for templates, exclusions, and operational settings.</p>
      </div>

      {/* Configuration Values */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">Config Values</h2>
          <span className="hint">Key • Value • Description</span>
        </div>
        
        <div className="row" style={{ marginBottom: '12px' }}>
          <input 
            type="text" 
            placeholder="Search Key / Value / Description" 
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
                <th>Key</th>
                <th>Value</th>
                <th>Description</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {filteredData.map((record, index) => (
                <tr key={`${tenant}-${record.name}-${index}`}>
                  <td><code>{record.name}</code></td>
                  <td><code>{record.value}</code></td>
                  <td>{record.desc}</td>
                  <td>
                    <button className="btn ghost" style={{ padding: '4px 8px', fontSize: '12px' }}>
                      Edit
                    </button>
                    <button className="btn ghost" style={{ padding: '4px 8px', fontSize: '12px', color: 'var(--err)' }}>
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* PDL Key Code Templates */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🏷️ PDL Key Code Templates</h2>
        </div>
        
        <div className="template-management">
          <h3>Template Configuration</h3>
          <div className="kv">
            <div className="muted">Active Template</div>
            <div>
              <select style={{ minWidth: '200px' }}>
                <option value="default" selected>keycode.template.default</option>
                <option value="generic">keycode.template.generic</option>
                <option value="manufacturer">keycode.template.manufacturer</option>
              </select>
            </div>
            
            <div className="muted">Pattern</div>
            <div><code>GSN|brand6|rx_otc|pkg6</code></div>
            
            <div className="muted">Description</div>
            <div>GSN + first 6 of Brand + RX/OTC + Package size</div>
          </div>
          
          <div className="divider"></div>
          
          <h3>Available Templates</h3>
          <div className="template-examples">
            <div className="template-item">
              <strong>Default Template:</strong>
              <code>GSN + first 6 of Brand + RX/OTC + Package size</code>
              <div className="muted">Example: 10001|AMOXIC|RX|30</div>
            </div>
            
            <div className="template-item">
              <strong>Generic Template:</strong>
              <code>GSN + first 6 of Brand + RX/OTC + first 6 of Generic</code>
              <div className="muted">Example: 10001|AMOXIC|RX|AMOXIC</div>
            </div>
            
            <div className="template-item">
              <strong>Manufacturer Template:</strong>
              <code>GSN + first 6 of Brand + RX/OTC + first 6 of MFR</code>
              <div className="muted">Example: 10001|AMOXIC|RX|ALPHA</div>
            </div>
          </div>
        </div>
      </div>

      {/* Exclusion Rules */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🚫 Exclusion Rules</h2>
        </div>
        
        <div className="exclusion-config">
          <h3>Automatic Exclusions</h3>
          <div className="exclusion-rules">
            <div className="exclusion-rule">
              <input type="checkbox" defaultChecked />
              <strong>Federally Rebateable</strong>
              <span className="muted">Exclude NDCs marked as federally rebateable</span>
            </div>
            
            <div className="exclusion-rule">
              <input type="checkbox" defaultChecked />
              <strong>MFT Obsolete</strong>
              <span className="muted">Exclude obsolete manufacturer records</span>
            </div>
            
            <div className="exclusion-rule">
              <input type="checkbox" defaultChecked />
              <strong>Future-Dated Rows</strong>
              <span className="muted">Exclude records with future effective dates</span>
            </div>
            
            <div className="exclusion-rule">
              <input type="checkbox" />
              <strong>Specific NDC List</strong>
              <span className="muted">Exclude manually specified NDCs</span>
              <button className="btn ghost" style={{ padding: '4px 8px', fontSize: '12px' }}>Manage List</button>
            </div>
          </div>
          
          <div className="divider"></div>
          
          <h3>Override Hierarchy (Config-Driven)</h3>
          <div className="override-hierarchy">
            <div className="hierarchy-item">
              <span className="status success">1</span>
              <strong>NDC</strong> - Specific NDC override (highest priority)
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
              <strong>GC3/HIC3</strong> - Generic code 3 / HIC level 3 (primary)
            </div>
            <div className="hierarchy-item">
              <span className="status success">5</span>
              <strong>DCC</strong> - Drug category code (lowest priority)
            </div>
          </div>
        </div>
      </div>

      {/* System Settings */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🖥️ System Settings</h2>
        </div>
        
        <div className="system-settings">
          <h3>Database Configuration</h3>
          <div className="kv">
            <div className="muted">OLTP Store</div>
            <div>Lakebase (operational rows)</div>
            
            <div className="muted">OLAP Store</div>
            <div>Delta (analytics & weekly extracts)</div>
            
            <div className="muted">FDB File Count</div>
            <div>~20 files aggregated</div>
            
            <div className="muted">Update Frequency</div>
            <div>Daily FDB loads, Weekly reviews</div>
          </div>
          
          <div className="divider"></div>
          
          <h3>Export Settings</h3>
          <div className="kv">
            <div className="muted">POS Export Path</div>
            <div><code>/exports/pos/</code></div>
            
            <div className="muted">CSV Export Path</div>
            <div><code>/exports/csv/</code></div>
            
            <div className="muted">Retention Period</div>
            <div>90 days</div>
            
            <div className="muted">Compression</div>
            <div>
              <input type="checkbox" defaultChecked /> Enable GZIP compression
            </div>
          </div>
          
          <div className="divider"></div>
          
          <h3>Validation Settings</h3>
          <div className="validation-settings">
            <div className="validation-rule">
              <input type="checkbox" defaultChecked />
              <strong>MBID Conflict Detection</strong>
              <span className="muted">Warn if FMT Drug Name falls in two MBIDs</span>
            </div>
            
            <div className="validation-rule">
              <input type="checkbox" defaultChecked />
              <strong>Status Conflict Blocking</strong>
              <span className="muted">Block approve if NDCs for same FMT Drug across MBIDs differ in status</span>
            </div>
            
            <div className="validation-rule">
              <input type="checkbox" defaultChecked />
              <strong>Date Range Validation</strong>
              <span className="muted">Validate effective date ranges for overlaps</span>
            </div>
          </div>
        </div>
      </div>

      {/* API Integration */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🔗 Future API Integration</h2>
        </div>
        
        <div className="alert info">
          <strong>Configuration API Endpoints</strong>
          <div className="muted mt-1">
            Configuration management will be integrated with Lakebase OLTP for persistent settings.
          </div>
        </div>
        
        <div className="kv">
          <div className="muted">Config Endpoint</div>
          <div><code>GET/POST /api/config</code></div>
          
          <div className="muted">Template Endpoint</div>
          <div><code>GET/POST /api/config/templates</code></div>
          
          <div className="muted">Exclusion Endpoint</div>
          <div><code>GET/POST /api/config/exclusions</code></div>
          
          <div className="muted">Operations</div>
          <div>
            <ul>
              <li>Dynamic configuration updates without deployment</li>
              <li>Template validation and testing</li>
              <li>Exclusion rule management</li>
              <li>System setting persistence</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

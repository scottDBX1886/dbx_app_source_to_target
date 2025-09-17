/**
 * Settings Page - Authentication Configuration
 */

export function Settings() {
  return (
    <div className="settings-page">
      <div className="page-header">
        <h1>⚙️ Authentication Settings</h1>
        <p className="muted">Configuration for hybrid authentication model (User + Service Principal)</p>
      </div>

      {/* Authentication Overview */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">Authentication Overview</h2>
        </div>
        
        <div className="auth-overview">
          <div className="alert info mb-2">
            <strong>Hybrid Authentication Model</strong>
            <div className="muted mt-1">
              This app uses both User Authentication (for data access) and Service Principal (for infrastructure operations)
            </div>
          </div>
          
          <div className="auth-flow">
            <div className="auth-step">
              <div className="step-icon">👤</div>
              <div className="step-content">
                <h3>User Authentication</h3>
                <p>Databricks Apps forwards user identity via HTTP headers</p>
                <ul>
                  <li>Real user identity verification</li>
                  <li>Data access scoped to user permissions</li>
                  <li>Unity Catalog authorization</li>
                </ul>
              </div>
            </div>
            
            <div className="step-divider">+</div>
            
            <div className="auth-step">
              <div className="step-icon">🤖</div>
              <div className="step-content">
                <h3>Service Principal</h3>
                <p>Automated service account for infrastructure operations</p>
                <ul>
                  <li>Cluster management operations</li>
                  <li>Workspace-level operations</li>
                  <li>Job and pipeline management</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Service Principal Configuration */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🤖 Service Principal Configuration</h2>
          <span className="status success">Configured</span>
        </div>
        
        <div className="kv">
          <div className="muted">Client ID</div>
          <div className="code">sp-****-****-****</div>
          
          <div className="muted">Status</div>
          <div>
            <span className="status success">Active</span>
          </div>
          
          <div className="muted">OAuth Scopes</div>
          <div>
            <span className="pill" style={{ marginRight: '6px' }}>databricks-api</span>
            <span className="pill">offline_access</span>
          </div>
          
          <div className="muted">Permissions</div>
          <div className="permissions-grid">
            <div className="permission-item">
              <span className="status success">✓</span>
              <span>clusters:read</span>
            </div>
            <div className="permission-item">
              <span className="status success">✓</span>
              <span>clusters:write</span>
            </div>
            <div className="permission-item">
              <span className="status success">✓</span>
              <span>sql:read</span>
            </div>
            <div className="permission-item">
              <span className="status success">✓</span>
              <span>sql:write</span>
            </div>
            <div className="permission-item">
              <span className="status success">✓</span>
              <span>workspace:read</span>
            </div>
          </div>
        </div>
      </div>

      {/* User Authentication Configuration */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">👤 User Authentication Configuration</h2>
          <span className="status success">Enabled</span>
        </div>
        
        <div className="user-auth-info">
          <div className="alert info mb-2">
            <strong>Databricks Apps User Forwarding</strong>
            <div className="muted mt-1">
              Authentication is handled automatically by Databricks Apps platform
            </div>
          </div>
          
          <div className="kv">
            <div className="muted">Authentication Method</div>
            <div>Databricks Apps User Forwarding</div>
            
            <div className="muted">User Identity Source</div>
            <div>HTTP Headers (X-Forwarded-*)</div>
            
            <div className="muted">Token Management</div>
            <div>Automatic token forwarding</div>
            
            <div className="muted">Session Management</div>
            <div>Platform-managed sessions</div>
          </div>
        </div>
      </div>

      {/* Authorization Matrix */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🔐 Authorization Matrix</h2>
        </div>
        
        <div className="auth-matrix">
          <table className="data-table">
            <thead>
              <tr>
                <th>Operation Type</th>
                <th>User Auth</th>
                <th>Service Principal</th>
                <th>Purpose</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Data Access</td>
                <td><span className="status success">✓ Primary</span></td>
                <td><span className="status muted">○ Not Used</span></td>
                <td>User-scoped data permissions</td>
              </tr>
              <tr>
                <td>Cluster Management</td>
                <td><span className="status muted">○ Not Used</span></td>
                <td><span className="status success">✓ Primary</span></td>
                <td>Infrastructure operations</td>
              </tr>
              <tr>
                <td>SQL Warehouses</td>
                <td><span className="status muted">○ Not Used</span></td>
                <td><span className="status success">✓ Primary</span></td>
                <td>Compute resource management</td>
              </tr>
              <tr>
                <td>Unity Catalog</td>
                <td><span className="status success">✓ Primary</span></td>
                <td><span className="status warning">△ Fallback</span></td>
                <td>Data governance & permissions</td>
              </tr>
              <tr>
                <td>Job Management</td>
                <td><span className="status muted">○ Not Used</span></td>
                <td><span className="status success">✓ Primary</span></td>
                <td>Workflow orchestration</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

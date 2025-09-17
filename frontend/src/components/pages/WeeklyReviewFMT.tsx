/**
 * Weekly Review – FMT Page
 * Weekly review process for FMT with dual reviewer workflow
 */

export function WeeklyReviewFMT() {
  return (
    <div className="weekly-review-fmt-page">
      <div className="page-header">
        <h1>📅 Weekly Review – FMT</h1>
        <p className="muted">Weekly review process for new FMT drugs with dual reviewer workflow and final approval.</p>
      </div>

      {/* Week Selection */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">Week Selection & Pool</h2>
        </div>
        
        <div className="row">
          <div className="row" style={{ gap: '14px' }}>
            <label className="muted">Week ending</label>
            <input type="date" defaultValue="2024-12-15" />
          </div>
          <div className="row" style={{ gap: '8px', flex: 1 }}>
            <input 
              type="text" 
              placeholder="Search pool: NDC / Brand / GSN / HIC3 / MFR" 
              style={{ minWidth: '320px', flex: 1 }}
            />
            <button className="btn primary">Refresh</button>
          </div>
        </div>
        
        <div className="muted" style={{ marginTop: '8px' }}>
          Auto-selects latest week with data on first load. If selected week has none, pick another date.
        </div>
        
        <div className="divider"></div>
        
        <div className="pool-summary">
          <span className="muted">12 new drugs (100% match: 3, gsn match: 4, brand match: 2, no match: 3)</span>
        </div>
      </div>

      {/* Review Groups */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">📊 Review Groups</h2>
        </div>
        
        {/* 100% Match Group */}
        <details className="group" open>
          <summary>
            <span className="title">100% match</span>
            <span className="counts">
              <span className="c">A=1</span>
              <span className="c">B=1</span>
              <span className="c">both=1</span>
              <span className="c">rejected=0</span>
              <span className="c">pending=0</span>
            </span>
          </summary>
          
          <div className="panel" style={{ margin: '10px' }}>
            <div className="row">
              <label><input type="checkbox" /> Select all in '100% match'</label>
              <div className="row" style={{ marginLeft: 'auto', gap: '8px' }}>
                <button className="btn success">Add to Reviewer A</button>
                <button className="btn primary">Add to Reviewer B</button>
                <button className="btn warn">Reject selected</button>
              </div>
            </div>
            
            <div className="divider"></div>
            
            <div className="scroll">
              <table className="data-table">
                <thead>
                  <tr>
                    <th></th>
                    <th>NDC</th>
                    <th>Brand</th>
                    <th>GSN</th>
                    <th>HIC3</th>
                    <th>MFR</th>
                    <th>Load</th>
                    <th>Suggested MBID</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td><input type="checkbox" /></td>
                    <td><code>22233344455</code></td>
                    <td>Motrin</td>
                    <td>40002</td>
                    <td>555</td>
                    <td>Omega</td>
                    <td>2024-12-15</td>
                    <td><code>-</code></td>
                    <td><span className="status both">both</span></td>
                  </tr>
                  <tr>
                    <td><input type="checkbox" /></td>
                    <td><code>77766655544</code></td>
                    <td>AllerEase</td>
                    <td>50001</td>
                    <td>456</td>
                    <td>Delta</td>
                    <td>2024-12-15</td>
                    <td><code>AK123456</code></td>
                    <td><span className="status A">A</span></td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </details>

        {/* GSN Match Group */}
        <details className="group">
          <summary>
            <span className="title">gsn match</span>
            <span className="counts">
              <span className="c">A=2</span>
              <span className="c">B=1</span>
              <span className="c">both=0</span>
              <span className="c">rejected=0</span>
              <span className="c">pending=1</span>
            </span>
          </summary>
        </details>

        {/* Brand Match Group */}
        <details className="group">
          <summary>
            <span className="title">brand match</span>
            <span className="counts">
              <span className="c">A=1</span>
              <span className="c">B=0</span>
              <span className="c">both=0</span>
              <span className="c">rejected=0</span>
              <span className="c">pending=1</span>
            </span>
          </summary>
        </details>

        {/* No Match Group */}
        <details className="group">
          <summary>
            <span className="title">no match</span>
            <span className="counts">
              <span className="c">A=0</span>
              <span className="c">B=1</span>
              <span className="c">both=0</span>
              <span className="c">rejected=0</span>
              <span className="c">pending=2</span>
            </span>
          </summary>
        </details>
      </div>

      {/* Reviewer A */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">👤 Reviewer A – Assign MBID / Dates</h2>
        </div>
        
        <div className="scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>NDC</th>
                <th>MBID</th>
                <th>Eff Date</th>
                <th>End Date</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><code>77766655544</code></td>
                <td>
                  <select aria-label="MBID">
                    <option value="">(none)</option>
                    <option value="AK123456" selected>AK123456 - Antihistamines</option>
                    <option value="CV999001">CV999001 - Cardio – Statins</option>
                    <option value="IMM000777">IMM000777 - Immunology – Insulins</option>
                  </select>
                </td>
                <td><input type="date" defaultValue="2024-12-15" /></td>
                <td><input type="date" /></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {/* Reviewer B */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">👤 Reviewer B – Assign MBID / Dates</h2>
        </div>
        
        <div className="scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>NDC</th>
                <th>MBID</th>
                <th>Eff Date</th>
                <th>End Date</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><code>22233344455</code></td>
                <td>
                  <select aria-label="MBID">
                    <option value="">(none)</option>
                    <option value="AK123456">AK123456 - Antihistamines</option>
                    <option value="CV999001">CV999001 - Cardio – Statins</option>
                    <option value="IMM000777">IMM000777 - Immunology – Insulins</option>
                  </select>
                </td>
                <td><input type="date" defaultValue="2024-12-15" /></td>
                <td><input type="date" /></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {/* Compare & Resolve */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">⚖️ Compare & Resolve – Final Approver</h2>
        </div>
        
        <div className="scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>NDC</th>
                <th>A: MBID</th>
                <th>A: Eff</th>
                <th>A: End</th>
                <th>B: MBID</th>
                <th>B: Eff</th>
                <th>B: End</th>
                <th>Resolution</th>
                <th>Resolved Values</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><code>22233344455</code></td>
                <td><code>AK123456</code></td>
                <td>2024-12-15</td>
                <td></td>
                <td><code>AK123456</code></td>
                <td>2024-12-15</td>
                <td></td>
                <td>
                  <select>
                    <option value="AUTO" selected>AUTO (matched)</option>
                    <option value="A">Use A</option>
                    <option value="B">Use B</option>
                    <option value="CUSTOM">Custom</option>
                  </select>
                </td>
                <td>
                  <select>
                    <option value="AK123456" selected>AK123456 - Antihistamines</option>
                  </select>
                  <input type="date" defaultValue="2024-12-15" />
                  <input type="date" />
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        
        <div className="row right" style={{ marginTop: '12px' }}>
          <button className="btn primary">Promote Resolved → Final Review</button>
        </div>
      </div>

      {/* Final Review */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">✅ Final Review</h2>
          <span className="hint">Deduped by NDC</span>
          <button className="btn success" style={{ marginLeft: 'auto' }}>Approve & Sync</button>
        </div>
        
        <div className="scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>NDC</th>
                <th>MBID</th>
                <th>Eff</th>
                <th>End</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><code>22233344455</code></td>
                <td><code>AK123456</code></td>
                <td>2024-12-15</td>
                <td></td>
              </tr>
            </tbody>
          </table>
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
            Weekly review process will connect to Lakebase OLTP for operational data and approval workflows.
          </div>
        </div>
        
        <div className="kv">
          <div className="muted">Review Endpoint</div>
          <div><code>POST /api/review/submit</code></div>
          
          <div className="muted">Approval Endpoint</div>
          <div><code>POST /api/approve</code></div>
          
          <div className="muted">Operations</div>
          <div>
            <ul>
              <li>Weekly pool generation from new FDB data</li>
              <li>Dual reviewer assignment and validation</li>
              <li>Conflict resolution and final approval</li>
              <li>Automatic sync to FMT Master upon approval</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

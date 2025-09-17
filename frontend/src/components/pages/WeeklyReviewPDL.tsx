/**
 * Weekly Review – PDL Page
 * Weekly review process for PDL with automated POS export generation
 */

export function WeeklyReviewPDL() {
  return (
    <div className="weekly-review-pdl-page">
      <div className="page-header">
        <h1>📋 Weekly Review – PDL</h1>
        <p className="muted">Weekly review process for PDL coding with automated POS export file generation.</p>
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
          <span className="muted">8 new drugs for PDL coding (100% match: 2, gsn match: 3, brand match: 2, no match: 1)</span>
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
              <span className="c">both=0</span>
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
                    <th>Suggested Key Code</th>
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
                    <td><code>40002|MOTRIN|RX|60</code></td>
                    <td><span className="status A">A</span></td>
                  </tr>
                  <tr>
                    <td><input type="checkbox" /></td>
                    <td><code>77766655544</code></td>
                    <td>AllerEase</td>
                    <td>50001</td>
                    <td>456</td>
                    <td>Delta</td>
                    <td>2024-12-15</td>
                    <td><code>50001|ALLERE|OTC|30</code></td>
                    <td><span className="status B">B</span></td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </details>

        {/* Other Groups (collapsed for brevity) */}
        <details className="group">
          <summary>
            <span className="title">gsn match</span>
            <span className="counts">
              <span className="c">A=1</span>
              <span className="c">B=1</span>
              <span className="c">both=0</span>
              <span className="c">rejected=0</span>
              <span className="c">pending=1</span>
            </span>
          </summary>
        </details>

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

        <details className="group">
          <summary>
            <span className="title">no match</span>
            <span className="counts">
              <span className="c">A=0</span>
              <span className="c">B=0</span>
              <span className="c">both=0</span>
              <span className="c">rejected=0</span>
              <span className="c">pending=1</span>
            </span>
          </summary>
        </details>
      </div>

      {/* Reviewer A */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">👤 Reviewer A – PDL Key Code Assignment</h2>
        </div>
        
        <div className="scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>NDC</th>
                <th>Generated Key Code</th>
                <th>Custom Key Code</th>
                <th>Eff Date</th>
                <th>End Date</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><code>22233344455</code></td>
                <td><code>40002|MOTRIN|RX|60</code></td>
                <td><input type="text" placeholder="Override if needed" /></td>
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
          <h2 className="panel-title">👤 Reviewer B – PDL Key Code Assignment</h2>
        </div>
        
        <div className="scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>NDC</th>
                <th>Generated Key Code</th>
                <th>Custom Key Code</th>
                <th>Eff Date</th>
                <th>End Date</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><code>77766655544</code></td>
                <td><code>50001|ALLERE|OTC|30</code></td>
                <td><input type="text" placeholder="Override if needed" /></td>
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
                <th>A: Key Code</th>
                <th>A: Dates</th>
                <th>B: Key Code</th>
                <th>B: Dates</th>
                <th>Resolution</th>
                <th>Final Key Code</th>
                <th>Final Dates</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><code>22233344455</code></td>
                <td><code>40002|MOTRIN|RX|60</code></td>
                <td>2024-12-15</td>
                <td>-</td>
                <td>-</td>
                <td>
                  <select>
                    <option value="A" selected>Use A</option>
                    <option value="B">Use B</option>
                    <option value="CUSTOM">Custom</option>
                  </select>
                </td>
                <td><input type="text" defaultValue="40002|MOTRIN|RX|60" /></td>
                <td>
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
          <h2 className="panel-title">✅ Final Review & POS Export</h2>
          <span className="hint">Deduped by NDC</span>
          <button className="btn success" style={{ marginLeft: 'auto' }}>Approve & Generate POS Export</button>
        </div>
        
        <div className="scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>NDC</th>
                <th>PDL Key Code</th>
                <th>Eff</th>
                <th>End</th>
                <th>POS Action</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><code>22233344455</code></td>
                <td><code>40002|MOTRIN|RX|60</code></td>
                <td>2024-12-15</td>
                <td></td>
                <td><span className="status success">ADD</span></td>
              </tr>
              <tr>
                <td><code>77766655544</code></td>
                <td><code>50001|ALLERE|OTC|30</code></td>
                <td>2024-12-15</td>
                <td></td>
                <td><span className="status success">ADD</span></td>
              </tr>
            </tbody>
          </table>
        </div>
        
        <div className="alert success" style={{ marginTop: '12px' }}>
          <strong>POS Export Preview</strong>
          <div className="muted mt-1">
            File: <code>pos_export_2024-12-15.csv</code> | Records: 2 | Actions: 2 ADD, 0 REMOVE
          </div>
        </div>
      </div>

      {/* Key Code Templates */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">🔧 Active Key Code Template</h2>
        </div>
        
        <div className="kv">
          <div className="muted">Template Name</div>
          <div><code>keycode.template.default</code></div>
          
          <div className="muted">Template Pattern</div>
          <div><code>GSN|brand6|rx_otc|pkg6</code></div>
          
          <div className="muted">Description</div>
          <div>Default PDL keycode template</div>
          
          <div className="muted">Example</div>
          <div><code>40002|MOTRIN|RX|60</code> → GSN: 40002, Brand: MOTRIN (first 6), RX/OTC: RX, Package: 60</div>
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
            PDL weekly review will connect to Lakebase OLTP for data management and automated POS export generation.
          </div>
        </div>
        
        <div className="kv">
          <div className="muted">Review Endpoint</div>
          <div><code>POST /api/review/pdl-submit</code></div>
          
          <div className="muted">Approval Endpoint</div>
          <div><code>POST /api/approve/pdl</code></div>
          
          <div className="muted">Operations</div>
          <div>
            <ul>
              <li>Automated PDL key code generation using configurable templates</li>
              <li>Dual reviewer validation and conflict resolution</li>
              <li>POS export file generation (CSV format)</li>
              <li>Automatic sync to PDL Master upon approval</li>
              <li>Weekly scheduling via Delta OLAP</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

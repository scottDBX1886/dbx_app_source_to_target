/**
 * Weekly Review – PDL Page
 * Weekly review process for PDL with automated POS export generation
 */

import { useState, useEffect, useContext } from 'react';
import { TenantContext } from '../../App';
import { weeklyReviewApi, type WeeklyPoolResponse, type PDLAssignment } from '../../services/weekly-review-api';

export function WeeklyReviewPDL() {
  const { tenant } = useContext(TenantContext);
  const [weekEnding, setWeekEnding] = useState('2024-12-15');
  const [searchQuery, setSearchQuery] = useState('');
  const [poolData, setPoolData] = useState<WeeklyPoolResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedNDCs, setSelectedNDCs] = useState<Set<string>>(new Set());
  const [assignmentLoading, setAssignmentLoading] = useState(false);

  // Load pool data on component mount and when dependencies change
  useEffect(() => {
    loadPoolData();
  }, [tenant, weekEnding]);

  const loadPoolData = async () => {
    setLoading(true);
    setError(null);
    try {
      const poolResponse = await weeklyReviewApi.getWeeklyPool('pdl', tenant, weekEnding, searchQuery || undefined);
      
      setPoolData(poolResponse);
    } catch (err) {
      console.error('Error loading weekly review data:', err);
      setError(err instanceof Error ? err.message : 'Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = () => {
    loadPoolData();
  };

  const handleNDCSelection = (ndc: string, isChecked: boolean) => {
    const newSelected = new Set(selectedNDCs);
    if (isChecked) {
      newSelected.add(ndc);
    } else {
      newSelected.delete(ndc);
    }
    setSelectedNDCs(newSelected);
  };

  const handleSelectAllVisible = (isChecked: boolean) => {
    if (!poolData) return;
    
    const newSelected = new Set(selectedNDCs);
    poolData.pool_data.forEach(record => {
      if (isChecked) {
        newSelected.add(record.ndc);
      } else {
        newSelected.delete(record.ndc);
      }
    });
    setSelectedNDCs(newSelected);
  };

  const handleAssignToReviewer = async (reviewer: 'A' | 'B') => {
    if (selectedNDCs.size === 0) {
      alert('Please select at least one NDC to assign.');
      return;
    }

    setAssignmentLoading(true);
    try {
      const assignments: PDLAssignment[] = Array.from(selectedNDCs).map(ndc => ({
        ndc,
        reviewer,
        keycode: '', // TODO: Add keycode suggestion logic
        template: 'PENDING_REVIEW', // Default template
        eff_date: weekEnding
      }));

      await weeklyReviewApi.assignReviews({
        review_type: 'pdl',
        tenant,
        week_ending: weekEnding,
        assignments
      });

      // Clear selections and reload data to show updated status
      setSelectedNDCs(new Set());
      await loadPoolData();
      
      alert(`Successfully assigned ${assignments.length} NDCs to Reviewer ${reviewer}`);
    } catch (error) {
      console.error('Error assigning PDL reviews:', error);
      alert(`Failed to assign NDCs: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      setAssignmentLoading(false);
    }
  };

  const handleRejectSelected = async () => {
    if (selectedNDCs.size === 0) {
      alert('Please select at least one NDC to reject.');
      return;
    }

    const reason = prompt('Enter rejection reason:');
    if (!reason) return;

    setAssignmentLoading(true);
    try {
      await weeklyReviewApi.rejectItems({
        review_type: 'pdl',
        tenant,
        week_ending: weekEnding,
        rejected_ndcs: Array.from(selectedNDCs),
        rejection_reason: reason
      });

      // Clear selections and reload data
      setSelectedNDCs(new Set());
      await loadPoolData();
      
      alert(`Successfully rejected ${selectedNDCs.size} NDCs`);
    } catch (error) {
      console.error('Error rejecting PDL NDCs:', error);
      alert(`Failed to reject NDCs: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      setAssignmentLoading(false);
    }
  };
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
            <input 
              type="date" 
              value={weekEnding}
              onChange={(e) => setWeekEnding(e.target.value)}
            />
          </div>
          <div className="row" style={{ gap: '8px', flex: 1 }}>
            <input 
              type="text" 
              placeholder="Search pool: NDC / Brand / GSN / HIC3 / MFR" 
              style={{ minWidth: '320px', flex: 1 }}
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
            />
            <button className="btn primary" onClick={handleSearch} disabled={loading}>
              {loading ? 'Loading...' : 'Refresh'}
            </button>
          </div>
        </div>
        
        <div className="muted" style={{ marginTop: '8px' }}>
          Auto-selects latest week with data on first load. If selected week has none, pick another date.
        </div>
        
        <div className="divider"></div>
        
        <div className="pool-summary">
          {error ? (
            <span className="muted error">Error: {error}</span>
          ) : poolData ? (
            <span className="muted">
              {poolData.summary.total_new_drugs} new drugs for PDL coding ({Object.entries(poolData.summary.match_counts).map(([type, count]) => `${type}: ${count}`).join(', ')})
            </span>
          ) : (
            <span className="muted">{loading ? 'Loading pool data...' : 'No data loaded'}</span>
          )}
        </div>
      </div>

        {/* Review Groups - Dynamic Data */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">📊 PDL Review Pool</h2>
        </div>
        
        {loading && (
          <div style={{ padding: '20px', textAlign: 'center' }}>Loading PDL pool data...</div>
        )}

        {error && (
          <div className="alert error" style={{ margin: '10px' }}>{error}</div>
        )}

        {poolData && (
          <details className="group" open>
            <summary>
              <span className="title">PDL Pool ({poolData.summary.total_new_drugs} new NDCs)</span>
              <span className="counts">
                {Object.entries(poolData.summary.match_counts).map(([type, count]) => (
                  <span key={type} className="c">{type}={count}</span>
                ))}
              </span>
            </summary>
            
            <div className="panel" style={{ margin: '10px' }}>
              <div className="row">
                <label>
                  <input 
                    type="checkbox" 
                    onChange={(e) => handleSelectAllVisible(e.target.checked)}
                    checked={poolData.pool_data.every(record => selectedNDCs.has(record.ndc))}
                  /> 
                  Select all visible
                </label>
                <div className="row" style={{ marginLeft: 'auto', gap: '8px' }}>
                  <button 
                    className="btn success" 
                    onClick={() => handleAssignToReviewer('A')}
                    disabled={assignmentLoading || selectedNDCs.size === 0}
                  >
                    Add to Reviewer A
                  </button>
                  <button 
                    className="btn primary" 
                    onClick={() => handleAssignToReviewer('B')}
                    disabled={assignmentLoading || selectedNDCs.size === 0}
                  >
                    Add to Reviewer B
                  </button>
                  <button 
                    className="btn warn" 
                    onClick={handleRejectSelected}
                    disabled={assignmentLoading || selectedNDCs.size === 0}
                  >
                    Reject selected
                  </button>
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
                      <th>Match Type</th>
                    </tr>
                  </thead>
                  <tbody>
                    {poolData.pool_data.map((record) => (
                      <tr key={record.ndc}>
                        <td>
                          <input 
                            type="checkbox" 
                            checked={selectedNDCs.has(record.ndc)}
                            onChange={(e) => handleNDCSelection(record.ndc, e.target.checked)}
                          />
                        </td>
                        <td><code>{record.ndc}</code></td>
                        <td>{record.brand}</td>
                        <td>{record.gsn}</td>
                        <td>{record.hic3}</td>
                        <td>{record.mfr}</td>
                        <td>{record.load_date}</td>
                        <td><code>-</code></td>
                        <td><span className={`status ${record.match_type.replace(/\s+/g, '-')}`}>{record.match_type}</span></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </details>
        )}

        
        {!poolData && !loading && (
          <div className="muted" style={{ padding: '20px', textAlign: 'center' }}>
            No PDL pool data available. Try refreshing or selecting a different week.
          </div>
        )}

        {selectedNDCs.size > 0 && (
          <div className="alert info" style={{ margin: '10px 0' }}>
            {selectedNDCs.size} NDC{selectedNDCs.size !== 1 ? 's' : ''} selected
          </div>
        )}
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

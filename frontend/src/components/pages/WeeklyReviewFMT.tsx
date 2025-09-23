/**
 * Weekly Review – FMT Page
 * Weekly review process for FMT with dual reviewer workflow
 */

import { useState, useEffect, useContext } from 'react';
import { TenantContext } from '../../App';
import { weeklyReviewApi, type WeeklyPoolResponse, type ReviewGroupsResponse } from '../../services/weekly-review-api';

export function WeeklyReviewFMT() {
  const { tenant } = useContext(TenantContext);
  const [weekEnding, setWeekEnding] = useState('2024-12-15');
  const [searchQuery, setSearchQuery] = useState('');
  const [poolData, setPoolData] = useState<WeeklyPoolResponse | null>(null);
  const [groupsData, setGroupsData] = useState<ReviewGroupsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load pool data on component mount and when dependencies change
  useEffect(() => {
    loadPoolData();
  }, [tenant, weekEnding]);

  const loadPoolData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [poolResponse, groupsResponse] = await Promise.all([
        weeklyReviewApi.getWeeklyPool('fmt', tenant, weekEnding, searchQuery || undefined),
        weeklyReviewApi.getReviewGroups('fmt', tenant, weekEnding)
      ]);
      
      setPoolData(poolResponse);
      setGroupsData(groupsResponse);
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
              {poolData.summary.total_new_drugs} new drugs ({Object.entries(poolData.summary.match_counts).map(([type, count]) => `${type}: ${count}`).join(', ')})
            </span>
          ) : (
            <span className="muted">{loading ? 'Loading pool data...' : 'No data loaded'}</span>
          )}
        </div>
      </div>

      {/* Review Groups */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">📊 Review Groups</h2>
        </div>
        
        {groupsData && Object.entries(groupsData.groups).map(([matchType, group]) => (
          <details className="group" key={matchType} open={matchType === '100% match'}>
            <summary>
              <span className="title">{matchType}</span>
              <span className="counts">
                <span className="c">A={group.counts.A}</span>
                <span className="c">B={group.counts.B}</span>
                <span className="c">both={group.counts.both}</span>
                <span className="c">rejected={group.counts.rejected}</span>
                <span className="c">pending={group.counts.pending}</span>
              </span>
            </summary>
            
            <div className="panel" style={{ margin: '10px' }}>
              <div className="row">
                <label><input type="checkbox" /> Select all in '{matchType}'</label>
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
                    {group.records.map((record) => (
                      <tr key={record.ndc}>
                        <td><input type="checkbox" /></td>
                        <td><code>{record.ndc}</code></td>
                        <td>{record.brand}</td>
                        <td>{record.gsn}</td>
                        <td>{record.hic3}</td>
                        <td>{record.mfr}</td>
                        <td>{record.load_date}</td>
                        <td><code>{record.suggested_mbid || '-'}</code></td>
                        <td><span className={`status ${record.status}`}>{record.status}</span></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </details>
        ))}
        
        {!groupsData && !loading && (
          <div className="muted" style={{ padding: '20px', textAlign: 'center' }}>
            No review groups data available. Try refreshing or selecting a different week.
          </div>
        )}
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

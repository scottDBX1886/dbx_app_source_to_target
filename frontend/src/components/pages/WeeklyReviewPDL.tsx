/**
 * Weekly Review – PDL Page
 * Weekly review process for PDL with automated POS export generation
 */

import { useState, useEffect, useContext } from 'react';
import { TenantContext } from '../../App';
import { weeklyReviewApi, type WeeklyPoolResponse } from '../../services/weekly-review-api';

export function WeeklyReviewPDL() {
  const { tenant } = useContext(TenantContext);
  const [weekEnding, setWeekEnding] = useState('2024-12-15');
  const [searchQuery, setSearchQuery] = useState('');
  const [poolData, setPoolData] = useState<WeeklyPoolResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedNDCs, setSelectedNDCs] = useState<Set<string>>(new Set());
  const [reviewerAData, setReviewerAData] = useState<any[]>([]);
  const [reviewerBData, setReviewerBData] = useState<any[]>([]);
  const [finalApprovalData, setFinalApprovalData] = useState<any[]>([]);
  const [comparisonData, setComparisonData] = useState<any[]>([]);
  const [userKeycodeSelections] = useState<Record<string, string>>({});
  
  // Single data source - all data comes from this
  const [masterData, setMasterData] = useState<any[]>([]);

  // Load pool data on component mount and when dependencies change
  useEffect(() => {
    loadPoolData();
  }, [tenant, weekEnding]);

  // Debug effect to log when comparison data changes
  useEffect(() => {
    console.log('DEBUG: PDL Comparison data updated:', comparisonData);
  }, [comparisonData]);

  // Debug effect to log when user selections change
  useEffect(() => {
    console.log('DEBUG: PDL User keycode selections updated:', userKeycodeSelections);
  }, [userKeycodeSelections]);

  const loadPoolData = async () => {
    setLoading(true);
    setError(null);
    try {
      console.log('DEBUG: Loading PDL pool data with search query:', searchQuery);
      const poolResponse = await weeklyReviewApi.getWeeklyPool('pdl', tenant, weekEnding, searchQuery || undefined);
      
      console.log('DEBUG: PDL Pool response records:', poolResponse.pool_data?.length);
      setPoolData(poolResponse);
      
      // Set master data - single source of truth
      setMasterData(poolResponse.pool_data || []);
      
      // Initialize other data as empty (frontend-only management)
      setReviewerAData([]);
      setReviewerBData([]);
      setFinalApprovalData([]);
      setComparisonData([]);
      
      // Transform pool data to groups format (not used in PDL)
      transformPoolDataToGroups(poolResponse);
      
    } catch (err) {
      console.error('Error loading weekly review data:', err);
      setError(err instanceof Error ? err.message : 'Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  // Transform pool data to groups format (similar to FMT)
  const transformPoolDataToGroups = (poolResponse: any) => {
    if (!poolResponse.pool_data || poolResponse.pool_data.length === 0) {
      return {
        groups: {},
        tenant: poolResponse.tenant,
        week_ending: poolResponse.week_ending,
        review_type: poolResponse.review_type
      };
    }

    const groups: any = {};
    poolResponse.pool_data.forEach((record: any) => {
      const matchType = record.match_type || 'no match';
      if (!groups[matchType]) {
        groups[matchType] = {
          records: [],
          counts: { 'A': 0, 'B': 0, 'both': 0, 'rejected': 0, 'pending': 0 }
        };
      }
      groups[matchType].records.push({
        ndc: record.ndc,
        brand: record.brand,
        load_date: record.load_date,
        status: 'pending', // Hardcoded 'pending' status
        suggested_keycode: ''
      });
      groups[matchType].counts.pending += 1;
    });
    return {
      groups,
      tenant: poolResponse.tenant,
      week_ending: poolResponse.week_ending,
      review_type: poolResponse.review_type
    };
  };

  // Update comparison data when reviewer data changes
  const updateComparisonData = () => {
    const reviewerANDCs = new Set(reviewerAData.map(item => item.ndc));
    const reviewerBNDCs = new Set(reviewerBData.map(item => item.ndc));
    
    const commonNDCs = [...reviewerANDCs].filter(ndc => reviewerBNDCs.has(ndc));
    
    const newComparisonData = commonNDCs.map(ndc => {
      const reviewerAItem = reviewerAData.find(item => item.ndc === ndc);
      const reviewerBItem = reviewerBData.find(item => item.ndc === ndc);
      
      return {
        ndc,
        reviewer_a: reviewerAItem,
        reviewer_b: reviewerBItem,
        auto_resolution: reviewerAItem?.keycode === reviewerBItem?.keycode ? 'AUTO' : 'CUSTOM',
        user_selected_keycode: userKeycodeSelections[ndc] || '',
        updated_at: new Date().toISOString()
      };
    });
    
    setComparisonData(newComparisonData);
  };

  // Update comparison data when reviewer data or user selections change
  useEffect(() => {
    updateComparisonData();
  }, [reviewerAData, reviewerBData, userKeycodeSelections]);

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

  const handleAssignToReviewer = (reviewer: 'A' | 'B') => {
    if (selectedNDCs.size === 0) {
      alert('Please select at least one NDC to assign.');
      return;
    }

    // Get selected records from master data
    const selectedRecords = masterData.filter(record => selectedNDCs.has(record.ndc));
    const newAssignments = selectedRecords.map(record => ({
      ndc: record.ndc,
      brand: record.brand,
      keycode: '', // TODO: Add keycode suggestion logic
      template: 'PENDING_REVIEW', // Default template
      eff_date: weekEnding,
      end_date: null,
      assigned_at: new Date().toISOString()
    }));

    if (reviewer === 'A') {
      setReviewerAData(prev => [...prev, ...newAssignments]);
    } else {
      setReviewerBData(prev => [...prev, ...newAssignments]);
    }
    setSelectedNDCs(new Set());
    alert(`Successfully assigned ${newAssignments.length} NDCs to Reviewer ${reviewer}`);
  };

  const handleRejectSelected = () => {
    if (selectedNDCs.size === 0) {
      alert('Please select at least one NDC to reject.');
      return;
    }

    const reason = prompt('Enter rejection reason:');
    if (!reason) return;

    // Remove rejected NDCs from master data
    setMasterData(prev => prev.filter(record => !selectedNDCs.has(record.ndc)));
    setSelectedNDCs(new Set());
    alert(`Successfully rejected ${selectedNDCs.size} NDCs`);
  };

  const handlePromoteResolved = () => {
    if (comparisonData.length === 0) {
      alert('No conflicts to resolve.');
      return;
    }

    // Move resolved items to Final Review
    const resolvedItems = comparisonData.map(item => ({
      ndc: item.ndc,
      brand: item.reviewer_a?.brand || item.reviewer_b?.brand || '',
      keycode: item.user_selected_keycode || item.reviewer_a?.keycode || item.reviewer_b?.keycode || '',
      template: item.reviewer_a?.template || item.reviewer_b?.template || '',
      eff_date: item.reviewer_a?.eff_date || item.reviewer_b?.eff_date || weekEnding,
      end_date: item.reviewer_a?.end_date || item.reviewer_b?.end_date || null,
      resolved_at: new Date().toISOString()
    }));
    
    setFinalApprovalData(prev => [...prev, ...resolvedItems]);
    
    // Remove resolved NDCs from reviewer dataframes
    const resolvedNDCs = new Set(comparisonData.map(item => item.ndc));
    setReviewerAData(prev => prev.filter(item => !resolvedNDCs.has(item.ndc)));
    setReviewerBData(prev => prev.filter(item => !resolvedNDCs.has(item.ndc)));
    
    alert(`Successfully resolved ${resolvedItems.length} conflicts`);
  };

  const handleApproveSync = async () => {
    if (finalApprovalData.length === 0) {
      alert('No items ready for final approval.');
      return;
    }

    try {
      const approvedItems = finalApprovalData.map(item => ({
        ndc: item.ndc,
        keycode: item.keycode,
        template: item.template,
        eff_date: item.eff_date
      }));

      await weeklyReviewApi.approveWeeklyReview({
        review_type: 'pdl',
        tenant,
        week_ending: weekEnding,
        approved_items: approvedItems
      });

      // Clear final approval data (items are now synced)
      setFinalApprovalData([]);
      
      alert(`Successfully approved and synced ${approvedItems.length} items to master tables`);
    } catch (error) {
      console.error('Error approving items:', error);
      alert(`Failed to approve items: ${error instanceof Error ? error.message : 'Unknown error'}`);
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
                    disabled={selectedNDCs.size === 0}
                  >
                    Add to Reviewer A
                  </button>
                  <button 
                    className="btn primary" 
                    onClick={() => handleAssignToReviewer('B')}
                    disabled={selectedNDCs.size === 0}
                  >
                    Add to Reviewer B
                  </button>
                  <button 
                    className="btn warn" 
                    onClick={handleRejectSelected}
                    disabled={selectedNDCs.size === 0}
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
                      <th>Key Code</th>
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
                        <td><code>{record.keycode || '-'}</code></td>
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
                <th>Key Code</th>
                <th>Template</th>
                <th>Eff Date</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {false ? (
                <tr>
                  <td colSpan={5} style={{ textAlign: 'center', padding: '20px' }}>
                    Loading Reviewer A assignments...
                  </td>
                </tr>
              ) : reviewerAData.length === 0 ? (
                <tr>
                  <td colSpan={5} style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
                    No assignments for Reviewer A
                  </td>
                </tr>
              ) : (
                reviewerAData.map((assignment, index) => (
                  <tr key={assignment.assignment_id || index}>
                    <td><code>{assignment.ndc}</code></td>
                    <td>
                      <select 
                        value={assignment.keycode || ''}
                        onChange={(e) => {
                          // Update keycode in reviewer data
                          setReviewerAData(prev => prev.map(item => 
                            item.ndc === assignment.ndc ? { ...item, keycode: e.target.value } : item
                          ));
                        }}
                        style={{ minWidth: '120px' }}
                      >
                        <option value="">Select Keycode</option>
                        <option value="PA">PA - Prior Auth Required</option>
                        <option value="ST">ST - Step Therapy</option>
                        <option value="QL">QL - Quantity Limit</option>
                        <option value="DA">DA - Days Supply Limit</option>
                        <option value="GE">GE - Generic Required</option>
                        <option value="BR">BR - Brand Required</option>
                        <option value="EX">EX - Excluded</option>
                        <option value="IN">IN - Included</option>
                      </select>
                    </td>
                    <td><code>{assignment.template || '-'}</code></td>
                    <td><input type="date" defaultValue={assignment.eff_date || weekEnding} /></td>
                    <td><span className={`status ${assignment.status?.toLowerCase() || 'assigned'}`}>{assignment.status || 'ASSIGNED'}</span></td>
                  </tr>
                ))
              )}
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
                <th>Key Code</th>
                <th>Template</th>
                <th>Eff Date</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {false ? (
                <tr>
                  <td colSpan={5} style={{ textAlign: 'center', padding: '20px' }}>
                    Loading Reviewer B assignments...
                  </td>
                </tr>
              ) : reviewerBData.length === 0 ? (
                <tr>
                  <td colSpan={5} style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
                    No assignments for Reviewer B
                  </td>
                </tr>
              ) : (
                reviewerBData.map((assignment, index) => (
                  <tr key={assignment.assignment_id || index}>
                    <td><code>{assignment.ndc}</code></td>
                    <td>
                      <select 
                        value={assignment.keycode || ''}
                        onChange={(e) => {
                          // Update keycode in reviewer data
                          setReviewerBData(prev => prev.map(item => 
                            item.ndc === assignment.ndc ? { ...item, keycode: e.target.value } : item
                          ));
                        }}
                        style={{ minWidth: '120px' }}
                      >
                        <option value="">Select Keycode</option>
                        <option value="PA">PA - Prior Auth Required</option>
                        <option value="ST">ST - Step Therapy</option>
                        <option value="QL">QL - Quantity Limit</option>
                        <option value="DA">DA - Days Supply Limit</option>
                        <option value="GE">GE - Generic Required</option>
                        <option value="BR">BR - Brand Required</option>
                        <option value="EX">EX - Excluded</option>
                        <option value="IN">IN - Included</option>
                      </select>
                    </td>
                    <td><code>{assignment.template || '-'}</code></td>
                    <td><input type="date" defaultValue={assignment.eff_date || weekEnding} /></td>
                    <td><span className={`status ${assignment.status?.toLowerCase() || 'assigned'}`}>{assignment.status || 'ASSIGNED'}</span></td>
                  </tr>
                ))
              )}
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
                <th>A: Template</th>
                <th>A: Eff Date</th>
                <th>B: Key Code</th>
                <th>B: Template</th>
                <th>B: Eff Date</th>
                <th>Resolution</th>
                <th>Final Values</th>
              </tr>
            </thead>
            <tbody>
              {false ? (
                <tr>
                  <td colSpan={9} style={{ textAlign: 'center', padding: '20px' }}>
                    Loading comparison data...
                  </td>
                </tr>
              ) : comparisonData.length === 0 ? (
                <tr>
                  <td colSpan={9} style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
                    No conflicts to resolve
                  </td>
                </tr>
              ) : (
                comparisonData.map((item, index) => (
                  <tr key={item.ndc || index}>
                    <td><code>{item.ndc}</code></td>
                    <td><code>{item.reviewer_a?.keycode || '-'}</code></td>
                    <td><code>{item.reviewer_a?.template || '-'}</code></td>
                    <td>{item.reviewer_a?.eff_date || '-'}</td>
                    <td><code>{item.reviewer_b?.keycode || '-'}</code></td>
                    <td><code>{item.reviewer_b?.template || '-'}</code></td>
                    <td>{item.reviewer_b?.eff_date || '-'}</td>
                    <td>
                      <select defaultValue={item.auto_resolution || 'AUTO'}>
                        <option value="AUTO">AUTO (matched)</option>
                        <option value="A">Use A</option>
                        <option value="B">Use B</option>
                        <option value="CUSTOM">Custom</option>
                      </select>
                    </td>
                    <td>
                      <input type="text" defaultValue={item.reviewer_a?.keycode || ''} placeholder="Key code" />
                      <input type="text" defaultValue={item.reviewer_a?.template || ''} placeholder="Template" />
                      <input type="date" defaultValue={item.reviewer_a?.eff_date || weekEnding} />
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        
        <div className="row right" style={{ marginTop: '12px' }}>
          <button className="btn primary" onClick={handlePromoteResolved} disabled={comparisonData.length === 0}>
            Promote Resolved → Final Review
          </button>
        </div>
      </div>

      {/* Final Review */}
      <div className="panel">
        <div className="panel-header">
          <h2 className="panel-title">✅ Final Review</h2>
          <span className="hint">Deduped by NDC</span>
          <button className="btn success" style={{ marginLeft: 'auto' }} onClick={handleApproveSync} disabled={finalApprovalData.length === 0}>
            Approve & Sync
          </button>
        </div>
        
        <div className="scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>NDC</th>
                <th>Key Code</th>
                <th>Template</th>
                <th>Eff</th>
                <th>Resolution</th>
              </tr>
            </thead>
            <tbody>
              {false ? (
                <tr>
                  <td colSpan={5} style={{ textAlign: 'center', padding: '20px' }}>
                    Loading final approval data...
                  </td>
                </tr>
              ) : finalApprovalData.length === 0 ? (
                <tr>
                  <td colSpan={5} style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
                    No items ready for final approval
                  </td>
                </tr>
              ) : (
                finalApprovalData.map((item, index) => (
                  <tr key={item.ndc || index}>
                    <td><code>{item.ndc}</code></td>
                    <td><code>{item.keycode || '-'}</code></td>
                    <td><code>{item.template || '-'}</code></td>
                    <td>{item.eff_date || '-'}</td>
                    <td><span className="status auto">{item.resolution_type || 'AUTO'}</span></td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        
      </div>


    </div>
  );
}

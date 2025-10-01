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
  const [selectedNDCs, setSelectedNDCs] = useState<Set<string>>(new Set());
  const [reviewerAData, setReviewerAData] = useState<any[]>([]);
  const [reviewerBData, setReviewerBData] = useState<any[]>([]);
  const [finalApprovalData, setFinalApprovalData] = useState<any[]>([]);
  const [comparisonData, setComparisonData] = useState<any[]>([]);
  const [userMbidSelections, setUserMbidSelections] = useState<Record<string, string>>({});
  
  // Single data source - all data comes from this
  const [masterData, setMasterData] = useState<any[]>([]);

  // Load pool data on component mount and when dependencies change
  useEffect(() => {
    loadPoolData();
  }, [tenant, weekEnding]);

  // Debug effect to log when comparison data changes
  useEffect(() => {
    console.log('DEBUG: Comparison data updated:', comparisonData);
  }, [comparisonData]);

  // Debug effect to log when user selections change
  useEffect(() => {
    console.log('DEBUG: User MBID selections updated:', userMbidSelections);
  }, [userMbidSelections]);

  const loadPoolData = async () => {
    setLoading(true);
    setError(null);
    try {
      console.log('DEBUG: Loading pool data with search query:', searchQuery);
      const poolResponse = await weeklyReviewApi.getWeeklyPool('fmt', tenant, weekEnding, searchQuery || undefined);
      
      console.log('DEBUG: Pool response records:', poolResponse.pool_data?.length);
      
      // Set master data - single source of truth
      setMasterData(poolResponse.pool_data || []);
      
      // Transform pool data into groups format
      const groupsData = transformPoolDataToGroups(poolResponse);
      console.log('DEBUG: Transformed groups data:', groupsData);
      console.log('DEBUG: Groups data keys:', Object.keys(groupsData.groups || {}));
      console.log('DEBUG: Groups data counts:', Object.keys(groupsData.groups || {}).map(key => ({
        matchType: key,
        recordCount: groupsData.groups[key]?.records?.length || 0
      })));
      
      setPoolData(poolResponse);
      setGroupsData(groupsData);
      
      // Initialize all derived data as empty (will be populated by user actions)
      setReviewerAData([]);
      setReviewerBData([]);
      setFinalApprovalData([]);
      setComparisonData([]);
    } catch (err) {
      console.error('Error loading weekly review data:', err);
      setError(err instanceof Error ? err.message : 'Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  const transformPoolDataToGroups = (poolResponse: any) => {
    if (!poolResponse.pool_data || poolResponse.pool_data.length === 0) {
      return {
        groups: {},
        tenant: poolResponse.tenant,
        week_ending: poolResponse.week_ending,
        review_type: poolResponse.review_type
      };
    }

    // Group by match_type
    const groups: any = {};
    
    poolResponse.pool_data.forEach((record: any) => {
      const matchType = record.match_type || 'no match';
      
      if (!groups[matchType]) {
        groups[matchType] = {
          records: [],
          counts: { 'A': 0, 'B': 0, 'both': 0, 'rejected': 0, 'pending': 0 }
        };
      }
      
      // Add record to group (status will be 'pending' since no assignments yet)
      groups[matchType].records.push({
        ndc: record.ndc,
        brand: record.brand,
        mbid: record.mbid || '',
        load_date: record.load_date,
        status: 'pending',
        suggested_mbid: ''
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
    
    // Find NDCs that exist in both Reviewer A and B
    const commonNDCs = [...reviewerANDCs].filter(ndc => reviewerBNDCs.has(ndc));
    
    const newComparisonData = commonNDCs.map(ndc => {
      const reviewerAItem = reviewerAData.find(item => item.ndc === ndc);
      const reviewerBItem = reviewerBData.find(item => item.ndc === ndc);
      
      return {
        ndc,
        reviewer_a: reviewerAItem,
        reviewer_b: reviewerBItem,
        auto_resolution: reviewerAItem?.mbid === reviewerBItem?.mbid ? 'AUTO' : 'CUSTOM',
        user_selected_mbid: userMbidSelections[ndc] || '',
        updated_at: new Date().toISOString()
      };
    });
    
    setComparisonData(newComparisonData);
  };

  // Update comparison data when reviewer data or user selections change
  useEffect(() => {
    updateComparisonData();
  }, [reviewerAData, reviewerBData, userMbidSelections]);

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

  const handleSelectAllInGroup = (matchType: string, isChecked: boolean) => {
    if (!groupsData) return;
    
    const group = groupsData.groups[matchType];
    if (!group) return;
    
    const newSelected = new Set(selectedNDCs);
    group.records.forEach(record => {
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
    
    // Add to appropriate reviewer dataframe
    const newAssignments = selectedRecords.map(record => ({
      ndc: record.ndc,
      brand: record.brand,
      mbid: record.mbid || '',
      eff_date: weekEnding,
      end_date: null,
      assigned_at: new Date().toISOString()
    }));

    if (reviewer === 'A') {
      setReviewerAData(prev => [...prev, ...newAssignments]);
    } else {
      setReviewerBData(prev => [...prev, ...newAssignments]);
    }

    // Clear selections
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

    // Remove selected NDCs from master data (they are rejected)
    setMasterData(prev => prev.filter(record => !selectedNDCs.has(record.ndc)));
    
    // Clear selections
    setSelectedNDCs(new Set());
    
    alert(`Successfully rejected ${selectedNDCs.size} NDCs`);
  };

  const handlePromoteResolved = () => {
    if (comparisonData.length === 0) {
      alert('No conflicts to resolve.');
      return;
    }

    // Move resolved records to Final Review
    const resolvedItems = comparisonData.map(item => ({
      ndc: item.ndc,
      brand: item.reviewer_a?.brand || item.reviewer_b?.brand || '',
      mbid: item.user_selected_mbid || item.reviewer_a?.mbid || item.reviewer_b?.mbid || '',
      eff_date: item.reviewer_a?.eff_date || item.reviewer_b?.eff_date || weekEnding,
      end_date: item.reviewer_a?.end_date || item.reviewer_b?.end_date || null,
      resolved_at: new Date().toISOString()
    }));

    setFinalApprovalData(prev => [...prev, ...resolvedItems]);
    
    // Remove from reviewer dataframes
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
        mbid: item.mbid,
        eff_date: item.eff_date,
        end_date: item.end_date
      }));

      // Update MBID in the backend table for each NDC
      await weeklyReviewApi.approveWeeklyReview({
        review_type: 'fmt',
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

  const handleMbidSelectionChange = (ndc: string, selectedMbid: string) => {
    console.log(`DEBUG: User selected MBID ${selectedMbid} for NDC ${ndc}`);
    console.log(`DEBUG: Current comparison data before update:`, comparisonData);
    
    // Update user selections
    setUserMbidSelections(prev => {
      const updated = {
        ...prev,
        [ndc]: selectedMbid
      };
      console.log(`DEBUG: Updated user selections:`, updated);
      return updated;
    });

    // Update comparison data with user selection
    setComparisonData(prev => {
      const updated = prev.map(item => {
        if (item.ndc === ndc) {
          const updatedItem = {
            ...item,
            user_selected_mbid: selectedMbid,
            updated_at: new Date().toISOString()
          };
          console.log(`DEBUG: Updated item for NDC ${ndc}:`, updatedItem);
          return updatedItem;
        }
        return item;
      });
      console.log(`DEBUG: Updated comparison data:`, updated);
      return updated;
    });
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
                <label>
                  <input 
                    type="checkbox" 
                    onChange={(e) => handleSelectAllInGroup(matchType, e.target.checked)}
                    checked={group.records.every(record => selectedNDCs.has(record.ndc))}
                  /> 
                  Select all in '{matchType}'
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
                      <th>MBID</th>
                      <th>Load</th>
                      <th>Suggested MBID</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {group.records.map((record) => (
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
                        <td><code>{record.mbid || '-'}</code></td>
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

        {selectedNDCs.size > 0 && (
          <div className="alert info" style={{ margin: '10px 0' }}>
            {selectedNDCs.size} NDC{selectedNDCs.size !== 1 ? 's' : ''} selected
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
                      <select aria-label="MBID" defaultValue={assignment.mbid || ''}>
                        <option value="">(none)</option>
                        <option value="AK123456">AK123456 - Antihistamines</option>
                        <option value="CV999001">CV999001 - Cardio – Statins</option>
                        <option value="IMM000777">IMM000777 - Immunology – Insulins</option>
                      </select>
                    </td>
                    <td><input type="date" defaultValue={assignment.eff_date || weekEnding} /></td>
                    <td><input type="date" defaultValue={assignment.end_date || ''} /></td>
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
                      <select aria-label="MBID" defaultValue={assignment.mbid || ''}>
                        <option value="">(none)</option>
                        <option value="AK123456">AK123456 - Antihistamines</option>
                        <option value="CV999001">CV999001 - Cardio – Statins</option>
                        <option value="IMM000777">IMM000777 - Immunology – Insulins</option>
                      </select>
                    </td>
                    <td><input type="date" defaultValue={assignment.eff_date || weekEnding} /></td>
                    <td><input type="date" defaultValue={assignment.end_date || ''} /></td>
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
                    <td><code>{item.reviewer_a?.mbid || '-'}</code></td>
                    <td>{item.reviewer_a?.eff_date || '-'}</td>
                    <td>{item.reviewer_a?.end_date || '-'}</td>
                    <td><code>{item.reviewer_b?.mbid || '-'}</code></td>
                    <td>{item.reviewer_b?.eff_date || '-'}</td>
                    <td>{item.reviewer_b?.end_date || '-'}</td>
                    <td>
                      <select defaultValue={item.auto_resolution || 'AUTO'}>
                        <option value="AUTO">AUTO (matched)</option>
                        <option value="A">Use A</option>
                        <option value="B">Use B</option>
                        <option value="CUSTOM">Custom</option>
                      </select>
                    </td>
                    <td>
                      <select 
                        value={userMbidSelections[item.ndc] || item.reviewer_a?.mbid || ''}
                        onChange={(e) => handleMbidSelectionChange(item.ndc, e.target.value)}
                      >
                        <option value="">(none)</option>
                        <option value="AK123456">AK123456 - Antihistamines</option>
                        <option value="CV999001">CV999001 - Cardio – Statins</option>
                        <option value="IMM000777">IMM000777 - Immunology – Insulins</option>
                      </select>
                      <input type="date" defaultValue={item.reviewer_a?.eff_date || weekEnding} />
                      <input type="date" defaultValue={item.reviewer_a?.end_date || ''} />
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
                <th>MBID</th>
                <th>Eff</th>
                <th>End</th>
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
                    <td><code>{item.mbid || '-'}</code></td>
                    <td>{item.eff_date || '-'}</td>
                    <td>{item.end_date || '-'}</td>
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

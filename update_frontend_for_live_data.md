# Frontend Code Changes for Live FDB Data

## 📋 Overview

After uploading the FDB data to Databricks volume and updating the backend, here are the frontend changes needed to use live data instead of hardcoded arrays.

## 🔧 Step 1: Create FDB API Service

Create `/frontend/src/services/fdb-api.ts`:

```typescript
/**
 * FDB API service for live data from Databricks volume
 */

const API_BASE_URL = '/api/fdb';

export interface FDBRecord {
  ndc: string;
  gsn: number | null;
  brand: string;
  generic: string;
  rx_otc: string;
  pkg_size: string;
  hic3: string;
  hicl: string;
  dcc: string;
  mfr: string;
  obsolete: boolean;
  rebate: boolean;
  load_date: string;
  pkg_origin: string;
  gsn_desc: string;
  pkg_form: string;
  // Formulary fields (if available)
  formulary_status?: string;
  tier?: number;
  pa_required?: boolean;
  ql_limits?: string;
}

export interface FDBSearchResponse {
  tenant: string;
  query: string | null;
  limit: number;
  total_found: number;
  data_source: string;
  records: FDBRecord[];
}

export interface FDBDetailsResponse {
  ndc: string;
  tenant: string;
  data_source: string;
  sections: {
    Core: { [key: string]: any };
    Classification: { [key: string]: any };
    "Pricing & Flags": { [key: string]: any };
    "Packaging & Origin": { [key: string]: any };
    Formulary?: { [key: string]: any };
    "Pricing Details"?: { [key: string]: any };
    "Regional Info"?: { [key: string]: any };
  };
}

class FDBApiService {
  private async fetchWithErrorHandling<T>(url: string, options?: RequestInit): Promise<T> {
    try {
      const response = await fetch(`${API_BASE_URL}${url}`, {
        headers: {
          'Content-Type': 'application/json',
          ...options?.headers,
        },
        ...options,
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ 
          detail: response.statusText 
        }));
        throw new Error(`API Error: ${response.status} - ${errorData.detail || 'Unknown error'}`);
      }

      return await response.json();
    } catch (error) {
      console.error(`FDB API call failed for ${url}:`, error);
      throw error;
    }
  }

  async searchRecords(
    tenant: string, 
    query?: string, 
    limit: number = 100
  ): Promise<FDBSearchResponse> {
    const params = new URLSearchParams({ tenant, limit: limit.toString() });
    if (query) {
      params.append('query', query);
    }
    
    return this.fetchWithErrorHandling<FDBSearchResponse>(
      `/search?${params.toString()}`
    );
  }

  async getRecordDetails(ndc: string, tenant: string): Promise<FDBDetailsResponse> {
    const params = new URLSearchParams({ tenant });
    return this.fetchWithErrorHandling<FDBDetailsResponse>(
      `/details/${ndc}?${params.toString()}`
    );
  }

  async exportData(
    tenant: string, 
    format: 'csv' | 'json' = 'csv',
    query?: string,
    limit?: number
  ): Promise<Blob> {
    const params = new URLSearchParams({ tenant, format });
    if (query) params.append('query', query);
    if (limit) params.append('limit', limit.toString());

    const response = await fetch(`${API_BASE_URL}/export?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Export failed: ${response.statusText}`);
    }
    
    return response.blob();
  }
}

export const fdbApi = new FDBApiService();
```

## 🔧 Step 2: Update FDBSearch Component

Update `/frontend/src/components/pages/FDBSearch.tsx`:

```typescript
/**
 * FDB Search Page - Live data from Databricks Volume
 */
import React, { useState, useEffect, useContext } from 'react';
import { TenantContext } from '../../App';
import { fdbApi, type FDBRecord, type FDBDetailsResponse } from '../../services/fdb-api';

export function FDBSearch() {
  const { tenant } = useContext(TenantContext);
  const [searchQuery, setSearchQuery] = useState('');
  const [records, setRecords] = useState<FDBRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [selectedRecord, setSelectedRecord] = useState<FDBDetailsResponse | null>(null);

  // Load initial data when tenant changes
  useEffect(() => {
    loadRecords();
  }, [tenant]);

  const loadRecords = async (query?: string) => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await fdbApi.searchRecords(tenant, query, 100);
      setRecords(response.records);
      
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load data');
      console.error('Error loading FDB records:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = () => {
    loadRecords(searchQuery.trim() || undefined);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const openDrawer = async (ndc: string) => {
    try {
      setLoading(true);
      const details = await fdbApi.getRecordDetails(ndc, tenant);
      setSelectedRecord(details);
      setDrawerOpen(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load record details');
      console.error('Error loading record details:', err);
    } finally {
      setLoading(false);
    }
  };

  const closeDrawer = () => {
    setDrawerOpen(false);
    setSelectedRecord(null);
  };

  // Handle ESC key to close drawer
  useEffect(() => {
    const handleEsc = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        closeDrawer();
      }
    };
    document.addEventListener('keydown', handleEsc);
    return () => document.removeEventListener('keydown', handleEsc);
  }, []);

  const exportData = async (format: 'csv' | 'json') => {
    try {
      const blob = await fdbApi.exportData(tenant, format, searchQuery || undefined);
      
      // Create download link
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.style.display = 'none';
      a.href = url;
      a.download = `fdb_export_${tenant.toLowerCase()}_${new Date().toISOString().slice(0,10)}.${format}`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Export failed');
    }
  };

  return (
    <div className="page">
      <div className="panel-header">
        <h1 className="panel-title">FDB Search</h1>
        <div className="hint">Live data from Databricks Volume</div>
      </div>

      {/* Search Controls */}
      <div className="row" style={{ marginBottom: '24px', alignItems: 'flex-end', gap: '16px' }}>
        <div style={{ flex: '1 1 auto' }}>
          <label>Search NDC, Brand, Generic, or Manufacturer</label>
          <input 
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Enter search term..."
            disabled={loading}
          />
        </div>
        <button 
          onClick={handleSearch}
          disabled={loading}
          style={{ height: '40px' }}
        >
          {loading ? 'Searching...' : 'Search'}
        </button>
        <button 
          onClick={() => exportData('csv')}
          disabled={loading || records.length === 0}
          style={{ height: '40px' }}
        >
          Export CSV
        </button>
      </div>

      {/* Error Display */}
      {error && (
        <div style={{ 
          padding: '12px', 
          backgroundColor: '#dc3545', 
          color: 'white', 
          borderRadius: '4px', 
          marginBottom: '16px' 
        }}>
          {error}
        </div>
      )}

      {/* Results */}
      <div className="scroll" style={{ height: '500px' }}>
        {loading ? (
          <div style={{ textAlign: 'center', padding: '40px' }}>
            Loading FDB data...
          </div>
        ) : records.length === 0 ? (
          <div style={{ textAlign: 'center', padding: '40px', color: 'var(--color-text-muted)' }}>
            {searchQuery ? 'No records found for your search.' : 'Enter a search term to find FDB records.'}
          </div>
        ) : (
          <table className="data-table">
            <thead>
              <tr>
                <th style={{ width: '120px' }}>NDC</th>
                <th style={{ width: '200px' }}>Brand</th>
                <th style={{ width: '200px' }}>Generic</th>
                <th style={{ width: '80px' }}>Rx/OTC</th>
                <th style={{ width: '100px' }}>Pkg Size</th>
                <th style={{ width: '80px' }}>HIC3</th>
                <th style={{ width: '150px' }}>Manufacturer</th>
                <th style={{ width: '100px' }}>Load Date</th>
              </tr>
            </thead>
            <tbody>
              {records.map((record) => (
                <tr key={record.ndc}>
                  <td>
                    <button 
                      className="ndc-link" 
                      onClick={() => openDrawer(record.ndc)}
                    >
                      {record.ndc}
                    </button>
                  </td>
                  <td>{record.brand}</td>
                  <td>{record.generic}</td>
                  <td>{record.rx_otc}</td>
                  <td>{record.pkg_size}</td>
                  <td>{record.hic3}</td>
                  <td>{record.mfr}</td>
                  <td>{record.load_date}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Record Details Drawer */}
      {drawerOpen && selectedRecord && (
        <div className="drawer-overlay" onClick={closeDrawer}>
          <div className="drawer-aside" onClick={(e) => e.stopPropagation()}>
            <div className="drawer-header">
              <h2>NDC Details: {selectedRecord.ndc}</h2>
              <button className="btn-close" onClick={closeDrawer}>×</button>
            </div>
            <div className="drawer-content scroll">
              {Object.entries(selectedRecord.sections).map(([sectionName, sectionData]) => (
                <div key={sectionName} className="sec">
                  <h3>{sectionName}</h3>
                  <div className="kv">
                    {Object.entries(sectionData).map(([key, value]) => (
                      <div key={key} className="kv-row">
                        <div className="kv-key">{key}</div>
                        <div className="kv-value">{String(value)}</div>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
            <div className="drawer-footer">
              <button onClick={() => exportData('json')}>Export JSON</button>
              <button onClick={closeDrawer}>Close</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
```

## 🚀 Step 3: Deploy and Test

1. **Install pandas** (if not already done):
   ```bash
   cd /Users/scott.johnson/Documents/gainwell_main_app
   pip install pandas==2.1.3
   ```

2. **Build frontend**:
   ```bash
   cd frontend && npm run build:static
   ```

3. **Deploy app**:
   ```bash
   databricks apps deploy --source-dir . --app-name gainwell-fmt
   ```

4. **Test the integration**:
   - Open the deployed app
   - Go to FDB Search tab
   - Try searching for "lipitor" or "insulin"
   - Click on NDC links to see detailed records
   - Test CSV export functionality

## 📊 Key Benefits of Live Data Integration

### **🔄 Dynamic Updates**
- **Before**: Hardcoded arrays with 5 records per tenant
- **After**: 500+ records from volume, updated as files change

### **🔍 Real Search**
- **Before**: Client-side filtering only  
- **After**: Server-side search across all fields (NDC, brand, generic, manufacturer, HIC3, DCC)

### **📋 Rich Details**
- **Before**: Single data structure
- **After**: Aggregated data from multiple files (core, pricing, formulary, regional)

### **📤 Export Functionality**
- **Before**: Fake export buttons
- **After**: Real CSV/JSON exports with search filtering

### **🏢 Tenant-Specific Data**
- **Before**: Static tenant datasets
- **After**: Dynamic formulary merging for each tenant

### **⚡ Performance**
- **Before**: All data loaded on page load
- **After**: On-demand loading with search optimization

## 🔧 Volume Path Configuration

The backend automatically tries both path formats:
- Primary: `/Volumes/demo/gainwell/fdb_data/`
- Fallback: `/dbfs/Volumes/demo/gainwell/fdb_data/`

If your volume path is different, update `VOLUME_BASE_PATH` in `/backend/fdb/routes.py`.

## 📝 Next Steps

1. Upload data to volume using manual commands shown above
2. Replace the hardcoded FDBSearch component with the live version
3. Test search, details, and export functionality
4. Add error handling and loading states
5. Consider adding caching for better performance

**The integration provides a complete live data pipeline from Databricks volume to React frontend!** 🚀

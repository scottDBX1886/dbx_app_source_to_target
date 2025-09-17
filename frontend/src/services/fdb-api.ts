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

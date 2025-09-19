/**
 * FMT API service - interfaces and functions for FMT Master data
 */

export interface FMTRecord {
  ndc: string;
  fmt_drug: string;
  mbid: string;
  status: string;
  start_date: string;
  end_date: string;
}

export interface FMTDetailsResponse {
  ndc: string;
  core_info: {
    ndc: string;
    fmt_drug: string;
    status: string;
    load_date: string;
  };
  mbid_info: {
    mbid: string;
    description: string;
    tenant: string;
    begin_date: string;
  };
  date_info: {
    start_date: string;
    end_date: string;
    effective_date: string;
    expiration_date: string;
  };
  audit_info: {
    created_by: string;
    updated_by: string;
    review_status: string;
    notes: string;
  };
}

export interface FMTSearchResponse {
  tenant: string;
  query?: string;
  status?: string;
  total_found: number;
  records: FMTRecord[];
  error?: string;
}

class FMTApiService {
  private baseUrl = '/api/fmt';

  async searchRecords(
    tenant: string,
    query?: string,
    status?: string,
    limit?: number
  ): Promise<FMTSearchResponse> {
    const params = new URLSearchParams({ tenant });
    if (query) params.append('query', query);
    if (status) params.append('status', status);
    if (limit) params.append('limit', limit.toString());

    const response = await fetch(`${this.baseUrl}/search?${params}`);
    if (!response.ok) {
      throw new Error(`FMT search failed: ${response.statusText}`);
    }
    return response.json();
  }

  async getDetails(ndc: string, tenant: string): Promise<FMTDetailsResponse> {
    const params = new URLSearchParams({ tenant });
    const response = await fetch(`${this.baseUrl}/details/${ndc}?${params}`);
    if (!response.ok) {
      throw new Error(`FMT details failed: ${response.statusText}`);
    }
    return response.json();
  }

  async exportData(
    tenant: string,
    format: 'csv' | 'json' = 'csv',
    query?: string,
    status?: string,
    limit?: number
  ): Promise<Blob> {
    const params = new URLSearchParams({ tenant, format });
    if (query) params.append('query', query);
    if (status) params.append('status', status);
    if (limit) params.append('limit', limit.toString());

    const response = await fetch(`${this.baseUrl}/export?${params}`);
    if (!response.ok) {
      throw new Error(`FMT export failed: ${response.statusText}`);
    }
    return response.blob();
  }
}

export const fmtApi = new FMTApiService();

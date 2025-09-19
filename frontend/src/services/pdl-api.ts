/**
 * PDL API service - interfaces and functions for PDL Coding data
 */

export interface PDLRecord {
  ndc: string;
  pdl_drug: string;
  key_code: string;
  status: string;
  template: string;
  effective_date: string;
}

export interface PDLDetailsResponse {
  ndc: string;
  core_info: {
    ndc: string;
    pdl_drug: string;
    status: string;
    load_date: string;
  };
  keycode_info: {
    key_code: string | null;
    template: string | null;
    tenant: string | null;
    generation_date: string | null;
  };
  date_info: {
    effective_date: string;
    expiration_date: string | null;
    last_updated: string | null;
  };
  audit_info: {
    created_by: string | null;
    updated_by: string | null;
    notes: string | null;
    pos_export_status: string | null;
  };
}

export interface PDLSearchResponse {
  tenant: string;
  query: string | null;
  status: string | null;
  total_found: number;
  records: PDLRecord[];
  error?: string;
}

class PDLApiService {
  private baseUrl = '/api/pdl';

  async searchRecords(
    tenant: string,
    query?: string,
    status?: string,
    limit?: number
  ): Promise<PDLSearchResponse> {
    const params = new URLSearchParams({ tenant });
    if (query) params.append('query', query);
    if (status) params.append('status', status);
    if (limit) params.append('limit', limit.toString());

    const response = await fetch(`${this.baseUrl}/search?${params}`);
    if (!response.ok) {
      throw new Error(`PDL search failed: ${response.statusText}`);
    }
    return response.json();
  }

  async getDetails(ndc: string, tenant: string): Promise<PDLDetailsResponse> {
    const params = new URLSearchParams({ tenant });
    const response = await fetch(`${this.baseUrl}/details/${ndc}?${params}`);
    if (!response.ok) {
      throw new Error(`PDL details failed: ${response.statusText}`);
    }
    return response.json();
  }

  async exportData(
    tenant: string,
    format: 'csv' | 'json',
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
      throw new Error(`PDL export failed: ${response.statusText}`);
    }
    return response.blob();
  }
}

export const pdlApi = new PDLApiService();

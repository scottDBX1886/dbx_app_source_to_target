/**
 * Weekly Review API Service
 * Handles all API calls for weekly review functionality (FMT & PDL)
 */

// ============= INTERFACES =============

export interface WeeklyPoolData {
  ndc: string;
  brand: string;
  gsn: string;
  hic3: string;
  mfr: string;
  load_date: string;
  status: string;
  match_type: '100% match' | 'brand match' | 'no match';
}

export interface WeeklyPoolResponse {
  pool_data: WeeklyPoolData[];
  summary: {
    total_new_drugs: number;
    match_counts: Record<string, number>;
  };
  tenant: string;
  week_ending: string;
  review_type: string;
}

export interface ReviewGroupData {
  ndc: string;
  brand: string;
  gsn: string;
  hic3: string;
  mfr: string;
  load_date: string;
  status: 'A' | 'B' | 'both' | 'rejected' | 'pending';
  suggested_mbid: string;
}

export interface ReviewGroup {
  records: ReviewGroupData[];
  counts: {
    A: number;
    B: number;
    both: number;
    rejected: number;
    pending: number;
  };
}

export interface ReviewGroupsResponse {
  groups: Record<string, ReviewGroup>;
  tenant: string;
  week_ending: string;
  review_type: string;
}

export interface ReviewAssignment {
  ndc: string;
  reviewer: 'A' | 'B';
  mbid: string;
  eff_date: string;
  end_date: string | null;
  status: string;
  assignment_date: string;
}

export interface ReviewAssignmentsResponse {
  assignments: {
    reviewer_a: ReviewAssignment[];
    reviewer_b: ReviewAssignment[];
  };
  tenant: string;
  week_ending: string;
  review_type: string;
}

export interface AssignmentRequest {
  review_type: 'fmt' | 'pdl';
  tenant: string;
  week_ending: string;
  assignments: {
    ndc: string;
    reviewer: 'A' | 'B';
    mbid: string;
    eff_date: string;
    end_date: string | null;
  }[];
}

export interface ResolutionRequest {
  review_type: 'fmt' | 'pdl';
  tenant: string;
  week_ending: string;
  resolutions: {
    ndc: string;
    resolution: 'AUTO' | 'A' | 'B' | 'CUSTOM';
    final_mbid: string;
    final_eff_date: string;
    final_end_date: string | null;
  }[];
}

export interface ApprovalRequest {
  review_type: 'fmt' | 'pdl';
  tenant: string;
  week_ending: string;
  approved_items: {
    ndc: string;
    mbid: string;
    eff_date: string;
    end_date: string | null;
  }[];
}

export interface RejectionRequest {
  review_type: 'fmt' | 'pdl';
  tenant: string;
  week_ending: string;
  rejected_ndcs: string[];
  rejection_reason: string;
}

export interface ComparisonData {
  ndc: string;
  reviewer_a: ReviewAssignment;
  reviewer_b: ReviewAssignment;
  has_conflict: boolean;
  auto_resolution: 'AUTO' | 'CUSTOM';
}

export interface ComparisonResponse {
  comparison_data: ComparisonData[];
  total_conflicts: number;
  total_matches: number;
  tenant: string;
  week_ending: string;
  review_type: string;
}

// ============= API SERVICE CLASS =============

class WeeklyReviewApiService {
  private baseUrl = '/api/weekly';

  /**
   * Get weekly review pool data
   */
  async getWeeklyPool(
    reviewType: 'fmt' | 'pdl',
    tenant: string,
    weekEnding: string,
    search?: string
  ): Promise<WeeklyPoolResponse> {
    const params = new URLSearchParams({
      tenant,
      week_ending: weekEnding,
    });
    
    if (search) {
      params.append('search', search);
    }

    const response = await fetch(`${this.baseUrl}/pool/${reviewType}?${params}`);
    
    if (!response.ok) {
      throw new Error(`Failed to fetch weekly pool: ${response.statusText}`);
    }
    
    return response.json();
  }

  /**
   * Get review groups categorized by match type
   */
  async getReviewGroups(
    reviewType: 'fmt' | 'pdl',
    tenant: string,
    weekEnding: string
  ): Promise<ReviewGroupsResponse> {
    const params = new URLSearchParams({
      tenant,
      week_ending: weekEnding,
    });

    const response = await fetch(`${this.baseUrl}/groups/${reviewType}?${params}`);
    
    if (!response.ok) {
      throw new Error(`Failed to fetch review groups: ${response.statusText}`);
    }
    
    return response.json();
  }

  /**
   * Get current reviewer assignments
   */
  async getReviewerAssignments(
    reviewType: 'fmt' | 'pdl',
    tenant: string,
    weekEnding: string
  ): Promise<ReviewAssignmentsResponse> {
    const params = new URLSearchParams({
      tenant,
      week_ending: weekEnding,
    });

    const response = await fetch(`${this.baseUrl}/assignments/${reviewType}?${params}`);
    
    if (!response.ok) {
      throw new Error(`Failed to fetch reviewer assignments: ${response.statusText}`);
    }
    
    return response.json();
  }

  /**
   * Assign NDCs to reviewers
   */
  async assignReviews(data: AssignmentRequest): Promise<any> {
    const response = await fetch(`${this.baseUrl}/assign`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });
    
    if (!response.ok) {
      throw new Error(`Failed to assign reviews: ${response.statusText}`);
    }
    
    return response.json();
  }

  /**
   * Resolve conflicts between reviewers
   */
  async resolveConflicts(data: ResolutionRequest): Promise<any> {
    const response = await fetch(`${this.baseUrl}/resolve`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });
    
    if (!response.ok) {
      throw new Error(`Failed to resolve conflicts: ${response.statusText}`);
    }
    
    return response.json();
  }

  /**
   * Final approval and sync to master tables
   */
  async approveWeeklyReview(data: ApprovalRequest): Promise<any> {
    const response = await fetch(`${this.baseUrl}/approve`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });
    
    if (!response.ok) {
      throw new Error(`Failed to approve weekly review: ${response.statusText}`);
    }
    
    return response.json();
  }

  /**
   * Reject selected items
   */
  async rejectItems(data: RejectionRequest): Promise<any> {
    const response = await fetch(`${this.baseUrl}/reject`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });
    
    if (!response.ok) {
      throw new Error(`Failed to reject items: ${response.statusText}`);
    }
    
    return response.json();
  }

  /**
   * Get comparison data for conflict resolution
   */
  async getComparisonData(
    reviewType: 'fmt' | 'pdl',
    tenant: string,
    weekEnding: string
  ): Promise<ComparisonResponse> {
    const params = new URLSearchParams({
      tenant,
      week_ending: weekEnding,
    });

    const response = await fetch(`${this.baseUrl}/comparison/${reviewType}?${params}`);
    
    if (!response.ok) {
      throw new Error(`Failed to fetch comparison data: ${response.statusText}`);
    }
    
    return response.json();
  }
}

// ============= EXPORT =============

export const weeklyReviewApi = new WeeklyReviewApiService();

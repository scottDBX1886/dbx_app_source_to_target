/**
 * Authentication API service
 * Handles user auth and service principal information
 */

export interface UserInfo {
  user_id: string | null;
  email: string | null;
  display_name: string | null;
  groups: string[];
  active: boolean;
}

export interface AuthStatus {
  authenticated: boolean;
  user_info: UserInfo;
  app: string;
  has_user_token: boolean;
  authorization_type: 'user' | 'service_principal' | 'hybrid';
  data_source: string;
}

export interface ServicePrincipalInfo {
  client_id: string;
  configured: boolean;
  permissions: string[];
  scope: string[];
}

export interface AuthConfiguration {
  auth_mode: string;
  user_auth_enabled: boolean;
  service_principal_enabled: boolean;
  databricks_host: string;
}

class AuthApiService {
  private readonly API_BASE = '/api';

  private async fetchApi<T>(endpoint: string): Promise<T> {
    try {
      const response = await fetch(`${this.API_BASE}${endpoint}`, {
        headers: {
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      return await response.json();
    } catch (error) {
      console.error(`Auth API call failed for ${endpoint}:`, error);
      throw error;
    }
  }

  // Get current user authentication status
  async getUserAuthStatus(): Promise<AuthStatus> {
    return this.fetchApi<AuthStatus>('/health/user-info');
  }

  // Get service principal configuration (mock for demo)
  async getServicePrincipalInfo(): Promise<ServicePrincipalInfo> {
    // In a real implementation, this would fetch from a secure endpoint
    return {
      client_id: process.env.DATABRICKS_CLIENT_ID || 'sp-****-****-****',
      configured: true,
      permissions: [
        'clusters:read',
        'clusters:write', 
        'sql:read',
        'sql:write',
        'workspace:read'
      ],
      scope: [
        'databricks-api',
        'offline_access'
      ]
    };
  }

  // Get authentication configuration
  async getAuthConfiguration(): Promise<AuthConfiguration> {
    return {
      auth_mode: 'hybrid',
      user_auth_enabled: true,
      service_principal_enabled: true,
      databricks_host: window.location.hostname.includes('databricksapps.com') 
        ? window.location.hostname.replace('.databricksapps.com', '') 
        : 'databricks-workspace'
    };
  }

  // Test authentication endpoints
  async testUserAuth(): Promise<{ success: boolean; message: string }> {
    try {
      const status = await this.getUserAuthStatus();
      return {
        success: status.authenticated,
        message: status.authenticated 
          ? `User authenticated as ${status.user_info.email}` 
          : 'User not authenticated'
      };
    } catch (error) {
      return {
        success: false,
        message: `Auth test failed: ${error instanceof Error ? error.message : 'Unknown error'}`
      };
    }
  }

  async testServicePrincipal(): Promise<{ success: boolean; message: string }> {
    try {
      const sp = await this.getServicePrincipalInfo();
      return {
        success: sp.configured,
        message: sp.configured 
          ? `Service Principal configured with ${sp.permissions.length} permissions` 
          : 'Service Principal not configured'
      };
    } catch (error) {
      return {
        success: false,
        message: `SP test failed: ${error instanceof Error ? error.message : 'Unknown error'}`
      };
    }
  }

  // Debug SCIM API call - shows exactly what Databricks returns
  async getScimDebugInfo(): Promise<any> {
    return this.fetchApi<any>('/debug/scim-raw');
  }
}

export const authApi = new AuthApiService();

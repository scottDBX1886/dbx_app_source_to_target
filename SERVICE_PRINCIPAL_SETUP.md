# Service Principal Authentication Setup Guide

## Overview
The authentication system has been updated to use **Service Principal** authentication instead of individual user authentication. This provides better security and automation capabilities for the Gainwell Main App.

## Changes Made

### 1. Updated Authentication Routes (`backend/auth/routes.py`)
- **Old Endpoint**: `/api/health/user-info` (individual user auth)
- **New Endpoints**:
  - `/api/health/service-principal` (GET) - Check service principal authentication status
  - `/api/auth/token` (POST) - Get fresh access token

### 2. Authentication Method
- **Before**: Used forwarded headers (`x-forwarded-user`, `x-forwarded-email`, etc.)
- **After**: Uses OAuth2 client credentials flow with service principal

### 3. Required Environment Variables
```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_CLIENT_ID=your-service-principal-client-id  
DATABRICKS_CLIENT_SECRET=your-service-principal-client-secret
```

## Setup Steps

### 1. Create Service Principal in Databricks

1. Go to your Databricks workspace
2. Navigate to **Settings** → **Identity and access** → **Service principals**
3. Click **Add service principal**
4. Provide a name (e.g., "gainwell-main-app-sp")
5. Save the **Application ID** (this is your `CLIENT_ID`)
6. Generate a **Client secret** and save it securely (this is your `CLIENT_SECRET`)

### 2. Configure Permissions

Grant the service principal necessary permissions:
- **Workspace access**: Add to appropriate groups
- **Cluster access**: Grant "Can attach to" permissions on clusters
- **SQL warehouse access**: If using SQL endpoints
- **Unity Catalog**: Grant appropriate permissions on catalogs/schemas

### 3. Set Environment Variables

Create a `.env` file or set environment variables:
```bash
DATABRICKS_HOST=https://adb-1234567890123456.78.azuredatabricks.net
DATABRICKS_CLIENT_ID=12345678-1234-1234-1234-123456789012
DATABRICKS_CLIENT_SECRET=your-generated-secret-key
```

### 4. Test Authentication

Test the new endpoints:

```bash
# Test service principal authentication
curl -X GET "http://localhost:8000/api/health/service-principal"

# Get access token
curl -X POST "http://localhost:8000/api/auth/token"
```

## API Response Changes

### Service Principal Info Response
```json
{
  "service_principal_info": {
    "client_id": "12345678-1234-1234-1234-123456789012",
    "workspace_id": "workspace-object-id",
    "workspace_name": "Databricks Workspace", 
    "authenticated": true,
    "permissions": ["group1", "group2"],
    "user_name": "service-principal"
  },
  "authenticated": true,
  "app": "Gainwell Main App",
  "authorization_type": "service_principal",
  "data_source": "databricks_oauth2",
  "token_valid": true
}
```

### Access Token Response  
```json
{
  "access_token": "dapi1234567890abcdef...",
  "token_type": "Bearer",
  "scope": "all-apis",
  "client_id": "12345678-1234-1234-1234-123456789012"
}
```

## Security Benefits

1. **No User Dependency**: Authentication doesn't depend on individual user sessions
2. **Programmatic Access**: Perfect for automated workflows and API integrations  
3. **Credential Management**: Centralized secret management
4. **Audit Trail**: Better tracking of API usage
5. **Scalability**: No user session limits

## Migration Considerations

### Frontend Updates Needed
Update any frontend code that calls the old `/api/health/user-info` endpoint:

```javascript
// Old
const userInfo = await fetch('/api/health/user-info');

// New 
const authInfo = await fetch('/api/health/service-principal');
```

### Other Services
Update other backend services that depend on user authentication to use the new service principal authentication.

## Helper Functions

The updated code includes these utility functions:

- `get_service_principal_token()` - Get OAuth2 access token
- `validate_service_principal()` - Validate token and get workspace info  
- `get_authenticated_token()` - FastAPI dependency for authenticated requests

## Troubleshooting

### Common Issues

1. **"DATABRICKS_HOST environment variable is required"**
   - Ensure all environment variables are set correctly

2. **"Failed to get access token: 401"**
   - Check client ID and secret are correct
   - Verify service principal exists and is active

3. **"Failed to validate token"**  
   - Check service principal has workspace access permissions
   - Verify workspace URL is correct

### Debug Tips

- Check logs for detailed error messages
- Test authentication with Databricks CLI first
- Verify service principal permissions in Databricks UI

## Next Steps

1. Set up environment variables
2. Create service principal in Databricks
3. Test the new endpoints
4. Update frontend/client applications
5. Update any dependent services
6. Remove old user-based authentication code when ready
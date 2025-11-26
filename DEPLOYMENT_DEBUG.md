# Deployment Debugging Guide

## Issue Analysis
The deployment error shows:
```
ModuleNotFoundError: No module named 'backend.auth.routes'
```

This indicates an import issue during app startup in the Databricks environment.

## Debugging Steps

### 1. Use Debug App First
Try deploying with `app_debug.py` instead of `app.py`:

```bash
# In your databricks.yml or deployment config, change:
# artifact_path: /Workspace/Users/your.email@domain.com/gainwell_main_app
# - app.py
# TO:
# - app_debug.py
```

### 2. Check Simplified Routes
The debug app includes simplified FDB routes (`routes_simple.py`) that don't have authentication dependencies.

### 3. Test Endpoints
Once deployed with debug mode:
- Visit `/` for basic health check
- Visit `/health` for environment check  
- Visit `/api/fdb/health` for FDB service check
- Visit `/api/fdb/search` for simplified search

### 4. Check Environment Variables
The debug health endpoint will show if your environment variables are properly set:
- `DATABRICKS_HOST`
- `DATABRICKS_CLIENT_ID`  
- `DATABRICKS_CLIENT_SECRET`

### 5. Gradual Import Testing
If debug mode works, gradually add back the full routes:

1. First, try importing just auth routes
2. Then add FDB routes with authentication
3. Finally add all other routes

## Possible Fixes

### Option A: Fix Circular Import
The issue might be circular imports between auth routes and FDB routes. The current fix uses a local wrapper:

```python
# In FDB routes
async def get_authenticated_token() -> str:
    from backend.auth.routes import get_authenticated_token as auth_token
    return await auth_token()
```

### Option B: Environment-Specific Import
Use conditional imports based on environment:

```python
try:
    from backend.auth.routes import get_authenticated_token
except ImportError:
    # Fallback for development/debug
    async def get_authenticated_token():
        return "debug-token"
```

### Option C: Lazy Loading
Import authentication functions only when needed rather than at module level.

## Next Steps

1. Deploy with `app_debug.py`
2. Check if basic functionality works
3. Review logs for any remaining import errors
4. Gradually add back full functionality

## Files Created for Debugging
- `app_debug.py` - Minimal app with error handling
- `routes_simple.py` - Simplified FDB routes without auth
- `test_imports.py` - Step-by-step import testing
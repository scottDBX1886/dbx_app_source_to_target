"""
Minimal FastAPI app for testing imports and deployment
"""
import os
import logging
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Gainwell Main App - Debug Mode",
    description="Simplified app for debugging deployment issues",
    version="2.0.0-debug"
)

@app.get("/")
async def root():
    """Root endpoint for health checking"""
    return {
        "message": "Gainwell Main App is running",
        "status": "healthy",
        "mode": "debug",
        "version": "2.0.0-debug"
    }

@app.get("/health")
async def health_check():
    """Comprehensive health check"""
    try:
        # Test environment variables
        env_status = {
            "DATABRICKS_HOST": "✅" if os.getenv("DATABRICKS_HOST") else "❌",
            "DATABRICKS_CLIENT_ID": "✅" if os.getenv("DATABRICKS_CLIENT_ID") else "❌",
            "DATABRICKS_CLIENT_SECRET": "✅" if os.getenv("DATABRICKS_CLIENT_SECRET") else "❌"
        }
        
        return {
            "status": "healthy",
            "environment_variables": env_status,
            "python_path": str(Path(__file__).parent),
            "message": "All basic health checks passed"
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "unhealthy",
                "error": str(e),
                "message": "Health check failed"
            }
        )

# Try to import and include routers with error handling
logger.info("Starting router imports...")

try:
    from backend.fdb.routes_simple import router as fdb_simple_router
    app.include_router(fdb_simple_router)
    logger.info("✅ Simple FDB routes included")
except Exception as e:
    logger.error(f"❌ Failed to include simple FDB routes: {e}")

try:
    from backend.auth.routes import router as auth_router
    app.include_router(auth_router)
    logger.info("✅ Auth routes included")
except Exception as e:
    logger.error(f"❌ Failed to include auth routes: {e}")

logger.info("App initialization complete")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
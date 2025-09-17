"""
Streamlined FastAPI application for Databricks Apps
Modular backend with separate API modules per functionality
"""
import os
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import logging

# Import modular routers
from backend.auth.routes import router as auth_router
from backend.fdb.routes import router as fdb_router
from backend.fmt.routes import router as fmt_router
from backend.pdl.routes import router as pdl_router
from backend.config.routes import router as config_router
from backend.weekly.routes import router as weekly_router

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Gainwell Main App",
    description="Formulary Management Tool with modular backend architecture",
    version="2.0.0"
)

# Get the directory structure
project_root = Path(__file__).parent
static_dir = project_root / "static"

# Mount static files for React app
if static_dir.exists():
    if (static_dir / "assets").exists():
        app.mount("/assets", StaticFiles(directory=str(static_dir / "assets")), name="assets")

# Include all modular routers
app.include_router(auth_router)    # /api/health/*, /api/debug/*
app.include_router(fdb_router)     # /api/fdb/*
app.include_router(fmt_router)     # /api/fmt/*
app.include_router(pdl_router)     # /api/pdl/*
app.include_router(config_router)  # /api/config/*
app.include_router(weekly_router)  # /api/weekly/*

# ================== STATIC FILE SERVING ==================

@app.get("/")
async def serve_react_app():
    """Serve the React app's index.html"""
    index_file = static_dir / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    return {"message": "Gainwell Main App", "status": "React app not built"}

@app.get("/api/health/")
async def health_check():
    """Basic health check endpoint"""
    return {
        "status": "healthy", 
        "message": "Gainwell Main App is running",
        "version": "2.0.0",
        "architecture": "modular backend",
        "modules": [
            "auth", "fdb", "fmt", "pdl", "config", "weekly"
        ]
    }

# ================== SPA ROUTING (MUST BE LAST) ==================
# This catch-all route must be defined AFTER all API routes

@app.get("/{path:path}")
async def serve_spa_routes(path: str):
    """Serve React app for SPA routing - MUST BE LAST ROUTE"""
    if path.startswith("api/"):
        return {"error": "API route not found"}
    if path.startswith("assets/"):
        return {"error": "Asset not found"}
        
    index_file = static_dir / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    return {"error": "Frontend not available"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

# Scripts

This directory contains deployment and utility scripts for the Gainwell Main App.

## Files

### Deployment Scripts
- **`deploy-simple.sh`** - Simple deployment script for Databricks Apps
  - Builds React frontend
  - Syncs files to Databricks
  - Deploys the application

### Data Management Scripts  
- **`upload_to_databricks.sh`** - Uploads sample data files to Databricks volumes
  - Manages FDB, FMT, and PDL sample data uploads
  - Creates necessary volume directories

## Usage

### Deploy Application
```bash
./deploy-simple.sh
```

### Upload Sample Data
```bash
./upload_to_databricks.sh
```

## Prerequisites
- Databricks CLI configured
- Valid workspace access
- Node.js/npm installed for frontend builds

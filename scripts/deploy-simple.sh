#!/bin/bash

# 🚀 Simple Gainwell Main App Deployment
# Uses direct Databricks CLI sync and deploy commands

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="dbx_shared_demo"
APP_NAME="gainwell-main-app"
WORKSPACE_PATH="/Workspace/Users/scott.johnson@databricks.com/.bundle/gainwell-main-app/demo/files"

echo "🔸 === Building React Frontend === 🔸"
cd "${PROJECT_ROOT}/frontend"
npm run clean
npm run build
cd "${PROJECT_ROOT}"
echo "✅ Frontend built"

echo "🔸 === Syncing Files === 🔸"
databricks sync . "${WORKSPACE_PATH}" --profile "${PROFILE}"
echo "✅ Files synced"

echo "🔸 === Deploying App === 🔸"
databricks apps deploy "${APP_NAME}" --source-code-path "${WORKSPACE_PATH}" --profile "${PROFILE}"
echo "✅ App deployed"

echo "🎉 Deployment complete!"
echo "🌐 Visit: https://gainwell-main-app-3438839487639471.11.azure.databricksapps.com"
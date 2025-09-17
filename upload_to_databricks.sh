#!/bin/bash

# Upload FDB Sample Dataset to Databricks Volume
# Usage: ./upload_to_databricks.sh [catalog] [schema] [volume_name]

set -e

# Configuration
CATALOG=${1:-"main"}
SCHEMA=${2:-"default"} 
VOLUME_NAME=${3:-"fdb_data"}
LOCAL_DATA_DIR="sample_fdb_data"
PROFILE="dbx_shared_demo"  # Your Databricks CLI profile

VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/${VOLUME_NAME}"

echo "🚀 Uploading FDB Sample Dataset to Databricks Volume"
echo "   📁 Local directory: ${LOCAL_DATA_DIR}/"
echo "   🎯 Target volume: ${VOLUME_PATH}/"
echo "   👤 Profile: ${PROFILE}"
echo

# Check if data directory exists
if [ ! -d "$LOCAL_DATA_DIR" ]; then
    echo "❌ Error: Local data directory '$LOCAL_DATA_DIR' not found!"
    echo "   Run 'python generate_fdb_data.py' first to create the sample data."
    exit 1
fi

# Count files to upload
FILE_COUNT=$(find "$LOCAL_DATA_DIR" -name "*.csv" -o -name "*.json" | wc -l | tr -d ' ')
echo "📊 Found $FILE_COUNT files to upload"
echo

# Create volume directory (if it doesn't exist)
echo "🔧 Creating volume directory..."
databricks fs mkdirs "$VOLUME_PATH" --profile "$PROFILE" || {
    echo "⚠️  Volume directory may already exist (this is okay)"
}

# Upload all files
echo "📤 Uploading files..."
echo

CSV_FILES=($(find "$LOCAL_DATA_DIR" -name "*.csv" | sort))
JSON_FILES=($(find "$LOCAL_DATA_DIR" -name "*.json" | sort))

# Upload CSV files
for file in "${CSV_FILES[@]}"; do
    filename=$(basename "$file")
    echo "   📄 Uploading $filename..."
    databricks fs cp "$file" "$VOLUME_PATH/$filename" --profile "$PROFILE" --overwrite
done

# Upload JSON files  
for file in "${JSON_FILES[@]}"; do
    filename=$(basename "$file")
    echo "   📄 Uploading $filename..."
    databricks fs cp "$file" "$VOLUME_PATH/$filename" --profile "$PROFILE" --overwrite
done

# Upload README
if [ -f "$LOCAL_DATA_DIR/README.md" ]; then
    echo "   📄 Uploading README.md..."
    databricks fs cp "$LOCAL_DATA_DIR/README.md" "$VOLUME_PATH/README.md" --profile "$PROFILE" --overwrite
fi

echo
echo "✅ Upload completed successfully!"
echo
echo "🔍 Verify upload:"
echo "   databricks fs ls $VOLUME_PATH --profile $PROFILE"
echo
echo "🐍 Test in Python notebook:"
echo "   import pandas as pd"
echo "   df = pd.read_csv('$VOLUME_PATH/fdb_core_drugs.csv')"
echo "   print(f'Loaded {len(df)} drug records')"
echo
echo "📊 SQL query example:"
echo "   SELECT COUNT(*) as drug_count FROM csv.\`$VOLUME_PATH/fdb_core_drugs.csv\`"
echo
echo "🚀 Ready to update backend/fdb/routes.py to use volume data!"

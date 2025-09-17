# 🔍 FDB Search API Debug Guide

## 🚨 Issue: API Error 500 on FDB Search

The FDB search is returning a 500 error. I've deployed debugging improvements to help identify the issue.

## 🛠️ Debug Steps

### **Step 1: Check FDB Debug Endpoint**
Navigate to your app and add this to the URL path:
```
https://gainwell-main-app-3438839487639471.11.azure.databricksapps.com/api/fdb/debug
```

This debug endpoint will show:
- ✅ Whether pandas is installed and available
- 📁 Which volume paths exist/don't exist  
- 📂 Contents of directories if accessible
- 🧪 Test pandas file reading from different paths

### **Step 2: Try FDB Search Again**
Go back to the FDB Search tab and try searching. You should now get **more detailed error messages** instead of just "500 - Search failed".

---

## 🔍 Likely Issues & Solutions

### **Issue 1: Volume Path Wrong**
**Symptoms:** Debug shows no volume directories exist
**Solution:** 
- Verify volume created: `databricks fs ls /Volumes/demo/gainwell/ --profile dbx_shared_demo`
- Check if files uploaded: `databricks fs ls /Volumes/demo/gainwell/fdb_data --profile dbx_shared_demo`

### **Issue 2: Volume Path Format** 
**Symptoms:** Debug shows different path format needed
**Solution:** Update volume paths in `backend/fdb/routes.py`:
```python
VOLUME_BASE_PATH = "/correct/path/from/debug"
FALLBACK_VOLUME_PATH = "/alternative/path/format"
```

### **Issue 3: Pandas Not Available**
**Symptoms:** Debug shows `pandas_available: false`
**Solution:** 
- Check `requirements.txt` has `pandas==2.1.3`
- Redeploy app to install dependencies

### **Issue 4: File Permissions**
**Symptoms:** Paths exist but reading fails
**Solution:** Check Databricks App permissions for volume access

### **Issue 5: Wrong Catalog/Schema**
**Symptoms:** Volume paths don't exist  
**Current Config:** `demo.gainwell.fdb_data`
**Solution:** Update paths if your volume is in different catalog/schema

---

## 📋 What I've Added

### **1. Debug Endpoint (`/api/fdb/debug`)**
- Checks pandas availability
- Tests volume path access
- Lists directory contents
- Tests file reading

### **2. Better Error Messages**
Instead of generic 500 errors, you'll now see specific messages like:
- `"Pandas library not installed"`  
- `"FDB core data file not found at /path/to/file"`
- `"Failed to read core FDB file: [specific error]"`

### **3. Enhanced Logging**
- Detailed logging for each step of data loading
- Path checking and file existence verification
- Graceful handling of missing formulary files

---

## 🚀 Next Steps

1. **Check the debug endpoint** first to understand the exact issue
2. **Try FDB search** again to see the improved error message
3. **Based on the results**, we can:
   - Fix volume paths
   - Ensure proper file upload
   - Adjust permissions
   - Update configuration

**Let me know what the debug endpoint shows and I'll help fix the specific issue!** 🔧

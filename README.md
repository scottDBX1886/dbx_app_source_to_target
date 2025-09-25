# Gainwell Main App

A modular Databricks application with React frontend and FastAPI backend, designed for deployment on Databricks Apps platform. Features FDB search, FMT management, PDL coding, and weekly review workflows.

## Project Structure

```
gainwell_main_app/
├── backend/                    # FastAPI backend application
│   ├── auth/                  # Authentication endpoints
│   ├── config/                # Configuration and settings
│   ├── fdb/                   # FDB Search module
│   ├── fmt/                   # FMT Master module
│   ├── pdl/                   # PDL Coding module
│   ├── weekly/                # Weekly Review modules (FMT/PDL)
│   └── services/              # Core services (connector, tables)
├── frontend/                   # React frontend application
│   ├── src/
│   │   ├── components/pages/  # Page components (FDB, FMT, PDL, Weekly Review)
│   │   ├── services/          # API service layer
│   │   ├── styles/            # Global CSS styles
│   │   └── types/             # TypeScript type definitions
│   ├── package.json           # Node.js dependencies
│   └── vite.config.ts         # Vite configuration
├── database/                   # Database setup and DDL files
│   ├── create_assignment_tables.sql      # Complete DDL for assignment tables
│   ├── create_assignment_tables_databricks.py # Python execution script
│   ├── Create_Assignment_Tables.py       # Databricks notebook format
│   └── README.md              # Database documentation
├── data_generation/            # Sample data generation scripts
│   ├── generate_fdb_data.py   # FDB sample data generator
│   ├── generate_fmt_tenant_data.py # FMT tenant-specific data
│   ├── generate_pdl_tenant_data.py # PDL tenant-specific data
│   ├── verify_tenant_data.py  # Data quality verification
│   └── README.md              # Data generation documentation
├── scripts/                    # Deployment and utility scripts
│   ├── deploy-simple.sh       # Application deployment script
│   ├── upload_to_databricks.sh # Data upload script
│   └── README.md              # Scripts documentation
├── docs/                       # Documentation and prototypes
│   ├── fmt_interactive_prototype.html # Original FMT UI prototype
│   ├── settings_backup        # Configuration backup
│   └── README.md              # Documentation index
├── sample_fdb_data/           # Generated FDB sample data
├── sample_fmt_data/           # Generated FMT sample data
├── sample_pdl_data/           # Generated PDL sample data
├── static/                    # Built frontend assets
├── app.py                     # Main FastAPI application
├── databricks.yml            # Databricks Apps configuration
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

## Application Modules

### FDB Search
- **Drug Database Search**: Search and filter pharmaceutical drug database
- **Tenant-Specific Data**: Filter by tenant (MASTER, AK, MO) for state-specific formularies
- **Detailed Drug Information**: Click NDC for comprehensive drug details in sidebar drawer
- **Export Functionality**: Export search results to CSV/JSON formats

### FMT Master (Formulary Management Tool)
- **Formulary Management**: Manage Master Benefit IDs (MBIDs) and drug assignments
- **Dual Interface**: Main data table with detailed sidebar drawer
- **MBID Tracking**: Track formulary decisions and effective dates
- **Status Management**: Monitor assignment status and approval workflows

### PDL Coding (Preferred Drug List)
- **Key Code Assignment**: Assign PDL key codes (PA, QL, ST, etc.) to drugs
- **Template Management**: Use predefined templates for coding decisions
- **POS Export**: Generate Point of Service export files
- **Tenant-Specific Rules**: Different coding rules per tenant

### Weekly Review Workflow
- **FMT Review**: Weekly review process for new FMT assignments
- **PDL Review**: Weekly review process for new PDL coding
- **Dual Reviewer System**: Assign records to Reviewer A or B
- **Conflict Resolution**: Resolve differences between reviewers
- **Approval Process**: Final approval and sync to master tables

## Features

### Backend (FastAPI)
- **Modular Architecture**: Organized into domain-specific modules (FDB, FMT, PDL, Weekly)
- **Live Data Integration**: Direct connection to Databricks SQL Warehouse
- **Pandas/Spark Integration**: Seamless data processing with both pandas and Spark DataFrames
- **Assignment Tables**: Full CRUD operations for reviewer assignments
- **User Authentication**: Automatic user authentication through Databricks Apps headers
- **API Documentation**: Auto-generated OpenAPI/Swagger documentation

### Frontend (React + TypeScript)
- **Modern React**: Built with React 18+ and TypeScript
- **Responsive Design**: Clean, professional UI with consistent styling
- **Interactive Tables**: Searchable, filterable data tables with detailed drawers
- **Tenant Switching**: Dynamic tenant selection with immediate data updates
- **Real-time Feedback**: Loading states, error handling, and success notifications
- **Type Safety**: Full TypeScript integration with proper API interfaces

### Database Integration
- **Delta Lake Tables**: Optimized for Databricks with partitioning and clustering
- **Change Data Feed**: Full audit trail for all data changes
- **Tenant Isolation**: Data partitioned by tenant for performance and security
- **Sample Data**: Comprehensive test data for development and validation

## Getting Started

### Prerequisites

- Node.js 18+ and npm
- Python 3.9+
- Access to a Databricks workspace
- Databricks CLI (for deployment)

### Development Setup

1. **Clone and Setup Project**:
   ```bash
   cd gainwell_main_app
   
   # Copy environment template
   cp .env.example .env
   # Edit .env with your Databricks configuration
   ```

2. **Backend Setup**:
   ```bash
   cd backend
   
   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   
   # Run FastAPI development server
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

3. **Frontend Setup**:
   ```bash
   cd frontend
   
   # Install dependencies
   npm install
   
   # Start development server
   npm run dev
   ```

4. **Build for Production**:
   ```bash
   # Build React app
   cd frontend
   npm run build
   
   # Files will be built to frontend/build/
   ```

### Deployment to Databricks

1. **Quick Deployment** (Recommended):
   ```bash
   # Use the provided deployment script
   ./scripts/deploy-simple.sh
   ```

2. **Manual Deployment**:
   ```bash
   # Configure Databricks CLI
   databricks configure
   
   # Deploy using bundle
   databricks bundle deploy --target dev
   ```

3. **Setup Sample Data**:
   ```bash
   # Generate and upload sample data
   python data_generation/generate_fdb_data.py
   python data_generation/generate_fmt_tenant_data.py
   python data_generation/generate_pdl_tenant_data.py
   
   # Upload to Databricks volumes
   ./scripts/upload_to_databricks.sh
   ```

4. **Create Assignment Tables**:
   ```bash
   # Using Python script
   python database/create_assignment_tables_databricks.py
   
   # Or import the notebook into Databricks workspace
   # database/Create_Assignment_Tables.py
   ```

5. **Access Application**:
   The app will be available at the URL provided by Databricks Apps after deployment.

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

```env
# Databricks Configuration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token-here

# Service Principal (recommended for production)
DATABRICKS_CLIENT_ID=your-client-id
DATABRICKS_CLIENT_SECRET=your-client-secret

# App Configuration
APP_SERVICE_PRINCIPAL_ID=your-app-sp-id
REQUIRE_USER_AUTH=true
DEBUG=false
```

### Databricks Apps Configuration

The `databricks.yml` file configures the application for Databricks deployment:

- **Static Files**: Serves React build files and assets
- **Environment Variables**: Automatically injects Databricks context
- **Health Checks**: Configures health monitoring
- **Permissions**: Sets up service principal permissions
- **Compute**: Configures serverless compute resources

## Authentication & Security

### Service Principal Authentication
- The app uses a dedicated service principal for Databricks API access
- Service principal credentials are automatically injected by Databricks Apps
- No manual token management required in production

### User Authentication
- User authentication is handled automatically by Databricks Apps
- User information is available via HTTP headers:
  - `X-Forwarded-User-Id`
  - `X-Forwarded-User-Email`
  - `X-Forwarded-User-Display-Name`
  - `X-Forwarded-User-Groups`

### Data Access Control
- Users are verified against Databricks workspace permissions
- Additional authorization can be implemented based on user groups
- Service principal ensures secure API access

## API Endpoints

### Health Endpoints
- `GET /api/health/` - Basic health check
- `GET /api/health/user-info` - Current user information
- `GET /api/health/databricks-status` - Databricks connection status

### Future Endpoints
The modular structure allows easy addition of new endpoints for:
- Data querying and analysis
- Job management
- Notebook operations
- Custom business logic

## Development

### Adding New Features

1. **Backend Endpoints**:
   - Add new endpoint files in `backend/app/api/endpoints/`
   - Register routes in the main application
   - Add business logic in `backend/app/services/`

2. **Frontend Components**:
   - Add new pages in `frontend/src/components/Pages/`
   - Update routing in `App.tsx`
   - Add navigation items in `Sidebar.tsx`

3. **API Integration**:
   - Extend `frontend/src/services/api.ts`
   - Create custom hooks in `frontend/src/hooks/`
   - Update TypeScript types in `frontend/src/types/`

### Testing

```bash
# Backend testing
cd backend
pytest

# Frontend testing
cd frontend
npm test

# End-to-end testing
npm run test:e2e
```

## Troubleshooting

### Common Issues

1. **Authentication Failures**:
   - Verify service principal credentials
   - Check Databricks workspace permissions
   - Ensure environment variables are set correctly

2. **Build Failures**:
   - Verify Node.js and Python versions
   - Clear node_modules and reinstall dependencies
   - Check for TypeScript compilation errors

3. **Databricks Connection Issues**:
   - Verify DATABRICKS_HOST is correct
   - Check network connectivity
   - Ensure service principal has necessary permissions

### Logs and Debugging

- Backend logs: Available in Databricks Apps logs
- Frontend logs: Check browser developer console
- Health checks: Monitor `/api/health/` endpoints

## Contributing

1. Follow the modular architecture patterns
2. Add comprehensive error handling
3. Update TypeScript types for new features
4. Test both locally and in Databricks environment
5. Update documentation for new features

## License

[Add your license information here]

## Support

For support and questions, please [add your support contact information here].

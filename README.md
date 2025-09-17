# Gainwell Main App

A modular Databricks application with React frontend and FastAPI backend, designed for deployment on Databricks Apps platform.

## Project Structure

```
gainwell_main_app/
├── backend/                    # FastAPI backend application
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py            # Main FastAPI application
│   │   ├── api/               # API endpoints
│   │   │   ├── __init__.py
│   │   │   └── endpoints/
│   │   │       ├── __init__.py
│   │   │       └── health.py  # Health check endpoints
│   │   ├── core/              # Core functionality
│   │   │   ├── __init__.py
│   │   │   ├── config.py      # Configuration settings
│   │   │   └── auth.py        # Authentication and authorization
│   │   └── services/          # Business logic services
│   │       ├── __init__.py
│   │       └── databricks_service.py
│   └── requirements.txt       # Python dependencies
├── frontend/                   # React frontend application
│   ├── public/
│   ├── src/
│   │   ├── components/        # React components
│   │   │   ├── Layout/        # Layout components
│   │   │   └── Pages/         # Page components
│   │   ├── services/          # API service layer
│   │   ├── hooks/             # Custom React hooks
│   │   ├── types/             # TypeScript type definitions
│   │   ├── App.tsx           # Main App component
│   │   └── main.tsx          # Application entry point
│   ├── package.json          # Node.js dependencies
│   └── vite.config.ts        # Vite configuration
├── static/                    # Static files directory
├── databricks.yml            # Databricks Apps configuration
├── .env.example              # Environment variables template
└── README.md                 # This file
```

## Features

### Backend (FastAPI)
- **Modular Architecture**: Organized into core, API, and service layers
- **Service Principal Authentication**: Secure authentication using Databricks service principals
- **User Verification**: Automatic user authentication through Databricks Apps headers
- **Databricks Integration**: Built-in services for interacting with Databricks APIs
- **Health Checks**: Comprehensive health monitoring endpoints
- **Static File Serving**: Serves React build files and static assets

### Frontend (React + TypeScript)
- **Modern React**: Built with React 18+ and TypeScript
- **Databricks Design System**: Uses official Databricks UI components
- **Responsive Layout**: Professional layout with navigation and routing
- **API Integration**: Complete integration with FastAPI backend
- **Real-time Status**: Live connection status and user information
- **Component Organization**: Well-structured component hierarchy

### Databricks Integration
- **Service Principal Auth**: Automatic service principal authentication
- **User Forwarding**: Leverages Databricks Apps user forwarding
- **Workspace Integration**: Direct integration with Databricks workspace
- **Cluster Management**: View and interact with Databricks clusters

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

1. **Configure Databricks CLI**:
   ```bash
   databricks configure
   ```

2. **Deploy Application**:
   ```bash
   # Deploy to development
   databricks bundle deploy --target dev
   
   # Deploy to production
   databricks bundle deploy --target prod
   ```

3. **Access Application**:
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

/**
 * Formulary Management Tool (Prototype)
 * Lakebase OLTP + Delta OLAP with hybrid authentication
 */
import { useState, createContext, useContext } from 'react';
import { BrowserRouter as Router, Routes, Route, Link, Navigate, useLocation } from 'react-router-dom';
import { FDBSearch } from './components/pages/FDBSearch';
import { FMTMaster } from './components/pages/FMTMaster';
import { PDLCoding } from './components/pages/PDLCoding';
import { WeeklyReviewFMT } from './components/pages/WeeklyReviewFMT';
import { WeeklyReviewPDL } from './components/pages/WeeklyReviewPDL';
import { Config } from './components/pages/Config';
import { AuthPage } from './components/pages/AuthPage';
import { Settings } from './components/pages/Settings';

// Create context for tenant selection
const TenantContext = createContext<{
  tenant: string;
  setTenant: (tenant: string) => void;
}>({
  tenant: 'MASTER',
  setTenant: () => {}
});

// Create context for user simulation
const UserContext = createContext<{
  user: { name: string; role: string; allowedTenants: string[] };
  setUser: (user: { name: string; role: string; allowedTenants: string[] }) => void;
}>({
  user: { name: 'Admin User', role: 'admin', allowedTenants: ['MASTER', 'AK', 'MO'] },
  setUser: () => {}
});

const TABS = [
  { key: '/fdb-search', label: 'FDB Search', icon: '🔍' },
  { key: '/fmt-master', label: 'FMT Master', icon: '📋' },
  { key: '/pdl-coding', label: 'PDL Coding', icon: '🏷️' },
  { key: '/weekly-review-fmt', label: 'Weekly Review – FMT', icon: '📅' },
  { key: '/weekly-review-pdl', label: 'Weekly Review – PDL', icon: '📋' },
  { key: '/config', label: 'Config', icon: '⚙️' },
  { key: '/auth', label: 'User Auth', icon: '🔐' },
  { key: '/settings', label: 'Settings', icon: '🛠️' }
];

// Sample users for simulation
const sampleUsers = [
  { name: 'Admin User', role: 'admin', allowedTenants: ['MASTER', 'AK', 'MO'] },
  { name: 'AK User', role: 'user', allowedTenants: ['AK'] },
  { name: 'MO User', role: 'user', allowedTenants: ['MO'] },
  { name: 'Master User', role: 'user', allowedTenants: ['MASTER'] }
];

function Header() {
  const { tenant, setTenant } = useContext(TenantContext);
  const { user, setUser } = useContext(UserContext);
  
  // When user changes, auto-select their first allowed tenant
  const handleUserChange = (selectedUser: typeof sampleUsers[0]) => {
    setUser(selectedUser);
    // Auto-switch to user's first allowed tenant
    if (selectedUser.allowedTenants.length > 0 && !selectedUser.allowedTenants.includes(tenant)) {
      setTenant(selectedUser.allowedTenants[0]);
    }
  };
  
  return (
    <header className="app-header">
      <div className="wrap" style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
        <div style={{ flex: '1 1 auto' }}>
          <h1>Formulary Management Tool (Prototype)</h1>
          <div className="muted">Lakebase OLTP + Delta OLAP (future API wiring)</div>
        </div>
        <div className="row" style={{ gap: '16px', marginLeft: '24px', alignItems: 'center' }}>
          <div className="row" style={{ alignItems: 'center' }}>
            <label style={{ minWidth: '32px' }}>User</label>
            <select 
              aria-label="User selector"
              value={`${user.name}|${user.role}`}
              onChange={(e) => {
                const [name, role] = e.target.value.split('|');
                const selectedUser = sampleUsers.find(u => u.name === name && u.role === role);
                if (selectedUser) handleUserChange(selectedUser);
              }}
              style={{ minWidth: '160px' }}
            >
              {sampleUsers.map((u) => (
                <option key={`${u.name}|${u.role}`} value={`${u.name}|${u.role}`}>
                  {u.name} ({u.role === 'admin' ? 'Admin' : 'User'})
                </option>
              ))}
            </select>
          </div>
          <div className="row" style={{ alignItems: 'center' }}>
            <label style={{ minWidth: '48px' }}>Tenant</label>
            <select 
              aria-label="Tenant selector" 
              value={tenant}
              onChange={(e) => setTenant(e.target.value)}
              disabled={user.allowedTenants.length <= 1}
              style={{ minWidth: '140px' }}
            >
              {user.allowedTenants.includes('MASTER') && <option value="MASTER">MASTER (Mother)</option>}
              {user.allowedTenants.includes('AK') && <option value="AK">AK (Child)</option>}
              {user.allowedTenants.includes('MO') && <option value="MO">MO (Child)</option>}
            </select>
          </div>
        </div>
      </div>
    </header>
  );
}

function Navigation() {
  const location = useLocation();
  
  return (
    <nav className="wrap tabs" aria-label="Primary">
      {TABS.map((tab) => (
        <Link
          key={tab.key}
          to={tab.key}
          className={`tab ${location.pathname === tab.key ? 'active' : ''}`}
          role="tab"
          aria-selected={location.pathname === tab.key ? 'true' : 'false'}
        >
          <span className="nav-icon">{tab.icon}</span>
          {tab.label}
        </Link>
      ))}
    </nav>
  );
}

function AppContent() {
  return (
    <div>
      <Header />
      <Navigation />
      
      <main className="wrap" id="app" tabIndex={-1}>
        <Routes>
          <Route path="/" element={<Navigate to="/fdb-search" replace />} />
          <Route path="/fdb-search" element={<FDBSearch />} />
          <Route path="/fmt-master" element={<FMTMaster />} />
          <Route path="/pdl-coding" element={<PDLCoding />} />
          <Route path="/weekly-review-fmt" element={<WeeklyReviewFMT />} />
          <Route path="/weekly-review-pdl" element={<WeeklyReviewPDL />} />
          <Route path="/config" element={<Config />} />
          <Route path="/auth" element={<AuthPage />} />
          <Route path="/settings" element={<Settings />} />
        </Routes>
      </main>
    </div>
  );
}

function App() {
  const [tenant, setTenant] = useState('MASTER');
  const [user, setUser] = useState(sampleUsers[0]); // Start with Admin User
  
  return (
    <UserContext.Provider value={{ user, setUser }}>
      <TenantContext.Provider value={{ tenant, setTenant }}>
        <Router>
          <AppContent />
        </Router>
      </TenantContext.Provider>
    </UserContext.Provider>
  );
}

export default App;
export { TenantContext, UserContext };
import { Link, useParams, useLocation, Outlet } from 'react-router-dom';
import { useAuth } from '../lib/auth';

export default function AppShell() {
  const { user } = useAuth();
  const { slug } = useParams();
  const location = useLocation();

  const navItems = slug ? [
    { path: `/projects/${slug}/pipeline`, label: '📊 Production Pipeline' },
    { path: `/projects/${slug}/dev`, label: '💻 Development' },
    { path: `/projects/${slug}/git`, label: '🔀 Git / Branches' },
    { path: `/projects/${slug}/settings`, label: '⚙️ Settings' },
  ] : [];

  return (
    <div className="flex flex-col h-screen bg-[#0f1117]">
      <header className="h-12 bg-[#22232d] border-b border-[#2a2b36] flex items-center px-4 gap-4">
        <Link to="/projects" className="font-bold text-[#818cf8]">Conductor</Link>
        <div className="flex-1" />
        {user?.is_admin && (
          <Link to="/admin" className="text-sm text-gray-400 hover:text-white">Admin Panel</Link>
        )}
        <div className="w-7 h-7 rounded-full bg-[#6366f1] flex items-center justify-center text-xs font-bold">
          {user?.display_name?.[0] || '?'}
        </div>
      </header>
      <div className="flex flex-1 overflow-hidden">
        {slug && (
          <nav className="w-56 bg-[#1a1b23] border-r border-[#2a2b36] p-2 overflow-y-auto">
            <p className="text-xs text-gray-500 uppercase px-3 py-2 font-semibold">{slug}</p>
            {navItems.map(item => (
              <Link key={item.path} to={item.path}
                className={`flex items-center gap-2 px-3 py-2 rounded text-sm mb-0.5 ${
                  location.pathname.startsWith(item.path) ? 'bg-[#2a2b36] text-white' : 'text-gray-400 hover:text-white'
                }`}>
                {item.label}
              </Link>
            ))}
          </nav>
        )}
        <main className="flex-1 overflow-auto p-6">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
import { useEffect, useState } from 'react';
import { apiFetch } from '../lib/api';
import { RoleBadge } from '../components/RoleBadge';

// ── Types ──────────────────────────────────────────────────────────────────

interface User {
  id: string;
  email: string;
  display_name: string;
  is_admin: boolean;
  is_active: boolean;
  created_at: string;
}

interface Project {
  id: string;
  name: string;
  slug: string;
  member_count: number;
  airflow_status: string;
  lifecycle_status?: string;
  created_at: string;
}

interface Role {
  id: string;
  name: string;
  description: string;
  is_system: boolean;
  created_at: string;
}

interface Permission {
  id: string;
  resource: string;
  action: string;
  constraint: string | null;
}

// ── Permission scopes ──────────────────────────────────────────────────────

const PERMISSION_SCOPES = [
  { key: 'dag.view', label: 'View DAGs', resource: 'dag', action: 'view' },
  { key: 'dag.run', label: 'Run DAGs', resource: 'dag', action: 'run' },
  { key: 'branch.create_delete', label: 'Create/delete branches', resource: 'branch', action: 'create_delete' },
  { key: 'mr.create', label: 'Create MRs', resource: 'mr', action: 'create' },
  { key: 'mr.merge', label: 'Merge MRs', resource: 'mr', action: 'merge' },
  { key: 'dev.access', label: 'Access development mode', resource: 'dev', action: 'access' },
  { key: 'settings.view', label: 'View settings', resource: 'settings', action: 'view' },
  { key: 'settings.change', label: 'Change settings', resource: 'settings', action: 'change' },
  { key: 'members.manage', label: 'Manage members', resource: 'members', action: 'manage' },
];

// ── Status helpers ─────────────────────────────────────────────────────────

function statusBadge(status: string) {
  const colors: Record<string, string> = {
    running: 'bg-green-900 text-green-400',
    ready: 'bg-green-900 text-green-400',
    provisioning: 'bg-yellow-900 text-yellow-400',
    provision_failed: 'bg-red-900 text-red-400',
    not_provisioned: 'bg-gray-800 text-gray-400',
    stopped: 'bg-red-900 text-red-400',
    failed: 'bg-red-900 text-red-400',
  };
  const cls = colors[status] ?? 'bg-gray-800 text-gray-400';
  return (
    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${cls}`}>
      {status.replace(/_/g, ' ')}
    </span>
  );
}

// ── Section components ─────────────────────────────────────────────────────

function UsersTable() {
  const [users, setUsers] = useState<User[]>([]);
  const [editingId, setEditingId] = useState<string | null>(null);

  useEffect(() => {
    apiFetch('/admin/users').then(setUsers).catch(() => setUsers([]));
  }, []);

  async function toggleField(user: User, field: 'is_admin' | 'is_active') {
    const body = { [field]: !user[field] };
    await apiFetch(`/admin/users/${user.id}`, {
      method: 'PATCH',
      body: JSON.stringify(body),
    });
    setUsers(prev =>
      prev.map(u => (u.id === user.id ? { ...u, [field]: !u[field] } : u))
    );
  }

  return (
    <div>
      <h2 className="text-lg font-semibold text-white mb-4">
        Users ({users.length})
      </h2>
      <div className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#2a2b36]">
              <th className="text-left p-3 text-gray-400 font-medium">User</th>
              <th className="text-left p-3 text-gray-400 font-medium">Role</th>
              <th className="text-left p-3 text-gray-400 font-medium">Status</th>
              <th className="text-left p-3 text-gray-400 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody>
            {users.map(u => (
              <tr key={u.id} className="border-b border-[#2a2b36]">
                <td className="p-3">
                  <div className="text-white">{u.display_name || u.email}</div>
                  <div className="text-xs text-gray-500">{u.email}</div>
                </td>
                <td className="p-3">
                  <RoleBadge role={u.is_admin ? 'super_admin' : 'user'} />
                </td>
                <td className="p-3">
                  <span className={u.is_active ? 'text-green-400' : 'text-red-400'}>
                    {u.is_active ? 'Active' : 'Inactive'}
                  </span>
                </td>
                <td className="p-3">
                  <div className="flex gap-2">
                    <button
                      onClick={() => toggleField(u, 'is_admin')}
                      className="text-xs px-2 py-1 rounded bg-[#22232d] text-gray-300 hover:bg-[#2a2b36] border border-[#2a2b36]"
                    >
                      {u.is_admin ? 'Revoke Admin' : 'Make Admin'}
                    </button>
                    <button
                      onClick={() => toggleField(u, 'is_active')}
                      className="text-xs px-2 py-1 rounded bg-[#22232d] text-gray-300 hover:bg-[#2a2b36] border border-[#2a2b36]"
                    >
                      {u.is_active ? 'Deactivate' : 'Activate'}
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ProjectsTable() {
  const [projects, setProjects] = useState<Project[]>([]);

  useEffect(() => {
    apiFetch('/admin/projects').then(setProjects).catch(() => setProjects([]));
  }, []);

  return (
    <div>
      <h2 className="text-lg font-semibold text-white mb-4">
        All Projects ({projects.length})
      </h2>
      <div className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#2a2b36]">
              <th className="text-left p-3 text-gray-400 font-medium">Name</th>
              <th className="text-left p-3 text-gray-400 font-medium">Slug</th>
              <th className="text-left p-3 text-gray-400 font-medium">Members</th>
              <th className="text-left p-3 text-gray-400 font-medium">Lifecycle</th>
              <th className="text-left p-3 text-gray-400 font-medium">Airflow</th>
            </tr>
          </thead>
          <tbody>
            {projects.map(p => (
              <tr key={p.id} className="border-b border-[#2a2b36]">
                <td className="p-3 text-white">{p.name}</td>
                <td className="p-3 font-mono text-xs text-gray-400">{p.slug}</td>
                <td className="p-3 text-gray-400">{p.member_count}</td>
                <td className="p-3">{statusBadge(p.lifecycle_status || 'unknown')}</td>
                <td className="p-3">{statusBadge(p.airflow_status)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function RolesEditor() {
  const [roles, setRoles] = useState<Role[]>([]);
  const [selectedRoleId, setSelectedRoleId] = useState<string | null>(null);
  const [perms, setPerms] = useState<Permission[]>([]);
  const [showNewRoleForm, setShowNewRoleForm] = useState(false);
  const [newRoleName, setNewRoleName] = useState('');
  const [newRoleDesc, setNewRoleDesc] = useState('');
  const [editName, setEditName] = useState('');
  const [editDesc, setEditDesc] = useState('');

  useEffect(() => {
    apiFetch('/admin/roles').then(setRoles).catch(() => setRoles([]));
  }, []);

  useEffect(() => {
    if (!selectedRoleId) {
      setPerms([]);
      return;
    }
    apiFetch(`/admin/roles/${selectedRoleId}/permissions`)
      .then(setPerms)
      .catch(() => setPerms([]));
    const role = roles.find(r => r.id === selectedRoleId);
    if (role) {
      setEditName(role.name);
      setEditDesc(role.description || '');
    }
  }, [selectedRoleId, roles]);

  const selectedRole = roles.find(r => r.id === selectedRoleId);

  function hasPermission(resource: string, action: string) {
    return perms.some(p => p.resource === resource && p.action === action);
  }

  async function togglePermission(resource: string, action: string) {
    if (!selectedRoleId) return;
    const existing = perms.find(p => p.resource === resource && p.action === action);
    if (existing) {
      await apiFetch(`/admin/roles/${selectedRoleId}/permissions/${existing.id}`, {
        method: 'DELETE',
      });
      setPerms(prev => prev.filter(p => p.id !== existing.id));
    } else {
      const newPerm = await apiFetch(`/admin/roles/${selectedRoleId}/permissions`, {
        method: 'POST',
        body: JSON.stringify({ resource, action }),
      });
      setPerms(prev => [...prev, newPerm]);
    }
  }

  async function createRole() {
    if (!newRoleName.trim()) return;
    const role = await apiFetch('/admin/roles', {
      method: 'POST',
      body: JSON.stringify({ name: newRoleName.trim(), description: newRoleDesc.trim() }),
    });
    setRoles(prev => [...prev, role]);
    setNewRoleName('');
    setNewRoleDesc('');
    setShowNewRoleForm(false);
    setSelectedRoleId(role.id);
  }

  async function updateRole() {
    if (!selectedRoleId) return;
    const updated = await apiFetch(`/admin/roles/${selectedRoleId}`, {
      method: 'PATCH',
      body: JSON.stringify({ name: editName.trim(), description: editDesc.trim() }),
    });
    setRoles(prev => prev.map(r => (r.id === selectedRoleId ? { ...r, ...updated } : r)));
  }

  async function deleteRole() {
    if (!selectedRoleId) return;
    await apiFetch(`/admin/roles/${selectedRoleId}`, { method: 'DELETE' });
    setRoles(prev => prev.filter(r => r.id !== selectedRoleId));
    setSelectedRoleId(null);
  }

  return (
    <div className="flex gap-4 h-full">
      {/* Left panel: roles list */}
      <div className="w-72 flex-shrink-0">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-lg font-semibold text-white">Roles</h2>
          <button
            onClick={() => setShowNewRoleForm(true)}
            className="text-xs px-2 py-1 rounded bg-[#6366f1] text-white hover:bg-[#4f46e5]"
          >
            + Create Custom Role
          </button>
        </div>

        {showNewRoleForm && (
          <div className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg p-3 mb-3">
            <input
              placeholder="Role name"
              value={newRoleName}
              onChange={e => setNewRoleName(e.target.value)}
              className="w-full bg-[#0f1117] border border-[#2a2b36] rounded px-2 py-1 text-white text-sm mb-2"
            />
            <input
              placeholder="Description"
              value={newRoleDesc}
              onChange={e => setNewRoleDesc(e.target.value)}
              className="w-full bg-[#0f1117] border border-[#2a2b36] rounded px-2 py-1 text-white text-sm mb-2"
            />
            <div className="flex gap-2">
              <button
                onClick={createRole}
                className="text-xs px-2 py-1 rounded bg-[#6366f1] text-white hover:bg-[#4f46e5]"
              >
                Create
              </button>
              <button
                onClick={() => setShowNewRoleForm(false)}
                className="text-xs px-2 py-1 rounded bg-[#22232d] text-gray-300 hover:bg-[#2a2b36]"
              >
                Cancel
              </button>
            </div>
          </div>
        )}

        <div className="space-y-2">
          {roles.map(role => {
            const isSelected = role.id === selectedRoleId;
            return (
              <button
                key={role.id}
                onClick={() => setSelectedRoleId(role.id)}
                className={`w-full text-left p-3 rounded-lg border transition-colors ${
                  isSelected
                    ? 'border-dashed border-[#6366f1] bg-[#1a1b23]'
                    : 'border border-[#2a2b36] bg-[#1a1b23] hover:border-[#3a3b46]'
                }`}
              >
                <div className="flex items-center gap-2">
                  <span className="text-white font-medium text-sm">{role.name}</span>
                  <span
                    className={`text-xs px-1.5 py-0.5 rounded ${
                      role.is_system
                        ? 'bg-gray-800 text-gray-400'
                        : 'bg-[#6366f1]/20 text-[#818cf8]'
                    }`}
                  >
                    {role.is_system ? 'system' : 'custom'}
                  </span>
                </div>
                {role.description && (
                  <div className="text-xs text-gray-500 mt-1">{role.description}</div>
                )}
              </button>
            );
          })}
        </div>
      </div>

      {/* Right panel: role editor */}
      <div className="flex-1">
        {selectedRole ? (
          <div className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg p-4">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold text-white">
                Edit Role: {selectedRole.name}
              </h2>
              {!selectedRole.is_system && (
                <button
                  onClick={deleteRole}
                  className="text-xs px-2 py-1 rounded bg-red-900 text-red-400 hover:bg-red-800 border border-red-800"
                >
                  Delete Role
                </button>
              )}
            </div>

            <div className="mb-4 space-y-2">
              <input
                value={editName}
                onChange={e => setEditName(e.target.value)}
                className="w-full bg-[#0f1117] border border-[#2a2b36] rounded px-3 py-2 text-white text-sm"
                placeholder="Role name"
              />
              <input
                value={editDesc}
                onChange={e => setEditDesc(e.target.value)}
                className="w-full bg-[#0f1117] border border-[#2a2b36] rounded px-3 py-2 text-white text-sm"
                placeholder="Description"
              />
              <button
                onClick={updateRole}
                className="text-xs px-3 py-1.5 rounded bg-[#6366f1] text-white hover:bg-[#4f46e5]"
              >
                Save Changes
              </button>
            </div>

            <h3 className="text-sm font-medium text-gray-400 mb-3">Permissions</h3>
            <div className="grid grid-cols-2 gap-2">
              {PERMISSION_SCOPES.map(scope => (
                <label
                  key={scope.key}
                  className="flex items-center gap-2 p-2 rounded bg-[#22232d] cursor-pointer hover:bg-[#2a2b36]"
                >
                  <input
                    type="checkbox"
                    checked={hasPermission(scope.resource, scope.action)}
                    onChange={() => togglePermission(scope.resource, scope.action)}
                    className="accent-[#6366f1]"
                  />
                  <span className="text-sm text-gray-300">{scope.label}</span>
                </label>
              ))}
            </div>
          </div>
        ) : (
          <div className="flex items-center justify-center h-full text-gray-500">
            Select a role to edit permissions
          </div>
        )}
      </div>
    </div>
  );
}

function SystemSettings() {
  return (
    <div className="flex items-center justify-center h-64 text-gray-500">
      System settings coming soon
    </div>
  );
}

// ── Main AdminPage ─────────────────────────────────────────────────────────

type Section = 'users' | 'projects' | 'roles' | 'system';

const SECTIONS: { key: Section; label: string; icon: string }[] = [
  { key: 'users', label: 'Users', icon: '👥' },
  { key: 'projects', label: 'All Projects', icon: '📁' },
  { key: 'roles', label: 'Roles & Permissions', icon: '🔐' },
  { key: 'system', label: 'System Settings', icon: '⚙️' },
];

export default function AdminPage() {
  const [section, setSection] = useState<Section>('users');

  return (
    <div className="flex h-full">
      {/* LEFT: internal admin sidebar */}
      <nav className="w-56 bg-[#1a1b23] border-r border-[#2a2b36] p-2 flex flex-col gap-1">
        <h1 className="text-sm font-semibold text-gray-400 uppercase tracking-wider px-2 py-2">
          Admin
        </h1>
        {SECTIONS.map(s => (
          <button
            key={s.key}
            onClick={() => setSection(s.key)}
            className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm text-left transition-colors ${
              section === s.key
                ? 'bg-[#6366f1] text-white'
                : 'text-gray-400 hover:bg-[#22232d] hover:text-white'
            }`}
          >
            <span>{s.icon}</span>
            {s.label}
          </button>
        ))}
      </nav>

      {/* RIGHT: section content */}
      <div className="flex-1 p-4 overflow-auto">
        {section === 'users' && <UsersTable />}
        {section === 'projects' && <ProjectsTable />}
        {section === 'roles' && <RolesEditor />}
        {section === 'system' && <SystemSettings />}
      </div>
    </div>
  );
}
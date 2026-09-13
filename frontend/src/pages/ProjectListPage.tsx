import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiFetch } from '../lib/api';
import { RoleBadge } from '../components/RoleBadge';

const OPERATION_STORAGE_KEY = 'conductor_project_operations';
const PENDING_CREATE_STORAGE_KEY = 'conductor_pending_project_creates';
const ACTIVE_OPERATION_STATUSES = new Set(['pending', 'running', 'retry_wait']);

interface StoredOperation {
  projectId: string;
  projectSlug: string;
  operationId: string;
  operation: string;
  idempotencyKey: string;
  status: string;
  currentStep?: string | null;
  errorCode?: string | null;
  errorMessage?: string | null;
}

interface PendingCreateRequest {
  name: string;
  slug: string;
  description: string | null;
  idempotencyKey: string;
}

interface OperationResponse {
  id: string;
  operation: string;
  status: string;
  current_step?: string | null;
  error_code?: string | null;
  error_message?: string | null;
}

interface Project {
  id: string;
  name: string;
  slug: string;
  description: string | null;
  member_count: number;
  role: string | null;
  created_at: string;
}

function loadStoredOperations(): StoredOperation[] {
  try {
    const stored = localStorage.getItem(OPERATION_STORAGE_KEY);
    return stored ? JSON.parse(stored) : [];
  } catch {
    return [];
  }
}

function replaceStoredOperation(operation: StoredOperation) {
  const operations = loadStoredOperations();
  const replacementIndex = operations.findIndex((candidate) => candidate.projectSlug === operation.projectSlug);
  if (replacementIndex === -1) operations.push(operation);
  else operations[replacementIndex] = operation;
  localStorage.setItem(OPERATION_STORAGE_KEY, JSON.stringify(operations));
  return operations;
}

function loadPendingCreates(): PendingCreateRequest[] {
  try {
    const stored = localStorage.getItem(PENDING_CREATE_STORAGE_KEY);
    return stored ? JSON.parse(stored) : [];
  } catch {
    return [];
  }
}

function savePendingCreate(request: PendingCreateRequest) {
  const pendingCreates = loadPendingCreates().filter((candidate) => candidate.slug !== request.slug);
  pendingCreates.push(request);
  localStorage.setItem(PENDING_CREATE_STORAGE_KEY, JSON.stringify(pendingCreates));
}

function removePendingCreate(slug: string) {
  localStorage.setItem(
    PENDING_CREATE_STORAGE_KEY,
    JSON.stringify(loadPendingCreates().filter((candidate) => candidate.slug !== slug)),
  );
}

function CreateProjectModal({
  open,
  onClose,
  onCreated,
  onOperationAccepted,
}: {
  open: boolean;
  onClose: () => void;
  onCreated: () => void;
  onOperationAccepted: (operations: StoredOperation[]) => void;
}) {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [idempotencyKey, setIdempotencyKey] = useState('');

  if (!open) return null;

  const slug = name
    .toLowerCase()
    .trim()
    .replace(/\s+/g, '-')
    .replace(/[^a-z0-9-]/g, '')
    .replace(/-+/g, '-');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    const request = { name: name.trim(), slug, description: description.trim() || null };
    const matchingPendingCreate = loadPendingCreates().find((candidate) => (
      candidate.name === request.name
      && candidate.slug === request.slug
      && candidate.description === request.description
    ));
    const requestKey = idempotencyKey || matchingPendingCreate?.idempotencyKey || crypto.randomUUID();
    setIdempotencyKey(requestKey);
    savePendingCreate({ ...request, idempotencyKey: requestKey });
    setSubmitting(true);
    setError('');
    try {
      const response = await apiFetch('/projects', {
        method: 'POST',
        headers: { 'Idempotency-Key': requestKey },
        body: JSON.stringify(request),
      }) as { project: Pick<Project, 'id' | 'slug'>; operation: OperationResponse };
      const operations = replaceStoredOperation({
        projectId: response.project.id,
        projectSlug: response.project.slug,
        operationId: response.operation.id,
        operation: response.operation.operation,
        idempotencyKey: requestKey,
        status: response.operation.status,
      });
      onOperationAccepted(operations);
      onCreated();
      onClose();
      removePendingCreate(response.project.slug);
      setName('');
      setDescription('');
      setIdempotencyKey('');
    } catch (err: any) {
      setError(`${err.message || 'Failed to create project'}. Send again will reuse the same request key.`);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
      <div className="bg-[#1a1b23] border border-[#2a2b36] rounded-xl p-6 w-full max-w-md">
        <h2 className="text-lg font-semibold text-white mb-4">New Project</h2>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-xs text-gray-400 mb-1">Name</label>
            <input
              autoFocus
              value={name}
              onChange={(e) => setName(e.target.value)}
              className="w-full bg-[#0f1117] border border-[#2a2b36] rounded-md px-3 py-2 text-white text-sm focus:border-[#6366f1] outline-none"
              placeholder="My Project"
              required
            />
            {slug && (
              <p className="text-xs text-gray-500 mt-1">
                Slug: <span className="font-mono text-gray-400">{slug}</span>
              </p>
            )}
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Description</label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              className="w-full bg-[#0f1117] border border-[#2a2b36] rounded-md px-3 py-2 text-white text-sm focus:border-[#6366f1] outline-none resize-none"
              rows={3}
              placeholder="Optional description..."
            />
          </div>
          {error && <p className="text-red-400 text-xs">{error}</p>}
          <div className="flex gap-2 justify-end">
            <button
              type="button"
              onClick={onClose}
              className="px-3 py-1.5 text-xs text-gray-400 hover:text-white"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={submitting || !name.trim()}
              className="px-3 py-1.5 bg-[#6366f1] text-white text-xs rounded-md hover:bg-[#4f46e5] disabled:opacity-50"
            >
              {submitting ? 'Creating...' : 'Create'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

function lifecycleStatusLabel(status: string) {
  return status.replace(/_/g, ' ');
}

function OperationPanel({
  operations,
  onRetry,
}: {
  operations: StoredOperation[];
  onRetry: (operation: StoredOperation) => Promise<void>;
}) {
  if (operations.length === 0) return null;

  return (
    <section className="mb-6 space-y-3" aria-label="Project provisioning operations">
      <h2 className="text-sm font-semibold text-white">Project provisioning</h2>
      {operations.map((operation) => (
        <div key={operation.projectSlug} className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg p-4">
          <div className="flex items-center justify-between gap-3">
            <div>
              <p className="text-sm font-medium text-white">{operation.projectSlug}</p>
              <p className="text-xs text-gray-400 mt-1">
                {operation.operation} · {lifecycleStatusLabel(operation.status)}
                {operation.currentStep ? ` · ${operation.currentStep}` : ''}
              </p>
            </div>
            {operation.status === 'failed' && (
              <button
                type="button"
                onClick={() => void onRetry(operation)}
                className="px-3 py-1.5 bg-[#6366f1] text-white text-xs rounded-md hover:bg-[#4f46e5]"
              >
                Retry provisioning
              </button>
            )}
          </div>
          {operation.errorMessage && (
            <p className="text-xs text-red-400 mt-3">
              {operation.errorCode ? `${operation.errorCode}: ` : ''}{operation.errorMessage}
            </p>
          )}
        </div>
      ))}
    </section>
  );
}

export default function ProjectListPage() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [operations, setOperations] = useState<StoredOperation[]>(loadStoredOperations);
  const navigate = useNavigate();

  const fetchProjects = () => {
    apiFetch('/projects')
      .then(setProjects)
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    fetchProjects();
  }, []);

  useEffect(() => {
    let cancelled = false;

    const refreshOperations = async () => {
      const storedOperations = loadStoredOperations();
      const activeOperations = storedOperations.filter((operation) => ACTIVE_OPERATION_STATUSES.has(operation.status));
      if (activeOperations.length === 0) return;

      const refreshed = await Promise.all(activeOperations.map(async (operation) => {
        try {
          const response = await apiFetch(
            `/admin/projects/${operation.projectSlug}/operations/${operation.operationId}`,
          ) as OperationResponse;
          return {
            ...operation,
            status: response.status,
            currentStep: response.current_step,
            errorCode: response.error_code,
            errorMessage: response.error_message,
          };
        } catch {
          return operation;
        }
      }));

      if (cancelled) return;
      const nextOperations = [
        ...storedOperations.filter((operation) => !ACTIVE_OPERATION_STATUSES.has(operation.status)),
        ...refreshed,
      ];
      localStorage.setItem(OPERATION_STORAGE_KEY, JSON.stringify(nextOperations));
      setOperations(nextOperations);
      if (refreshed.some((operation) => operation.status === 'succeeded')) fetchProjects();
    };

    void refreshOperations();
    const interval = window.setInterval(() => void refreshOperations(), 3_000);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, []);

  const retryOperation = async (operation: StoredOperation) => {
    const retryKey = crypto.randomUUID();
    try {
      const response = await apiFetch(
        `/admin/projects/${operation.projectSlug}/operations/${operation.operationId}/retry`,
        { method: 'POST', headers: { 'Idempotency-Key': retryKey } },
      ) as Pick<OperationResponse, 'id' | 'operation' | 'status'>;
      setOperations(replaceStoredOperation({
        ...operation,
        operationId: response.id,
        operation: response.operation,
        idempotencyKey: retryKey,
        status: response.status,
        currentStep: null,
        errorCode: null,
        errorMessage: null,
      }));
    } catch (err: any) {
      setOperations(replaceStoredOperation({
        ...operation,
        errorCode: 'RETRY_FAILED',
        errorMessage: err.message || 'Retry request failed',
      }));
    }
  };

  if (loading) return <div className="text-gray-400 p-8">Loading projects...</div>;

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-xl font-bold text-white">Your Projects</h1>
        <button
          onClick={() => setShowCreate(true)}
          className="px-3 py-1.5 bg-[#6366f1] text-white text-xs rounded-md hover:bg-[#4f46e5] transition-colors"
        >
          + New Project
        </button>
      </div>

      <OperationPanel operations={operations} onRetry={retryOperation} />

      <div className="grid grid-cols-2 gap-4">
        {projects.map((p) => (
          <div
            key={p.id}
            onClick={() => navigate(`/projects/${p.slug}/pipeline`)}
            className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg p-4 cursor-pointer hover:border-[#6366f1] transition-colors"
          >
            <h3 className="text-white font-semibold">{p.name}</h3>
            <p className="text-xs text-gray-400 mt-1">
              {p.description || 'No description'}
            </p>
            <div className="flex items-center gap-2 mt-2 text-xs text-gray-500">
              <span>{p.member_count} members</span>
              {p.role && <RoleBadge role={p.role} />}
            </div>
          </div>
        ))}
      </div>

      {projects.length === 0 && (
        <p className="text-gray-500 text-sm text-center mt-12">
          No projects yet. Create one to get started.
        </p>
      )}

      <CreateProjectModal
        open={showCreate}
        onClose={() => setShowCreate(false)}
        onCreated={fetchProjects}
        onOperationAccepted={setOperations}
      />
    </div>
  );
}

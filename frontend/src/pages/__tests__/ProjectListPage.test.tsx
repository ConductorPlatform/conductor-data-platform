import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import ProjectListPage from '../../pages/ProjectListPage';

const mockProjects = [
  { id: '1', name: 'Data Warehouse', slug: 'data-warehouse', description: 'Main DW', member_count: 5, role: 'super_admin', created_at: '2026-01-01' },
  { id: '2', name: 'Marketing', slug: 'marketing', description: null, member_count: 3, role: 'developer', created_at: '2026-02-01' },
];

function response(payload: unknown, status = 200) {
  return Promise.resolve({ ok: status >= 200 && status < 300, status, json: () => Promise.resolve(payload) });
}

function renderProjectList() {
  return render(
    <MemoryRouter>
      <ProjectListPage />
    </MemoryRouter>
  );
}

describe('ProjectListPage', () => {
  beforeEach(() => {
    localStorage.clear();
    vi.stubGlobal('crypto', { randomUUID: vi.fn(() => '11111111-1111-4111-8111-111111111111') });
    window.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve(mockProjects),
    });
  });

  afterEach(() => {
    delete (window as any).fetch;
    vi.unstubAllGlobals();
  });

  it('renders project cards after loading', async () => {
    renderProjectList();
    await waitFor(() => {
      expect(screen.getByText('Data Warehouse')).toBeInTheDocument();
      expect(screen.getByText('Marketing')).toBeInTheDocument();
    });
  });

  it('shows member count', async () => {
    renderProjectList();
    await waitFor(() => {
      expect(screen.getByText('5 members')).toBeInTheDocument();
      expect(screen.getByText('3 members')).toBeInTheDocument();
    });
  });

  it('shows role badges', async () => {
    renderProjectList();
    await waitFor(() => {
      expect(screen.getByText('super admin')).toBeInTheDocument();
      expect(screen.getByText('developer')).toBeInTheDocument();
    });
  });

  it('shows "+ New Project" button', async () => {
    renderProjectList();
    await waitFor(() => {
      expect(screen.getByText('+ New Project')).toBeInTheDocument();
    });
  });

  it('opens create modal on "+ New Project" click', async () => {
    renderProjectList();
    await waitFor(() => {
      expect(screen.getByText('Data Warehouse')).toBeInTheDocument();
    });
    fireEvent.click(screen.getByText('+ New Project'));
    expect(screen.getByText('New Project')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('My Project')).toBeInTheDocument();
  });

  it('keeps one idempotency key for an uncertain create retry and shows the accepted operation', async () => {
    const fetchMock = vi.fn((_url: string, options?: RequestInit) => {
      if (options?.method === 'POST') {
        const postCount = fetchMock.mock.calls.filter(([, request]) => request?.method === 'POST').length;
        if (postCount === 1) return response({ detail: 'Network unavailable' }, 503);
        return response({
          project: { id: 'project-1', slug: 'new-project' },
          operation: { id: 'operation-1', operation: 'provision', status: 'pending' },
        }, 202);
      }
      return response(mockProjects);
    });
    window.fetch = fetchMock as any;

    renderProjectList();
    await screen.findByText('Data Warehouse');
    fireEvent.click(screen.getByText('+ New Project'));
    fireEvent.change(screen.getByPlaceholderText('My Project'), { target: { value: 'New Project' } });
    fireEvent.click(screen.getByText('Create'));

    await screen.findByText(/Send again will reuse the same request key/);
    fireEvent.click(screen.getByText('Create'));

    await screen.findByText('Project provisioning');
    expect(screen.getByText(/provision · pending/)).toBeInTheDocument();
    const postCalls = fetchMock.mock.calls.filter(([, options]) => options?.method === 'POST');
    expect(postCalls).toHaveLength(2);
    expect(postCalls[0][1]?.headers).toMatchObject({
      'Idempotency-Key': '11111111-1111-4111-8111-111111111111',
    });
    expect(postCalls[1][1]?.headers).toMatchObject({
      'Idempotency-Key': '11111111-1111-4111-8111-111111111111',
    });
    expect(JSON.parse(localStorage.getItem('conductor_project_operations')!)).toEqual([
      expect.objectContaining({
        operationId: 'operation-1',
        idempotencyKey: '11111111-1111-4111-8111-111111111111',
        status: 'pending',
      }),
    ]);
  });

  it('shows backend-confirmed success without inferring READY from the accepted request', async () => {
    localStorage.setItem('conductor_project_operations', JSON.stringify([{
      projectId: 'project-1',
      projectSlug: 'new-project',
      operationId: 'operation-1',
      operation: 'provision',
      idempotencyKey: 'request-key',
      status: 'pending',
    }]));
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/operations/operation-1')) {
        return response({ id: 'operation-1', operation: 'provision', status: 'succeeded' });
      }
      return response(mockProjects);
    });
    window.fetch = fetchMock as any;

    renderProjectList();

    await screen.findByText(/provision · succeeded/);
    expect(screen.queryByText('READY')).not.toBeInTheDocument();
  });

  it('restores a failed operation, displays its sanitized error, and retries with a new key', async () => {
    localStorage.setItem('conductor_project_operations', JSON.stringify([{
      projectId: 'project-1',
      projectSlug: 'failed-project',
      operationId: 'operation-1',
      operation: 'provision',
      idempotencyKey: 'old-key',
      status: 'pending',
    }]));
    (crypto.randomUUID as any).mockReturnValueOnce('22222222-2222-4222-8222-222222222222');
    const fetchMock = vi.fn((url: string, options?: RequestInit) => {
      if (url.includes('/operations/operation-1') && !options?.method) {
        return response({
          id: 'operation-1',
          operation: 'provision',
          status: 'failed',
          current_step: 'airflow-init',
          error_code: 'INIT_FAILED',
          error_message: 'Airflow initialization failed',
        });
      }
      if (url.includes('/retry') && options?.method === 'POST') {
        return response({ id: 'operation-2', operation: 'reconcile', status: 'pending' }, 202);
      }
      return response(mockProjects);
    });
    window.fetch = fetchMock as any;

    renderProjectList();
    await screen.findByText('INIT_FAILED: Airflow initialization failed');
    fireEvent.click(screen.getByText('Retry provisioning'));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        '/api/v1/admin/projects/failed-project/operations/operation-1/retry',
        expect.objectContaining({
          method: 'POST',
          headers: expect.objectContaining({ 'Idempotency-Key': '22222222-2222-4222-8222-222222222222' }),
        }),
      );
    });
    expect(screen.getByText(/reconcile · pending/)).toBeInTheDocument();
  });
});

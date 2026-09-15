import { describe, it, expect, vi, beforeAll, beforeEach, afterAll } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import PipelinePage from '../../pages/PipelinePage';

const auth = vi.hoisted(() => ({
  user: null as { is_admin: boolean; projects: Array<{ slug: string; role: string }> } | null,
}));

vi.mock('../../lib/auth', () => ({
  useAuth: () => ({ user: auth.user }),
}));

const mockStats = {
  active_dags: 5,
  paused_dags: 2,
  running: 3,
  queued: 1,
  runs_today: 12,
  failed_24h: 2,
};

const mockDags = [
  { dag_id: 'etl_main', description: 'Main ETL', is_paused: false },
  { dag_id: 'cleanup', description: null, is_paused: true },
];

const mockRuns = [
  {
    run_id: 'run_1',
    run_type: 'scheduled',
    state: 'success',
    execution_date: '2026-07-15T00:00:00Z',
    start_date: null,
    end_date: null,
  },
  {
    run_id: 'run_2',
    run_type: 'manual',
    state: 'failed',
    execution_date: '2026-07-14T00:00:00Z',
    start_date: null,
    end_date: null,
  },
];

describe('PipelinePage', () => {
  beforeAll(() => {
    window.fetch = vi.fn((input: RequestInfo | URL) => {
      const urlStr = String(input);
      if (urlStr.includes('/stats')) {
        return Promise.resolve(Response.json(mockStats));
      }
      if (urlStr.includes('/runs')) {
        return Promise.resolve(Response.json(mockRuns));
      }
      return Promise.resolve(Response.json(mockDags));
    }) as typeof fetch;
  });

  beforeEach(() => {
    auth.user = {
      is_admin: false,
      projects: [{ slug: 'test', role: 'developer' }],
    };
    localStorage.setItem('conductor_token', 'test-token');
  });

  afterAll(() => {
    delete (window as any).fetch;
  });

  it('renders stat cards with API data', async () => {
    render(
      <MemoryRouter initialEntries={['/projects/test/pipeline']}>
        <Routes>
          <Route path="/projects/:slug/pipeline" element={<PipelinePage />} />
        </Routes>
      </MemoryRouter>
    );
    await waitFor(() => {
      expect(screen.getByText('5')).toBeInTheDocument();
      expect(screen.getByText('Active DAGs')).toBeInTheDocument();
      expect(screen.getByText('Running')).toBeInTheDocument();
      expect(screen.getByText('Queued')).toBeInTheDocument();
      expect(screen.getByText('Runs Today')).toBeInTheDocument();
      expect(screen.getByText('Failed (24h)')).toBeInTheDocument();
    });
  });

  it('renders DAG list items', async () => {
    render(
      <MemoryRouter initialEntries={['/projects/test/pipeline']}>
        <Routes>
          <Route path="/projects/:slug/pipeline" element={<PipelinePage />} />
        </Routes>
      </MemoryRouter>
    );
    await waitFor(() => {
      expect(screen.getByText('etl_main')).toBeInTheDocument();
      expect(screen.getByText('cleanup')).toBeInTheDocument();
    });
  });

  it('shows DAG status badges', async () => {
    render(
      <MemoryRouter initialEntries={['/projects/test/pipeline']}>
        <Routes>
          <Route path="/projects/:slug/pipeline" element={<PipelinePage />} />
        </Routes>
      </MemoryRouter>
    );
    await waitFor(() => {
      expect(screen.getByText('ACTIVE')).toBeInTheDocument();
      expect(screen.getByText('PAUSED')).toBeInTheDocument();
    });
  });

  it('renders tabs', async () => {
    render(
      <MemoryRouter initialEntries={['/projects/test/pipeline']}>
        <Routes>
          <Route path="/projects/:slug/pipeline" element={<PipelinePage />} />
        </Routes>
      </MemoryRouter>
    );
    await waitFor(() => {
      expect(screen.getByText('DAGs Overview')).toBeInTheDocument();
      expect(screen.getByText('Recent Runs')).toBeInTheDocument();
      expect(screen.getByText('Schedule')).toBeInTheDocument();
    });
  });

  it('bootstraps a scoped proxy session before opening Airflow', async () => {
    const popup = {
      opener: window,
      location: { replace: vi.fn() },
      close: vi.fn(),
    } as unknown as Window;
    const open = vi.spyOn(window, 'open').mockImplementation(() => popup);
    render(
      <MemoryRouter initialEntries={['/projects/test/pipeline']}>
        <Routes>
          <Route path="/projects/:slug/pipeline" element={<PipelinePage />} />
        </Routes>
      </MemoryRouter>
    );
    fireEvent.click(await screen.findByRole('button', { name: /Open in Airflow/ }));

    await waitFor(() => {
      expect(window.fetch).toHaveBeenCalledWith(
        '/api/v1/projects/test/airflow-proxy/bootstrap',
        expect.objectContaining({ method: 'POST' }),
      );
      expect(open).toHaveBeenCalledWith('about:blank', '_blank');
      expect(popup.opener).toBeNull();
      expect(popup.location.replace).toHaveBeenCalledWith('/api/v1/projects/test/airflow-proxy/');
    });
    open.mockRestore();
  });

  it('bootstraps before assigning the embedded Airflow proxy URL', async () => {
    render(
      <MemoryRouter initialEntries={['/projects/test/pipeline']}>
        <Routes>
          <Route path="/projects/:slug/pipeline" element={<PipelinePage />} />
        </Routes>
      </MemoryRouter>
    );
    fireEvent.click(await screen.findByText('etl_main'));

    await waitFor(() => {
      expect(window.fetch).toHaveBeenCalledWith(
        '/api/v1/projects/test/airflow-proxy/bootstrap',
        expect.objectContaining({ method: 'POST' }),
      );
      expect(document.querySelector('iframe')).toHaveAttribute(
        'src',
        '/api/v1/projects/test/airflow-proxy/dags/etl_main',
      );
    });
  });

  it('shows a forbidden state instead of an empty pipeline', async () => {
    window.fetch = vi.fn(() => Promise.resolve(Response.json(
      { detail: 'Access denied' },
      { status: 403 },
    ))) as typeof fetch;
    render(
      <MemoryRouter initialEntries={['/projects/test/pipeline']}>
        <Routes><Route path="/projects/:slug/pipeline" element={<PipelinePage />} /></Routes>
      </MemoryRouter>,
    );

    expect(await screen.findByRole('alert')).toHaveTextContent('Access to this pipeline is forbidden.');
    expect(screen.queryByText('No DAGs found')).not.toBeInTheDocument();
  });

  it('shows an upstream API failure instead of an empty pipeline', async () => {
    window.fetch = vi.fn(() => Promise.resolve(Response.json(
      { detail: 'Airflow API error' },
      { status: 502 },
    ))) as typeof fetch;
    render(
      <MemoryRouter initialEntries={['/projects/test/pipeline']}>
        <Routes><Route path="/projects/:slug/pipeline" element={<PipelinePage />} /></Routes>
      </MemoryRouter>,
    );

    expect(await screen.findByRole('alert')).toHaveTextContent('Pipeline data is unavailable.');
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument();
  });

  it('shows run provenance, diagnostics, links, and triggers the selected DAG', async () => {
    const detailedRun = {
      run_id: 'run_with_provenance',
      run_type: 'scheduled',
      state: 'failed',
      execution_date: '2026-07-15T00:00:00Z',
      start_date: null,
      end_date: null,
      duration: null,
      commit_sha: '0123456789abcdef0123456789abcdef',
      artifacts: [{
        name: 'manifest.json',
        download_url: '/api/v1/projects/test/airflow/dags/etl_main/runs/run_with_provenance/artifacts/manifest.json',
      }],
    };
    window.fetch = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes('/stats')) return Promise.resolve(Response.json(mockStats));
      if (url.includes('/diagnostics')) return Promise.resolve(Response.json({
        task_id: 'dbt_run',
        state: 'failed',
        try_number: 1,
        map_index: -1,
        summary: 'Task dbt_run failed on try 1',
        logs_url: '/api/v1/projects/test/airflow-proxy/api/v2/dags/etl_main/dagRuns/run_with_provenance/taskInstances/dbt_run/logs/1?full_content=true&map_index=-1',
      }));
      if (init?.method === 'POST' && url.endsWith('/runs')) return Promise.resolve(Response.json(detailedRun, { status: 201 }));
      if (url.includes('/runs')) return Promise.resolve(Response.json([detailedRun]));
      return Promise.resolve(Response.json(mockDags));
    }) as typeof fetch;
    render(
      <MemoryRouter initialEntries={['/projects/test/pipeline']}>
        <Routes><Route path="/projects/:slug/pipeline" element={<PipelinePage />} /></Routes>
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole('button', { name: /etl_main/ }));
    fireEvent.click(screen.getByText('Recent Runs'));
    expect(await screen.findByTitle('0123456789abcdef0123456789abcdef')).toHaveTextContent('0123456789ab');
    expect(screen.getByText('SCHEDULED')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Show failure details' }));
    expect(await screen.findByText('Task dbt_run failed on try 1')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Logs' })).toHaveAttribute(
      'href',
      '/api/v1/projects/test/airflow-proxy/api/v2/dags/etl_main/dagRuns/run_with_provenance/taskInstances/dbt_run/logs/1?full_content=true&map_index=-1',
    );

    const createObjectURL = vi.fn(() => 'blob:manifest');
    const revokeObjectURL = vi.fn();
    const click = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});
    Object.assign(URL, { createObjectURL, revokeObjectURL });

    fireEvent.click(screen.getByRole('button', { name: 'Download manifest.json' }));
    await waitFor(() => expect(window.fetch).toHaveBeenCalledWith(
      detailedRun.artifacts[0].download_url,
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: 'Bearer test-token' }),
      }),
    ));
    expect(createObjectURL).toHaveBeenCalled();
    expect(revokeObjectURL).toHaveBeenCalledWith('blob:manifest');
    click.mockRestore();

    fireEvent.click(screen.getByRole('button', { name: 'Run now' }));
    await waitFor(() => expect(window.fetch).toHaveBeenCalledWith(
      '/api/v1/projects/test/airflow/dags/etl_main/runs',
      expect.objectContaining({ method: 'POST' }),
    ));
  });

  it('hides the trigger from viewers while retaining readable pipeline controls', async () => {
    auth.user = {
      is_admin: false,
      projects: [{ slug: 'test', role: 'viewer' }],
    };
    render(
      <MemoryRouter initialEntries={['/projects/test/pipeline']}>
        <Routes><Route path="/projects/:slug/pipeline" element={<PipelinePage />} /></Routes>
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole('button', { name: /etl_main/ }));
    expect(screen.queryByRole('button', { name: 'Run now' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Open in Airflow/ })).toBeInTheDocument();
  });
});
import { describe, it, expect, vi, beforeAll, afterAll } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import DevPage from '../../pages/DevPage';

const mockToken = { token: 'jwt-token', expires_in: 900, iframe_url: 'http://codeserver:8080?token=jwt' };
const mockWorkspace = { branch: 'feature/new-model', ahead: 3, behind: 0, files: ['dags/', 'dbt/models/'] };

describe('DevPage', () => {
  beforeAll(() => {
    window.fetch = vi.fn((input: RequestInfo | URL) => {
      const urlStr = String(input);
      if (urlStr.includes('/setup-workspace')) {
        return Promise.resolve(Response.json({ message: 'ok' }));
      }
      if (urlStr.includes('/token')) {
        return Promise.resolve(Response.json(mockToken));
      }
      if (urlStr.includes('/workspace-info')) {
        return Promise.resolve(Response.json(mockWorkspace));
      }
      return Promise.resolve(Response.json({}));
    }) as typeof fetch;
  });

  afterAll(() => {
    delete (window as any).fetch;
  });

  it('renders branch info after loading', async () => {
    render(
      <MemoryRouter initialEntries={['/projects/test/dev']}>
        <Routes>
          <Route path="/projects/:slug/dev" element={<DevPage />} />
        </Routes>
      </MemoryRouter>
    );
    await waitFor(() => {
      expect(screen.getByText('feature/new-model')).toBeInTheDocument();
    });
  });

  it('shows ahead indicator', async () => {
    render(
      <MemoryRouter initialEntries={['/projects/test/dev']}>
        <Routes>
          <Route path="/projects/:slug/dev" element={<DevPage />} />
        </Routes>
      </MemoryRouter>
    );
    await waitFor(() => {
      expect(screen.getByText('↑ 3 ahead of main')).toBeInTheDocument();
    });
  });

  it('shows workspace files', async () => {
    render(
      <MemoryRouter initialEntries={['/projects/test/dev']}>
        <Routes>
          <Route path="/projects/:slug/dev" element={<DevPage />} />
        </Routes>
      </MemoryRouter>
    );
    await waitFor(() => {
      expect(screen.getByText('📁 dags/')).toBeInTheDocument();
      expect(screen.getByText('📁 dbt/models/')).toBeInTheDocument();
    });
  });

  it('shows JWT countdown', async () => {
    render(
      <MemoryRouter initialEntries={['/projects/test/dev']}>
        <Routes>
          <Route path="/projects/:slug/dev" element={<DevPage />} />
        </Routes>
      </MemoryRouter>
    );
    await waitFor(() => {
      expect(screen.getByText(/remaining/)).toBeInTheDocument();
    });
  });

  it('shows Sync and Open buttons', async () => {
    render(
      <MemoryRouter initialEntries={['/projects/test/dev']}>
        <Routes>
          <Route path="/projects/:slug/dev" element={<DevPage />} />
        </Routes>
      </MemoryRouter>
    );
    await waitFor(() => {
      expect(screen.getByText('Sync')).toBeInTheDocument();
      expect(screen.getByText('Open in new tab ↗')).toBeInTheDocument();
    });
  });
});
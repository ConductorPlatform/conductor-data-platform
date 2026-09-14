const API = '/api/v1';

export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

function apiUrl(path: string) {
  return path.startsWith(`${API}/`) ? path : `${API}${path}`;
}

function requestHeaders(options: RequestInit): Record<string, string> {
  const token = localStorage.getItem('conductor_token');
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string> || {}),
  };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  return headers;
}

async function raiseForError(response: Response) {
  if (response.status === 401) {
    localStorage.removeItem('conductor_token');
    window.location.href = '/login';
    throw new Error('Unauthorized');
  }
  if (!response.ok) {
    const err = await response.json().catch(() => ({ detail: response.statusText }));
    throw new ApiError(response.status, err.detail || 'Request failed');
  }
}

export async function apiFetch(path: string, options: RequestInit = {}) {
  const r = await fetch(apiUrl(path), { ...options, headers: requestHeaders(options) });
  await raiseForError(r);
  if (r.status === 204) return null;
  return r.json();
}

export async function apiDownload(path: string, options: RequestInit = {}) {
  const r = await fetch(apiUrl(path), { ...options, headers: requestHeaders(options) });
  await raiseForError(r);
  return r.blob();
}
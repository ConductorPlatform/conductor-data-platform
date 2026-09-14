import { useCallback, useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { ApiError, apiDownload, apiFetch } from '../lib/api';
import { useAuth } from '../lib/auth';
import { StatCard } from '../components/StatCard';
import { TabBar } from '../components/TabBar';

interface DAG {
  dag_id: string;
  description: string | null;
  is_paused: boolean;
  latest_run_state: string | null;
}

interface DAGRun {
  run_id: string;
  state: string;
  execution_date: string;
  start_date: string | null;
  end_date: string | null;
  duration: number | null;
  commit_sha: string | null;
  error_summary: string | null;
  logs_url: string | null;
  artifacts: Array<{
    name: 'manifest.json' | 'run_results.json';
    download_url: string;
  }>;
}

interface AirflowStats {
  active_dags: number;
  paused_dags: number;
  running: number;
  queued: number;
  runs_today: number;
  failed_24h: number;
}

type DAGRunArtifact = DAGRun['artifacts'][number];

const triggerRoles = new Set(['super_admin', 'project_admin', 'maintainer', 'developer']);

const emptyStats = (): AirflowStats => ({
  active_dags: 0,
  paused_dags: 0,
  running: 0,
  queued: 0,
  runs_today: 0,
  failed_24h: 0,
});

const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : 'Unable to reach the pipeline API.';

const errorKind = (error: unknown) =>
  error instanceof ApiError && error.status === 403 ? 'forbidden' : 'api';

export default function PipelinePage() {
  const { slug } = useParams();
  const { user } = useAuth();
  const [dags, setDags] = useState<DAG[]>([]);
  const [runs, setRuns] = useState<DAGRun[]>([]);
  const [stats, setStats] = useState<AirflowStats>(emptyStats);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<{ kind: 'forbidden' | 'api'; message: string } | null>(null);
  const [tab, setTab] = useState('overview');
  const [selectedDag, setSelectedDag] = useState<string | null>(null);
  const [dagRunsLoading, setDagRunsLoading] = useState(false);
  const [runsError, setRunsError] = useState<string | null>(null);
  const [triggerLoading, setTriggerLoading] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [iframePath, setIframePath] = useState<string | null>(null);
  const projectRole = user?.projects.find((project) => project.slug === slug)?.role;
  const canTrigger = Boolean(user?.is_admin || (projectRole && triggerRoles.has(projectRole)));

  const loadPipeline = useCallback(async () => {
    setLoading(true);
    setLoadError(null);
    try {
      const [nextStats, nextDags] = await Promise.all([
        apiFetch(`/projects/${slug}/airflow/stats`) as Promise<AirflowStats>,
        apiFetch(`/projects/${slug}/airflow/dags`) as Promise<DAG[]>,
      ]);
      setStats(nextStats);
      setDags(nextDags);
    } catch (error) {
      setStats(emptyStats());
      setDags([]);
      setLoadError({ kind: errorKind(error), message: errorMessage(error) });
    } finally {
      setLoading(false);
    }
  }, [slug]);

  useEffect(() => {
    void loadPipeline();
  }, [loadPipeline]);

  const fetchDagRuns = async (dagId: string) => {
    setDagRunsLoading(true);
    setRunsError(null);
    setRuns([]);
    try {
      const data = await apiFetch(`/projects/${slug}/airflow/dags/${dagId}/runs`) as DAGRun[];
      setRuns(data);
    } catch (error) {
      setRunsError(errorMessage(error));
    } finally {
      setDagRunsLoading(false);
    }
  };

  const bootstrapAirflowProxy = () => apiFetch(`/projects/${slug}/airflow-proxy/bootstrap`, {
    method: 'POST',
  });

  const proxyPath = (path: string) => `/api/v1/projects/${slug}/airflow-proxy/${path}`;

  const handleSelectDag = async (dagId: string) => {
    setSelectedDag(dagId);
    setIframePath(null);
    setActionError(null);
    void fetchDagRuns(dagId);
    try {
      await bootstrapAirflowProxy();
      setIframePath(proxyPath(`dags/${dagId}`));
    } catch (error) {
      setActionError(errorMessage(error));
    }
  };

  const handleOpenAirflow = () => {
    const popup = window.open('about:blank', '_blank');
    if (!popup) return;
    popup.opener = null;
    bootstrapAirflowProxy()
      .then(() => popup.location.replace(proxyPath('')))
      .catch((error) => {
        popup.close();
        setActionError(errorMessage(error));
      });
  };

  const handleTrigger = async () => {
    if (!selectedDag) return;
    setTriggerLoading(true);
    setActionError(null);
    try {
      await apiFetch(`/projects/${slug}/airflow/dags/${selectedDag}/runs`, { method: 'POST' });
      await fetchDagRuns(selectedDag);
    } catch (error) {
      setActionError(errorMessage(error));
    } finally {
      setTriggerLoading(false);
    }
  };

  const handleDownloadArtifact = async (artifact: DAGRunArtifact) => {
    setActionError(null);
    try {
      const blob = await apiDownload(artifact.download_url);
      const objectUrl = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = objectUrl;
      link.download = artifact.name;
      link.click();
      URL.revokeObjectURL(objectUrl);
    } catch (error) {
      setActionError(errorMessage(error));
    }
  };

  const runStateColor = (state: string) => {
    switch (state) {
      case 'success': return 'text-green-400';
      case 'running': return 'text-blue-400';
      case 'failed': return 'text-red-400';
      case 'queued': return 'text-yellow-400';
      default: return 'text-gray-400';
    }
  };

  return (
    <div>
      <h1 className="text-xl font-bold text-white mb-4">Production Pipeline</h1>

      {loadError && (
        <div role="alert" className="mb-4 rounded-lg border border-red-800 bg-red-950/40 p-4 text-sm text-red-200">
          <p className="font-medium">
            {loadError.kind === 'forbidden' ? 'Access to this pipeline is forbidden.' : 'Pipeline data is unavailable.'}
          </p>
          <p className="mt-1 text-red-300">{loadError.message}</p>
          <button type="button" onClick={() => void loadPipeline()} className="mt-3 text-xs text-red-100 underline">
            Retry
          </button>
        </div>
      )}

      {actionError && <p role="alert" className="mb-3 text-sm text-red-300">{actionError}</p>}

      {!loading && !loadError && (
        <div className="grid grid-cols-5 gap-3 mb-6">
          <StatCard value={stats.active_dags} label="Active DAGs" color="green" />
          <StatCard value={stats.running} label="Running" color="blue" />
          <StatCard value={stats.queued} label="Queued" color="yellow" />
          <StatCard value={stats.runs_today} label="Runs Today" color="green" />
          <StatCard value={stats.failed_24h} label="Failed (24h)" color="red" />
        </div>
      )}

      <TabBar
        tabs={[
          { id: 'overview', label: 'DAGs Overview' },
          { id: 'runs', label: 'Recent Runs' },
          { id: 'schedule', label: 'Schedule' },
        ]}
        active={tab}
        onChange={setTab}
        rightAction={
          <button
            type="button"
            onClick={handleOpenAirflow}
            className="text-xs text-[#818cf8] hover:underline"
          >
            Open in Airflow ↗
          </button>
        }
      />

      {tab === 'overview' && (
        <div className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg overflow-hidden">
          {loading && <div className="p-4 text-gray-400 text-sm">Loading DAGs...</div>}
          {!loading && !loadError && dags.length === 0 && (
            <div className="p-8 text-center text-gray-500">
              <p>No DAGs found</p>
              <p className="text-xs mt-2">Provision Airflow for this project first</p>
            </div>
          )}
          {!loadError && dags.map((dag) => (
            <button
              type="button"
              key={dag.dag_id}
              onClick={() => void handleSelectDag(dag.dag_id)}
              className={`flex w-full items-center px-4 py-3 border-b border-[#2a2b36] hover:bg-[#22232d] cursor-pointer ${
                selectedDag === dag.dag_id ? 'border-l-2 border-l-[#6366f1]' : ''
              }`}
            >
              <span className="flex-1 text-left text-sm text-white font-medium">{dag.dag_id}</span>
              <span className="w-24 h-1.5 bg-[#2a2b36] rounded-full overflow-hidden mx-3">
                <span
                  className={`block h-full rounded-full ${dag.is_paused ? 'bg-gray-500' : 'bg-green-400'}`}
                  style={{ width: dag.is_paused ? '0%' : '100%' }}
                />
              </span>
              <span className={`text-xs px-2 py-0.5 rounded-full ${dag.is_paused ? 'bg-yellow-900/50 text-yellow-400' : 'bg-green-900/50 text-green-400'}`}>
                {dag.is_paused ? 'PAUSED' : 'ACTIVE'}
              </span>
            </button>
          ))}
        </div>
      )}

      {tab === 'runs' && (
        <div className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg overflow-hidden">
          {dagRunsLoading && <div className="p-4 text-gray-400 text-sm">Loading runs...</div>}
          {selectedDag && !dagRunsLoading && runsError && (
            <div role="alert" className="p-4 text-sm text-red-300">Unable to load run history: {runsError}</div>
          )}
          {!selectedDag && !dagRunsLoading && (
            <div className="p-8 text-center text-gray-500"><p>Select a DAG to see its runs</p></div>
          )}
          {selectedDag && !dagRunsLoading && !runsError && runs.length === 0 && (
            <div className="p-8 text-center text-gray-500"><p>No runs found for this DAG</p></div>
          )}
          {runs.map((run) => (
            <div key={run.run_id} className="flex flex-wrap items-center gap-y-1 px-4 py-2.5 border-b border-[#2a2b36]">
              <span className="flex-1 text-sm text-gray-300 font-mono text-xs">{run.run_id}</span>
              <span className={`text-xs font-medium ${runStateColor(run.state)}`}>{run.state.toUpperCase()}</span>
              <span className="text-xs text-gray-500 ml-4 w-36 text-right">
                {run.execution_date ? new Date(run.execution_date).toLocaleString() : '—'}
              </span>
              {run.commit_sha && (
                <span title={run.commit_sha} className="ml-4 text-xs font-mono text-[#a5b4fc]">{run.commit_sha.slice(0, 12)}</span>
              )}
              {run.error_summary && <p className="ml-4 text-xs text-red-300">{run.error_summary}</p>}
              {run.logs_url && <a className="ml-4 text-xs text-[#818cf8] hover:underline" href={run.logs_url}>Logs</a>}
              {run.artifacts?.map((artifact) => (
                <button
                  key={artifact.name}
                  type="button"
                  aria-label={`Download ${artifact.name}`}
                  onClick={() => void handleDownloadArtifact(artifact)}
                  className="ml-3 text-xs text-[#818cf8] hover:underline"
                >
                  {artifact.name}
                </button>
              ))}
            </div>
          ))}
        </div>
      )}

      {tab === 'schedule' && (
        <div className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg p-8 text-center text-gray-500">
          <p>Schedules are managed in Airflow.</p>
          <p className="text-xs mt-2">Open Airflow to review or change a DAG schedule.</p>
        </div>
      )}

      {selectedDag && (
        <div className="mt-4">
          <div className="flex items-center gap-2 mb-2 text-sm text-gray-400">
            <span>Airflow graph embedded: <code className="text-[#818cf8]">{selectedDag}</code></span>
            {canTrigger && (
              <button
                type="button"
                onClick={() => void handleTrigger()}
                disabled={triggerLoading}
                className="ml-auto rounded bg-[#6366f1] px-3 py-1 text-xs text-white disabled:opacity-60"
              >
                {triggerLoading ? 'Starting…' : 'Run now'}
              </button>
            )}
          </div>
          <div className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg overflow-hidden" style={{ height: '400px' }}>
            {iframePath && <iframe src={iframePath} title={`Airflow graph for ${selectedDag}`} className="w-full h-full border-none" sandbox="allow-scripts allow-same-origin" />}
          </div>
        </div>
      )}
    </div>
  );
}

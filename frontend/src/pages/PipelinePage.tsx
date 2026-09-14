import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { apiFetch } from '../lib/api';
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
}

interface AirflowStats {
  active_dags: number;
  paused_dags: number;
  running: number;
  queued: number;
  runs_today: number;
  failed_24h: number;
}

const EMPTY_STATS: AirflowStats = {
  active_dags: 0,
  paused_dags: 0,
  running: 0,
  queued: 0,
  runs_today: 0,
  failed_24h: 0,
};

export default function PipelinePage() {
  const { slug } = useParams();
  const [dags, setDags] = useState<DAG[]>([]);
  const [runs, setRuns] = useState<DAGRun[]>([]);
  const [stats, setStats] = useState<AirflowStats>(EMPTY_STATS);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState('overview');
  const [selectedDag, setSelectedDag] = useState<string | null>(null);
  const [dagRunsLoading, setDagRunsLoading] = useState(false);
  const [iframePath, setIframePath] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([
      apiFetch(`/projects/${slug}/airflow/stats`).catch(() => EMPTY_STATS),
      apiFetch(`/projects/${slug}/airflow/dags`).catch(() => []),
    ])
      .then(([s, d]) => {
        setStats(s);
        setDags(d);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [slug]);

  const fetchDagRuns = async (dagId: string) => {
    setDagRunsLoading(true);
    try {
      const data = await apiFetch(`/projects/${slug}/airflow/dags/${dagId}/runs`);
      setRuns(data);
    } catch {
      setRuns([]);
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
    fetchDagRuns(dagId);
    try {
      await bootstrapAirflowProxy();
      setIframePath(proxyPath(`dags/${dagId}`));
    } catch {
      setIframePath(null);
    }
  };

  const handleOpenAirflow = () => {
    const popup = window.open('about:blank', '_blank');
    if (!popup) return;
    popup.opener = null;
    bootstrapAirflowProxy()
      .then(() => popup.location.replace(proxyPath('')))
      .catch(() => popup.close());
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

      {/* Stat cards */}
      {!loading && (
        <div className="grid grid-cols-5 gap-3 mb-6">
          <StatCard value={stats.active_dags} label="Active DAGs" color="green" />
          <StatCard value={stats.running} label="Running" color="blue" />
          <StatCard value={stats.queued} label="Queued" color="yellow" />
          <StatCard value={stats.runs_today} label="Runs Today" color="green" />
          <StatCard value={stats.failed_24h} label="Failed (24h)" color="red" />
        </div>
      )}

      {/* Tabs */}
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

      {/* DAGs Overview */}
      {tab === 'overview' && (
        <div className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg overflow-hidden">
          {loading && <div className="p-4 text-gray-400 text-sm">Loading DAGs...</div>}
          {!loading && dags.length === 0 && (
            <div className="p-8 text-center text-gray-500">
              <p>No DAGs found</p>
              <p className="text-xs mt-2">Provision Airflow for this project first</p>
            </div>
          )}
          {dags.map((dag) => (
            <div
              key={dag.dag_id}
              onClick={() => handleSelectDag(dag.dag_id)}
              className={`flex items-center px-4 py-3 border-b border-[#2a2b36] hover:bg-[#22232d] cursor-pointer ${
                selectedDag === dag.dag_id ? 'border-l-2 border-l-[#6366f1]' : ''
              }`}
            >
              <span className="flex-1 text-sm text-white font-medium">{dag.dag_id}</span>
              <div className="w-24 h-1.5 bg-[#2a2b36] rounded-full overflow-hidden mx-3">
                <div
                  className={`h-full rounded-full ${dag.is_paused ? 'bg-gray-500' : 'bg-green-400'}`}
                  style={{ width: dag.is_paused ? '0%' : '100%' }}
                />
              </div>
              <span
                className={`text-xs px-2 py-0.5 rounded-full ${
                  dag.is_paused
                    ? 'bg-yellow-900/50 text-yellow-400'
                    : 'bg-green-900/50 text-green-400'
                }`}
              >
                {dag.is_paused ? 'PAUSED' : 'ACTIVE'}
              </span>
              <span className="text-xs text-gray-500 ml-2 w-12 text-right">—</span>
            </div>
          ))}
        </div>
      )}

      {/* Recent Runs */}
      {tab === 'runs' && (
        <div className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg overflow-hidden">
          {dagRunsLoading && <div className="p-4 text-gray-400 text-sm">Loading runs...</div>}
          {!dagRunsLoading && runs.length === 0 && (
            <div className="p-8 text-center text-gray-500">
              <p>Select a DAG to see its runs</p>
            </div>
          )}
          {runs.map((run) => (
            <div
              key={run.run_id}
              className="flex items-center px-4 py-2.5 border-b border-[#2a2b36]"
            >
              <span className="flex-1 text-sm text-gray-300 font-mono text-xs">
                {run.run_id}
              </span>
              <span className={`text-xs font-medium ${runStateColor(run.state)}`}>
                {run.state.toUpperCase()}
              </span>
              <span className="text-xs text-gray-500 ml-4 w-36 text-right">
                {run.execution_date ? new Date(run.execution_date).toLocaleString() : '—'}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* Schedule (placeholder) */}
      {tab === 'schedule' && (
        <div className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg p-8 text-center text-gray-500">
          <p>Schedule view coming soon</p>
          <p className="text-xs mt-2">DAG schedule intervals will appear here</p>
        </div>
      )}

      {/* Iframe with DAG label */}
      {selectedDag && (
        <div className="mt-4">
          <div className="flex items-center gap-2 mb-2 text-sm text-gray-400">
            <svg
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.5"
              className="w-4 h-4"
            >
              <rect x="2" y="3" width="20" height="14" rx="2" />
              <path d="M8 21h8M12 17v4" />
            </svg>
            <span>
              Airflow graph embedded:{' '}
              <code className="text-[#818cf8]">{selectedDag}</code>
            </span>
          </div>
          <div
            className="bg-[#1a1b23] border border-[#2a2b36] rounded-lg overflow-hidden"
            style={{ height: '400px' }}
          >
            {iframePath && (
              <iframe
                src={iframePath}
                className="w-full h-full border-none"
                sandbox="allow-scripts allow-same-origin"
              />
            )}
          </div>
        </div>
      )}
    </div>
  );
}
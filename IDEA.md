# Project Core Concept: Conductor

## 1. Objective & Vision
**Conductor** integrates **dbt Core**, **Apache Airflow 3.x**, **code-server (VS Code IDE)**, and a **Server-Side AI Assistant** into a single, unified workspace for data analysts and engineers.

The main value proposition is eliminating per-seat SaaS costs and ensuring absolute data privacy by keeping all metadata and LLM inference inside the client's infrastructure loop.

---

## 2. Core Architectural Pillars & Constraints

### 🛡️ Cloud-Agnostic & Container-First
* **Strict Rule:** Every microservice must be isolated inside standard Docker containers.
* **No Cloud Lock-in:** Do NOT use cloud-specific managed services (e.g., No AWS MWAA, No AWS Cognito, No DynamoDB).
* **Target Stack:** The initial launch targets a standard `docker-compose` setup for local development/small teams, with an evolutionary path to **Kubernetes (Helm Charts)** for Enterprise deployment.
* **Storage & DB:** Use standard **PostgreSQL** for application metadata. File persistence (dbt logs, artifacts) must use abstract storage layers (`fsspec`) to seamlessly support AWS S3, Google Cloud Storage, or Azure Blob by simply changing environment variables.

### 👥 Multi-Tenancy & Resource Isolation
The system is architected for multiple tenants/users from Day 1:
1. **Compute Isolation:** The FastAPI backend dynamically spins up/downs isolated Docker containers (or Kubernetes Pods) with `code-server` for each individual user. One analyst's heavy `dbt run` must never freeze another user's IDE session.
2. **Data Isolation:** Connections to target data warehouses (Snowflake, BigQuery, ClickHouse) must be encrypted at the database level using a master-key pattern, preventing cross-tenant data visibility.
3. **RBAC Ready:** Authentication relies on standard **OAuth2 / OIDC** protocols, making it easy to plug in Google Workspace or Okta SSO without rewriting the core auth logic.

### 🤖 Server-Side AI Orchestration (No Client-Side LLM Dependencies)
* To bypass network visibility friction (Corporate VPNs/Firewalls) and guarantee deterministic UX, **all LLM inference happens strictly on the server side**.
* The AI layer must support OpenAI-compatible API schemas.
* **Deployment Options:**
  - *SaaS:* Connects to external APIs (OpenAI/Anthropic) using the client's private API keys.
  - *Enterprise:* Interacts with a local **vLLM** container running on a dedicated GPU instance inside the company's network or routes via **AWS Bedrock** using secure IAM roles.
* **AI Integration Model:** Push (observability on every dbt run) + Chat-in-IDE (VS Code extension queries server-side AI proxy). Implementation deferred — focus on core first.

---

## 3. Base Technology Stack & Core Components

The platform orchestrates the following open-source building blocks, pre-configured to work together out of the box:

*   **FastAPI (The Platform Core):**
    Acts as the main control plane. It handles user authentication, routes UI requests, serves the API, interacts with the Docker/Kubernetes API to provision user pods, and orchestrates contexts for the Server-Side AI.
*   **React (The Control Panel UI):**
    A modern, clean web interface. It includes tenant administration, a dashboard for monitoring pipeline runs, user management, and an iframe/web view to access the dynamically assigned personal VS Code instances.
*   **PostgreSQL (Metadata & State Store):**
    The central source of truth for the platform. It stores tenant configurations, encrypted data warehouse credentials, user session states, RBAC roles, and metadata linking specific users to their running containers. Must remain pure SQL/Postgres, cloud-agnostic.
*   **Apache Airflow 3.x (The Pipeline Engine):**
    Responsible for scheduling, orchestrating, and executing **production** data workflows. The FastAPI core triggers jobs via Airflow's REST API. Airflow workers run in isolated environments to execute dbt tasks.
    *   **Executor:** CeleryExecutor (Redis-based) — for development; future upgrade to KubernetesExecutor.
    *   **API Server:** Runs both `core` (REST API + UI) and `execution` (Task SDK) roles.
    *   **Dag Processor:** Always a separate process (Airflow 3.x requirement).
    *   **Triggerer:** Skipped on launch (not needed for dbt pipelines).
*   **dbt Core (The Transformation Layer):**
    The underlying engine for T (Transformation) in ELT. Installed within each user's **code-server environment** (for development) and on **Airflow workers** (for production). The platform parses `manifest.json` artifacts to display lineage and model statuses in the React UI.
*   **code-server / VS Code (The Embedded IDE):**
    An open-source web-based implementation of VS Code. Dynamically provisioned per user with pre-installed extensions (e.g., dbt Power User, Python, Git). It mounts the user's isolated workspace directory, allowing them to develop directly in the browser.
*   **Hermes AI Proxy (The Smart Layer):**
    An internal AI proxy service that safely captures system states, dbt compilation logs, or Airflow error tracebacks, enriches them with schema metadata, and routes them to the configured server-side LLM (vLLM / AWS Bedrock / OpenAI) for autonomous debugging and code generation.

---

## 4. Development Workflow (Dev → Prod Pipeline)

### Local Development (in code-server)
1. User opens code-server via the Conductor dashboard.
2. Each user gets their own **git worktree** on a feature branch.
3. User writes dbt models, runs `dbt run` / `dbt test` / `dbt build` **directly in the code-server terminal** (or via a VS Code extension / UI helper).
4. dbt artifacts (`manifest.json`, `run_results.json`) are generated locally in the workspace.
5. **No Airflow involvement** — the user iterates fast.

### Production Pipeline (in Airflow 3.x)
1. User merges feature branch → **master/main** branch.
2. Airflow 3.x **Git-based Dag Bundle** auto-detects the new commit and pulls updated DAG files.
3. The Dag Processor parses the new DAGs and serializes them into the metadata database.
4. The Scheduler picks up the new DAG version and executes on the **Celery workers**.
5. Workers have dbt Core installed and run the production dbt models.
6. Logs and artifacts are stored in the configured object storage (S3/GCS/local).

---

## 5. Architecture Decisions (Confirmed)

| Decision | Choice | Rationale |
|---|---|---|
| Executor | **CeleryExecutor** (Redis) | Mature, simple, well-documented. K8sExecutor later. |
| Dag Bundle | **Git-based** (native Airflow 3.x) | Dev → Prod: merge to master triggers auto-update. |
| API Server | **`--apps all`** (core + execution) | Both roles needed. Separable for production scaling. |
| Triggerer | **Skip on launch** | Not needed for dbt pipelines. |
| AI Model | Deferred | Focus on core first. Push + Chat-in-IDE. |
| Encryption | **Deferred** | Return to this topic later. |

---

## 6. High-Level System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Browser (User)                           │
│       React Dashboard  ←  code-server IDE (iframe)              │
└──────────────────────┬──────────────────────────────────────────┘
                       │ HTTPS
┌──────────────────────▼──────────────────────────────────────────┐
│                    FastAPI (Control Plane)                      │
│  • Auth (OAuth2/OIDC)   • Container Orchestration (Docker/K8s)  │
│  • Manifest Sync        • AI Gate (future)                      │
│  • User Session Mgmt    • RBAC Enforcement                      │
└────────┬──────────────┬──────────────────┬──────────────────────┘
         │              │                  │
         ▼              ▼                  ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────────────────────┐
│  PostgreSQL  │ │    Redis     │ │  Docker/K8s API              │
│  (Metadata)  │ │  (Celery +   │ │  • Spawn per-user code-server│
│              │ │   Cache)     │ │  • Isolated workspaces       │
└──────────────┘ └──────┬───────┘ └──────────────────────────────┘
                        │
┌───────────────────────▼────────────────────────────────────────┐
│              Apache Airflow 3.x (Production)                   │
│                                                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Dag         │  │  Scheduler   │  │  API Server          │  │
│  │  Processor   │  │  (Celery     │  │  ┌──────┐ ┌───────┐  │  │
│  │  (Git Bundle)│  │   Executor)  │  │  │core  │ │exec   │  │  │
│  └──────────────┘  └──────┬───────┘  │  │(UI)  │ │(Task  │  │  │
│                           │          │  │      │ │ SDK)  │  │  │
│                           ▼          │  └──────┘ └───────┘  │  |
│                    ┌──────────────┐  └──────────────────────┘  │
│                    │  Worker      │                            │
│                    │  (dbt Core)  │                            │
│                    └──────────────┘                            │
└────────────────────────────────────────────────────────────────┘
```

---

## 7. Development Sandbox Reference (Docker Compose)

The original development sandbox uses **Docker Compose** on an Apple Silicon machine. The table below is architecture context, not a statement of current task status.

### Docker Compose Services (Airflow 3.x)

| Service | Image | Role |
|---|---|---|
| `postgres` | postgres:18-alpine | Metadata DB for Conductor + Airflow |
| `redis` | redis:8.6.4-alpine | Celery broker + FastAPI cache/sessions |
| `fastapi` | _(build)_ | Core backend: auth, orchestration, API |
| `react-ui` | _(build, nginx)_ | Dashboard + IDE access |
| `airflow-db-init` | apache/airflow:3.3.0 | DB migrations & Dag Bundle setup |
| `airflow-scheduler` | apache/airflow:3.3.0 | Scheduler + CeleryExecutor |
| `airflow-dag-processor` | apache/airflow:3.3.0 | Parse DAGs from Git bundle |
| `airflow-api-server` | apache/airflow:3.3.0 | `--apps all` (core + execution) |
| `airflow-worker` | apache/airflow:3.3.0 | Execute dbt tasks |
| `code-server` | codercom/code-server | Per-user VS Code (dev mode: 1 instance) |

### Current work
Current tasks and their acceptance criteria are maintained on the Hermes board
`conductor-data-platform`. This document describes the product vision, not a task queue.

---

## 8. Instructions for Hermes Agent
When drafting code, generating configs, or debugging scripts:
1. Keep the code strictly modular. Encapsulate AI calling logic, secret storage, and Docker container spawning into isolated services.
2. Assume the system will be deployed in a multi-user environment. Never hardcode absolute paths or write global, un-scoped database queries.
3. Optimize Python backend logic for minimal dependencies and raw speed.
4. Use Alembic for database migrations from day one.
5. All Airflow 3.x configs: use `airflow api-server` (not webserver), separate `dag-processor`, Git-based Dag Bundles.
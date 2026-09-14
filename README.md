# Conductor

**Conductor** is an open-source foundation for building and operating data transformations. It combines dbt Core, Apache Airflow, a FastAPI control plane, and a React dashboard. Its target model is one control plane that manages multiple independent project runtimes. The repository is in active development; the current local Compose path does not yet provision that target model end to end.

## Stack

| Component | Version / implementation |
| --- | --- |
| Control plane | FastAPI on Python 3.14 with async SQLAlchemy and Alembic |
| Dashboard | React 19, TypeScript, Vite, Tailwind CSS |
| Orchestration | Apache Airflow 3.3.0 with CeleryExecutor |
| Transformations | dbt-core 2.0.0a4 |
| Development IDE | code-server with JWT authentication |
| Data store | PostgreSQL 18 |
| Cache and broker | Redis 8.6.4 |

## Current implementation status

- JWT authentication, refresh tokens, and a seeded local super-admin account are implemented.
- The API has project, membership, environment, Git configuration, Airflow, workspace, and administration endpoints.
- Configured Git credentials are encrypted at rest and never returned by the API.
- The dashboard provides routes and views for projects, pipeline data, development, Git, settings, and administration.
- The shared development Airflow instance can be initialised and started locally.

The following pieces are present in the codebase but are **not a working local project workflow** yet:

- The project-creation UI does not send the API's required `Idempotency-Key` header.
- Project creation queues a provisioning job, but Docker Compose does not run a lifecycle worker and the default runner registry is intentionally empty. A project therefore remains in `PROVISIONING`.
- The checked-in `data-warehouse` and `marketing` Compose groups are example project instances; they expect PostgreSQL databases that the checked-in Compose flow does not create.
- The code-server endpoint returns the Docker-internal URL `http://code-server:8080`, so the dashboard cannot open the published `localhost:8443` port in a host browser.
- Git branch and commit views currently return placeholder data.

## Quick start: supported local services

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with Docker Compose v2
- Git
- Apple Silicon is supported natively; no Rosetta emulation is required.

### 1. Clone the repository

```bash
git clone https://github.com/ConductorPlatform/conductor-data-platform.git
cd conductor-data-platform
```

### 2. Create local environment files

The root `.env` is required by Docker Compose. It holds deployment-level settings, including a stable secret used to derive the Fernet key for stored credentials. It is gitignored and must never be committed.

Create a stable credentials-encryption value without printing it to the terminal:

```bash
umask 077
touch .env
chmod 600 .env

if ! grep -q '^CONDUCTOR_CREDENTIALS_ENCRYPTION_KEY=' .env; then
  python3 - <<'PY'
import base64
import secrets
from pathlib import Path

key = base64.urlsafe_b64encode(secrets.token_bytes(32))
path = Path(".env")
with path.open("a+b") as env_file:
    env_file.seek(0, 2)
    if env_file.tell():
        env_file.seek(-1, 2)
        separator = b"" if env_file.read(1) == b"\n" else b"\n"
    else:
        separator = b""
    env_file.seek(0, 2)
    env_file.write(separator + b"CONDUCTOR_CREDENTIALS_ENCRYPTION_KEY=" + key + b"\n")
PY
fi

[ -f backend/.env ] || cp backend/.env.example backend/.env
chmod 600 backend/.env
```

For a shared or non-local deployment, set strong values in the root `.env` before starting the stack:

```dotenv
CONDUCTOR_JWT_SECRET=replace-with-a-long-random-value
CONDUCTOR_CODE_SERVER_JWT_SECRET=replace-with-a-separate-long-random-value
CONDUCTOR_AIRFLOW_ADMIN_PASSWORD=replace-with-a-strong-password
CONDUCTOR_DB_PASSWORD=replace-with-a-strong-password
```

Keep `CONDUCTOR_CREDENTIALS_ENCRYPTION_KEY` unchanged for the lifetime of the data. Rotating or losing it makes previously encrypted Git credentials unreadable. The application-specific variables and defaults are documented in [`backend/.env.example`](backend/.env.example).

### 3. Build the canonical project Airflow image

Build the exact `conductor-airflow:latest` image used by the worker-generated
project runtime template:

```bash
docker compose build project-airflow-runtime-image
```

### 4. Start the supported local services

Start the control plane, dashboard, and the lifecycle worker. The worker is the
only component that creates project Airflow resources; there is no shared,
generic, DW, or Marketing Airflow runtime in the root Compose path.

```bash
docker compose up -d \
  postgres redis fastapi lifecycle-worker frontend

docker compose ps
```

### 5. Access local services

| Service | URL | Local development access |
| --- | --- | --- |
| Conductor dashboard | <http://localhost:3000> | Sign in with the seeded `admin@conductor.local` / `admin`, or register a user |
| Conductor API | <http://localhost:8000/docs> | OpenAPI / Swagger UI |
| API health check | <http://localhost:8000/api/v1/health> | Reports PostgreSQL and Redis connectivity |
| Project Airflow | Browser proxy after project is `READY` | No root shared Airflow service; the proxy cookie is explicitly non-Secure only for this HTTP localhost Compose quick-start |
| PostgreSQL | `localhost:5432` | `CONDUCTOR_DB_USER` / `CONDUCTOR_DB_PASSWORD` (both default to `conductor`) |
| Redis | `localhost:6379` | Local development port |

The supplied credentials are development defaults only. Change them before exposing any service outside your machine.

## Dashboard and API limitations

The dashboard is useful for exercising authentication and inspecting the in-progress UI. Project provisioning requires the canonical image build and the lifecycle worker; a project is not ready until its generated Airflow runtime passes an authenticated readiness check.

If you call `POST /api/v1/projects` directly, it requires a super-admin bearer token and an `Idempotency-Key` UUID header. It returns an accepted provisioning operation, not an immediately ready project. The Swagger UI at <http://localhost:8000/docs> is the authoritative reference for the implemented API contracts.

The root Compose file has one build-only canonical project Airflow image target and a worker-managed project runtime path. code-server remains explicitly disabled behind the `mvp-04-ide` profile.

## Target architecture

This diagram is the intended multi-project architecture. It separates the shared control plane from project runtimes; it does **not** claim that the current Docker Compose implementation already provisions every project service automatically.

```mermaid
flowchart TB
    Browser[Browser]

    subgraph ControlPlane[Conductor Control Plane]
        direction TB
        Frontend[React dashboard]
        API[FastAPI control plane]
        ControlDB[(PostgreSQL: users · projects · settings)]
        ControlRedis[(Redis: sessions · control-plane cache)]

        Frontend --> API
        API --> ControlDB
        API --> ControlRedis
    end

    subgraph Projects[Projects × N]
        direction LR

        subgraph Marketing[Project: Marketing]
            direction TB
            MarketingAPI[Airflow API Server]
            MarketingScheduler[Scheduler]
            MarketingProcessor[DAG Processor]
            MarketingWorkers[Celery Workers]
            MarketingDB[(Airflow metadata DB)]
            MarketingQueue[(Project Redis queue)]

            MarketingAPI --> MarketingDB
            MarketingScheduler --> MarketingDB
            MarketingProcessor --> MarketingDB
            MarketingScheduler --> MarketingQueue
            MarketingWorkers --> MarketingQueue
        end

        subgraph Warehouse[Project: Data Warehouse]
            direction TB
            WarehouseAPI[Airflow API Server]
            WarehouseScheduler[Scheduler]
            WarehouseProcessor[DAG Processor]
            WarehouseWorkers[Celery Workers]
            WarehouseDB[(Airflow metadata DB)]
            WarehouseQueue[(Project Redis queue)]

            WarehouseAPI --> WarehouseDB
            WarehouseScheduler --> WarehouseDB
            WarehouseProcessor --> WarehouseDB
            WarehouseScheduler --> WarehouseQueue
            WarehouseWorkers --> WarehouseQueue
        end

        MoreProjects[⋯ additional projects]
    end

    Browser --> Frontend
    API -->|provision · authorize · proxy| MarketingAPI
    API -->|provision · authorize · proxy| WarehouseAPI
    API -. same contract .-> MoreProjects
```

The React dashboard proxies `/api` requests to FastAPI. The control plane owns authentication, authorization, project metadata, credential encryption, and lifecycle orchestration. Each project runtime owns its Airflow API server, scheduler, DAG processor, Celery workers, Airflow metadata database, and isolated Redis queue/namespace. The shared development Airflow services in the current Compose file are a bootstrap implementation, not a separate product-level “base project”.

## Repository layout

```text
.
├── backend/                 # FastAPI application, migrations, and backend tests
│   ├── app/                 # Routers, models, schemas, services, workers
│   ├── alembic/             # Database migrations
│   └── .env.example         # Application environment-variable reference
├── frontend/                # React/Vite dashboard and frontend tests
├── dags/                    # Base, data-warehouse, and marketing DAG directories
├── docker/                  # Airflow and code-server image definitions
├── logs/                    # Airflow task logs (runtime artifact)
├── plugins/                 # Shared Airflow plugins
├── workspaces/              # code-server workspaces (runtime artifact)
├── docker-compose.yml       # Local development stack
└── README.md
```

## Development

The Docker stack mounts the backend application and frontend source for development. After starting the supported services, run:

```bash
# Backend tests
docker compose exec fastapi pytest

# Frontend checks
docker compose exec frontend npm run lint
docker compose exec frontend npm run build
docker compose exec frontend npx vitest run
```

For local backend tooling outside Docker, Python 3.14 is required:

```bash
python3.14 -m venv .venv
env -u PYTHONPATH .venv/bin/pip install -e "backend[dev]"
```

`env -u PYTHONPATH` prevents injected site-packages from affecting the local environment.

## Operations and troubleshooting

### Stop or reset the supported local stack

```bash
# Stop containers and retain volumes
docker compose down

# Remove containers and volumes for a completely fresh local start
docker compose down -v
```

After `docker compose down -v`, rebuild the [canonical project Airflow image](#3-build-the-canonical-project-airflow-image) before provisioning another disposable project.

### View logs

```bash
docker compose logs -f fastapi
docker compose logs -f frontend
docker compose logs -f lifecycle-worker
```

### Docker Compose requires an encryption key

If Compose reports that `CONDUCTOR_CREDENTIALS_ENCRYPTION_KEY` is missing, create the root `.env` file with the command in [Create local environment files](#2-create-local-environment-files). Do not substitute a short plain-text value: this setting must be a stable, high-entropy value of at least 32 characters.

### Migrating from an older PostgreSQL volume

PostgreSQL 18 stores data in a version-specific volume path. For an expendable local environment created with an older image, reset the volumes and initialise again:

```bash
docker compose down -v
docker compose --profile init up airflow-db-init
docker compose up -d \
  postgres redis fastapi frontend \
  airflow-api-server airflow-scheduler airflow-dag-processor airflow-worker
```

## License

Conductor is licensed under the [Apache License 2.0](LICENSE).

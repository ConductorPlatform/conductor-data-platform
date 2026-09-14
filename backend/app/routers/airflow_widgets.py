from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal, cast

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import get_current_user
from app.database import get_db_session
from app.models.user import User
from app.schemas.airflow import AirflowStatsResponse
from app.schemas.dag import DAGRunArtifact, DAGRunInfo, DAGSummary
from app.services.airflow_session import AirflowSessionManager
from app.services.project_airflow_context import resolve_project_airflow_context

router = APIRouter()
_ARTIFACT_NAMES = frozenset({"manifest.json", "run_results.json"})


def _airflow_response_data(response: httpx.Response) -> dict:
    """Return successful Airflow JSON under the established upstream-error contract."""
    if not 200 <= response.status_code < 300:
        raise HTTPException(status_code=502, detail="Airflow API error")
    try:
        data = response.json()
    except ValueError as error:
        raise HTTPException(status_code=502, detail="Airflow API error") from error
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="Airflow API error")
    return data


def _airflow_error() -> HTTPException:
    return HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Airflow API error")


def _airflow_dags(data: dict, *, require_dag_id: bool) -> list[dict]:
    """Validate the nested DAG collection before consuming upstream fields."""
    dags = data.get("dags")
    if not isinstance(dags, list):
        raise _airflow_error()

    validated_dags: list[dict] = []
    for dag in dags:
        if not isinstance(dag, dict):
            raise _airflow_error()
        if require_dag_id and _optional_string(dag.get("dag_id")) is None:
            raise _airflow_error()
        if "is_paused" in dag and not isinstance(dag["is_paused"], bool):
            raise _airflow_error()
        validated_dags.append(dag)
    return validated_dags


def _airflow_total_entries(data: dict) -> int:
    """Return a valid Airflow pagination total without leaking malformed payloads."""
    total_entries = data.get("total_entries", 0)
    if isinstance(total_entries, bool) or not isinstance(total_entries, int) or total_entries < 0:
        raise _airflow_error()
    return total_entries


async def _airflow_get(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, str | int] | None = None,
) -> httpx.Response:
    try:
        if params is None:
            return await client.get(url, headers=headers)
        return await client.get(url, headers=headers, params=params)
    except httpx.HTTPError as error:
        raise _airflow_error() from error


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _project_proxy_url(slug: str, path: object) -> str | None:
    """Expose only a local, project-scoped proxy path for an upstream log link."""
    if not isinstance(path, str) or not path or path.startswith(("/", "\\")) or "://" in path:
        return None
    return f"/api/v1/projects/{slug}/airflow-proxy/{path}"


def _dag_run_artifacts(slug: str, dag_id: str, run_id: str, value: object) -> list[DAGRunArtifact]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise _airflow_error()

    artifacts: list[DAGRunArtifact] = []
    for artifact in value:
        name = artifact if isinstance(artifact, str) else artifact.get("name") if isinstance(artifact, dict) else None
        if name not in _ARTIFACT_NAMES:
            raise _airflow_error()
        artifacts.append(
            DAGRunArtifact(
                name=cast(Literal["manifest.json", "run_results.json"], name),
                download_url=(
                    f"/api/v1/projects/{slug}/airflow/dags/{dag_id}/runs/{run_id}/artifacts/{name}"
                ),
            )
        )
    return artifacts


def _dag_run_info(slug: str, dag_id: str, run: object) -> DAGRunInfo:
    if not isinstance(run, dict):
        raise _airflow_error()
    run_id = _optional_string(run.get("dag_run_id"))
    execution_date = run.get("logical_date", run.get("execution_date"))
    if run_id is None or not isinstance(execution_date, str) or not execution_date:
        raise _airflow_error()
    try:
        return DAGRunInfo.model_validate(
            {
                "run_id": run_id,
                "state": _optional_string(run.get("state")) or "",
                "execution_date": execution_date,
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
                "duration": run.get("duration"),
                "commit_sha": _optional_string(run.get("commit_sha")),
                "error_summary": _optional_string(run.get("error_summary")),
                "logs_url": _project_proxy_url(slug, run.get("logs_url")),
                "artifacts": _dag_run_artifacts(slug, dag_id, run_id, run.get("artifacts")),
            }
        )
    except ValidationError as error:
        raise _airflow_error() from error


@router.get("/projects/{slug}/airflow/dags", response_model=list[DAGSummary])
async def list_dags(
    slug: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    context = await resolve_project_airflow_context(slug, user, db, "project.dag.view", "read")
    access_token = await AirflowSessionManager().get_access_token(context, db)
    async with httpx.AsyncClient() as client:
        resp = await _airflow_get(
            client,
            f"{context.airflow_base_url}/api/v2/dags",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    data = _airflow_response_data(resp)
    dags = _airflow_dags(data, require_dag_id=True)
    return [
        DAGSummary(
            dag_id=dag["dag_id"],
            description=dag.get("description"),
            is_paused=dag.get("is_paused", False),
            latest_run_state=None,
            latest_run_start=None,
            latest_run_end=None,
            next_dagrun=None,
        )
        for dag in dags
    ]


@router.get("/projects/{slug}/airflow/dags/{dag_id}/runs", response_model=list[DAGRunInfo])
async def list_dag_runs(
    slug: str,
    dag_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    context = await resolve_project_airflow_context(slug, user, db, "project.dag.view", "read")
    access_token = await AirflowSessionManager().get_access_token(context, db)
    async with httpx.AsyncClient() as client:
        resp = await _airflow_get(
            client,
            f"{context.airflow_base_url}/api/v2/dags/{dag_id}/dagRuns",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    data = _airflow_response_data(resp)
    runs = data.get("dag_runs", [])
    if not isinstance(runs, list):
        raise _airflow_error()
    return [_dag_run_info(slug, dag_id, run) for run in runs]


@router.post(
    "/projects/{slug}/airflow/dags/{dag_id}/runs",
    response_model=DAGRunInfo,
    status_code=status.HTTP_201_CREATED,
)
async def trigger_dag_run(
    slug: str,
    dag_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Create a DAG run without accepting arbitrary client-supplied Airflow conf."""
    context = await resolve_project_airflow_context(slug, user, db, "project.dag.run", "write")
    access_token = await AirflowSessionManager().get_access_token(context, db)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{context.airflow_base_url}/api/v2/dags/{dag_id}/dagRuns",
                headers={"Authorization": f"Bearer {access_token}"},
                json={},
            )
    except httpx.HTTPError as error:
        raise _airflow_error() from error
    return _dag_run_info(slug, dag_id, _airflow_response_data(resp))


@router.get("/projects/{slug}/airflow/dags/{dag_id}/runs/{run_id}/artifacts/{name}")
async def download_dag_run_artifact(
    slug: str,
    dag_id: str,
    run_id: str,
    name: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Reserve an RBAC-protected artifact download boundary for runtime storage integration."""
    if name not in _ARTIFACT_NAMES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Artifact not found")
    await resolve_project_airflow_context(slug, user, db, "project.dag.view", "read")
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Artifact not available")


@router.get("/projects/{slug}/airflow/stats", response_model=AirflowStatsResponse)
async def get_airflow_stats(
    slug: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Aggregate DAG statistics from Airflow REST API."""
    context = await resolve_project_airflow_context(slug, user, db, "project.dag.view", "read")
    access_token = await AirflowSessionManager().get_access_token(context, db)

    async with httpx.AsyncClient() as client:
        headers = {"Authorization": f"Bearer {access_token}"}
        base = f"{context.airflow_base_url}/api/v2"

        dags_resp = await _airflow_get(client, f"{base}/dags", headers=headers)
        dags_data = _airflow_response_data(dags_resp)
        dags = _airflow_dags(dags_data, require_dag_id=True)
        active = sum(1 for dag in dags if not dag.get("is_paused", False))
        paused = sum(1 for dag in dags if dag.get("is_paused", False))

        dag_runs_url = f"{base}/dags/~/dagRuns"
        running_resp = await _airflow_get(
            client,
            dag_runs_url,
            headers=headers,
            params={"state": "running", "limit": 100},
        )
        running = _airflow_total_entries(_airflow_response_data(running_resp))

        queued_resp = await _airflow_get(
            client,
            dag_runs_url,
            headers=headers,
            params={"state": "queued", "limit": 100},
        )
        queued = _airflow_total_entries(_airflow_response_data(queued_resp))

        today = (
            datetime.now(UTC)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .isoformat()
        )
        today_resp = await _airflow_get(
            client,
            dag_runs_url,
            headers=headers,
            params={"start_date_gte": today, "limit": 200},
        )
        runs_today = _airflow_total_entries(_airflow_response_data(today_resp))

        last_24h = (datetime.now(UTC) - timedelta(hours=24)).isoformat()
        failed_resp = await _airflow_get(
            client,
            dag_runs_url,
            headers=headers,
            params={"start_date_gte": last_24h, "state": "failed", "limit": 100},
        )
        failed_24h = _airflow_total_entries(_airflow_response_data(failed_resp))

    return AirflowStatsResponse(
        active_dags=active,
        paused_dags=paused,
        running=running,
        queued=queued,
        runs_today=runs_today,
        failed_24h=failed_24h,
    )

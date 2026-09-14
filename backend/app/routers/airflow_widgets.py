from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import get_current_user
from app.database import get_db_session
from app.models.user import User
from app.schemas.airflow import AirflowStatsResponse
from app.schemas.dag import DAGRunInfo, DAGSummary
from app.services.airflow_session import AirflowSessionManager
from app.services.dbt_artifact_reader import (
    ArtifactIntegrityError,
    ArtifactNotFoundError,
    ArtifactUnavailableError,
    read_run_artifact,
)
from app.services.project_airflow_context import resolve_project_airflow_context
from app.config import settings

router = APIRouter()


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


@router.get("/projects/{slug}/airflow/dags", response_model=list[DAGSummary])
async def list_dags(
    slug: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    context = await resolve_project_airflow_context(slug, user, db, "project.dag.view", "read")
    access_token = await AirflowSessionManager().get_access_token(context, db)
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{context.airflow_base_url}/api/v2/dags",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    data = _airflow_response_data(resp)
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
        for dag in data.get("dags", [])
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
        resp = await client.get(
            f"{context.airflow_base_url}/api/v2/dags/{dag_id}/dagRuns",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    data = _airflow_response_data(resp)
    return [
        DAGRunInfo(
            run_id=run["dag_run_id"],
            state=run.get("state", ""),
            execution_date=run.get("logical_date", run.get("execution_date", "")),
            start_date=run.get("start_date"),
            end_date=run.get("end_date"),
            duration=run.get("duration"),
        )
        for run in data.get("dag_runs", [])
    ]


@router.get("/projects/{slug}/airflow/dags/{dag_id}/runs/{run_id}/artifacts/{artifact_name}")
async def get_dbt_run_artifact(
    slug: str,
    dag_id: str,
    run_id: str,
    artifact_name: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Serve only the two indexed dbt artifacts for the exact Airflow run."""

    context = await resolve_project_airflow_context(slug, user, db, "project.dag.view", "read")
    access_token = await AirflowSessionManager().get_access_token(context, db)
    async with httpx.AsyncClient() as client:
        upstream = await client.get(
            f"{context.airflow_base_url}/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/{quote(run_id, safe='')}",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    if upstream.status_code == 404:
        raise HTTPException(status_code=404, detail="Airflow run not found")
    run = _airflow_response_data(upstream)
    bundle_version = run.get("bundle_version")
    if not isinstance(bundle_version, str):
        raise HTTPException(status_code=502, detail="Airflow run provenance is unavailable")
    try:
        artifact = read_run_artifact(
            artifact_root=settings.lifecycle_runtime_artifact_root,
            project_id=context.project_id,
            generation=context.deployment_generation,
            dag_id=dag_id,
            run_id=run_id,
            bundle_commit_sha=bundle_version,
            artifact_name=artifact_name,
        )
    except ArtifactNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Artifact not found") from exc
    except ArtifactUnavailableError as exc:
        raise HTTPException(status_code=409, detail="Artifact was not stored") from exc
    except ArtifactIntegrityError as exc:
        raise HTTPException(status_code=502, detail="Artifact integrity verification failed") from exc
    return Response(
        content=artifact.content,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{artifact.filename}"'},
    )


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

        dags_resp = await client.get(f"{base}/dags", headers=headers)
        dags_data = _airflow_response_data(dags_resp)
        active = sum(1 for dag in dags_data.get("dags", []) if not dag.get("is_paused", False))
        paused = sum(1 for dag in dags_data.get("dags", []) if dag.get("is_paused", False))

        dag_runs_url = f"{base}/dags/~/dagRuns"
        running_resp = await client.get(
            dag_runs_url,
            headers=headers,
            params={"state": "running", "limit": 100},
        )
        running = _airflow_response_data(running_resp).get("total_entries", 0)

        queued_resp = await client.get(
            dag_runs_url,
            headers=headers,
            params={"state": "queued", "limit": 100},
        )
        queued = _airflow_response_data(queued_resp).get("total_entries", 0)

        today = (
            datetime.now(UTC)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .isoformat()
        )
        today_resp = await client.get(
            dag_runs_url,
            headers=headers,
            params={"start_date_gte": today, "limit": 200},
        )
        runs_today = _airflow_response_data(today_resp).get("total_entries", 0)

        last_24h = (datetime.now(UTC) - timedelta(hours=24)).isoformat()
        failed_resp = await client.get(
            dag_runs_url,
            headers=headers,
            params={"start_date_gte": last_24h, "state": "failed", "limit": 100},
        )
        failed_24h = _airflow_response_data(failed_resp).get("total_entries", 0)

    return AirflowStatsResponse(
        active_dags=active,
        paused_dags=paused,
        running=running,
        queued=queued,
        runs_today=runs_today,
        failed_24h=failed_24h,
    )

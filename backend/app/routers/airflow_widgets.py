from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import get_current_user
from app.database import get_db_session
from app.models.user import User
from app.schemas.airflow import AirflowStatsResponse
from app.schemas.dag import DAGRunInfo, DAGSummary
from app.services.airflow_session import AirflowSessionManager
from app.services.project_airflow_context import resolve_project_airflow_context

router = APIRouter()


@router.get("/projects/{slug}/airflow/dags", response_model=list[DAGSummary])
async def list_dags(
    slug: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    context = await resolve_project_airflow_context(slug, user, db, "project.dag.view", "read")
    session = await AirflowSessionManager().get_session(context, db)
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{context.airflow_base_url}/api/v1/dags",
            cookies={"session": session},
        )
    if resp.status_code != 200:
        raise HTTPException(502, "Airflow API error")
    data = resp.json()
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
    session = await AirflowSessionManager().get_session(context, db)
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{context.airflow_base_url}/api/v1/dags/{dag_id}/dagRuns",
            cookies={"session": session},
        )
    if resp.status_code != 200:
        raise HTTPException(502, "Airflow API error")
    data = resp.json()
    return [
        DAGRunInfo(
            run_id=run["dag_run_id"],
            state=run.get("state", ""),
            execution_date=run.get("execution_date", ""),
            start_date=run.get("start_date"),
            end_date=run.get("end_date"),
            duration=run.get("duration"),
        )
        for run in data.get("dag_runs", [])
    ]


@router.get("/projects/{slug}/airflow/stats", response_model=AirflowStatsResponse)
async def get_airflow_stats(
    slug: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Aggregate DAG statistics from Airflow REST API."""
    context = await resolve_project_airflow_context(slug, user, db, "project.dag.view", "read")
    session = await AirflowSessionManager().get_session(context, db)

    async with httpx.AsyncClient() as client:
        cookies = {"session": session}
        base = f"{context.airflow_base_url}/api/v1"

        dags_resp = await client.get(f"{base}/dags", cookies=cookies)
        dags_data = dags_resp.json() if dags_resp.status_code == 200 else {}
        active = sum(1 for dag in dags_data.get("dags", []) if not dag.get("is_paused", False))
        paused = sum(1 for dag in dags_data.get("dags", []) if dag.get("is_paused", False))

        running_resp = await client.get(f"{base}/dagRuns?state=running&limit=100", cookies=cookies)
        running = (
            running_resp.json().get("total_entries", 0) if running_resp.status_code == 200 else 0
        )

        queued_resp = await client.get(f"{base}/dagRuns?state=queued&limit=100", cookies=cookies)
        queued = queued_resp.json().get("total_entries", 0) if queued_resp.status_code == 200 else 0

        today = (
            datetime.now(timezone.utc)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .isoformat()
        )
        today_resp = await client.get(
            f"{base}/dagRuns?start_date_gte={today}&limit=200", cookies=cookies
        )
        runs_today = (
            today_resp.json().get("total_entries", 0) if today_resp.status_code == 200 else 0
        )

        last_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        failed_resp = await client.get(
            f"{base}/dagRuns?start_date_gte={last_24h}&state=failed&limit=100", cookies=cookies
        )
        failed_24h = (
            failed_resp.json().get("total_entries", 0) if failed_resp.status_code == 200 else 0
        )

    return AirflowStatsResponse(
        active_dags=active,
        paused_dags=paused,
        running=running,
        queued=queued,
        runs_today=runs_today,
        failed_24h=failed_24h,
    )

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import get_current_user
from app.config import settings
from app.database import get_db_session
from app.models.user import User
from app.schemas.airflow import AirflowStatsResponse
from app.schemas.dag import DAGRunArtifact, DAGRunDiagnostics, DAGRunInfo, DAGSummary
from app.services.airflow_session import AirflowSessionManager
from app.services.dbt_artifact_reader import (
    ArtifactIntegrityError,
    ArtifactNotFoundError,
    ArtifactUnavailableError,
    read_run_artifact,
)
from app.services.project_airflow_context import resolve_project_airflow_context

router = APIRouter()
_ARTIFACT_NAMES = frozenset({"manifest.json", "run_results.json"})
_ARTIFACT_ORDER = ("manifest.json", "run_results.json")


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
        if "is_paused" not in dag or not isinstance(dag["is_paused"], bool):
            raise _airflow_error()
        description = dag.get("description")
        if description is not None and not isinstance(description, str):
            raise _airflow_error()
        validated_dags.append(dag)
    return validated_dags


def _airflow_total_entries(data: dict) -> int:
    """Return a valid Airflow pagination total without leaking malformed payloads."""
    if "total_entries" not in data:
        raise _airflow_error()
    total_entries = data["total_entries"]
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


def _nullable_string(data: dict, field: str) -> str | None:
    """Return an optional string while rejecting malformed non-null upstream values."""
    value = data.get(field)
    if value is not None and not isinstance(value, str):
        raise _airflow_error()
    return value


def _nullable_duration(data: dict) -> int | float | None:
    value = data.get("duration")
    if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))):
        raise _airflow_error()
    return value


def _is_immutable_commit(value: str | None) -> bool:
    return (
        value is not None
        and len(value) in {40, 64}
        and all(character in "0123456789abcdef" for character in value)
    )


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
    run_type = _optional_string(run.get("run_type"))
    state = _optional_string(run.get("state"))
    execution_date = run.get("logical_date", run.get("execution_date"))
    if run_id is None or run_type is None or state is None or not isinstance(execution_date, str) or not execution_date:
        raise _airflow_error()
    commit_sha = _nullable_string(run, "commit_sha") or _nullable_string(run, "bundle_version")
    artifacts = _dag_run_artifacts(slug, dag_id, run_id, run.get("artifacts"))
    if run.get("artifacts") is None and _is_immutable_commit(commit_sha):
        artifacts = _dag_run_artifacts(slug, dag_id, run_id, list(_ARTIFACT_ORDER))
    try:
        return DAGRunInfo.model_validate(
            {
                "run_id": run_id,
                "run_type": run_type,
                "state": state,
                "execution_date": execution_date,
                "start_date": _nullable_string(run, "start_date"),
                "end_date": _nullable_string(run, "end_date"),
                "duration": _nullable_duration(run),
                # Airflow 3.3 exposes the immutable native bundle SHA as
                # ``bundle_version``. Keep accepting the legacy normalized
                # field so an already-adapted upstream remains compatible.
                "commit_sha": commit_sha,
                "artifacts": artifacts,
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
    try:
        return [
            DAGSummary(
                dag_id=dag["dag_id"],
                description=dag.get("description"),
                is_paused=dag["is_paused"],
                latest_run_state=None,
                latest_run_start=None,
                latest_run_end=None,
                next_dagrun=None,
            )
            for dag in dags
        ]
    except ValidationError as error:
        raise _airflow_error() from error


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
            f"{context.airflow_base_url}/api/v2/dags/{quote(dag_id, safe='')}/dagRuns",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    data = _airflow_response_data(resp)
    runs = data.get("dag_runs")
    if not isinstance(runs, list):
        raise _airflow_error()
    return [_dag_run_info(slug, dag_id, run) for run in runs]


def _task_instance_diagnostics(
    task_instances: object,
) -> tuple[str, Literal["failed", "upstream_failed"], int, int]:
    """Select one validated terminal failure without accepting arbitrary log URLs."""
    if not isinstance(task_instances, list):
        raise _airflow_error()

    candidates: list[tuple[int, str, int, int, Literal["failed", "upstream_failed"]]] = []
    for task_instance in task_instances:
        if not isinstance(task_instance, dict):
            raise _airflow_error()
        state = task_instance.get("state")
        if state not in {"failed", "upstream_failed"}:
            continue
        task_id = _optional_string(task_instance.get("task_id"))
        try_number = task_instance.get("try_number")
        map_index = task_instance.get("map_index")
        if (
            task_id is None
            or isinstance(try_number, bool)
            or not isinstance(try_number, int)
            or try_number < 0
            or isinstance(map_index, bool)
            or not isinstance(map_index, int)
        ):
            raise _airflow_error()
        candidates.append((0 if state == "failed" else 1, task_id, map_index, try_number, state))

    if not candidates:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Failed task instance not found")
    _, task_id, map_index, try_number, state = min(candidates)
    return task_id, state, try_number, map_index


def _task_log_data(data: dict) -> None:
    """Validate the native log response without placing log contents in our API."""
    content = data.get("content")
    continuation_token = data.get("continuation_token")
    if (
        not isinstance(content, list)
        or any(not isinstance(item, (str, dict)) for item in content)
        or (continuation_token is not None and not isinstance(continuation_token, str))
    ):
        raise _airflow_error()


def _diagnostic_logs_proxy_url(
    slug: str, dag_id: str, run_id: str, task_id: str, try_number: int, map_index: int
) -> str:
    path = "/".join(
        (
            "api/v2/dags",
            quote(dag_id, safe=""),
            "dagRuns",
            quote(run_id, safe=""),
            "taskInstances",
            quote(task_id, safe=""),
            "logs",
            str(try_number),
        )
    )
    return (
        f"/api/v1/projects/{quote(slug, safe='')}/airflow-proxy/{path}"
        f"?full_content=true&map_index={map_index}"
    )


@router.get(
    "/projects/{slug}/airflow/dags/{dag_id}/runs/{run_id}/diagnostics",
    response_model=DAGRunDiagnostics,
)
async def get_dag_run_diagnostics(
    slug: str,
    dag_id: str,
    run_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Retrieve one failed task's native diagnostics only when a user requests it."""
    context = await resolve_project_airflow_context(slug, user, db, "project.dag.view", "read")
    access_token = await AirflowSessionManager().get_access_token(context, db)
    headers = {"Authorization": f"Bearer {access_token}"}
    base_url = (
        f"{context.airflow_base_url}/api/v2/dags/{quote(dag_id, safe='')}"
        f"/dagRuns/{quote(run_id, safe='')}"
    )
    async with httpx.AsyncClient() as client:
        instances_response = await _airflow_get(client, f"{base_url}/taskInstances", headers=headers)
        instances_data = _airflow_response_data(instances_response)
        task_id, state, try_number, map_index = _task_instance_diagnostics(
            instances_data.get("task_instances")
        )
        logs_url = None
        if state == "failed":
            if try_number < 1:
                raise _airflow_error()
            logs_response = await _airflow_get(
                client,
                f"{base_url}/taskInstances/{quote(task_id, safe='')}/logs/{try_number}",
                headers=headers,
                params={"full_content": "true", "map_index": map_index},
            )
            _task_log_data(_airflow_response_data(logs_response))
            logs_url = _diagnostic_logs_proxy_url(slug, dag_id, run_id, task_id, try_number, map_index)

    return DAGRunDiagnostics(
        task_id=task_id,
        state=state,
        try_number=try_number,
        map_index=map_index,
        summary=f"Task {task_id} {state.replace('_', ' ')} on try {try_number}",
        logs_url=logs_url,
    )


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
                f"{context.airflow_base_url}/api/v2/dags/{quote(dag_id, safe='')}/dagRuns",
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
    """Serve only the two indexed dbt artifacts for the exact Airflow run."""

    if name not in _ARTIFACT_NAMES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Artifact not found")
    context = await resolve_project_airflow_context(slug, user, db, "project.dag.view", "read")
    access_token = await AirflowSessionManager().get_access_token(context, db)
    async with httpx.AsyncClient() as client:
        upstream = await _airflow_get(
            client,
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
            artifact_name=name,
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

        dags_resp = await _airflow_get(client, f"{base}/dags", headers=headers)
        dags_data = _airflow_response_data(dags_resp)
        dags = _airflow_dags(dags_data, require_dag_id=True)
        active = sum(1 for dag in dags if not dag["is_paused"])
        paused = sum(1 for dag in dags if dag["is_paused"])

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

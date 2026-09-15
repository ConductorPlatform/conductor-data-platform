from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest
from fastapi import HTTPException

import app.routers.airflow_widgets as widgets
from app.models.user import User
from app.services.project_airflow_context import ProjectAirflowContext


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"dags": []}, []),
        ({"dags": [{"dag_id": "example", "is_paused": False}]}, ["example"]),
    ],
)
def test_airflow_dags_accepts_explicit_empty_and_complete_dag_payloads(payload, expected):
    dags = widgets._airflow_dags(payload, require_dag_id=True)

    assert [dag["dag_id"] for dag in dags] == expected


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"dags": [{"dag_id": "example"}]},
        {"dags": [{"dag_id": "example", "is_paused": None}]},
        {"dags": [{"dag_id": "example", "is_paused": 0}]},
        {"dags": [{"dag_id": "example", "is_paused": False, "description": 42}]},
    ],
)
def test_airflow_dags_rejects_missing_or_invalid_consumed_fields(payload):
    with pytest.raises(HTTPException) as error:
        widgets._airflow_dags(payload, require_dag_id=True)

    assert error.value.status_code == 502
    assert error.value.detail == "Airflow API error"


@pytest.mark.parametrize("payload", [{}, {"total_entries": True}, {"total_entries": -1}, {"total_entries": "0"}])
def test_airflow_total_entries_rejects_missing_or_invalid_counts(payload):
    with pytest.raises(HTTPException) as error:
        widgets._airflow_total_entries(payload)

    assert error.value.status_code == 502
    assert error.value.detail == "Airflow API error"


def test_airflow_total_entries_accepts_explicit_zero():
    assert widgets._airflow_total_entries({"total_entries": 0}) == 0


@pytest.mark.parametrize(
    "run",
    [
        {"dag_run_id": "run-a", "logical_date": "2026-02-03T04:05:06+00:00"},
        {"dag_run_id": "run-a", "state": None, "logical_date": "2026-02-03T04:05:06+00:00"},
        {"dag_run_id": "run-a", "state": "", "logical_date": "2026-02-03T04:05:06+00:00"},
        {"dag_run_id": "run-a", "state": 42, "logical_date": "2026-02-03T04:05:06+00:00"},
        {
            "dag_run_id": "run-a",
            "state": "success",
            "logical_date": "2026-02-03T04:05:06+00:00",
            "start_date": [],
        },
        {
            "dag_run_id": "run-a",
            "state": "success",
            "logical_date": "2026-02-03T04:05:06+00:00",
            "duration": True,
        },
        {
            "dag_run_id": "run-a",
            "state": "success",
            "logical_date": "2026-02-03T04:05:06+00:00",
            "commit_sha": 42,
        },
    ],
)
def test_dag_run_info_sanitizes_missing_and_invalid_consumed_fields(run):
    with pytest.raises(HTTPException) as error:
        widgets._dag_run_info("project-a", "example", run)

    assert error.value.status_code == 502
    assert error.value.detail == "Airflow API error"


def test_dag_run_info_exposes_native_bundle_provenance_and_artifact_links():
    commit = "a" * 40

    run = widgets._dag_run_info(
        "project-a",
        "example",
        {
            "dag_run_id": "run-a",
            "run_type": "scheduled",
            "state": "success",
            "logical_date": "2026-02-03T04:05:06+00:00",
            "bundle_version": commit,
        },
    )

    assert run.commit_sha == commit
    assert run.run_type == "scheduled"
    assert [(artifact.name, artifact.download_url) for artifact in run.artifacts] == [
        ("manifest.json", "/api/v1/projects/project-a/airflow/dags/example/runs/run-a/artifacts/manifest.json"),
        ("run_results.json", "/api/v1/projects/project-a/airflow/dags/example/runs/run-a/artifacts/run_results.json"),
    ]


@pytest.mark.parametrize(
    "task_instances",
    [
        [],
        [{"task_id": "dbt", "state": "failed", "try_number": True, "map_index": -1}],
        [{"task_id": "dbt", "state": "failed", "try_number": 1, "map_index": True}],
        [{"task_id": "", "state": "upstream_failed", "try_number": 0, "map_index": -1}],
    ],
)
def test_task_instance_diagnostics_rejects_missing_or_malformed_failure_data(task_instances):
    with pytest.raises(HTTPException) as error:
        widgets._task_instance_diagnostics(task_instances)

    assert error.value.status_code in {404, 502}


def test_task_instance_diagnostics_prefers_real_failed_task_and_encodes_local_log_url():
    assert widgets._task_instance_diagnostics([
        {"task_id": "downstream", "state": "upstream_failed", "try_number": 0, "map_index": -1},
        {"task_id": "dbt / run", "state": "failed", "try_number": 2, "map_index": 3},
        {"task_id": "success", "state": "success"},
    ]) == ("dbt / run", "failed", 2, 3)
    assert widgets._diagnostic_logs_proxy_url(
        "project a", "dag / id", "run / id", "dbt / run", 2, 3
    ) == (
        "/api/v1/projects/project%20a/airflow-proxy/api/v2/dags/dag%20%2F%20id/"
        "dagRuns/run%20%2F%20id/taskInstances/dbt%20%2F%20run/logs/2?full_content=true&map_index=3"
    )


@pytest.mark.asyncio
async def test_diagnostics_uses_native_task_instances_then_logs_with_project_view_permission(monkeypatch):
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="http://airflow-project-a:8080",
        account_key="viewer",
    )
    calls = []

    async def resolve(*args):
        calls.append(("resolve", args[3:]))
        return context

    async def token(*_args):
        return "airflow-access-token"

    class Response:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, **kwargs):
            calls.append(("get", url, kwargs))
            if url.endswith("/taskInstances"):
                return Response({"task_instances": [
                    {"task_id": "succeeded", "state": "success"},
                    {"task_id": "dbt / run", "state": "failed", "try_number": 2, "map_index": -1},
                ]})
            return Response({"content": ["dbt test failure marker"], "continuation_token": None})

    monkeypatch.setattr(widgets, "resolve_project_airflow_context", resolve)
    monkeypatch.setattr(widgets.AirflowSessionManager, "get_access_token", token)
    monkeypatch.setattr(widgets.httpx, "AsyncClient", Client)

    diagnostics = await widgets.get_dag_run_diagnostics(
        "project-a", "dag / id", "run / id", cast(User, SimpleNamespace(id="viewer")), object()
    )

    assert calls[0] == ("resolve", ("project.dag.view", "read"))
    assert calls[1:] == [
        (
            "get",
            "http://airflow-project-a:8080/api/v2/dags/dag%20%2F%20id/dagRuns/run%20%2F%20id/taskInstances",
            {"headers": {"Authorization": "Bearer airflow-access-token"}},
        ),
        (
            "get",
            "http://airflow-project-a:8080/api/v2/dags/dag%20%2F%20id/dagRuns/run%20%2F%20id/taskInstances/dbt%20%2F%20run/logs/2",
            {"headers": {"Authorization": "Bearer airflow-access-token"}, "params": {"full_content": "true", "map_index": -1}},
        ),
    ]
    assert diagnostics.model_dump() == {
        "task_id": "dbt / run",
        "state": "failed",
        "try_number": 2,
        "map_index": -1,
        "summary": "Task dbt / run failed on try 2",
        "logs_url": "/api/v1/projects/project-a/airflow-proxy/api/v2/dags/dag%20%2F%20id/dagRuns/run%20%2F%20id/taskInstances/dbt%20%2F%20run/logs/2?full_content=true&map_index=-1",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(("payload", "expected_runs"), [({"dag_runs": []}, []), ({}, None)])
async def test_list_dag_runs_distinguishes_explicit_empty_from_missing_collection(
    monkeypatch, payload, expected_runs
):
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="http://airflow-project-a:8080",
        account_key="dev",
    )

    async def resolve(*_args):
        return context

    async def get_access_token(*_args):
        return "airflow-access-token"

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return payload

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, **_kwargs):
            return Response()

    monkeypatch.setattr(widgets, "resolve_project_airflow_context", resolve)
    monkeypatch.setattr(widgets.AirflowSessionManager, "get_access_token", get_access_token)
    monkeypatch.setattr(widgets.httpx, "AsyncClient", Client)

    if expected_runs is None:
        with pytest.raises(HTTPException) as error:
            await widgets.list_dag_runs(
                "project-a", "example", cast(User, SimpleNamespace(id="user-a")), object()
            )

        assert error.value.status_code == 502
        assert error.value.detail == "Airflow API error"
    else:
        runs = await widgets.list_dag_runs(
            "project-a", "example", cast(User, SimpleNamespace(id="user-a")), object()
        )

        assert runs == expected_runs

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
            "state": "success",
            "logical_date": "2026-02-03T04:05:06+00:00",
            "bundle_version": commit,
        },
    )

    assert run.commit_sha == commit
    assert [(artifact.name, artifact.download_url) for artifact in run.artifacts] == [
        ("manifest.json", "/api/v1/projects/project-a/airflow/dags/example/runs/run-a/artifacts/manifest.json"),
        ("run_results.json", "/api/v1/projects/project-a/airflow/dags/example/runs/run-a/artifacts/run_results.json"),
    ]


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

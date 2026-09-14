from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.config import settings
from app.services.project_airflow_context import ProjectAirflowContext


PROJECT_ID = "0123456789abcdef0123456789abcdef"
COMMIT = "b" * 40


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _artifact(root: Path) -> None:
    directory = root / PROJECT_ID / "7" / _digest("dag") / _digest("run") / COMMIT / "1"
    directory.mkdir(parents=True)
    payload = b'{"artifact": true}\n'
    (directory / "manifest.json").write_bytes(payload)
    (directory / "index.json").write_text(json.dumps({
        "project_id": PROJECT_ID,
        "generation": "7",
        "dag_id": "dag",
        "dag_run_id": "run",
        "bundle_commit_sha": COMMIT,
        "try_number": "1",
        "stage": "test",
        "exit_code": 0,
        "files": {
            "manifest.json": {"status": "stored", "size": len(payload), "sha256": hashlib.sha256(payload).hexdigest()},
            "run_results.json": {"status": "missing"},
        },
    }))


@pytest.mark.asyncio
async def test_artifact_endpoint_authorizes_run_provenance_and_streams_indexed_bytes(monkeypatch, tmp_path: Path) -> None:
    import app.routers.airflow_widgets as widgets

    _artifact(tmp_path)
    context = ProjectAirflowContext(
        project_id=PROJECT_ID,
        deployment_id="deployment",
        deployment_generation=7,
        airflow_base_url="http://airflow-project:8080",
        account_key="viewer",
    )

    async def resolve(*_args):
        return context

    async def token(*_args):
        return "airflow-token"

    class Upstream:
        status_code = 200

        @staticmethod
        def json():
            return {"bundle_version": COMMIT}

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, **kwargs):
            assert url.endswith("/api/v2/dags/dag/dagRuns/run")
            assert kwargs["headers"] == {"Authorization": "Bearer airflow-token"}
            return Upstream()

    monkeypatch.setattr(settings, "lifecycle_runtime_artifact_root", tmp_path)
    monkeypatch.setattr(widgets, "resolve_project_airflow_context", resolve)
    monkeypatch.setattr(widgets.AirflowSessionManager, "get_access_token", token)
    monkeypatch.setattr(widgets.httpx, "AsyncClient", Client)

    response = await widgets.get_dbt_run_artifact(
        "project", "dag", "run", "manifest.json", SimpleNamespace(id="user"), object()
    )

    assert response.status_code == 200
    assert response.body == b'{"artifact": true}\n'
    assert response.headers["content-disposition"] == 'attachment; filename="manifest.json"'

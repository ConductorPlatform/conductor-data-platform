from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from app.main import create_app
from app.services.airflow_session import AirflowSessionManager
from app.services.project_airflow_context import (
    ProjectAirflowContext,
    resolve_project_airflow_context,
)


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/projects/project-a/airflow/dags",
        "/api/v1/projects/project-a/airflow/dags/example/runs",
        "/api/v1/projects/project-a/airflow/stats",
    ],
)
async def test_widget_api_boundaries_reject_unauthenticated_requests(path):
    async with AsyncClient(
        transport=ASGITransport(app=create_app()), base_url="http://test"
    ) as client:
        response = await client.get(path)

    assert response.status_code == 401


class _ResolverDb:
    def __init__(self, *values):
        self.values = list(values)
        self.execute_calls = 0

    async def execute(self, _statement):
        self.execute_calls += 1
        return _ScalarResult(self.values.pop(0))


@pytest.mark.asyncio
async def test_context_returns_only_safe_server_derived_fields(monkeypatch):
    import app.services.project_airflow_context as context_service

    project = SimpleNamespace(id="project-a")
    member = SimpleNamespace(role=SimpleNamespace(name="developer"))
    deployment = SimpleNamespace(
        id="deployment-a",
        generation=7,
        airflow_external_url="https://a.airflow.example.test",
    )
    db = _ResolverDb(member, deployment)

    async def load_ready(*_args):
        return project

    async def allowed(*_args):
        return True

    monkeypatch.setattr(context_service, "load_ready_project_for_user", load_ready)
    monkeypatch.setattr(context_service, "check_permission", allowed)

    context = await resolve_project_airflow_context(
        "project-a", SimpleNamespace(id="user-a"), db, "project.dag.view", "read"
    )

    assert context == ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="https://a.airflow.example.test",
        account_key="dev",
    )
    assert not any("password" in field or "secret" in field for field in vars(context))


@pytest.mark.asyncio
async def test_context_denies_before_looking_up_deployment(monkeypatch):
    import app.services.project_airflow_context as context_service

    db = _ResolverDb(SimpleNamespace(role=SimpleNamespace(name="viewer")))

    async def load_ready(*_args):
        return SimpleNamespace(id="project-a")

    async def denied(*_args):
        return False

    monkeypatch.setattr(context_service, "load_ready_project_for_user", load_ready)
    monkeypatch.setattr(context_service, "check_permission", denied)

    with pytest.raises(HTTPException) as error:
        await resolve_project_airflow_context(
            "project-a", SimpleNamespace(id="user-a"), db, "project.dag.run", "write"
        )

    assert error.value.status_code == 403
    assert error.value.detail == "Access denied"
    assert db.execute_calls == 1


class _Redis:
    def __init__(self, cached: str | None = None):
        self.cached = cached
        self.get_keys: list[str] = []
        self.setex_calls: list[tuple[str, int, str]] = []

    async def get(self, key: str) -> str | None:
        self.get_keys.append(key)
        return self.cached

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self.setex_calls.append((key, ttl, value))


class _SessionDb:
    def __init__(self, deployment):
        self.deployment = deployment
        self.get_calls: list[tuple] = []

    async def get(self, *args):
        self.get_calls.append(args)
        return self.deployment


@pytest.mark.asyncio
async def test_session_cache_is_scoped_to_deployment_generation_and_account(monkeypatch):
    manager = AirflowSessionManager()
    redis = _Redis(cached="opaque-session")

    async def get_redis():
        return redis

    monkeypatch.setattr(manager, "_get_redis", get_redis)
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="https://a.airflow.example.test",
        account_key="dev",
    )
    db = _SessionDb(SimpleNamespace(project_id="project-a", generation=7))

    assert await manager.get_session(context, db) == "opaque-session"
    assert redis.get_keys == ["airflow_session:deployment-a:7:dev"]
    assert len(db.get_calls) == 1


@pytest.mark.asyncio
async def test_session_rejects_a_stale_context_before_decryption(monkeypatch):
    import app.services.airflow_session as session_service

    manager = AirflowSessionManager()
    redis = _Redis()

    async def get_redis():
        return redis

    def forbidden_decrypt(_encrypted_password):
        raise AssertionError("stale context must be rejected before decryption")

    monkeypatch.setattr(manager, "_get_redis", get_redis)
    monkeypatch.setattr(session_service, "decrypt_token", forbidden_decrypt)
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="https://a.airflow.example.test",
        account_key="admin",
    )
    stale_deployment = SimpleNamespace(project_id="project-b", generation=7)

    with pytest.raises(HTTPException) as error:
        await manager.get_session(context, _SessionDb(stale_deployment))

    assert error.value.status_code == 404
    assert error.value.detail == "Airflow not provisioned"

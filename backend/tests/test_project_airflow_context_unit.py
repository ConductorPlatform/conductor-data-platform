from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

import httpx
import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

import app.routers.airflow_widgets as widgets
from app.main import create_app
from app.models.user import User
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
        "/api/v1/projects/project-a/airflow/dags/example/runs/run-a/artifacts/manifest.json",
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
        airflow_base_url="http://airflow-project-a:8080",
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
async def test_access_token_cache_is_scoped_to_deployment_generation_and_account(monkeypatch):
    manager = AirflowSessionManager()
    redis = _Redis(cached="opaque-access-token")

    async def get_redis():
        return redis

    monkeypatch.setattr(manager, "_get_redis", get_redis)
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="http://airflow-project-a:8080",
        account_key="dev",
    )
    db = _SessionDb(SimpleNamespace(project_id="project-a", generation=7))

    assert await manager.get_access_token(context, db) == "opaque-access-token"
    assert redis.get_keys == ["airflow_access_token:deployment-a:7:dev"]
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
        airflow_base_url="http://airflow-project-a:8080",
        account_key="admin",
    )
    stale_deployment = SimpleNamespace(project_id="project-b", generation=7)

    with pytest.raises(HTTPException) as error:
        await manager.get_access_token(context, _SessionDb(stale_deployment))

    assert error.value.status_code == 404
    assert error.value.detail == "Airflow not provisioned"


@pytest.mark.asyncio
async def test_access_token_uses_airflow_token_endpoint_and_caches_its_bearer_value(monkeypatch):
    import app.services.airflow_session as session_service

    manager = AirflowSessionManager()
    redis = _Redis()
    requests = []

    async def get_redis():
        return redis

    class TokenResponse:
        status_code = 201

        @staticmethod
        def json():
            return {"access_token": "airflow-access-token"}

    class TokenClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, url, **kwargs):
            requests.append((url, kwargs))
            return TokenResponse()

    monkeypatch.setattr(manager, "_get_redis", get_redis)
    monkeypatch.setattr(session_service, "decrypt_token", lambda value: f"decrypted-{value}")
    monkeypatch.setattr(session_service.httpx, "AsyncClient", TokenClient)
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="http://airflow-project-a:8080",
        account_key="dev",
    )
    deployment = SimpleNamespace(
        project_id="project-a",
        generation=7,
        airflow_dev_user="dev",
        airflow_dev_password_encrypted="dev-password",
    )

    assert await manager.get_access_token(context, _SessionDb(deployment)) == "airflow-access-token"
    assert requests == [
        (
            "http://airflow-project-a:8080/auth/token",
            {"json": {"username": "dev", "password": "decrypted-dev-password"}},
        )
    ]
    assert redis.setex_calls == [
        ("airflow_access_token:deployment-a:7:dev", 3300, "airflow-access-token")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_code", "payload"),
    [
        (401, {"access_token": "unexpected-token"}),
        (201, {}),
        (201, {"access_token": ""}),
        (201, {"access_token": 42}),
        (201, None),
        (201, []),
        (201, "airflow-access-token"),
        (201, 42),
        (201, ValueError("invalid JSON")),
    ],
)
async def test_access_token_rejects_unsuccessful_or_invalid_token_response(
    monkeypatch, status_code, payload
):
    import app.services.airflow_session as session_service

    manager = AirflowSessionManager()
    redis = _Redis()

    async def get_redis():
        return redis

    class TokenResponse:
        def __init__(self, response_status_code):
            self.status_code = response_status_code

        def json(self):
            if isinstance(payload, ValueError):
                raise payload
            return payload

    class TokenClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, _url, **_kwargs):
            return TokenResponse(status_code)

    monkeypatch.setattr(manager, "_get_redis", get_redis)
    monkeypatch.setattr(session_service, "decrypt_token", lambda value: value)
    monkeypatch.setattr(session_service.httpx, "AsyncClient", TokenClient)
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="http://airflow-project-a:8080",
        account_key="dev",
    )
    deployment = SimpleNamespace(
        project_id="project-a",
        generation=7,
        airflow_dev_user="dev",
        airflow_dev_password_encrypted="dev-password",
    )

    with pytest.raises(HTTPException) as error:
        await manager.get_access_token(context, _SessionDb(deployment))

    assert error.value.status_code == 502
    assert error.value.detail == "Airflow authentication failed"
    assert redis.setex_calls == []


@pytest.mark.asyncio
async def test_list_dags_uses_airflow_v2_with_a_bearer_token(monkeypatch):
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="http://airflow-project-a:8080",
        account_key="dev",
    )
    calls = []

    async def resolve(*_args):
        return context

    async def get_access_token(*_args):
        return "airflow-access-token"

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {"dags": [{"dag_id": "example", "description": "Example"}]}

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, **kwargs):
            calls.append((url, kwargs))
            return Response()

    monkeypatch.setattr(widgets, "resolve_project_airflow_context", resolve)
    monkeypatch.setattr(widgets.AirflowSessionManager, "get_access_token", get_access_token)
    monkeypatch.setattr(widgets.httpx, "AsyncClient", Client)

    dags = await widgets.list_dags("project-a", SimpleNamespace(id="user-a"), object())

    assert [dag.dag_id for dag in dags] == ["example"]
    assert calls == [
        (
            "http://airflow-project-a:8080/api/v2/dags",
            {"headers": {"Authorization": "Bearer airflow-access-token"}},
        )
    ]


@pytest.mark.asyncio
async def test_list_dag_runs_maps_airflow_logical_date_to_execution_date(monkeypatch):
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="http://airflow-project-a:8080",
        account_key="dev",
    )
    calls = []

    async def resolve(*_args):
        return context

    async def get_access_token(*_args):
        return "airflow-access-token"

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "dag_runs": [
                    {
                        "dag_run_id": "scheduled__2026-02-03T04:05:06+00:00",
                        "state": "success",
                        "logical_date": "2026-02-03T04:05:06+00:00",
                        "start_date": "2026-02-03T04:05:10+00:00",
                        "end_date": "2026-02-03T04:06:12+00:00",
                        "duration": 62.0,
                    }
                ]
            }

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, **kwargs):
            calls.append((url, kwargs))
            return Response()

    monkeypatch.setattr(widgets, "resolve_project_airflow_context", resolve)
    monkeypatch.setattr(widgets.AirflowSessionManager, "get_access_token", get_access_token)
    monkeypatch.setattr(widgets.httpx, "AsyncClient", Client)

    runs = await widgets.list_dag_runs(
        "project-a", "example", cast(User, SimpleNamespace(id="user-a")), object()
    )

    assert runs[0].execution_date == datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC)
    assert calls == [
        (
            "http://airflow-project-a:8080/api/v2/dags/example/dagRuns",
            {"headers": {"Authorization": "Bearer airflow-access-token"}},
        )
    ]


@pytest.mark.asyncio
async def test_airflow_stats_uses_all_dags_dag_runs_route(monkeypatch):
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="http://airflow-project-a:8080",
        account_key="dev",
    )
    requests: list[httpx.Request] = []
    payloads = [
        {"dags": [{"dag_id": "active", "is_paused": False}, {"dag_id": "paused", "is_paused": True}]},
        {"total_entries": 3},
        {"total_entries": 2},
        {"total_entries": 8},
        {"total_entries": 1},
    ]

    async def resolve(*_args):
        return context

    async def get_access_token(*_args):
        return "airflow-access-token"

    class FrozenDateTime:
        @classmethod
        def now(cls, _timezone):
            return datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC)

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=payloads[len(requests) - 1])

    def client_factory() -> AsyncClient:
        return AsyncClient(transport=httpx.MockTransport(handler))

    monkeypatch.setattr(widgets, "resolve_project_airflow_context", resolve)
    monkeypatch.setattr(widgets.AirflowSessionManager, "get_access_token", get_access_token)
    monkeypatch.setattr(widgets, "datetime", FrozenDateTime)
    monkeypatch.setattr(widgets.httpx, "AsyncClient", client_factory)

    stats = await widgets.get_airflow_stats(
        "project-a", cast(User, SimpleNamespace(id="user-a")), object()
    )

    assert stats.model_dump() == {
        "active_dags": 1,
        "paused_dags": 1,
        "running": 3,
        "queued": 2,
        "runs_today": 8,
        "failed_24h": 1,
    }
    assert [request.url.path for request in requests] == [
        "/api/v2/dags",
        "/api/v2/dags/~/dagRuns",
        "/api/v2/dags/~/dagRuns",
        "/api/v2/dags/~/dagRuns",
        "/api/v2/dags/~/dagRuns",
    ]
    assert [dict(request.url.params) for request in requests[1:]] == [
        {"state": "running", "limit": "100"},
        {"state": "queued", "limit": "100"},
        {"start_date_gte": "2026-02-03T00:00:00+00:00", "limit": "200"},
        {
            "start_date_gte": "2026-02-02T04:05:06+00:00",
            "state": "failed",
            "limit": "100",
        },
    ]
    assert all(request.headers["Authorization"] == "Bearer airflow-access-token" for request in requests)
    for request in requests[3:]:
        query = request.url.query.decode()
        assert "%2B00%3A00" in query
        assert "+00:00" not in query


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, content=b"not-json"),
        httpx.Response(200, json=None),
        httpx.Response(200, json=[]),
        httpx.Response(200, json="not-an-object"),
        httpx.Response(200, json=42),
    ],
)
def test_airflow_response_data_rejects_malformed_or_non_object_success_response(response):
    with pytest.raises(HTTPException) as error:
        widgets._airflow_response_data(response)

    assert error.value.status_code == 502
    assert error.value.detail == "Airflow API error"


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_response_index", range(5))
async def test_airflow_stats_surfaces_each_upstream_error(monkeypatch, failing_response_index):
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="http://airflow-project-a:8080",
        account_key="dev",
    )
    calls = []

    async def resolve(*_args):
        return context

    async def get_access_token(*_args):
        return "airflow-access-token"

    class Response:
        def __init__(self, status_code):
            self.status_code = status_code

        @staticmethod
        def json():
            return {"dags": [], "total_entries": 0}

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, **kwargs):
            request_index = len(calls)
            calls.append((url, kwargs))
            return Response(503 if request_index == failing_response_index else 200)

    monkeypatch.setattr(widgets, "resolve_project_airflow_context", resolve)
    monkeypatch.setattr(widgets.AirflowSessionManager, "get_access_token", get_access_token)
    monkeypatch.setattr(widgets.httpx, "AsyncClient", Client)

    with pytest.raises(HTTPException) as error:
        await widgets.get_airflow_stats(
            "project-a", cast(User, SimpleNamespace(id="user-a")), object()
        )

    assert error.value.status_code == 502
    assert error.value.detail == "Airflow API error"
    assert len(calls) == failing_response_index + 1


@pytest.mark.asyncio
async def test_trigger_dag_run_uses_run_permission_and_returns_provenance(monkeypatch):
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=7,
        airflow_base_url="http://airflow-project-a:8080",
        account_key="dev",
    )
    calls = []

    async def resolve(*args):
        calls.append(("resolve", args[3:]))
        return context

    async def get_access_token(*_args):
        return "airflow-access-token"

    class Response:
        status_code = 201

        @staticmethod
        def json():
            return {
                "dag_run_id": "manual__2026-02-03T04:05:06+00:00",
                "state": "queued",
                "logical_date": "2026-02-03T04:05:06+00:00",
                "commit_sha": "0123456789abcdef",
                "logs_url": "dags/example/runs/manual/logs",
                "artifacts": ["manifest.json", {"name": "run_results.json"}],
            }

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, url, **kwargs):
            calls.append(("post", url, kwargs))
            return Response()

    monkeypatch.setattr(widgets, "resolve_project_airflow_context", resolve)
    monkeypatch.setattr(widgets.AirflowSessionManager, "get_access_token", get_access_token)
    monkeypatch.setattr(widgets.httpx, "AsyncClient", Client)

    run = await widgets.trigger_dag_run(
        "project-a", "example", cast(User, SimpleNamespace(id="user-a")), object()
    )

    assert calls == [
        ("resolve", ("project.dag.run", "write")),
        (
            "post",
            "http://airflow-project-a:8080/api/v2/dags/example/dagRuns",
            {"headers": {"Authorization": "Bearer airflow-access-token"}, "json": {}},
        ),
    ]
    assert run.commit_sha == "0123456789abcdef"
    assert run.logs_url == "/api/v1/projects/project-a/airflow-proxy/dags/example/runs/manual/logs"
    assert [artifact.name for artifact in run.artifacts] == ["manifest.json", "run_results.json"]


@pytest.mark.asyncio
async def test_trigger_denial_happens_before_airflow_session_or_upstream(monkeypatch):
    async def denied(*_args):
        raise HTTPException(status_code=403, detail="Access denied")

    async def forbidden_session(*_args):
        raise AssertionError("authorization must happen before session access")

    monkeypatch.setattr(widgets, "resolve_project_airflow_context", denied)
    monkeypatch.setattr(widgets.AirflowSessionManager, "get_access_token", forbidden_session)

    with pytest.raises(HTTPException) as error:
        await widgets.trigger_dag_run(
            "project-b", "example", cast(User, SimpleNamespace(id="outsider")), object()
        )

    assert error.value.status_code == 403


@pytest.mark.asyncio
async def test_artifact_route_authorizes_allowed_name_before_reporting_unavailable(monkeypatch):
    calls = []

    async def resolve(*args):
        calls.append(args[3:])
        return SimpleNamespace()

    monkeypatch.setattr(widgets, "resolve_project_airflow_context", resolve)

    with pytest.raises(HTTPException) as error:
        await widgets.download_dag_run_artifact(
            "project-a", "example", "run-a", "manifest.json", cast(User, SimpleNamespace(id="viewer")), object()
        )

    assert error.value.status_code == 404
    assert error.value.detail == "Artifact not available"
    assert calls == [("project.dag.view", "read")]


def test_dag_run_info_rejects_malformed_artifact_and_does_not_expose_external_log_url():
    with pytest.raises(HTTPException) as error:
        widgets._dag_run_info(
            "project-a",
            "example",
            {
                "dag_run_id": "run-a",
                "logical_date": "2026-02-03T04:05:06+00:00",
                "artifacts": ["not-allowed.json"],
            },
        )

    assert error.value.status_code == 502
    safe_run = widgets._dag_run_info(
        "project-a",
        "example",
        {
            "dag_run_id": "run-a",
            "logical_date": "2026-02-03T04:05:06+00:00",
            "logs_url": "https://attacker.test/logs",
        },
    )
    assert safe_run.logs_url is None


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [{"dags": {}}, {"dags": [None]}, {"dags": [{}]}])
async def test_list_dags_sanitizes_malformed_nested_airflow_payload(monkeypatch, payload):
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

    with pytest.raises(HTTPException) as error:
        await widgets.list_dags("project-a", cast(User, SimpleNamespace(id="user-a")), object())

    assert error.value.status_code == 502
    assert error.value.detail == "Airflow API error"


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [{"dags": {}}, {"dags": [None]}, {"dags": [{}]}])
async def test_airflow_stats_sanitizes_malformed_nested_dags_payload(monkeypatch, payload):
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

    with pytest.raises(HTTPException) as error:
        await widgets.get_airflow_stats(
            "project-a", cast(User, SimpleNamespace(id="user-a")), object()
        )

    assert error.value.status_code == 502
    assert error.value.detail == "Airflow API error"

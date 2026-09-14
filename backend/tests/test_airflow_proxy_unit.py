from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import httpx
import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient
from jose import jwt
from starlette.requests import Request

import app.routers.airflow_proxy as proxy
from app.auth.jwt import create_access_token
from app.config import settings
from app.database import get_db_session
from app.main import create_app
from app.models.user import User
from app.services.project_airflow_context import ProjectAirflowContext


def _request(
    method: str = "GET",
    *,
    headers: dict[str, str] | None = None,
    body: bytes = b"",
    query_string: bytes = b"",
) -> Request:
    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": method,
            "path": "/api/v1/projects/project-a/airflow-proxy/api/v2/dags",
            "query_string": query_string,
            "headers": [
                (name.lower().encode(), value.encode()) for name, value in (headers or {}).items()
            ],
        },
        receive=receive,
    )


@pytest.mark.parametrize(
    ("method", "path", "expected"),
    [
        ("GET", "api/v2/dags", ("project.dag.view", "read")),
        ("HEAD", "dags/example", ("project.dag.view", "read")),
        ("POST", "api/v2/dags/example/dagRuns", ("project.dag.run", "write")),
    ],
)
def test_proxy_route_permission_matrix(method, path, expected):
    assert proxy._permission_for_proxy_route(method, path) == expected


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("POST", "api/v1/connections"),
        ("PUT", "api/v1/variables/example"),
        ("PATCH", "api/v1/users/example"),
        ("DELETE", "api/v1/config"),
    ],
)
def test_proxy_rejects_unsupported_non_dag_writes(method, path):
    with pytest.raises(HTTPException) as error:
        proxy._permission_for_proxy_route(method, path)

    assert error.value.status_code == 404


@pytest.mark.parametrize(
    "path",
    ["/api/v2/dags", "//evil.test", "https://evil.test", "..%2Fadmin", "safe/../admin", r"\\evil"],
)
def test_proxy_rejects_paths_that_can_change_the_trusted_route(path):
    with pytest.raises(HTTPException) as error:
        proxy._validate_proxy_path(path)

    assert error.value.status_code == 404


def test_proxy_target_is_derived_from_the_persisted_context_only():
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=1,
        airflow_base_url="https://airflow.project-a.test/base",
        account_key="viewer",
    )

    assert proxy._target_url(context, "api/v2/dags?not-a-query") == (
        "https://airflow.project-a.test/base/api/v2/dags%3Fnot-a-query"
    )


@pytest.mark.asyncio
async def test_bootstrap_sets_an_opaque_project_scoped_http_only_cookie(monkeypatch):
    calls = []

    async def authorize(slug, user, db, resource, action):
        calls.append((slug, user.id, resource, action))
        return SimpleNamespace()

    monkeypatch.setattr(proxy, "resolve_project_airflow_context", authorize)
    user = SimpleNamespace(id="user-a")

    response = await proxy.airflow_iframe("project-a", "home", user, object())

    assert response.status_code == 303
    assert response.headers["location"] == "/api/v1/projects/project-a/airflow-proxy/home"
    cookie = response.headers["set-cookie"]
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie
    assert "Secure" in cookie
    assert "Path=/api/v1/projects/project-a/airflow-proxy" in cookie
    assert "session=opaque-airflow-service-cookie" not in cookie
    assert calls == [("project-a", "user-a", "project.dag.view", "read")]


@pytest.mark.asyncio
async def test_proxy_session_cookie_cannot_be_reused_for_another_project():
    token = jwt.encode(
        {
            "sub": "user-a",
            "slug": "project-a",
            "type": "airflow_proxy_session",
        },
        settings.secret_key,
        algorithm=settings.algorithm,
    )

    with pytest.raises(HTTPException) as error:
        await proxy._get_proxy_session_user(
            "project-b", _request(headers={"cookie": f"airflow_proxy_session={token}"}), object()
        )

    assert error.value.status_code == 401


@pytest.mark.asyncio
async def test_proxy_accepts_an_active_user_access_token_without_forwarding_it():
    user = SimpleNamespace(id="user-a", email="user-a@test.local", is_admin=False, is_active=True)

    class ScalarResult:
        def scalar_one_or_none(self):
            return user

    class Db:
        async def execute(self, _statement):
            return ScalarResult()

    token = create_access_token(user.id, user.email, user.is_admin)
    resolved_user = await proxy._get_proxy_session_user(
        "project-a", _request(headers={"authorization": f"Bearer {token}"}), Db()
    )

    assert resolved_user is user


@pytest.mark.asyncio
async def test_bearer_bootstrap_establishes_browser_proxy_session_without_proxy_authorization(
    monkeypatch,
):
    user = SimpleNamespace(id="user-a", email="user-a@test.local", is_admin=False, is_active=True)
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=3,
        airflow_base_url="https://airflow.project-a.test",
        account_key="viewer",
    )
    resolver_calls = []

    class ScalarResult:
        def scalar_one_or_none(self):
            return user

    class Db:
        async def execute(self, _statement):
            return ScalarResult()

    async def db_override():
        yield Db()

    async def authorize(slug, received_user, db, resource, action):
        resolver_calls.append((slug, received_user.id, resource, action))
        return context

    async def session_for_context(*_args):
        return "opaque-airflow-service-cookie"

    class UpstreamClient:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def request(self, **_kwargs):
            return httpx.Response(200, content=b"ok")

    app = create_app()
    app.dependency_overrides[get_db_session] = db_override
    monkeypatch.setattr(proxy, "resolve_project_airflow_context", authorize)
    monkeypatch.setattr(proxy.AirflowSessionManager, "get_access_token", session_for_context)
    monkeypatch.setattr(proxy.httpx, "AsyncClient", UpstreamClient)

    access_token = create_access_token(user.id, user.email, user.is_admin)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="https://conductor.test", follow_redirects=False
    ) as client:
        bootstrap = await client.post(
            "/api/v1/projects/project-a/airflow-proxy/bootstrap",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        proxied = await client.get("/api/v1/projects/project-a/airflow-proxy/home")

    assert bootstrap.status_code == 204
    assert proxied.status_code == 200
    assert proxied.content == b"ok"
    assert resolver_calls == [
        ("project-a", "user-a", "project.dag.view", "read"),
        ("project-a", "user-a", "project.dag.view", "read"),
    ]


@pytest.mark.asyncio
async def test_non_dag_write_is_denied_before_context_session_or_upstream(monkeypatch):
    async def forbidden_context(*_args):
        raise AssertionError("unsupported write must be rejected before context resolution")

    monkeypatch.setattr(proxy, "resolve_project_airflow_context", forbidden_context)

    with pytest.raises(HTTPException) as error:
        await proxy.airflow_proxy(
            "project-a",
            "api/v1/connections",
            _request(method="POST", headers={"authorization": "Bearer user-access-token"}),
            cast(User, SimpleNamespace(id="user-a")),
            object(),
        )

    assert error.value.status_code == 404


@pytest.mark.asyncio
async def test_cookie_authenticated_unsafe_requests_require_same_origin_proof(monkeypatch):
    user = SimpleNamespace(id="user-a", email="user-a@test.local", is_admin=False, is_active=True)
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=3,
        airflow_base_url="https://airflow.project-a.test",
        account_key="viewer",
    )
    resolver_calls = []

    class ScalarResult:
        def scalar_one_or_none(self):
            return user

    class Db:
        async def execute(self, _statement):
            return ScalarResult()

    async def db_override():
        yield Db()

    async def authorize(slug, received_user, db, resource, action):
        resolver_calls.append((slug, received_user.id, resource, action))
        return context

    async def forbidden_session(*_args):
        raise AssertionError("CSRF rejection must happen before service-session access")

    app = create_app()
    app.dependency_overrides[get_db_session] = db_override
    monkeypatch.setattr(proxy, "resolve_project_airflow_context", authorize)
    monkeypatch.setattr(proxy.AirflowSessionManager, "get_access_token", forbidden_session)

    access_token = create_access_token(user.id, user.email, user.is_admin)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="https://conductor.test", follow_redirects=False
    ) as client:
        bootstrap = await client.post(
            "/api/v1/projects/project-a/airflow-proxy/bootstrap",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        missing_proof = await client.post(
            "/api/v1/projects/project-a/airflow-proxy/api/v2/dags/example/dagRuns"
        )
        mismatched_proof = await client.post(
            "/api/v1/projects/project-a/airflow-proxy/api/v2/dags/example/dagRuns",
            headers={"Origin": "https://attacker.test"},
        )

    assert bootstrap.status_code == 204
    assert missing_proof.status_code == 403
    assert mismatched_proof.status_code == 403
    assert resolver_calls == [("project-a", "user-a", "project.dag.view", "read")]


@pytest.mark.asyncio
async def test_bearer_dag_run_request_bypasses_cookie_csrf_protection(monkeypatch):
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=3,
        airflow_base_url="https://airflow.project-a.test",
        account_key="editor",
    )
    resolver_calls = []

    async def authorize(slug, user, db, resource, action):
        resolver_calls.append((slug, user.id, resource, action))
        return context

    async def session_for_context(*_args):
        return "opaque-airflow-service-cookie"

    class UpstreamClient:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def request(self, **_kwargs):
            return httpx.Response(200, content=b"ok")

    monkeypatch.setattr(proxy, "resolve_project_airflow_context", authorize)
    monkeypatch.setattr(proxy.AirflowSessionManager, "get_access_token", session_for_context)
    monkeypatch.setattr(proxy.httpx, "AsyncClient", UpstreamClient)

    response = await proxy.airflow_proxy(
        "project-a",
        "api/v2/dags/example/dagRuns",
        _request(method="POST", headers={"authorization": "Bearer user-access-token"}),
        cast(User, SimpleNamespace(id="user-a")),
        object(),
    )

    assert response.status_code == 200
    assert resolver_calls == [("project-a", "user-a", "project.dag.run", "write")]


@pytest.mark.asyncio
async def test_proxy_authorizes_before_session_or_upstream_request(monkeypatch):
    async def denied(*_args):
        raise HTTPException(status_code=403, detail="Access denied")

    class ForbiddenClient:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("authorization must fail before upstream access")

    monkeypatch.setattr(proxy, "resolve_project_airflow_context", denied)
    monkeypatch.setattr(proxy.httpx, "AsyncClient", ForbiddenClient)

    with pytest.raises(HTTPException) as error:
        await proxy.airflow_proxy(
            "project-a",
            "api/v2/dags",
            _request(),
            cast(User, SimpleNamespace(id="user-a")),
            object(),
        )

    assert error.value.status_code == 403


@pytest.mark.asyncio
async def test_proxy_forwards_only_safe_headers_and_never_exposes_upstream_cookie(monkeypatch):
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=3,
        airflow_base_url="https://airflow.project-a.test",
        account_key="viewer",
    )
    authorize_calls = []
    captured = {}

    async def authorize(slug, user, db, resource, action):
        authorize_calls.append((slug, user.id, resource, action))
        return context

    async def session_for_context(self, received_context, db):
        assert received_context is context
        return "opaque-airflow-service-cookie"

    class UpstreamClient:
        def __init__(self, *, follow_redirects):
            assert follow_redirects is False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def request(self, **kwargs):
            captured.update(kwargs)
            return httpx.Response(
                200,
                content=b'{"dags": []}',
                headers={
                    "content-type": "application/json",
                    "set-cookie": "session=opaque-airflow-service-cookie",
                    "location": "https://airflow.project-a.test/login",
                    "x-upstream": "safe",
                },
            )

    monkeypatch.setattr(proxy, "resolve_project_airflow_context", authorize)
    monkeypatch.setattr(proxy.AirflowSessionManager, "get_access_token", session_for_context)
    monkeypatch.setattr(proxy.httpx, "AsyncClient", UpstreamClient)

    response = await proxy.airflow_proxy(
        "project-a",
        "api/v2/dags",
        _request(
            headers={
                "authorization": "Bearer user-access-token",
                "cookie": "airflow_proxy_session=client-cookie",
                "host": "conductor.test",
                "x-forwarded-host": "attacker.test",
                "content-type": "application/json",
            },
            body=b"{}",
            query_string=b"tag=first&tag=second",
        ),
        SimpleNamespace(id="user-a"),
        object(),
    )

    assert authorize_calls == [("project-a", "user-a", "project.dag.view", "read")]
    assert captured["url"] == "https://airflow.project-a.test/api/v2/dags"
    assert captured["params"] == [("tag", "first"), ("tag", "second")]
    assert captured["headers"] == {
        "Authorization": "Bearer opaque-airflow-service-cookie",
        "content-type": "application/json",
    }
    assert response.headers["content-type"] == "application/json"
    assert response.headers["x-upstream"] == "safe"
    assert "set-cookie" not in response.headers
    assert "location" not in response.headers


@pytest.mark.asyncio
async def test_proxy_replaces_upstream_redirect_with_generic_error(monkeypatch):
    context = ProjectAirflowContext(
        project_id="project-a",
        deployment_id="deployment-a",
        deployment_generation=3,
        airflow_base_url="https://airflow.project-a.test",
        account_key="viewer",
    )

    async def authorize(*_args):
        return context

    async def session_for_context(*_args):
        return "opaque-session"

    class RedirectingClient:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def request(self, **_kwargs):
            return httpx.Response(302, headers={"location": "https://attacker.test/"})

    monkeypatch.setattr(proxy, "resolve_project_airflow_context", authorize)
    monkeypatch.setattr(proxy.AirflowSessionManager, "get_access_token", session_for_context)
    monkeypatch.setattr(proxy.httpx, "AsyncClient", RedirectingClient)

    with pytest.raises(HTTPException) as error:
        await proxy.airflow_proxy(
            "project-a", "home", _request(), SimpleNamespace(id="user-a"), object()
        )

    assert error.value.status_code == 502
    assert error.value.detail == "Airflow unavailable"

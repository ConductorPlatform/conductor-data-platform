from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.config import settings
from app.models.git_config import GitConfig
from app.schemas.settings import GitConfigUpdateRequest
from app.services.airflow_session import AirflowSessionManager
from app.services.crypto import encrypt_token
from app.services.git_dag_bundle import (
    GitDagBundleSyncError,
    git_dag_connection_payload,
    sync_git_dag_connection,
)
from app.services.project_airflow_context import ProjectAirflowContext


@pytest.fixture(autouse=True)
def credentials_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        settings,
        "credentials_encryption_key",
        "test-only-credentials-encryption-key-1234567890",
    )


@pytest.mark.parametrize("path", ["dags", "dags/transforms", "dbt"])
def test_git_paths_are_normalized_to_repository_relative_posix_directories(path: str) -> None:
    request = GitConfigUpdateRequest(dags_path=path, dbt_path=path)

    assert request.dags_path == path
    assert request.dbt_path == path


@pytest.mark.parametrize("path", ["/etc", ".", "../dbt", "dbt/../secrets", r"dbt\\models", "dbt\x00models"])
def test_git_paths_reject_absolute_or_escaping_values(path: str) -> None:
    with pytest.raises(ValidationError, match="repository-relative POSIX"):
        GitConfigUpdateRequest(dags_path=path)


def test_git_dag_connection_payload_keeps_token_out_of_url_and_metadata() -> None:
    token = "secret-token-value"
    config = GitConfig(
        project_id="0123456789abcdef0123456789abcdef",
        repo_url="https://git.example.test/team/project.git",
        auth_type="token",
        credentials_encrypted=encrypt_token(token),
        default_branch="production",
        dags_path="orchestration/dags",
        dbt_path="transform/dbt",
    )

    payload = git_dag_connection_payload(config)

    assert payload.connection_id == "conductor_git"
    assert payload.conn_type == "git"
    assert payload.host == "https://git.example.test/team/project.git"
    assert payload.login == "oauth2"
    assert payload.password == token
    assert token not in payload.host
    assert token not in payload.extra
    assert payload.extra == (
        '{"conductor_dags_path":"orchestration/dags",'
        '"conductor_dbt_path":"transform/dbt",'
        '"conductor_tracking_ref":"production"}'
    )


def test_git_dag_connection_payload_rejects_non_mvp_configuration() -> None:
    config = GitConfig(
        project_id="0123456789abcdef0123456789abcdef",
        repo_url="https://git.example.test/team/project.git",
        auth_type="ssh",
        credentials_encrypted=encrypt_token("private-key"),
    )

    with pytest.raises(ValueError, match="HTTPS token"):
        git_dag_connection_payload(config)


@pytest.mark.asyncio
async def test_git_dag_connection_sync_creates_only_the_project_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.git_dag_bundle as bundle_service

    requests: list[tuple[str, str, dict[str, str], dict[str, str] | None]] = []

    class Response:
        def __init__(self, status_code: int) -> None:
            self.status_code = status_code

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args) -> bool:
            return False

        async def get(self, url: str, *, headers: dict[str, str]) -> Response:
            requests.append(("GET", url, headers, None))
            return Response(404)

        async def post(self, url: str, *, headers: dict[str, str], json: dict[str, str]) -> Response:
            requests.append(("POST", url, headers, json))
            return Response(201)

    class SessionManager(AirflowSessionManager):
        async def get_access_token(self, _context, _db) -> str:
            return "opaque-airflow-access-token"

    monkeypatch.setattr(bundle_service.httpx, "AsyncClient", Client)
    config = GitConfig(
        project_id="0123456789abcdef0123456789abcdef",
        repo_url="https://git.example.test/team/project.git",
        auth_type="token",
        credentials_encrypted=encrypt_token("git-token"),
        default_branch="production",
        dags_path="orchestration/dags",
        dbt_path="transform/dbt",
    )
    context = ProjectAirflowContext(
        project_id=config.project_id,
        deployment_id="deployment-id",
        deployment_generation=1,
        airflow_base_url="http://airflow-project:8080",
        account_key="admin",
    )

    await sync_git_dag_connection(
        context=context,
        config=config,
        db=object(),
        session_manager=SessionManager(),
    )

    assert [request[0] for request in requests] == ["GET", "POST"]
    assert requests[0][1].endswith("/api/v2/connections/conductor_git")
    assert requests[1][1].endswith("/api/v2/connections")
    assert requests[1][2] == {"Authorization": "Bearer opaque-airflow-access-token"}
    assert requests[1][3] is not None
    assert requests[1][3]["password"] == "git-token"
    assert "git-token" not in requests[1][3]["host"]
    assert "git-token" not in requests[1][3]["extra"]


@pytest.mark.asyncio
async def test_git_dag_connection_sync_reports_sanitized_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.git_dag_bundle as bundle_service

    class Response:
        status_code = 500

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args) -> bool:
            return False

        async def get(self, *_args, **_kwargs) -> Response:
            return Response()

    class SessionManager(AirflowSessionManager):
        async def get_access_token(self, _context, _db) -> str:
            return "opaque-airflow-access-token"

    monkeypatch.setattr(bundle_service.httpx, "AsyncClient", Client)
    config = GitConfig(
        project_id="0123456789abcdef0123456789abcdef",
        repo_url="https://git.example.test/team/project.git",
        auth_type="token",
        credentials_encrypted=encrypt_token("git-token"),
    )
    context = ProjectAirflowContext(
        project_id=config.project_id,
        deployment_id="deployment-id",
        deployment_generation=1,
        airflow_base_url="http://airflow-project:8080",
        account_key="admin",
    )

    with pytest.raises(GitDagBundleSyncError, match="connection lookup") as error:
        await sync_git_dag_connection(
            context=context,
            config=config,
            db=object(),
            session_manager=SessionManager(),
        )

    assert "git-token" not in str(error.value)

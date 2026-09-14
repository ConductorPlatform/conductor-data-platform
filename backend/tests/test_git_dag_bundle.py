from __future__ import annotations

from pathlib import Path

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


@pytest.mark.parametrize(
    "path",
    [
        "/etc",
        ".",
        "../dbt",
        "dbt/../secrets",
        "dbt//models",
        "dbt/./models",
        "dbt/",
        r"dbt\\models",
        "dbt\x00models",
    ],
)
def test_git_paths_reject_absolute_or_noncanonical_values(path: str) -> None:
    with pytest.raises(ValidationError, match="repository-relative POSIX"):
        GitConfigUpdateRequest(dags_path=path)


@pytest.mark.parametrize(
    "branch",
    ["bad branch", "main~1", "foo^bar", "foo:bar", "foo?bar", "foo@{bar", "foo.", "@", "topic.lock"],
)
def test_git_branch_validation_matches_git_ref_format_rejections(branch: str) -> None:
    with pytest.raises(ValidationError, match="safe Git ref"):
        GitConfigUpdateRequest(default_branch=branch)


@pytest.mark.parametrize("branch", ["main", "release/2026-09", "feature.with-dot"])
def test_git_branch_validation_accepts_canonical_branch_names(branch: str) -> None:
    assert GitConfigUpdateRequest(default_branch=branch).default_branch == branch


def test_git_dag_connection_payload_keeps_token_out_of_airflow_metadata() -> None:
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
    assert payload.password == ""
    assert token not in payload.host
    assert token not in payload.extra
    assert token not in payload.as_dict().values()
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


def _runtime_dir(tmp_path: Path, project_id: str, generation: int = 1) -> Path:
    directory = tmp_path / "runtimes" / project_id / str(generation)
    directory.mkdir(parents=True)
    directory.chmod(0o700)
    return directory


def _context(project_id: str) -> ProjectAirflowContext:
    return ProjectAirflowContext(
        project_id=project_id,
        deployment_id="deployment-id",
        deployment_generation=1,
        airflow_base_url="http://airflow-project:8080",
        account_key="admin",
    )


@pytest.mark.asyncio
async def test_git_dag_connection_sync_creates_secret_free_project_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
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

    project_id = "0123456789abcdef0123456789abcdef"
    runtime_directory = _runtime_dir(tmp_path, project_id)
    monkeypatch.setattr(settings, "lifecycle_runtime_root", tmp_path / "runtimes")
    monkeypatch.setattr(bundle_service.httpx, "AsyncClient", Client)
    config = GitConfig(
        project_id=project_id,
        repo_url="https://git.example.test/team/project.git",
        auth_type="token",
        credentials_encrypted=encrypt_token("git-token"),
        default_branch="production",
        dags_path="orchestration/dags",
        dbt_path="transform/dbt",
    )

    await sync_git_dag_connection(
        context=_context(project_id),
        config=config,
        db=object(),
        session_manager=SessionManager(),
    )

    assert [request[0] for request in requests] == ["GET", "POST"]
    assert requests[0][1].endswith("/api/v2/connections/conductor_git")
    assert requests[1][1].endswith("/api/v2/connections")
    assert requests[1][2] == {"Authorization": "Bearer opaque-airflow-access-token"}
    assert requests[1][3] is not None
    assert requests[1][3]["password"] == ""
    assert "git-token" not in requests[1][3]["host"]
    assert "git-token" not in requests[1][3]["extra"]
    token_path = runtime_directory / "git-token"
    assert token_path.read_text() == "git-token"
    assert token_path.stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_git_dag_connection_sync_reports_sanitized_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
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

    project_id = "0123456789abcdef0123456789abcdef"
    _runtime_dir(tmp_path, project_id)
    monkeypatch.setattr(settings, "lifecycle_runtime_root", tmp_path / "runtimes")
    monkeypatch.setattr(bundle_service.httpx, "AsyncClient", Client)
    config = GitConfig(
        project_id=project_id,
        repo_url="https://git.example.test/team/project.git",
        auth_type="token",
        credentials_encrypted=encrypt_token("git-token"),
    )

    with pytest.raises(GitDagBundleSyncError, match="connection lookup") as error:
        await sync_git_dag_connection(
            context=_context(project_id),
            config=config,
            db=object(),
            session_manager=SessionManager(),
        )

    assert "git-token" not in str(error.value)


@pytest.mark.asyncio
async def test_git_dag_connection_revocation_removes_runtime_secret(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import app.services.git_dag_bundle as bundle_service

    class Response:
        status_code = 204

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args) -> bool:
            return False

        async def delete(self, *_args, **_kwargs) -> Response:
            return Response()

    class SessionManager(AirflowSessionManager):
        async def get_access_token(self, _context, _db) -> str:
            return "opaque-airflow-access-token"

    project_id = "0123456789abcdef0123456789abcdef"
    runtime_directory = _runtime_dir(tmp_path, project_id)
    token_path = runtime_directory / "git-token"
    token_path.write_text("git-token")
    token_path.chmod(0o600)
    monkeypatch.setattr(settings, "lifecycle_runtime_root", tmp_path / "runtimes")
    monkeypatch.setattr(bundle_service.httpx, "AsyncClient", Client)

    await sync_git_dag_connection(
        context=_context(project_id),
        config=GitConfig(project_id=project_id, repo_url="https://git.example.test/team/project.git", auth_type="https"),
        db=object(),
        session_manager=SessionManager(),
    )

    assert not token_path.exists()

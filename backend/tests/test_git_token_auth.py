from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.models.git_config import GitConfig
from app.models.project import Project, ProjectLifecycleStatus
from app.services.crypto import (
    CredentialsEncryptionNotConfigured,
    decrypt_token,
    encrypt_token,
)
from app.services.workspace import _git_env


@pytest.fixture(autouse=True)
def configure_credentials_encryption_key(monkeypatch):
    monkeypatch.setattr(
        settings,
        "credentials_encryption_key",
        "test-only-credentials-encryption-key-1234567890",
    )

    async def sync_git_connection(*_args, **_kwargs):
        return None

    monkeypatch.setattr("app.routers.projects.sync_git_dag_connection", sync_git_connection)


async def _mark_project_ready(db_session: AsyncSession, project_id: str) -> None:
    project = await db_session.get(Project, project_id)
    assert project is not None
    project.lifecycle_status = ProjectLifecycleStatus.READY
    await db_session.commit()


@pytest.mark.asyncio
async def test_git_token_is_encrypted_and_never_returned(
    admin_client,
    _engine,
    db_session,
):
    slug = f"git-token-{uuid4().hex[:8]}"
    create_response = await admin_client.post(
        "/api/v1/projects",
        json={"name": "Git token project", "slug": slug},
        headers={"Idempotency-Key": str(uuid4())},
    )
    assert create_response.status_code == 202, create_response.text
    await _mark_project_ready(db_session, create_response.json()["project"]["id"])

    plaintext_token = "glpat-super-secret-token"
    update_response = await admin_client.put(
        f"/api/v1/projects/{slug}/git",
        json={
            "repo_url": "https://gitlab.example.com/team/repo.git",
            "auth_type": "token",
            "token": plaintext_token,
            "default_branch": "main",
            "dbt_path": "dbt/",
            "dags_path": "dags/",
        },
    )

    assert update_response.status_code == 200, update_response.text
    response_data = update_response.json()
    assert response_data["has_token"] is True
    assert response_data["has_credentials"] is True
    assert "token" not in response_data
    assert "credentials" not in response_data
    assert plaintext_token not in update_response.text

    session_factory = async_sessionmaker(
        _engine, class_=AsyncSession, expire_on_commit=False
    )
    async with session_factory() as session:
        project = (
            await session.execute(select(Project).where(Project.slug == slug))
        ).scalar_one()
        git_config = (
            await session.execute(
                select(GitConfig).where(GitConfig.project_id == project.id)
            )
        ).scalar_one()
        encrypted_token = git_config.credentials_encrypted

    assert encrypted_token
    assert encrypted_token != plaintext_token
    assert plaintext_token not in encrypted_token
    assert decrypt_token(encrypted_token) == plaintext_token

    get_response = await admin_client.get(f"/api/v1/projects/{slug}/git")
    assert get_response.status_code == 200
    assert get_response.json()["has_token"] is True
    assert plaintext_token not in get_response.text

    preserve_response = await admin_client.put(
        f"/api/v1/projects/{slug}/git",
        json={"default_branch": "develop"},
    )
    assert preserve_response.status_code == 200
    assert preserve_response.json()["has_token"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["repo_url", "auth_type"])
async def test_git_config_rejects_explicit_null_for_non_nullable_fields(
    admin_client, field
):
    response = await admin_client.put(
        "/api/v1/projects/validation-must-run-before-lookup/git",
        json={field: None},
    )

    assert response.status_code == 422, response.text
    assert any(
        error["loc"][-1] == field for error in response.json()["detail"]
    ), response.text


@pytest.mark.asyncio
async def test_git_config_omitted_fields_preserve_existing_values(
    admin_client,
    db_session,
):
    slug = f"git-partial-{uuid4().hex[:8]}"
    create_response = await admin_client.post(
        "/api/v1/projects",
        json={"name": "Partial Git update", "slug": slug},
        headers={"Idempotency-Key": str(uuid4())},
    )
    assert create_response.status_code == 202, create_response.text
    await _mark_project_ready(db_session, create_response.json()["project"]["id"])

    initial_response = await admin_client.put(
        f"/api/v1/projects/{slug}/git",
        json={
            "repo_url": "https://github.com/org/repo.git",
            "auth_type": "https",
        },
    )
    assert initial_response.status_code == 200, initial_response.text

    response = await admin_client.put(
        f"/api/v1/projects/{slug}/git",
        json={"default_branch": "develop"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["repo_url"] == "https://github.com/org/repo.git"
    assert response.json()["auth_type"] == "https"
    assert response.json()["default_branch"] == "develop"


@pytest.mark.asyncio
async def test_git_config_rejects_credentials_embedded_in_url(
    admin_client,
    db_session,
):
    slug = f"git-url-{uuid4().hex[:8]}"
    create_response = await admin_client.post(
        "/api/v1/projects",
        json={"name": "Unsafe URL project", "slug": slug},
        headers={"Idempotency-Key": str(uuid4())},
    )
    assert create_response.status_code == 202, create_response.text
    await _mark_project_ready(db_session, create_response.json()["project"]["id"])

    missing_token_response = await admin_client.put(
        f"/api/v1/projects/{slug}/git",
        json={
            "repo_url": "https://gitlab.example.com/team/repo.git",
            "auth_type": "token",
        },
    )
    assert missing_token_response.status_code == 422
    assert "token is required" in missing_token_response.text.lower()

    insecure_url_response = await admin_client.put(
        f"/api/v1/projects/{slug}/git",
        json={
            "repo_url": "http://gitlab.example.com/team/repo.git",
            "auth_type": "token",
            "token": "must-not-cross-plaintext-http",
        },
    )
    assert insecure_url_response.status_code == 422
    assert "https repository url" in insecure_url_response.text.lower()

    wrong_auth_response = await admin_client.put(
        f"/api/v1/projects/{slug}/git",
        json={
            "repo_url": "https://gitlab.example.com/team/repo.git",
            "token": "token-without-token-auth",
        },
    )
    assert wrong_auth_response.status_code == 422
    assert "token requires token authentication" in wrong_auth_response.text.lower()

    legacy_client_response = await admin_client.put(
        f"/api/v1/projects/{slug}/git",
        json={
            "repo_url": "https://gitlab.example.com/team/repo.git",
            "auth_type": "token",
            "credentials": "legacy-client-token",
        },
    )
    assert legacy_client_response.status_code == 200
    assert legacy_client_response.json()["has_token"] is True

    response = await admin_client.put(
        f"/api/v1/projects/{slug}/git",
        json={
            "repo_url": "https://oauth2:plaintext-token@gitlab.example.com/team/repo.git",
            "auth_type": "token",
        },
    )

    assert response.status_code == 422
    assert "must not contain embedded credentials" in response.text


def test_git_token_askpass_keeps_token_out_of_script(tmp_path):
    plaintext_token = "github_pat_secret"
    config = GitConfig(
        project_id="project-id",
        repo_url="https://github.com/org/repo.git",
        auth_type="token",
        credentials_encrypted=encrypt_token(plaintext_token),
    )

    env = _git_env(config, tmp_path)
    askpass_path = tmp_path / ".git-askpass"

    assert env["GIT_ASKPASS"] == str(askpass_path)
    assert env["GIT_TERMINAL_PROMPT"] == "0"
    assert env["CONDUCTOR_GIT_TOKEN"] == plaintext_token
    assert plaintext_token not in askpass_path.read_text()
    assert askpass_path.stat().st_mode & 0o777 == 0o700


def test_dedicated_encryption_key_can_read_legacy_ciphertext(monkeypatch):
    legacy_key = "legacy-secret-key-with-at-least-32-characters"
    monkeypatch.setattr(settings, "secret_key", legacy_key)
    monkeypatch.setattr(settings, "credentials_encryption_key", legacy_key)
    legacy_ciphertext = encrypt_token("legacy-token")

    monkeypatch.setattr(
        settings,
        "credentials_encryption_key",
        "new-dedicated-key-with-at-least-32-characters",
    )

    assert decrypt_token(legacy_ciphertext) == "legacy-token"
    assert decrypt_token(encrypt_token("new-token")) == "new-token"


def test_encryption_fails_closed_without_dedicated_key(monkeypatch):
    monkeypatch.setattr(settings, "credentials_encryption_key", None)

    with pytest.raises(CredentialsEncryptionNotConfigured):
        encrypt_token("must-not-use-jwt-key")


@pytest.mark.asyncio
async def test_git_config_returns_503_without_dedicated_key(
    admin_client,
    monkeypatch,
    db_session,
):
    slug = f"git-key-{uuid4().hex[:8]}"
    create_response = await admin_client.post(
        "/api/v1/projects",
        json={"name": "Missing encryption key", "slug": slug},
        headers={"Idempotency-Key": str(uuid4())},
    )
    assert create_response.status_code == 202, create_response.text
    await _mark_project_ready(db_session, create_response.json()["project"]["id"])
    monkeypatch.setattr(settings, "credentials_encryption_key", None)

    response = await admin_client.put(
        f"/api/v1/projects/{slug}/git",
        json={
            "repo_url": "https://github.com/org/repo.git",
            "auth_type": "token",
            "token": "must-not-be-encrypted-with-jwt-key",
        },
    )

    assert response.status_code == 503
    assert "CONDUCTOR_CREDENTIALS_ENCRYPTION_KEY" in response.text

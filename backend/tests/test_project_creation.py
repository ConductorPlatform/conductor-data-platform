from __future__ import annotations

import asyncio
import json
import secrets
import subprocess
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.auth.jwt import create_access_token
from app.auth.password import hash_password
from app.config import Settings, settings
from app.database import get_db_session
from app.main import create_app
from app.models.audit_event import AuditEvent
from app.models.environment import Environment
from app.models.project import Project, ProjectLifecycleStatus
from app.models.project_deployment import ProjectDeployment
from app.models.project_lifecycle_job import (
    LifecycleJobStatus,
    LifecycleOperation,
    ProjectLifecycleJob,
)
from app.models.project_member import ProjectMember
from app.models.user import User


def _idempotency_headers(key: str | None = None) -> dict[str, str]:
    return {"Idempotency-Key": key or str(uuid4())}


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [{}, {"Idempotency-Key": "not-a-uuid"}])
async def test_create_project_requires_uuid_idempotency_key(admin_client, headers):
    response = await admin_client.post(
        "/api/v1/projects",
        headers=headers,
        json={"name": "Atomic Project", "slug": "atomic-project"},
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_project_requires_super_admin(client, db_session):
    user = User(
        email="member@test.local",
        hashed_password=hash_password("member-password"),
        display_name="Member",
        is_active=True,
        is_admin=False,
    )
    db_session.add(user)
    await db_session.commit()
    client.headers["Authorization"] = f"Bearer {create_access_token(user.id, user.email, False)}"

    response = await client.post(
        "/api/v1/projects",
        headers=_idempotency_headers(),
        json={"name": "Forbidden Project", "slug": "forbidden-project"},
    )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_project_returns_202_and_persists_atomic_operation(
    admin_client,
    db_session,
    monkeypatch,
    caplog,
):
    monkeypatch.setattr(settings, "credentials_encryption_key", "task-four-test-key-material-32-bytes")
    generated_plaintexts = [f"task-four-fake-credential-{index}" for index in range(5)]
    generated = iter(generated_plaintexts)
    monkeypatch.setattr(secrets, "token_urlsafe", lambda _size: next(generated))
    idempotency_key = str(uuid4())

    response = await admin_client.post(
        "/api/v1/projects",
        headers=_idempotency_headers(idempotency_key),
        json={
            "name": "Atomic Project",
            "slug": "atomic-project",
            "description": "Created without runtime calls",
        },
    )

    assert response.status_code == 202, response.text
    payload = response.json()
    assert set(payload) == {"project", "operation"}
    assert set(payload["project"]) == {
        "id",
        "name",
        "slug",
        "description",
        "self_approve_enabled",
        "lifecycle_status",
        "created_at",
        "updated_at",
        "member_count",
        "role",
    }
    assert set(payload["operation"]) == {"id", "operation", "status"}
    assert payload["project"]["slug"] == "atomic-project"
    assert payload["project"]["lifecycle_status"] == "provisioning"
    assert payload["project"]["role"] == "super_admin"
    assert payload["project"]["member_count"] == 1
    assert payload["operation"]["operation"] == "provision"
    assert payload["operation"]["status"] == "pending"

    project = (
        await db_session.execute(select(Project).where(Project.slug == "atomic-project"))
    ).scalar_one()
    assert project.lifecycle_status is ProjectLifecycleStatus.PROVISIONING

    environments = (
        await db_session.execute(
            select(Environment).where(Environment.project_id == project.id).order_by(Environment.name)
        )
    ).scalars().all()
    assert [(env.name, env.branch_name, env.is_protected) for env in environments] == [
        ("development", "develop", False),
        ("production", "main", True),
    ]
    assert (
        await db_session.scalar(
            select(func.count()).select_from(ProjectMember).where(ProjectMember.project_id == project.id)
        )
    ) == 1

    deployment = (
        await db_session.execute(
            select(ProjectDeployment).where(ProjectDeployment.project_id == project.id)
        )
    ).scalar_one()
    assert deployment.airflow_external_url == "https://atomic-project.airflow.localhost"
    encrypted_values = [
        deployment.airflow_db_password_encrypted,
        deployment.airflow_admin_password_encrypted,
        deployment.airflow_dev_password_encrypted,
        deployment.airflow_viewer_password_encrypted,
        deployment.airflow_integration_password_encrypted,
    ]
    assert all(value.startswith("gAAAA") for value in encrypted_values)
    assert all(plaintext not in encrypted for plaintext in generated_plaintexts for encrypted in encrypted_values)

    operation = (
        await db_session.execute(
            select(ProjectLifecycleJob).where(ProjectLifecycleJob.id == payload["operation"]["id"])
        )
    ).scalar_one()
    assert operation.project_id == project.id
    assert operation.operation is LifecycleOperation.PROVISION
    assert operation.status is LifecycleJobStatus.PENDING
    assert operation.idempotency_key == idempotency_key
    assert len(operation.request_fingerprint) == 64

    audit = (
        await db_session.execute(
            select(AuditEvent).where(AuditEvent.project_id_snapshot == project.id)
        )
    ).scalar_one()
    assert audit.event_type == "project.provision.requested"
    assert audit.outcome == "requested"
    assert audit.correlation_id == operation.correlation_id
    assert audit.metadata_json == {"operation_id": operation.id}

    exposed_text = json.dumps(payload) + "\n" + caplog.text
    assert all(plaintext not in exposed_text for plaintext in generated_plaintexts)


def test_airflow_external_domain_settings_use_prefixed_env_and_local_fallback(monkeypatch):
    monkeypatch.delenv("CONDUCTOR_AIRFLOW_EXTERNAL_DOMAIN", raising=False)
    assert Settings(_env_file=None).airflow_external_domain == "localhost"

    monkeypatch.setenv("CONDUCTOR_AIRFLOW_EXTERNAL_DOMAIN", "airflow.example.test")
    assert Settings(_env_file=None).airflow_external_domain == "airflow.example.test"


@pytest.mark.asyncio
async def test_create_project_persists_configured_airflow_external_domain(
    admin_client,
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(settings, "credentials_encryption_key", "task-four-test-key-material-32-bytes")
    monkeypatch.setattr(settings, "airflow_external_domain", "platform.example.test", raising=False)

    response = await admin_client.post(
        "/api/v1/projects",
        headers=_idempotency_headers(),
        json={"name": "Domain Project", "slug": "domain-project"},
    )

    assert response.status_code == 202, response.text
    deployment = (await db_session.execute(select(ProjectDeployment))).scalar_one()
    assert deployment.airflow_external_url == "https://domain-project.airflow.platform.example.test"


@pytest.mark.asyncio
async def test_create_project_never_refreshes_after_its_only_commit(
    admin_client,
    monkeypatch,
):
    monkeypatch.setattr(settings, "credentials_encryption_key", "task-four-test-key-material-32-bytes")
    refresh_calls = 0

    async def record_refresh(*_args, **_kwargs):
        nonlocal refresh_calls
        refresh_calls += 1

    monkeypatch.setattr(AsyncSession, "refresh", record_refresh)

    response = await admin_client.post(
        "/api/v1/projects",
        headers=_idempotency_headers(),
        json={"name": "No Refresh", "slug": "no-refresh"},
    )

    assert response.status_code == 202, response.text
    assert response.json()["project"]["slug"] == "no-refresh"
    assert refresh_calls == 0


@pytest.mark.asyncio
async def test_create_project_does_not_call_runtime_integrations(
    admin_client,
    monkeypatch,
):
    monkeypatch.setattr(settings, "credentials_encryption_key", "task-four-test-key-material-32-bytes")

    def forbidden_runtime_call(*_args, **_kwargs):
        raise AssertionError("project creation must not call a runtime integration")

    async def forbidden_async_runtime_call(*_args, **_kwargs):
        raise AssertionError("project creation must not call a runtime integration")

    monkeypatch.setattr(httpx, "AsyncClient", forbidden_runtime_call)
    monkeypatch.setattr(subprocess, "run", forbidden_runtime_call)
    monkeypatch.setattr(subprocess, "Popen", forbidden_runtime_call)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", forbidden_async_runtime_call)

    response = await admin_client.post(
        "/api/v1/projects",
        headers=_idempotency_headers(),
        json={"name": "No Runtime", "slug": "no-runtime"},
    )

    assert response.status_code == 202, response.text


@pytest.mark.asyncio
async def test_create_project_rolls_back_when_credentials_cannot_be_encrypted(
    admin_client,
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(settings, "credentials_encryption_key", None)

    response = await admin_client.post(
        "/api/v1/projects",
        headers=_idempotency_headers(),
        json={"name": "Rollback Project", "slug": "rollback-project"},
    )

    assert response.status_code == 503
    assert "encryption" in response.json()["detail"].lower()
    assert await db_session.scalar(select(func.count()).select_from(Project)) == 0
    assert await db_session.scalar(select(func.count()).select_from(ProjectDeployment)) == 0
    assert await db_session.scalar(select(func.count()).select_from(ProjectLifecycleJob)) == 0
    assert await db_session.scalar(select(func.count()).select_from(AuditEvent)) == 0


@pytest.mark.asyncio
async def test_create_project_same_key_replays_original_response_without_reallocating_credentials(
    admin_client,
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(settings, "credentials_encryption_key", "task-four-test-key-material-32-bytes")
    generated_count = 0

    def fake_token_urlsafe(_size: int) -> str:
        nonlocal generated_count
        generated_count += 1
        return f"task-four-replay-credential-{generated_count}"

    monkeypatch.setattr(secrets, "token_urlsafe", fake_token_urlsafe)
    key = str(uuid4())
    request = {"name": "Replay Project", "slug": "replay-project", "description": "same"}

    first = await admin_client.post(
        "/api/v1/projects", headers=_idempotency_headers(key), json=request
    )
    replay = await admin_client.post(
        "/api/v1/projects", headers=_idempotency_headers(key), json=request
    )

    assert first.status_code == replay.status_code == 202
    assert replay.json() == first.json()
    assert generated_count == 5
    assert await db_session.scalar(select(func.count()).select_from(Project)) == 1
    assert await db_session.scalar(select(func.count()).select_from(ProjectDeployment)) == 1
    assert await db_session.scalar(select(func.count()).select_from(ProjectLifecycleJob)) == 1
    assert await db_session.scalar(select(func.count()).select_from(AuditEvent)) == 1


@pytest.mark.asyncio
async def test_create_project_same_key_with_different_body_returns_conflict(
    admin_client,
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(settings, "credentials_encryption_key", "task-four-test-key-material-32-bytes")
    key = str(uuid4())

    first = await admin_client.post(
        "/api/v1/projects",
        headers=_idempotency_headers(key),
        json={"name": "Fingerprint Project", "slug": "fingerprint-project", "description": "first"},
    )
    mismatch = await admin_client.post(
        "/api/v1/projects",
        headers=_idempotency_headers(key),
        json={"name": "Fingerprint Project", "slug": "fingerprint-project", "description": "changed"},
    )

    assert first.status_code == 202
    assert mismatch.status_code == 409
    assert "idempotency" in mismatch.json()["detail"].lower()
    assert await db_session.scalar(select(func.count()).select_from(Project)) == 1
    assert await db_session.scalar(select(func.count()).select_from(ProjectLifecycleJob)) == 1


@pytest.mark.asyncio
async def test_create_project_duplicate_slug_uses_dedicated_public_conflict(
    admin_client,
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(settings, "credentials_encryption_key", "task-four-test-key-material-32-bytes")

    first = await admin_client.post(
        "/api/v1/projects",
        headers=_idempotency_headers(),
        json={"name": "Original", "slug": "duplicate-slug"},
    )
    duplicate = await admin_client.post(
        "/api/v1/projects",
        headers=_idempotency_headers(),
        json={"name": "Different Request", "slug": "duplicate-slug"},
    )

    assert first.status_code == 202, first.text
    assert duplicate.status_code == 409
    assert duplicate.json() == {"detail": "Project slug already exists"}
    assert first.json()["project"]["id"] not in duplicate.text
    assert first.json()["operation"]["id"] not in duplicate.text
    assert await db_session.scalar(select(func.count()).select_from(Project)) == 1
    assert await db_session.scalar(select(func.count()).select_from(ProjectLifecycleJob)) == 1
    assert await db_session.scalar(select(func.count()).select_from(AuditEvent)) == 1


@pytest.mark.asyncio
async def test_create_project_same_key_same_body_is_scoped_to_requesting_admin(
    admin_client,
    client,
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(settings, "credentials_encryption_key", "task-four-test-key-material-32-bytes")
    second_admin = User(
        email="second-admin@test.local",
        hashed_password=hash_password("second-admin-password"),
        display_name="Second Admin",
        is_active=True,
        is_admin=True,
    )
    db_session.add(second_admin)
    await db_session.commit()
    client.headers["Authorization"] = (
        f"Bearer {create_access_token(second_admin.id, second_admin.email, True)}"
    )
    key = str(uuid4())
    request = {"name": "Actor Project", "slug": "actor-project", "description": "same"}

    first = await admin_client.post(
        "/api/v1/projects", headers=_idempotency_headers(key), json=request
    )
    cross_actor = await client.post(
        "/api/v1/projects", headers=_idempotency_headers(key), json=request
    )

    assert first.status_code == 202, first.text
    assert cross_actor.status_code == 409
    assert cross_actor.json() == {
        "detail": "Idempotency key already used with a different request"
    }
    assert first.json()["project"]["id"] not in cross_actor.text
    assert first.json()["operation"]["id"] not in cross_actor.text
    assert await db_session.scalar(select(func.count()).select_from(Project)) == 1
    assert await db_session.scalar(select(func.count()).select_from(ProjectLifecycleJob)) == 1
    audits = (await db_session.execute(select(AuditEvent))).scalars().all()
    assert len(audits) == 1
    assert audits[0].actor_user_id == "test-admin-001"


@pytest.mark.asyncio
async def test_create_project_concurrent_same_key_returns_one_operation(
    admin_client,
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(settings, "credentials_encryption_key", "task-four-test-key-material-32-bytes")
    generated_count = 0

    def fake_token_urlsafe(_size: int) -> str:
        nonlocal generated_count
        generated_count += 1
        return f"task-four-concurrent-credential-{generated_count}"

    monkeypatch.setattr(secrets, "token_urlsafe", fake_token_urlsafe)
    key = str(uuid4())
    request = {"name": "Concurrent Project", "slug": "concurrent-project"}

    first, second = await asyncio.gather(
        admin_client.post(
            "/api/v1/projects", headers=_idempotency_headers(key), json=request
        ),
        admin_client.post(
            "/api/v1/projects", headers=_idempotency_headers(key), json=request
        ),
    )

    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert await db_session.scalar(select(func.count()).select_from(Project)) == 1
    assert await db_session.scalar(select(func.count()).select_from(ProjectDeployment)) == 1
    assert await db_session.scalar(select(func.count()).select_from(ProjectLifecycleJob)) == 1
    assert await db_session.scalar(select(func.count()).select_from(AuditEvent)) == 1
    assert generated_count == 5


@pytest.mark.asyncio
async def test_create_project_concurrent_different_body_returns_one_generic_conflict(
    admin_client,
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(settings, "credentials_encryption_key", "task-four-test-key-material-32-bytes")
    generated_count = 0

    def fake_token_urlsafe(_size: int) -> str:
        nonlocal generated_count
        generated_count += 1
        return f"task-four-mismatch-credential-{generated_count}"

    monkeypatch.setattr(secrets, "token_urlsafe", fake_token_urlsafe)
    key = str(uuid4())

    first, second = await asyncio.gather(
        admin_client.post(
            "/api/v1/projects",
            headers=_idempotency_headers(key),
            json={
                "name": "Concurrent Mismatch",
                "slug": "concurrent-mismatch",
                "description": "one",
            },
        ),
        admin_client.post(
            "/api/v1/projects",
            headers=_idempotency_headers(key),
            json={
                "name": "Concurrent Mismatch",
                "slug": "concurrent-mismatch",
                "description": "two",
            },
        ),
    )

    accepted = first if first.status_code == 202 else second
    conflict = second if first.status_code == 202 else first
    assert accepted.status_code == 202
    assert conflict.status_code == 409
    assert conflict.json() == {
        "detail": "Idempotency key already used with a different request"
    }
    assert accepted.json()["project"]["id"] not in conflict.text
    assert accepted.json()["operation"]["id"] not in conflict.text
    assert generated_count == 5
    assert await db_session.scalar(select(func.count()).select_from(Project)) == 1
    assert await db_session.scalar(select(func.count()).select_from(AuditEvent)) == 1


@pytest.mark.asyncio
async def test_late_unrelated_integrity_failure_rolls_back_and_returns_redacted_500(
    _engine,
    db_session,
    monkeypatch,
    caplog,
):
    monkeypatch.setattr(settings, "credentials_encryption_key", "task-four-test-key-material-32-bytes")
    leaked_secret = "task-four-late-failure-secret"

    async def fail_commit(_session):
        raise IntegrityError(
            "late non-slug constraint",
            {},
            RuntimeError(leaked_secret),
        )

    monkeypatch.setattr(AsyncSession, "commit", fail_commit)
    factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)

    async def override_session():
        async with factory() as session:
            try:
                yield session
            finally:
                await session.rollback()

    app = create_app()
    app.dependency_overrides[get_db_session] = override_session
    token = create_access_token("test-admin-001", "admin@test.local", True)
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as non_raising_client:
        response = await non_raising_client.post(
            "/api/v1/projects",
            headers={
                "Authorization": f"Bearer {token}",
                **_idempotency_headers(),
            },
            json={"name": "Late Failure", "slug": "late-failure"},
        )

    assert response.status_code == 500
    assert response.text == "Internal Server Error"
    assert leaked_secret not in response.text
    assert leaked_secret not in caplog.text
    assert await db_session.scalar(select(func.count()).select_from(Project)) == 0
    assert await db_session.scalar(select(func.count()).select_from(ProjectDeployment)) == 0
    assert await db_session.scalar(select(func.count()).select_from(ProjectLifecycleJob)) == 0
    assert await db_session.scalar(select(func.count()).select_from(AuditEvent)) == 0


async def _failed_provision(admin_client, db_session, monkeypatch):
    monkeypatch.setattr(settings, "credentials_encryption_key", "task-four-test-key-material-32-bytes")
    created = await admin_client.post(
        "/api/v1/projects",
        headers=_idempotency_headers(),
        json={"name": "Retry Project", "slug": "retry-project"},
    )
    assert created.status_code == 202, created.text
    payload = created.json()
    project = await db_session.get(Project, payload["project"]["id"])
    operation = await db_session.get(ProjectLifecycleJob, payload["operation"]["id"])
    assert project is not None
    assert operation is not None
    deployment = (
        await db_session.execute(
            select(ProjectDeployment).where(ProjectDeployment.project_id == project.id)
        )
    ).scalar_one()
    project.lifecycle_status = ProjectLifecycleStatus.PROVISION_FAILED
    operation.status = LifecycleJobStatus.FAILED
    operation.attempt = operation.max_attempts
    operation.current_step = "initialize_airflow"
    operation.error_code = "AIRFLOW_INIT_FAILED"
    operation.error_message = "password=retry-operation-secret"
    operation.finished_at = datetime.now(UTC)
    await db_session.commit()
    return project, operation, deployment


@pytest.mark.asyncio
async def test_operation_status_is_scoped_and_redacts_persisted_error(
    admin_client,
    db_session,
    monkeypatch,
):
    project, operation, _deployment = await _failed_provision(admin_client, db_session, monkeypatch)

    response = await admin_client.get(
        f"/api/v1/projects/{project.slug}/operations/{operation.id}"
    )

    assert response.status_code == 200, response.text
    assert response.json() == {
        "id": operation.id,
        "operation": "provision",
        "status": "failed",
        "project_status": "provision_failed",
        "current_step": "initialize_airflow",
        "attempt": operation.max_attempts,
        "max_attempts": operation.max_attempts,
        "error_code": "AIRFLOW_INIT_FAILED",
        "error_message": "password=[REDACTED]",
    }
    assert "retry-operation-secret" not in response.text
    missing = await admin_client.get(
        f"/api/v1/projects/{project.slug}/operations/{uuid4().hex}"
    )
    assert missing.status_code == 404
    assert missing.json() == {"detail": "Operation not found"}


@pytest.mark.asyncio
async def test_operation_status_and_retry_require_super_admin(
    admin_client,
    client,
    db_session,
    monkeypatch,
):
    project, operation, _deployment = await _failed_provision(admin_client, db_session, monkeypatch)
    member = User(
        email="operation-member@test.local",
        hashed_password=hash_password("member-password"),
        display_name="Operation Member",
        is_active=True,
        is_admin=False,
    )
    db_session.add(member)
    await db_session.commit()
    client.headers["Authorization"] = f"Bearer {create_access_token(member.id, member.email, False)}"
    endpoint = f"/api/v1/projects/{project.slug}/operations/{operation.id}"

    status_response = await client.get(endpoint)
    retry_response = await client.post(f"{endpoint}/retry", headers=_idempotency_headers())

    assert status_response.status_code == 403
    assert retry_response.status_code == 403
    assert await db_session.scalar(select(func.count()).select_from(ProjectLifecycleJob)) == 1


@pytest.mark.asyncio
async def test_failed_provision_retry_is_idempotent_and_preserves_deployment_identity(
    admin_client,
    db_session,
    monkeypatch,
):
    project, failed, deployment = await _failed_provision(admin_client, db_session, monkeypatch)
    preserved_identity = (
        deployment.id,
        deployment.generation,
        deployment.compose_project_name,
        deployment.airflow_db_name,
        deployment.airflow_db_role,
        deployment.airflow_db_password_encrypted,
    )
    retry_key = str(uuid4())
    endpoint = f"/api/v1/projects/{project.slug}/operations/{failed.id}/retry"

    first = await admin_client.post(endpoint, headers=_idempotency_headers(retry_key))
    replay = await admin_client.post(endpoint, headers=_idempotency_headers(retry_key))

    assert first.status_code == replay.status_code == 202
    assert first.json() == replay.json()
    retry = await db_session.get(ProjectLifecycleJob, first.json()["id"])
    assert retry is not None
    assert retry.id != failed.id
    assert retry.operation is LifecycleOperation.PROVISION
    assert retry.status is LifecycleJobStatus.PENDING
    assert retry.attempt == 0
    assert retry.max_attempts == failed.max_attempts
    assert retry.current_step is None
    assert retry.idempotency_key == retry_key
    await db_session.refresh(project)
    await db_session.refresh(deployment)
    assert project.lifecycle_status is ProjectLifecycleStatus.PROVISIONING
    assert (
        deployment.id,
        deployment.generation,
        deployment.compose_project_name,
        deployment.airflow_db_name,
        deployment.airflow_db_role,
        deployment.airflow_db_password_encrypted,
    ) == preserved_identity
    assert await db_session.scalar(select(func.count()).select_from(ProjectLifecycleJob)) == 2
    audit = (
        await db_session.execute(
            select(AuditEvent).where(AuditEvent.event_type == "project.provision.retry_requested")
        )
    ).scalar_one()
    assert audit.metadata_json == {
        "operation_id": retry.id,
        "retry_of_operation_id": failed.id,
    }

    conflict = await admin_client.post(endpoint, headers=_idempotency_headers())
    assert conflict.status_code == 409
    assert conflict.json() == {"detail": "Only a failed project provision can be retried"}


@pytest.mark.asyncio
async def test_concurrent_failed_provision_retry_with_same_key_creates_one_operation(
    admin_client,
    db_session,
    monkeypatch,
):
    project, failed, _deployment = await _failed_provision(admin_client, db_session, monkeypatch)
    endpoint = f"/api/v1/projects/{project.slug}/operations/{failed.id}/retry"
    retry_key = str(uuid4())

    first, second = await asyncio.gather(
        admin_client.post(endpoint, headers=_idempotency_headers(retry_key)),
        admin_client.post(endpoint, headers=_idempotency_headers(retry_key)),
    )

    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert await db_session.scalar(select(func.count()).select_from(ProjectLifecycleJob)) == 2

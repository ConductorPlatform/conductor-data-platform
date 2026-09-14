"""Transactional project lifecycle operation creation."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import secrets
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.audit_event import AuditEvent
from app.models.environment import Environment
from app.models.project import Project, ProjectLifecycleStatus
from app.models.project_deployment import ProjectDeployment, ProvisionerKind
from app.models.project_lifecycle_job import (
    LifecycleJobStatus,
    LifecycleOperation,
    ProjectLifecycleJob,
)
from app.models.project_member import ProjectMember
from app.models.role import Role
from app.models.user import User
from app.services.crypto import encrypt_token
from app.services.project_lifecycle import derive_runtime_identity, validate_runtime_parameters

_DEFAULT_TEMPLATE_VERSION = "v1"
_DEFAULT_MAX_ATTEMPTS = 5


class IdempotencyKeyConflictError(RuntimeError):
    """The same idempotency key was used for a different effective request."""


class DuplicateProjectSlugError(RuntimeError):
    """The requested project slug is already persisted."""


def project_create_fingerprint(
    *,
    name: str,
    slug: str,
    description: str | None,
    requested_by_id: str,
) -> str:
    """Return a stable fingerprint for the effective create request and actor."""

    canonical_request = json.dumps(
        {
            "description": description,
            "name": name,
            "requested_by_id": requested_by_id,
            "slug": slug,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical_request.encode()).hexdigest()


async def create_project_operation(
    db: AsyncSession,
    *,
    name: str,
    slug: str,
    description: str | None,
    requested_by: User,
    idempotency_key: str,
) -> tuple[Project, ProjectLifecycleJob]:
    """Atomically persist a provisioning project and its durable operation."""

    fingerprint = project_create_fingerprint(
        name=name,
        slug=slug,
        description=description,
        requested_by_id=requested_by.id,
    )
    correlation_id = uuid4().hex
    project = Project(
        name=name,
        slug=slug,
        description=description,
        lifecycle_status=ProjectLifecycleStatus.PROVISIONING,
    )

    try:
        db.add(project)
        await db.flush()

        role = (
            await db.execute(select(Role).where(Role.name == "project_admin"))
        ).scalar_one_or_none()
        if role is None:
            raise RuntimeError("Required project_admin role is not configured")

        db.add_all(
            [
                Environment(
                    project_id=project.id,
                    name="production",
                    branch_name="main",
                    is_protected=True,
                    is_active=True,
                ),
                Environment(
                    project_id=project.id,
                    name="development",
                    branch_name="develop",
                    is_protected=False,
                    is_active=True,
                ),
                ProjectMember(
                    project_id=project.id,
                    user_id=requested_by.id,
                    role_id=role.id,
                ),
            ]
        )

        identity = derive_runtime_identity(
            project.id,
            project.slug,
            settings.airflow_external_domain,
        )
        plaintext_credentials = [secrets.token_urlsafe(32) for _ in range(6)]
        encrypted_credentials = [encrypt_token(value) for value in plaintext_credentials]
        deployment = ProjectDeployment(
            project_id=project.id,
            provisioner_kind=ProvisionerKind.DOCKER_COMPOSE,
            template_version=_DEFAULT_TEMPLATE_VERSION,
            generation=1,
            compose_project_name=identity.compose_project_name,
            airflow_external_url=identity.airflow_external_url,
            airflow_db_name=identity.airflow_db_name,
            airflow_db_role=identity.airflow_db_role,
            airflow_db_password_encrypted=encrypted_credentials[0],
            airflow_admin_user="admin",
            airflow_admin_password_encrypted=encrypted_credentials[1],
            airflow_dev_user="dev",
            airflow_dev_password_encrypted=encrypted_credentials[2],
            airflow_viewer_user="viewer",
            airflow_viewer_password_encrypted=encrypted_credentials[3],
            airflow_integration_user="integration",
            airflow_integration_password_encrypted=encrypted_credentials[4],
            warehouse_db_name=f"conductor_warehouse_{project.id}",
            warehouse_db_role=f"conductor_warehouse_{project.id}",
            warehouse_db_password_encrypted=encrypted_credentials[5],
            warehouse_schema="analytics",
            parameters=validate_runtime_parameters({}),
        )
        db.add(deployment)

        operation = ProjectLifecycleJob(
            project_id=project.id,
            operation=LifecycleOperation.PROVISION,
            status=LifecycleJobStatus.PENDING,
            attempt=0,
            max_attempts=_DEFAULT_MAX_ATTEMPTS,
            available_at=datetime.now(timezone.utc),
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            requested_by=requested_by.id,
            correlation_id=correlation_id,
        )
        db.add(operation)
        await db.flush()

        db.add(
            AuditEvent(
                event_type="project.provision.requested",
                actor_user_id=requested_by.id,
                project_id_snapshot=project.id,
                project_name_snapshot=project.name,
                project_slug_snapshot=project.slug,
                correlation_id=correlation_id,
                outcome="requested",
                metadata_json={"operation_id": operation.id},
            )
        )
        await db.commit()
        return project, operation
    except IntegrityError as conflict:
        await db.rollback()
        existing_operation = (
            await db.execute(
                select(ProjectLifecycleJob).where(
                    ProjectLifecycleJob.idempotency_key == idempotency_key
                )
            )
        ).scalar_one_or_none()
        if existing_operation is None:
            existing_project = (
                await db.execute(select(Project).where(Project.slug == slug))
            ).scalar_one_or_none()
            if existing_project is not None:
                raise DuplicateProjectSlugError(
                    "Project slug already exists"
                ) from conflict
            raise
        if existing_operation.request_fingerprint != fingerprint:
            raise IdempotencyKeyConflictError(
                "Idempotency key already used with a different request"
            ) from conflict
        existing_project = await db.get(Project, existing_operation.project_id)
        if existing_project is None:
            raise RuntimeError("Idempotent project operation has no project") from conflict
        return existing_project, existing_operation
    except Exception:
        await db.rollback()
        raise

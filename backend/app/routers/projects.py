from __future__ import annotations

from copy import copy
import hashlib
import json
import re
from datetime import UTC, datetime
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth.deps import get_current_user
from app.auth.permissions import require_super_admin
from app.database import get_db_session
from app.models.audit_event import AuditEvent
from app.models.environment import Environment
from app.models.git_config import GitConfig
from app.models.project import Project, ProjectLifecycleStatus
from app.models.project_deployment import ProjectDeployment
from app.models.project_lifecycle_job import (
    LifecycleJobStatus,
    LifecycleOperation,
    ProjectLifecycleJob,
)
from app.models.project_member import ProjectMember
from app.models.role import Role
from app.models.user import User
from app.schemas.member import AddMemberRequest, ChangeRoleRequest, MemberResponse
from app.schemas.project import (
    ProjectCreateRequest,
    ProjectCreateResponse,
    ProjectOperationResponse,
    ProjectOperationStatusResponse,
    ProjectResponse,
    ProjectUpdateRequest,
)
from app.schemas.settings import (
    EnvironmentCreateRequest,
    EnvironmentResponse,
    EnvironmentUpdateRequest,
    GitConfigResponse,
    GitConfigUpdateRequest,
    ProjectSettingsResponse,
    ProjectSettingsUpdateRequest,
)
from app.services.crypto import CredentialsEncryptionNotConfigured, encrypt_token
from app.services.git_dag_bundle import GitDagBundleSyncError, sync_git_dag_connection
from app.services.project_access import load_ready_project_for_user
from app.services.project_airflow_context import ProjectAirflowContext
from app.services.project_operations import (
    DuplicateProjectSlugError,
    IdempotencyKeyConflictError,
    create_project_operation,
)
from app.services.secret_redaction import redact_secret_text

router = APIRouter()

_PROVISION_RETRY_MAX_ATTEMPTS = 5


def _slugify(name: str) -> str:
    """Convert a name to a URL-safe slug."""
    slug = name.lower().strip().replace(" ", "-").replace("_", "-")
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


@router.get("/projects", response_model=list[ProjectResponse])
async def list_projects(
    search: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    stmt = (
        select(Project)
        .join(ProjectMember, ProjectMember.project_id == Project.id)
        .where(
            ProjectMember.user_id == user.id,
            Project.lifecycle_status == ProjectLifecycleStatus.READY,
        )
        .order_by(Project.created_at.desc())
    )

    if search:
        stmt = stmt.where(Project.name.ilike(f"%{search}%"))

    result = await db.execute(stmt)
    projects = result.scalars().all()

    # Batch-resolve roles for current user
    member_map: dict[str, str] = {}
    if projects and not user.is_admin:
        members_result = await db.execute(
            select(ProjectMember)
            .where(
                ProjectMember.user_id == user.id,
                ProjectMember.project_id.in_([p.id for p in projects]),
            )
            .options(selectinload(ProjectMember.role))
        )
        for m in members_result.scalars():
            member_map[m.project_id] = m.role.name if m.role else "member"

    response = []
    for p in projects:
        count_result = await db.execute(
            select(func.count()).select_from(ProjectMember).where(
                ProjectMember.project_id == p.id
            )
        )
        member_count = count_result.scalar() or 0
        role = member_map.get(p.id) if not user.is_admin else "super_admin"
        response.append(
            ProjectResponse(
                id=p.id,
                name=p.name,
                slug=p.slug,
                description=p.description,
                self_approve_enabled=p.self_approve_enabled,
                lifecycle_status=p.lifecycle_status,
                created_at=p.created_at,
                updated_at=p.updated_at,
                member_count=member_count,
                role=role,
            )
        )
    return response


@router.post(
    "/projects",
    response_model=ProjectCreateResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_project(
    body: ProjectCreateRequest,
    idempotency_key: UUID = Header(alias="Idempotency-Key"),
    user: User = Depends(require_super_admin),
    db: AsyncSession = Depends(get_db_session),
):
    slug = body.slug or _slugify(body.name)
    try:
        project, operation = await create_project_operation(
            db,
            name=body.name,
            slug=slug,
            description=body.description,
            requested_by=user,
            idempotency_key=str(idempotency_key),
        )
    except CredentialsEncryptionNotConfigured as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=redact_secret_text(str(exc)),
        ) from exc
    except IdempotencyKeyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key already used with a different request",
        ) from exc
    except DuplicateProjectSlugError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Project slug already exists",
        ) from exc

    return ProjectCreateResponse(
        project=ProjectResponse(
            id=project.id,
            name=project.name,
            slug=project.slug,
            description=project.description,
            self_approve_enabled=project.self_approve_enabled,
            lifecycle_status=project.lifecycle_status,
            created_at=project.created_at,
            updated_at=project.updated_at,
            member_count=1,
            role="super_admin",
        ),
        operation=ProjectOperationResponse(
            id=operation.id,
            operation=operation.operation,
            status=operation.status,
        ),
    )


def _operation_response(operation: ProjectLifecycleJob) -> ProjectOperationResponse:
    return ProjectOperationResponse(
        id=operation.id,
        operation=operation.operation,
        status=operation.status,
    )


def _operation_status_response(
    project: Project,
    operation: ProjectLifecycleJob,
) -> ProjectOperationStatusResponse:
    return ProjectOperationStatusResponse(
        **_operation_response(operation).model_dump(),
        project_status=project.lifecycle_status,
        current_step=operation.current_step,
        attempt=operation.attempt,
        max_attempts=operation.max_attempts,
        error_code=operation.error_code,
        error_message=(
            redact_secret_text(operation.error_message) if operation.error_message is not None else None
        ),
    )


def _provision_retry_fingerprint(*, actor_id: str, project_id: str, failed_operation_id: str) -> str:
    canonical_request = json.dumps(
        {
            "actor_id": actor_id,
            "failed_operation_id": failed_operation_id,
            "operation": "provision_retry",
            "project_id": project_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical_request.encode()).hexdigest()


async def _load_project_operation(
    db: AsyncSession,
    *,
    slug: str,
    operation_id: str,
    lock_project: bool = False,
    lock_operation: bool = False,
) -> tuple[Project, ProjectLifecycleJob]:
    project_query = select(Project).where(Project.slug == slug)
    if lock_project:
        project_query = project_query.with_for_update()
    project = (await db.execute(project_query)).scalar_one_or_none()
    if project is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    operation_query = select(ProjectLifecycleJob).where(
        ProjectLifecycleJob.id == operation_id,
        ProjectLifecycleJob.project_id == project.id,
    )
    if lock_operation:
        operation_query = operation_query.with_for_update()
    operation = (await db.execute(operation_query)).scalar_one_or_none()
    if operation is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Operation not found")
    return project, operation


@router.get(
    "/projects/{slug}/operations/{operation_id}",
    response_model=ProjectOperationStatusResponse,
)
async def get_project_operation(
    slug: str,
    operation_id: str,
    _user: User = Depends(require_super_admin),
    db: AsyncSession = Depends(get_db_session),
):
    """Return sanitized lifecycle progress only to the provision authority."""

    project, operation = await _load_project_operation(
        db,
        slug=slug,
        operation_id=operation_id,
    )
    return _operation_status_response(project, operation)


@router.post(
    "/projects/{slug}/operations/{operation_id}/retry",
    response_model=ProjectOperationResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def retry_project_provision(
    slug: str,
    operation_id: str,
    idempotency_key: UUID = Header(alias="Idempotency-Key"),
    user: User = Depends(require_super_admin),
    db: AsyncSession = Depends(get_db_session),
):
    """Atomically requeue one failed provision without reallocating its runtime identity."""

    key = str(idempotency_key)
    project: Project | None = None
    fingerprint: str | None = None
    try:
        project, failed_operation = await _load_project_operation(
            db,
            slug=slug,
            operation_id=operation_id,
            lock_project=True,
            lock_operation=True,
        )
        fingerprint = _provision_retry_fingerprint(
            actor_id=user.id,
            project_id=project.id,
            failed_operation_id=failed_operation.id,
        )
        replay = (
            await db.execute(
                select(ProjectLifecycleJob).where(ProjectLifecycleJob.idempotency_key == key)
            )
        ).scalar_one_or_none()
        if replay is not None:
            if (
                replay.project_id == project.id
                and replay.operation is LifecycleOperation.PROVISION
                and replay.requested_by == user.id
                and replay.request_fingerprint == fingerprint
            ):
                response = _operation_response(replay)
                await db.rollback()
                return response
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key already used with a different request",
            )

        if (
            failed_operation.operation is not LifecycleOperation.PROVISION
            or failed_operation.status is not LifecycleJobStatus.FAILED
            or project.lifecycle_status is not ProjectLifecycleStatus.PROVISION_FAILED
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Only a failed project provision can be retried",
            )

        now = datetime.now(UTC)
        retry = ProjectLifecycleJob(
            project_id=project.id,
            operation=LifecycleOperation.PROVISION,
            status=LifecycleJobStatus.PENDING,
            attempt=0,
            max_attempts=failed_operation.max_attempts or _PROVISION_RETRY_MAX_ATTEMPTS,
            available_at=now,
            idempotency_key=key,
            request_fingerprint=fingerprint,
            requested_by=user.id,
            correlation_id=uuid4().hex,
        )
        project.lifecycle_status = ProjectLifecycleStatus.PROVISIONING
        db.add(retry)
        await db.flush()
        db.add(
            AuditEvent(
                event_type="project.provision.retry_requested",
                actor_user_id=user.id,
                project_id_snapshot=project.id,
                project_name_snapshot=project.name,
                project_slug_snapshot=project.slug,
                correlation_id=retry.correlation_id,
                outcome="requested",
                metadata_json={
                    "operation_id": retry.id,
                    "retry_of_operation_id": failed_operation.id,
                },
            )
        )
        await db.commit()
        return _operation_response(retry)
    except HTTPException:
        await db.rollback()
        raise
    except IntegrityError:
        await db.rollback()
        replay = (
            await db.execute(
                select(ProjectLifecycleJob).where(ProjectLifecycleJob.idempotency_key == key)
            )
        ).scalar_one_or_none()
        if (
            replay is not None
            and project is not None
            and replay.project_id == project.id
            and replay.operation is LifecycleOperation.PROVISION
            and replay.requested_by == user.id
            and replay.request_fingerprint == fingerprint
        ):
            return _operation_response(replay)
        if replay is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key already used with a different request",
            ) from None
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Project provision retry conflicts with another request",
        ) from None


@router.get("/projects/{slug}", response_model=ProjectResponse)
async def get_project(
    slug: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)

    # Check access
    await _ensure_access(user, project.id, db)

    count_result = await db.execute(
        select(func.count()).select_from(ProjectMember).where(
            ProjectMember.project_id == project.id
        )
    )

    role = "super_admin" if user.is_admin else None
    if not user.is_admin:
        m_result = await db.execute(
            select(ProjectMember).where(
                ProjectMember.project_id == project.id, ProjectMember.user_id == user.id,
            ).options(selectinload(ProjectMember.role))
        )
        m = m_result.scalar_one_or_none()
        if m:
            role = m.role.name

    return ProjectResponse(
        id=project.id,
        name=project.name,
        slug=project.slug,
        description=project.description,
        self_approve_enabled=project.self_approve_enabled,
        lifecycle_status=project.lifecycle_status,
        created_at=project.created_at,
        updated_at=project.updated_at,
        member_count=count_result.scalar() or 0,
        role=role,
    )


@router.patch("/projects/{slug}", response_model=ProjectResponse)
async def update_project(
    slug: str,
    body: ProjectUpdateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)

    await _ensure_admin_access(user, project.id, db)

    update_data = body.model_dump(exclude_unset=True)
    if update_data:
        await db.execute(
            update(Project).where(Project.id == project.id).values(**update_data)
        )
        await db.commit()
        await db.refresh(project)

    count_result = await db.execute(
        select(func.count()).select_from(ProjectMember).where(
            ProjectMember.project_id == project.id
        )
    )

    return ProjectResponse(
        id=project.id,
        name=project.name,
        slug=project.slug,
        description=project.description,
        self_approve_enabled=project.self_approve_enabled,
        lifecycle_status=project.lifecycle_status,
        created_at=project.created_at,
        updated_at=project.updated_at,
        member_count=count_result.scalar() or 0,
        role="super_admin" if user.is_admin else "project_admin",
    )


# ─── Helpers ───


async def _ensure_access(user: User, project_id: str, db: AsyncSession):
    """Check user is a member of the project, or is Super Admin."""
    if user.is_admin:
        return
    result = await db.execute(
        select(ProjectMember).where(
            ProjectMember.project_id == project_id,
            ProjectMember.user_id == user.id,
        )
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Access denied")


async def _ensure_admin_access(user: User, project_id: str, db: AsyncSession):
    """Check user has admin role in the project, or is Super Admin."""
    if user.is_admin:
        return
    result = await db.execute(
        select(ProjectMember)
        .where(
            ProjectMember.project_id == project_id,
            ProjectMember.user_id == user.id,
        )
        .options(selectinload(ProjectMember.role))
    )
    member = result.scalar_one_or_none()
    if not member or member.role.name not in ("project_admin",):
        raise HTTPException(status_code=403, detail="Project admin access required")


# ─── Members ───


@router.get("/projects/{slug}/members", response_model=list[MemberResponse])
async def list_members(
    slug: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)

    await _ensure_access(user, project.id, db)

    stmt = (
        select(ProjectMember)
        .where(ProjectMember.project_id == project.id)
        .options(selectinload(ProjectMember.user), selectinload(ProjectMember.role))
        .order_by(ProjectMember.created_at)
    )
    result = await db.execute(stmt)
    members = result.scalars().all()

    return [
        MemberResponse(
            user_id=m.user.id,
            email=m.user.email,
            display_name=m.user.display_name,
            role_name=m.role.name,
            role_id=m.role.id,
            joined_at=m.created_at,
        )
        for m in members
    ]


@router.post("/projects/{slug}/members", response_model=MemberResponse, status_code=201)
async def add_member(
    slug: str,
    body: AddMemberRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)

    await _ensure_admin_access(user, project.id, db)

    # Find user by email
    user_result = await db.execute(
        select(User).where(User.email == body.email)
    )
    target_user = user_result.scalar_one_or_none()
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found by email")

    # Check not already a member
    existing = await db.execute(
        select(ProjectMember).where(
            ProjectMember.project_id == project.id,
            ProjectMember.user_id == target_user.id,
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="User is already a member")

    # Find role
    role_result = await db.execute(
        select(Role).where(Role.name == body.role_name)
    )
    role = role_result.scalar_one_or_none()
    if not role:
        raise HTTPException(status_code=400, detail=f"Role '{body.role_name}' not found")

    member = ProjectMember(
        project_id=project.id,
        user_id=target_user.id,
        role_id=role.id,
    )
    db.add(member)
    await db.commit()
    await db.refresh(member)

    return MemberResponse(
        user_id=target_user.id,
        email=target_user.email,
        display_name=target_user.display_name,
        role_name=role.name,
        role_id=role.id,
        joined_at=member.created_at,
    )


@router.patch("/projects/{slug}/members/{user_id}", response_model=MemberResponse)
async def change_member_role(
    slug: str,
    user_id: str,
    body: ChangeRoleRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)

    await _ensure_admin_access(user, project.id, db)

    # Find role
    role_result = await db.execute(
        select(Role).where(Role.name == body.role_name)
    )
    role = role_result.scalar_one_or_none()
    if not role:
        raise HTTPException(status_code=400, detail=f"Role '{body.role_name}' not found")

    # Find member
    result = await db.execute(
        select(ProjectMember).where(
            ProjectMember.project_id == project.id,
            ProjectMember.user_id == user_id,
        )
    )
    member = result.scalar_one_or_none()
    if not member:
        raise HTTPException(status_code=404, detail="Member not found")

    member.role_id = role.id
    await db.commit()
    await db.refresh(member)

    # Fetch user+role for response
    u_result = await db.execute(select(User).where(User.id == user_id))
    target_user = u_result.scalar_one()

    return MemberResponse(
        user_id=target_user.id,
        email=target_user.email,
        display_name=target_user.display_name,
        role_name=role.name,
        role_id=role.id,
        joined_at=member.created_at,
    )


@router.delete("/projects/{slug}/members/{user_id}", status_code=204)
async def remove_member(
    slug: str,
    user_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)

    await _ensure_admin_access(user, project.id, db)

    result = await db.execute(
        select(ProjectMember).where(
            ProjectMember.project_id == project.id,
            ProjectMember.user_id == user_id,
        )
    )
    member = result.scalar_one_or_none()
    if not member:
        raise HTTPException(status_code=404, detail="Member not found")

    # Prevent removing the last project_admin
    if member.role_id == "project_admin":
        admin_count = await db.execute(
            select(func.count()).select_from(ProjectMember).where(
                ProjectMember.project_id == project.id,
                ProjectMember.role_id == "project_admin",
            )
        )
        count_val = admin_count.scalar() or 0
        if count_val <= 1:
            raise HTTPException(
                status_code=400, detail="Cannot remove the last project admin"
            )

    await db.delete(member)
    await db.commit()


# ─── Settings ───


@router.get("/projects/{slug}/git", response_model=GitConfigResponse)
async def get_git_config(
    slug: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)
    await _ensure_access(user, project.id, db)

    git_result = await db.execute(
        select(GitConfig).where(GitConfig.project_id == project.id)
    )
    config = git_result.scalar_one_or_none()
    if not config:
        raise HTTPException(status_code=404, detail="Git config not found")

    return GitConfigResponse(
        repo_url=config.repo_url,
        auth_type=config.auth_type,
        default_branch=config.default_branch,
        dbt_path=config.dbt_path,
        dags_path=config.dags_path,
        has_credentials=bool(config.credentials_encrypted),
        has_token=config.auth_type == "token" and bool(config.credentials_encrypted),
        created_at=config.created_at,
        updated_at=config.updated_at,
    )


@router.put("/projects/{slug}/git", response_model=GitConfigResponse)
async def update_git_config(
    slug: str,
    body: GitConfigUpdateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)
    await _ensure_admin_access(user, project.id, db)

    git_result = await db.execute(
        select(GitConfig).where(GitConfig.project_id == project.id).with_for_update()
    )
    config = git_result.scalar_one_or_none()
    previous_config = copy(config) if config is not None else None
    if not config:
        config = GitConfig(project_id=project.id, repo_url="", auth_type="https")
        db.add(config)
        await db.flush()

    previous_auth_type = config.auth_type
    effective_auth_type = body.auth_type or previous_auth_type
    credential = body.token if body.token is not None else body.credentials
    if body.token is not None and effective_auth_type != "token":
        raise HTTPException(status_code=422, detail="Token requires token authentication")
    if body.credentials is not None and effective_auth_type not in ("token", "ssh"):
        raise HTTPException(
            status_code=422,
            detail="Credentials require token or SSH authentication",
        )
    if effective_auth_type == "token" and previous_auth_type != "token" and credential is None:
        raise HTTPException(
            status_code=422,
            detail="A token is required when enabling token authentication",
        )

    update_data = body.model_dump(
        exclude_unset=True,
        exclude={"token", "credentials", "webhook_secret"},
    )
    if update_data:
        for key, value in update_data.items():
            setattr(config, key, value)

    if config.auth_type == "token":
        parsed_repo_url = urlsplit(config.repo_url)
        if parsed_repo_url.scheme != "https" or not parsed_repo_url.netloc:
            raise HTTPException(
                status_code=422,
                detail="Token authentication requires an HTTPS repository URL",
            )

    # Handle encrypted fields
    try:
        if credential is not None:
            config.credentials_encrypted = encrypt_token(credential)
        elif body.auth_type is not None and body.auth_type != previous_auth_type:
            config.credentials_encrypted = None
        if body.webhook_secret is not None:
            config.webhook_secret_encrypted = encrypt_token(body.webhook_secret)
    except CredentialsEncryptionNotConfigured as exc:
        raise HTTPException(
            status_code=503,
            detail=redact_secret_text(str(exc)),
        ) from exc

    if config.auth_type == "token" and not config.credentials_encrypted:
        raise HTTPException(status_code=422, detail="Token authentication requires a token")

    # Legacy unauthenticated/SSH configurations remain persisted, but only the
    # token path is an MVP production bundle. Switching away from it revokes
    # the deterministic connection without attempting to support another mode.
    deployment: ProjectDeployment | None = None
    try:
        if config.auth_type == "token" or previous_auth_type == "token":
            deployment = (
                await db.execute(select(ProjectDeployment).where(ProjectDeployment.project_id == project.id))
            ).scalar_one_or_none()
            if deployment is None:
                raise HTTPException(status_code=404, detail="Airflow not provisioned")
            await sync_git_dag_connection(
                context=ProjectAirflowContext(
                    project_id=project.id,
                    deployment_id=deployment.id,
                    deployment_generation=deployment.generation,
                    airflow_base_url=f"http://airflow-{project.id}:8080",
                    account_key="admin",
                ),
                config=config,
                db=db,
                previous_config=previous_config,
            )
        await db.commit()
        await db.refresh(config)
    except GitDagBundleSyncError as exc:
        await db.rollback()
        raise HTTPException(status_code=502, detail="Airflow Git connection update failed") from exc
    except Exception as exc:
        # External state is applied before the DB transaction. Reconcile to the
        # locked snapshot after a failed commit, retaining no token if the
        # helper cannot prove a matched old metadata/token pair.
        await db.rollback()
        if previous_config is not None and deployment is not None:
            try:
                await sync_git_dag_connection(
                    context=ProjectAirflowContext(
                        project_id=project.id,
                        deployment_id=deployment.id,
                        deployment_generation=deployment.generation,
                        airflow_base_url=f"http://airflow-{project.id}:8080",
                        account_key="admin",
                    ),
                    config=previous_config,
                    previous_config=config,
                    db=db,
                )
            except GitDagBundleSyncError:
                pass
        raise HTTPException(status_code=502, detail="Airflow Git connection update failed") from exc

    return GitConfigResponse(
        repo_url=config.repo_url,
        auth_type=config.auth_type,
        default_branch=config.default_branch,
        dbt_path=config.dbt_path,
        dags_path=config.dags_path,
        has_credentials=bool(config.credentials_encrypted),
        has_token=config.auth_type == "token" and bool(config.credentials_encrypted),
        created_at=config.created_at,
        updated_at=config.updated_at,
    )


@router.get("/projects/{slug}/environments", response_model=list[EnvironmentResponse])
async def list_environments(
    slug: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)
    await _ensure_access(user, project.id, db)

    env_result = await db.execute(
        select(Environment)
        .where(Environment.project_id == project.id)
        .order_by(Environment.name)
    )
    return env_result.scalars().all()


@router.post("/projects/{slug}/environments", response_model=EnvironmentResponse, status_code=201)
async def create_environment(
    slug: str,
    body: EnvironmentCreateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)
    await _ensure_admin_access(user, project.id, db)

    env = Environment(
        project_id=project.id,
        name=body.name,
        branch_name=body.branch_name,
        is_protected=body.is_protected,
    )
    db.add(env)
    await db.commit()
    await db.refresh(env)
    return env


@router.patch("/projects/{slug}/environments/{env_id}", response_model=EnvironmentResponse)
async def update_environment(
    slug: str,
    env_id: str,
    body: EnvironmentUpdateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)
    await _ensure_admin_access(user, project.id, db)

    env_result = await db.execute(
        select(Environment).where(
            Environment.id == env_id, Environment.project_id == project.id
        )
    )
    env = env_result.scalar_one_or_none()
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")

    update_data = body.model_dump(exclude_unset=True)
    if update_data:
        for key, value in update_data.items():
            setattr(env, key, value)
        await db.commit()
        await db.refresh(env)
    return env


@router.delete("/projects/{slug}/environments/{env_id}", status_code=204)
async def delete_environment(
    slug: str,
    env_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)
    await _ensure_admin_access(user, project.id, db)

    env_result = await db.execute(
        select(Environment).where(
            Environment.id == env_id, Environment.project_id == project.id
        )
    )
    env = env_result.scalar_one_or_none()
    if not env:
        raise HTTPException(status_code=404, detail="Environment not found")
    await db.delete(env)
    await db.commit()


@router.get("/projects/{slug}/settings", response_model=ProjectSettingsResponse)
async def get_settings(
    slug: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)
    await _ensure_access(user, project.id, db)

    return ProjectSettingsResponse(
        self_approve_enabled=project.self_approve_enabled,
    )


@router.patch("/projects/{slug}/settings", response_model=ProjectSettingsResponse)
async def update_settings(
    slug: str,
    body: ProjectSettingsUpdateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    project = await load_ready_project_for_user(slug, user, db)
    await _ensure_admin_access(user, project.id, db)

    update_data = body.model_dump(exclude_unset=True)
    if update_data:
        for key, value in update_data.items():
            setattr(project, key, value)
        await db.commit()
        await db.refresh(project)

    return ProjectSettingsResponse(
        self_approve_enabled=project.self_approve_enabled,
    )

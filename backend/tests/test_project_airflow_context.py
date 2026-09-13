from __future__ import annotations

from dataclasses import fields

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from app.models.project import Project, ProjectLifecycleStatus
from app.models.project_deployment import ProjectDeployment, ProvisionerKind
from app.models.project_member import ProjectMember
from app.models.role import Permission, Role
from app.models.user import User
from app.services.project_airflow_context import resolve_project_airflow_context


async def _ready_project(
    db_session,
    *,
    slug: str,
    member: User | None,
    role_name: str = "project_admin",
    with_deployment: bool = True,
) -> Project:
    project = Project(
        name=slug.replace("-", " ").title(),
        slug=slug,
        lifecycle_status=ProjectLifecycleStatus.READY,
    )
    db_session.add(project)
    await db_session.flush()

    if member is not None:
        role = (await db_session.execute(select(Role).where(Role.name == role_name))).scalar_one()
        db_session.add(ProjectMember(project_id=project.id, user_id=member.id, role_id=role.id))

    if with_deployment:
        db_session.add(
            ProjectDeployment(
                project_id=project.id,
                provisioner_kind=ProvisionerKind.DOCKER_COMPOSE,
                template_version="v1",
                generation=4,
                compose_project_name=f"conductor-{slug}",
                airflow_external_url=f"https://{slug}.airflow.example.test",
                airflow_db_name=f"airflow_{slug.replace('-', '_')}",
                airflow_db_role=f"airflow_{slug.replace('-', '_')}_role",
                airflow_db_password_encrypted="encrypted-db-secret",
                airflow_admin_user="admin",
                airflow_admin_password_encrypted="encrypted-admin-secret",
                airflow_dev_user="dev",
                airflow_dev_password_encrypted="encrypted-dev-secret",
                airflow_viewer_user="viewer",
                airflow_viewer_password_encrypted="encrypted-viewer-secret",
                airflow_integration_user="integration",
                airflow_integration_password_encrypted="encrypted-integration-secret",
                parameters={},
            )
        )

    await db_session.commit()
    return project


@pytest.mark.asyncio
async def test_context_is_server_derived_and_credential_free_for_project_admin(db_session):
    admin = await db_session.get(User, "test-admin-001")
    project = await _ready_project(db_session, slug="creator-project", member=admin)

    context = await resolve_project_airflow_context(
        "creator-project", admin, db_session, "project.dag.view", "read"
    )

    assert context.project_id == project.id
    assert context.deployment_generation == 4
    assert context.airflow_base_url == f"http://airflow-{project.id}:8080"
    assert context.account_key == "admin"
    assert {field.name for field in fields(context)} == {
        "project_id",
        "deployment_id",
        "deployment_generation",
        "airflow_base_url",
        "account_key",
    }
    assert all(
        "password" not in field.name and "secret" not in field.name for field in fields(context)
    )


@pytest.mark.asyncio
async def test_context_rejects_user_outside_the_slugged_project(db_session):
    admin = await db_session.get(User, "test-admin-001")
    await _ready_project(db_session, slug="authorized-project", member=admin)
    await _ready_project(db_session, slug="other-project", member=None)

    with pytest.raises(HTTPException) as error:
        await resolve_project_airflow_context(
            "other-project", admin, db_session, "project.dag.view", "read"
        )

    assert error.value.status_code == 403
    assert error.value.detail == "Access denied"


@pytest.mark.asyncio
async def test_context_requires_the_exact_project_permission(db_session):
    developer = User(
        email="developer@test.local",
        hashed_password="unused",
        display_name="Developer",
        is_active=True,
        is_admin=False,
    )
    db_session.add(developer)
    await db_session.commit()
    await _ready_project(
        db_session,
        slug="developer-project",
        member=developer,
        role_name="developer",
    )

    with pytest.raises(HTTPException) as error:
        await resolve_project_airflow_context(
            "developer-project", developer, db_session, "project.dag.view", "read"
        )
    assert error.value.status_code == 403

    developer_role = (
        await db_session.execute(select(Role).where(Role.name == "developer"))
    ).scalar_one()
    db_session.add(
        Permission(role_id=developer_role.id, resource="project.dag.view", action="read")
    )
    await db_session.commit()

    context = await resolve_project_airflow_context(
        "developer-project", developer, db_session, "project.dag.view", "read"
    )
    assert context.account_key == "dev"

    with pytest.raises(HTTPException) as denied_run:
        await resolve_project_airflow_context(
            "developer-project", developer, db_session, "project.dag.run", "write"
        )
    assert denied_run.value.status_code == 403


@pytest.mark.asyncio
async def test_context_rejects_non_ready_projects_before_deployment_access(db_session):
    admin = await db_session.get(User, "test-admin-001")
    project = Project(
        name="Deleting Project",
        slug="deleting-project",
        lifecycle_status=ProjectLifecycleStatus.DELETING,
    )
    db_session.add(project)
    await db_session.flush()
    role = (await db_session.execute(select(Role).where(Role.name == "project_admin"))).scalar_one()
    db_session.add(ProjectMember(project_id=project.id, user_id=admin.id, role_id=role.id))
    await db_session.commit()

    with pytest.raises(HTTPException) as error:
        await resolve_project_airflow_context(
            "deleting-project", admin, db_session, "project.dag.view", "read"
        )

    assert error.value.status_code == 404
    assert error.value.detail == "Project not found"


@pytest.mark.asyncio
async def test_context_rejects_ready_project_without_a_deployment(db_session):
    admin = await db_session.get(User, "test-admin-001")
    await _ready_project(
        db_session,
        slug="missing-deployment",
        member=admin,
        with_deployment=False,
    )

    with pytest.raises(HTTPException) as error:
        await resolve_project_airflow_context(
            "missing-deployment", admin, db_session, "project.dag.view", "read"
        )

    assert error.value.status_code == 404
    assert error.value.detail == "Airflow not provisioned"

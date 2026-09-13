"""Authorization-first, credential-free project Airflow context resolution."""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth.permissions import check_permission
from app.models.project_deployment import ProjectDeployment
from app.models.project_member import ProjectMember
from app.models.user import User
from app.services.airflow_role import resolve_airflow_account
from app.services.project_access import load_ready_project_for_user


@dataclass(frozen=True)
class ProjectAirflowContext:
    """Safe server-derived identity for one authorized Airflow operation.

    This deliberately contains no credential material. The session manager uses
    the deployment identity to load and decrypt service credentials internally.
    """

    project_id: str
    deployment_id: str
    deployment_generation: int
    airflow_base_url: str
    account_key: str


async def resolve_project_airflow_context(
    slug: str,
    user: User,
    db: AsyncSession,
    resource: str,
    action: str,
) -> ProjectAirflowContext:
    """Resolve a READY member project and authorize one Airflow operation.

    The slug is resolved server-side; no client-supplied project identifier is
    accepted. Membership is required even for global administrators before the
    permission check and deployment lookup occur.
    """

    project = await load_ready_project_for_user(slug, user, db)
    member = (
        await db.execute(
            select(ProjectMember)
            .where(
                ProjectMember.project_id == project.id,
                ProjectMember.user_id == user.id,
            )
            .options(selectinload(ProjectMember.role))
        )
    ).scalar_one_or_none()
    if member is None:
        # Defensive guard: load_ready_project_for_user already requires this.
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    if not await check_permission(user, project.id, resource, action, db):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    deployment = (
        await db.execute(
            select(ProjectDeployment).where(ProjectDeployment.project_id == project.id)
        )
    ).scalar_one_or_none()
    if deployment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Airflow not provisioned")

    return ProjectAirflowContext(
        project_id=project.id,
        deployment_id=deployment.id,
        deployment_generation=deployment.generation,
        # Runtime APIs are reachable only on the controlled worker/backend
        # ingress network. The public URL is not a backend-to-runtime route.
        airflow_base_url=f"http://airflow-{project.id}:8080",
        account_key=resolve_airflow_account(member.role.name),
    )

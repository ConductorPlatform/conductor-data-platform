from __future__ import annotations

import re
from collections.abc import Callable

from fastapi import Depends, HTTPException, Path, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import get_current_user
from app.database import get_db_session
from app.models.project import Project
from app.models.project_member import ProjectMember
from app.models.role import Permission
from app.models.user import User


def _match_resource(pattern: str, resource: str) -> bool:
    """Match a resource string against a wildcard pattern.

    Patterns use `*` as wildcard:
      - `*` matches everything
      - `project.*` matches `project.dag.view`, `project.git.branch.write`, etc.
      - `project.dag.*` matches `project.dag.view`, `project.dag.run`
    """
    if pattern == "*":
        return True
    regex = "^" + re.escape(pattern).replace(r"\*", "[^.]+") + "$"
    return bool(re.match(regex, resource))


def _match_action(perm_action: str, required_action: str) -> bool:
    """Match action. `admin` action matches any required action."""
    if perm_action == "admin":
        return True
    return perm_action == required_action


async def check_permission(
    user: User,
    project_id: str,
    resource: str,
    action: str,
    db: AsyncSession,
) -> bool:
    """Check if user has a specific permission in a project.

    Super Admin bypasses all checks.
    """
    if user.is_admin:
        return True

    # Find user's role in the project
    stmt = select(ProjectMember).where(
        ProjectMember.project_id == project_id,
        ProjectMember.user_id == user.id,
    )
    result = await db.execute(stmt)
    member = result.scalar_one_or_none()

    if member is None:
        return False

    # Query permissions directly rather than relying on a relationship cached
    # earlier in the request; grants made in this session must apply immediately.
    permissions = await db.execute(select(Permission).where(Permission.role_id == member.role_id))
    for perm in permissions.scalars():
        if _match_resource(perm.resource, resource) and _match_action(perm.action, action):
            return True

    return False


async def resolve_project_id(
    slug: str = Path(),
    db: AsyncSession = Depends(get_db_session),
) -> str:
    """Resolve project slug to project ID. FastAPI dependency."""
    result = await db.execute(select(Project).where(Project.slug == slug))
    project = result.scalar_one_or_none()
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return project.id


def require_permission(resource: str, action: str) -> Callable:
    """Factory for permission-checking FastAPI dependency.

    Usage:
        @router.get("/projects/{slug}/dags")
        async def list_dags(
            _: None = Depends(require_permission("project.dag.view", "read")),
            ...
        ):
    """

    async def _check(
        project_id: str = Depends(resolve_project_id),
        user: User = Depends(get_current_user),
        db: AsyncSession = Depends(get_db_session),
    ) -> None:
        if not await check_permission(user, project_id, resource, action, db):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing permission: {resource}/{action}",
            )

    return _check


async def require_super_admin(
    user: User = Depends(get_current_user),
) -> User:
    """Simple dependency: Super Admin only. Returns the authenticated user."""
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Super Admin access required",
        )
    return user


async def require_project_member(
    project_id: str = Depends(resolve_project_id),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
) -> str:
    """Check user is a member of the project. Returns project_id."""
    result = await db.execute(
        select(ProjectMember).where(
            ProjectMember.project_id == project_id,
            ProjectMember.user_id == user.id,
        )
    )
    if result.scalar_one_or_none() is None and not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not a member of this project",
        )
    return project_id
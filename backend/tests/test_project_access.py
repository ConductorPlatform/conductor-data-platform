from __future__ import annotations

from dataclasses import dataclass

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from app.config import settings
from app.models.git_config import GitConfig
from app.models.merge_request import MergeRequest
from app.models.project import Project, ProjectLifecycleStatus
from app.models.project_member import ProjectMember
from app.models.role import Role
from app.models.user import User


@dataclass(frozen=True)
class RouteCase:
    method: str
    path: str
    json: dict | None = None


NON_READY_ROUTE_CASES = (
    RouteCase("GET", "/api/v1/projects/gated-project"),
    RouteCase("PATCH", "/api/v1/projects/gated-project", {"name": "Changed"}),
    RouteCase("DELETE", "/api/v1/projects/gated-project"),
    RouteCase("GET", "/api/v1/projects/gated-project/members"),
    RouteCase(
        "POST",
        "/api/v1/projects/gated-project/members",
        {"email": "nobody@test.local", "role_name": "developer"},
    ),
    RouteCase(
        "PATCH",
        "/api/v1/projects/gated-project/members/nobody",
        {"role_name": "viewer"},
    ),
    RouteCase("DELETE", "/api/v1/projects/gated-project/members/nobody"),
    RouteCase("GET", "/api/v1/projects/gated-project/git"),
    RouteCase(
        "PUT",
        "/api/v1/projects/gated-project/git",
        {"repo_url": "https://example.test/repo.git", "auth_type": "https"},
    ),
    RouteCase("GET", "/api/v1/projects/gated-project/environments"),
    RouteCase(
        "POST",
        "/api/v1/projects/gated-project/environments",
        {"name": "test", "branch_name": "test"},
    ),
    RouteCase(
        "PATCH",
        "/api/v1/projects/gated-project/environments/missing",
        {"is_active": False},
    ),
    RouteCase("DELETE", "/api/v1/projects/gated-project/environments/missing"),
    RouteCase("GET", "/api/v1/projects/gated-project/settings"),
    RouteCase(
        "PATCH",
        "/api/v1/projects/gated-project/settings",
        {"self_approve_enabled": True},
    ),
    RouteCase("GET", "/api/v1/projects/gated-project/git/branches"),
    RouteCase(
        "POST", "/api/v1/projects/gated-project/git/branches?name=feature&source=main"
    ),
    RouteCase("GET", "/api/v1/projects/gated-project/git/commits"),
    RouteCase("GET", "/api/v1/projects/gated-project/git/merge-requests"),
    RouteCase(
        "POST",
        "/api/v1/projects/gated-project/git/merge-requests",
        {"source_branch": "feature", "target_branch": "main", "title": "MR"},
    ),
    RouteCase(
        "POST", "/api/v1/projects/gated-project/git/merge-requests/missing/merge"
    ),
    RouteCase(
        "POST", "/api/v1/projects/gated-project/git/merge-requests/missing/close"
    ),
    RouteCase(
        "GET", "/api/v1/projects/gated-project/git/merge-requests/missing/checks"
    ),
    RouteCase("POST", "/api/v1/projects/gated-project/airflow/provision"),
    RouteCase("GET", "/api/v1/projects/gated-project/airflow/status"),
    RouteCase("POST", "/api/v1/projects/gated-project/airflow/restart"),
    RouteCase("DELETE", "/api/v1/projects/gated-project/airflow"),
    RouteCase("POST", "/api/v1/projects/gated-project/airflow-proxy/bootstrap"),
    RouteCase("GET", "/api/v1/projects/gated-project/airflow-proxy/api/v2/dags"),
    RouteCase("GET", "/api/v1/projects/gated-project/airflow-iframe/home"),
    RouteCase("GET", "/api/v1/projects/gated-project/airflow/dags"),
    RouteCase("GET", "/api/v1/projects/gated-project/airflow/dags/example/runs"),
    RouteCase("POST", "/api/v1/projects/gated-project/airflow/dags/example/runs"),
    RouteCase(
        "GET",
        "/api/v1/projects/gated-project/airflow/dags/example/runs/run-a/artifacts/manifest.json",
    ),
    RouteCase("GET", "/api/v1/projects/gated-project/airflow/stats"),
    RouteCase("POST", "/api/v1/projects/gated-project/codeserver/token"),
    RouteCase("GET", "/api/v1/projects/gated-project/codeserver/iframe"),
    RouteCase("GET", "/api/v1/projects/gated-project/codeserver/workspace-info"),
    RouteCase("POST", "/api/v1/projects/gated-project/codeserver/setup-workspace"),
)


async def _seed_project(
    db_session,
    *,
    slug: str,
    lifecycle_status: ProjectLifecycleStatus,
    member_user_id: str | None = "test-admin-001",
) -> Project:
    project = Project(
        name=slug.replace("-", " ").title(),
        slug=slug,
        lifecycle_status=lifecycle_status,
    )
    db_session.add(project)
    await db_session.flush()
    if member_user_id is not None:
        role = (
            await db_session.execute(select(Role).where(Role.name == "project_admin"))
        ).scalar_one()
        db_session.add(
            ProjectMember(
                project_id=project.id,
                user_id=member_user_id,
                role_id=role.id,
            )
        )
    await db_session.commit()
    return project


@pytest.mark.asyncio
async def test_ordinary_project_list_only_contains_ready_memberships(admin_client, db_session):
    await _seed_project(
        db_session,
        slug="ready-member",
        lifecycle_status=ProjectLifecycleStatus.READY,
    )
    await _seed_project(
        db_session,
        slug="ready-not-member",
        lifecycle_status=ProjectLifecycleStatus.READY,
        member_user_id=None,
    )
    await _seed_project(
        db_session,
        slug="provisioning-member",
        lifecycle_status=ProjectLifecycleStatus.PROVISIONING,
    )

    response = await admin_client.get("/api/v1/projects")

    assert response.status_code == 200
    assert [project["slug"] for project in response.json()] == ["ready-member"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lifecycle_status",
    [
        ProjectLifecycleStatus.PROVISIONING,
        ProjectLifecycleStatus.PROVISION_FAILED,
        ProjectLifecycleStatus.DELETING,
        ProjectLifecycleStatus.DELETION_FAILED,
    ],
)
async def test_ready_loader_hides_every_non_ready_state_from_member(
    db_session,
    lifecycle_status,
):
    from app.services.project_access import load_ready_project_for_user

    project = await _seed_project(
        db_session,
        slug=f"state-{lifecycle_status.value.replace('_', '-')}",
        lifecycle_status=lifecycle_status,
    )
    admin = await db_session.get(User, "test-admin-001")

    with pytest.raises(HTTPException) as error:
        await load_ready_project_for_user(project.slug, admin, db_session)

    assert error.value.status_code == 404
    assert error.value.detail == "Project not found"


@pytest.mark.asyncio
async def test_ready_loader_requires_membership_even_for_super_admin(db_session):
    from app.services.project_access import load_ready_project_for_user

    project = await _seed_project(
        db_session,
        slug="ready-without-membership",
        lifecycle_status=ProjectLifecycleStatus.READY,
        member_user_id=None,
    )
    admin = await db_session.get(User, "test-admin-001")

    with pytest.raises(HTTPException) as error:
        await load_ready_project_for_user(project.slug, admin, db_session)

    assert error.value.status_code == 403


@pytest.mark.asyncio
async def test_admin_loader_returns_non_ready_project_only_to_admin(db_session):
    from app.services.project_access import load_project_for_admin

    project = await _seed_project(
        db_session,
        slug="admin-diagnostic",
        lifecycle_status=ProjectLifecycleStatus.DELETION_FAILED,
    )
    admin = await db_session.get(User, "test-admin-001")
    ordinary_user = User(
        email="ordinary@test.local",
        hashed_password="unused",
        display_name="Ordinary",
        is_active=True,
        is_admin=False,
    )
    db_session.add(ordinary_user)
    await db_session.commit()

    loaded = await load_project_for_admin(project.slug, admin, db_session)
    assert loaded.id == project.id
    with pytest.raises(HTTPException) as error:
        await load_project_for_admin(project.slug, ordinary_user, db_session)
    assert error.value.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("case", NON_READY_ROUTE_CASES, ids=lambda case: f"{case.method}-{case.path}")
async def test_every_user_project_route_rejects_deleting_project(
    admin_client,
    db_session,
    case,
):
    await _seed_project(
        db_session,
        slug="gated-project",
        lifecycle_status=ProjectLifecycleStatus.DELETING,
    )

    response = await admin_client.request(case.method, case.path, json=case.json)

    legacy_delete = (
        case.method == "DELETE"
        and case.path == "/api/v1/projects/gated-project"
    )
    assert response.status_code in ({404, 405} if legacy_delete else {404}), (
        case,
        response.text,
    )
    if response.status_code == 404:
        assert response.json()["detail"] == "Project not found"


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["merge", "close", "checks"])
async def test_git_mr_actions_reject_cross_project_id_before_mutation_or_external_call(
    admin_client,
    db_session,
    monkeypatch,
    action,
):
    monkeypatch.setattr(
        settings,
        "credentials_encryption_key",
        "task-four-cross-project-test-key-material",
    )
    ready_project = await _seed_project(
        db_session,
        slug="ready-a",
        lifecycle_status=ProjectLifecycleStatus.READY,
    )
    ready_project.self_approve_enabled = True
    deleting_project = await _seed_project(
        db_session,
        slug="deleting-b",
        lifecycle_status=ProjectLifecycleStatus.DELETING,
    )
    mr = MergeRequest(
        project_id=deleting_project.id,
        author_id="test-admin-001",
        source_branch="foreign-feature",
        target_branch="main",
        title="Foreign MR",
    )
    db_session.add_all(
        [
            mr,
            GitConfig(
                project_id=ready_project.id,
                repo_url="https://github.com/example/ready-a.git",
                auth_type="token",
                credentials_encrypted="must-not-be-decrypted",
            ),
        ]
    )
    await db_session.commit()

    def forbidden_decrypt(*_args, **_kwargs):
        raise AssertionError("cross-project MR must be rejected before decryption")

    class ForbiddenHttpClient:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("cross-project MR must be rejected before HTTP")

    monkeypatch.setattr("app.routers.git.decrypt_token", forbidden_decrypt)
    monkeypatch.setattr("app.routers.git.httpx.AsyncClient", ForbiddenHttpClient)
    response = await admin_client.request(
        "GET" if action == "checks" else "POST",
        f"/api/v1/projects/ready-a/git/merge-requests/{mr.id}/{action}",
    )

    assert response.status_code == 404, response.text
    assert response.json() == {"detail": "Merge request not found"}
    await db_session.refresh(mr)
    assert mr.status == "open"


@pytest.mark.asyncio
async def test_git_mr_checks_validate_ownership_before_missing_git_config_early_return(
    admin_client,
    db_session,
    monkeypatch,
):
    await _seed_project(
        db_session,
        slug="ready-without-git",
        lifecycle_status=ProjectLifecycleStatus.READY,
    )
    deleting_project = await _seed_project(
        db_session,
        slug="deleting-with-mr",
        lifecycle_status=ProjectLifecycleStatus.DELETING,
    )
    mr = MergeRequest(
        project_id=deleting_project.id,
        author_id="test-admin-001",
        source_branch="foreign-feature",
        target_branch="main",
        title="Foreign MR",
    )
    db_session.add(mr)
    await db_session.commit()

    def forbidden_decrypt(*_args, **_kwargs):
        raise AssertionError("cross-project MR must be rejected before decryption")

    monkeypatch.setattr("app.routers.git.decrypt_token", forbidden_decrypt)
    response = await admin_client.get(
        f"/api/v1/projects/ready-without-git/git/merge-requests/{mr.id}/checks"
    )

    assert response.status_code == 404, response.text
    assert response.json() == {"detail": "Merge request not found"}

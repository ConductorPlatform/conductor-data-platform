from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.project import Project, ProjectLifecycleStatus
from app.models.project_deployment import ProjectDeployment, ProvisionerKind
from app.models.project_lifecycle_job import (
    LifecycleJobStatus,
    LifecycleOperation,
    ProjectLifecycleJob,
)
from app.models.project_runtime_resource import ProjectRuntimeResource, RuntimeResourceKind
from app.services.compose_provisioner import ComposeProvisioner, ObservedComposeResource, _json_list
from app.services.lifecycle_queue import ClaimedJob, JobOwnershipError
from app.services.project_database import ObservedDatabaseResource
from app.services.project_warehouse import ObservedWarehouseResource
from app.services.runtime_artifacts import RuntimeArtifact, RuntimeArtifactWriter


class _DatabaseManager:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def ensure_role(self, deployment: ProjectDeployment) -> ObservedDatabaseResource:
        self.calls.append("role")
        return ObservedDatabaseResource("role", deployment.airflow_db_role, deployment.project_id)

    async def ensure_database(self, deployment: ProjectDeployment) -> ObservedDatabaseResource:
        self.calls.append("database")
        return ObservedDatabaseResource(
            "database",
            deployment.airflow_db_name,
            deployment.project_id,
            owner=deployment.airflow_db_role,
        )

    async def drop_database(self, _deployment: ProjectDeployment) -> None:
        raise AssertionError("Provisioning must not delete a database")

    async def drop_role(self, _deployment: ProjectDeployment) -> None:
        raise AssertionError("Provisioning must not delete a role")

    async def verify_absent(self, _deployment: ProjectDeployment) -> bool:
        raise AssertionError("Provisioning must not verify deletion")


class _WarehouseManager:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def ensure_warehouse(
        self, deployment: ProjectDeployment
    ) -> tuple[ObservedWarehouseResource, ObservedWarehouseResource, ObservedWarehouseResource]:
        self.calls.append("warehouse")
        return (
            ObservedWarehouseResource("role", deployment.warehouse_db_role, deployment.project_id),
            ObservedWarehouseResource(
                "database", deployment.warehouse_db_name, deployment.project_id, deployment.warehouse_db_role
            ),
            ObservedWarehouseResource(
                "schema", deployment.warehouse_schema, deployment.project_id, deployment.warehouse_db_role
            ),
        )


class _ComposeClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def ensure_runtime_image(self) -> None:
        self.calls.append("image")

    async def validate(self, _artifact: RuntimeArtifact, _deployment: ProjectDeployment) -> None:
        self.calls.append("validate")

    async def run_init(self, _artifact: RuntimeArtifact, _deployment: ProjectDeployment) -> None:
        self.calls.append("init")

    async def start_services(self, _artifact: RuntimeArtifact, _deployment: ProjectDeployment) -> None:
        self.calls.append("services")

    async def inspect_resources(self, _artifact, deployment: ProjectDeployment) -> list[ObservedComposeResource]:
        self.calls.append("inspect")
        labels = {
            "conductor.managed": "true",
            "conductor.project_id": deployment.project_id,
            "conductor.template_version": deployment.template_version,
        }
        resources = [
            ObservedComposeResource(
                RuntimeResourceKind.CONTAINER,
                "airflow-init",
                "init-container-id",
                "project-airflow-init",
                "exited",
                labels,
            ),
        ]
        for service in (
            "project-redis",
            "airflow-api-server",
            "airflow-scheduler",
            "airflow-dag-processor",
            "airflow-worker",
        ):
            resources.append(
                ObservedComposeResource(
                    RuntimeResourceKind.CONTAINER,
                    service,
                    f"{service}-container-id",
                    f"project-{service}",
                    "running",
                    labels,
                )
            )
        resources.append(
            ObservedComposeResource(
                RuntimeResourceKind.NETWORK,
                "default",
                "network-id",
                f"{deployment.compose_project_name}_default",
                "present",
                labels,
            )
        )
        return resources


class _ReadinessChecker:
    def __init__(self) -> None:
        self.calls = 0

    async def wait_ready(self, _deployment: ProjectDeployment) -> None:
        self.calls += 1


def test_compose_resource_parser_accepts_compose_ndjson() -> None:
    assert _json_list('{"Name":"first"}\n{"Name":"second"}\n') == [
        {"Name": "first"},
        {"Name": "second"},
    ]


class _FailsReadinessOnce(_ReadinessChecker):
    async def wait_ready(self, deployment: ProjectDeployment) -> None:
        await super().wait_ready(deployment)
        if self.calls == 1:
            raise TimeoutError("injected readiness timeout")


async def _running_job(factory: async_sessionmaker[AsyncSession]) -> tuple[Project, ProjectLifecycleJob]:
    async with factory() as session:
        project = Project(
            name="Provisioned Project",
            slug="provisioned-project",
            lifecycle_status=ProjectLifecycleStatus.PROVISIONING,
        )
        session.add(project)
        await session.flush()
        identity = f"conductor_airflow_{project.id}"
        deployment = ProjectDeployment(
            project_id=project.id,
            provisioner_kind=ProvisionerKind.DOCKER_COMPOSE,
            template_version="v1",
            generation=1,
            compose_project_name=f"conductor-p-{project.id}",
            airflow_external_url="https://provisioned-project.airflow.example.test",
            airflow_db_name=identity,
            airflow_db_role=identity,
            airflow_db_password_encrypted="db-password",
            airflow_admin_user="admin",
            airflow_admin_password_encrypted="admin-password",
            airflow_dev_user="dev",
            airflow_dev_password_encrypted="dev-password",
            airflow_viewer_user="viewer",
            airflow_viewer_password_encrypted="viewer-password",
            airflow_integration_user="integration",
            airflow_integration_password_encrypted="integration-password",
            warehouse_db_name=f"conductor_warehouse_{project.id}",
            warehouse_db_role=f"conductor_warehouse_{project.id}",
            warehouse_db_password_encrypted="warehouse-password",
            warehouse_schema="analytics",
            parameters={},
        )
        job = ProjectLifecycleJob(
            project_id=project.id,
            operation=LifecycleOperation.PROVISION,
            status=LifecycleJobStatus.RUNNING,
            attempt=1,
            max_attempts=5,
            available_at=datetime.now(UTC),
            locked_by="worker:attempt",
            lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            idempotency_key="b42fd89e-7715-4bcd-8387-b5bdc8d753f4",
            request_fingerprint="a" * 64,
            correlation_id="provision-correlation",
        )
        session.add_all((deployment, job))
        await session.commit()
        return project, job


def _claimed(job: ProjectLifecycleJob) -> ClaimedJob:
    return ClaimedJob(
        id=job.id,
        project_id=job.project_id,
        operation=job.operation,
        attempt=job.attempt,
        max_attempts=job.max_attempts,
        worker_id="worker:attempt",
        correlation_id=job.correlation_id,
        requested_by=None,
    )


@pytest.mark.asyncio
async def test_provision_saga_is_ordered_resumable_and_publishes_ready_only_after_readiness(
    _engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.compose_provisioner as provisioner_module

    monkeypatch.setattr(provisioner_module, "decrypt_token", lambda value: value)
    factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    project, job = await _running_job(factory)
    database = _DatabaseManager()
    compose = _ComposeClient()
    readiness = _ReadinessChecker()
    provisioner = ComposeProvisioner(
        factory,
        database_manager=database,
        artifact_writer=RuntimeArtifactWriter(runtime_root=tmp_path / "runtime"),
        compose_client=compose,
        readiness_checker=readiness,
    )

    await provisioner.provision(_claimed(job))

    assert database.calls == ["role", "database"]
    assert compose.calls == [
        "image",
        "validate",
        "init",
        "inspect",
        "services",
        "inspect",
        "inspect",
    ]
    assert readiness.calls == 1
    async with factory() as session:
        persisted_project = await session.get(Project, project.id)
        persisted_job = await session.get(ProjectLifecycleJob, job.id)
        resources = (
            await session.execute(
                select(ProjectRuntimeResource)
                .where(ProjectRuntimeResource.project_id == project.id)
                .order_by(ProjectRuntimeResource.logical_name)
            )
        ).scalars().all()

    assert persisted_project is not None
    assert persisted_project.lifecycle_status is ProjectLifecycleStatus.READY
    assert persisted_job is not None
    assert persisted_job.current_step == "ready"
    statuses = {resource.logical_name: resource.observed_status for resource in resources}
    assert {
        ("airflow-init", "exited"),
        ("database", "present"),
        ("default", "present"),
        ("role", "present"),
        ("runtime_artifact", "rendered"),
    } <= set(statuses.items())
    for service in (
        "project-redis",
        "airflow-api-server",
        "airflow-scheduler",
        "airflow-dag-processor",
        "airflow-worker",
    ):
        assert statuses[service] == "running"


class _WorkerExitsAfterReadiness(_ComposeClient):
    async def inspect_resources(
        self, artifact, deployment: ProjectDeployment
    ) -> list[ObservedComposeResource]:
        resources = await super().inspect_resources(artifact, deployment)
        if self.calls.count("inspect") != 3:
            return resources
        return [
            ObservedComposeResource(
                resource.kind,
                resource.logical_name,
                resource.provider_id,
                resource.provider_name,
                "exited" if resource.logical_name == "airflow-worker" else resource.observed_status,
                resource.labels,
            )
            for resource in resources
        ]


@pytest.mark.asyncio
async def test_provision_does_not_publish_ready_when_worker_exits_after_api_readiness(
    _engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.compose_provisioner as provisioner_module

    monkeypatch.setattr(provisioner_module, "decrypt_token", lambda value: value)
    factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    project, job = await _running_job(factory)
    provisioner = ComposeProvisioner(
        factory,
        database_manager=cast(Any, _DatabaseManager()),
        artifact_writer=RuntimeArtifactWriter(runtime_root=tmp_path / "runtime"),
        compose_client=cast(Any, _WorkerExitsAfterReadiness()),
        readiness_checker=cast(Any, _ReadinessChecker()),
    )

    with pytest.raises(RuntimeError, match="airflow-worker"):
        await provisioner.provision(_claimed(job))

    async with factory() as session:
        persisted_project = await session.get(Project, project.id)
        persisted_job = await session.get(ProjectLifecycleJob, job.id)
        worker = (
            await session.execute(
                select(ProjectRuntimeResource).where(
                    ProjectRuntimeResource.project_id == project.id,
                    ProjectRuntimeResource.logical_name == "airflow-worker",
                )
            )
        ).scalar_one()
    assert persisted_project is not None
    assert persisted_project.lifecycle_status is ProjectLifecycleStatus.PROVISIONING
    assert persisted_job is not None
    assert persisted_job.current_step == "airflow_readiness"
    assert worker.observed_status == "exited"


@pytest.mark.asyncio
async def test_expired_lease_cannot_publish_ready_or_run_compose(
    _engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.compose_provisioner as provisioner_module

    monkeypatch.setattr(provisioner_module, "decrypt_token", lambda value: value)
    factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    project, job = await _running_job(factory)
    async with factory() as session:
        persisted = await session.get(ProjectLifecycleJob, job.id)
        assert persisted is not None
        persisted.lock_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()

    database = _DatabaseManager()
    compose = _ComposeClient()
    provisioner = ComposeProvisioner(
        factory,
        database_manager=database,
        artifact_writer=RuntimeArtifactWriter(runtime_root=tmp_path / "runtime"),
        compose_client=compose,
        readiness_checker=_ReadinessChecker(),
    )

    with pytest.raises(JobOwnershipError):
        await provisioner.provision(_claimed(job))

    assert database.calls == []
    assert compose.calls == []
    async with factory() as session:
        persisted_project = await session.get(Project, project.id)
    assert persisted_project is not None
    assert persisted_project.lifecycle_status is ProjectLifecycleStatus.PROVISIONING


@pytest.mark.asyncio
async def test_retry_after_partial_init_reuses_recorded_init_and_only_publishes_after_readiness(
    _engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.compose_provisioner as provisioner_module

    monkeypatch.setattr(provisioner_module, "decrypt_token", lambda value: value)
    factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    project, job = await _running_job(factory)
    compose = _ComposeClient()
    readiness = _FailsReadinessOnce()
    provisioner = ComposeProvisioner(
        factory,
        database_manager=_DatabaseManager(),
        artifact_writer=RuntimeArtifactWriter(runtime_root=tmp_path / "runtime"),
        compose_client=compose,
        readiness_checker=readiness,
    )

    with pytest.raises(TimeoutError, match="injected readiness timeout"):
        await provisioner.provision(_claimed(job))

    async with factory() as session:
        persisted_project = await session.get(Project, project.id)
        persisted_job = await session.get(ProjectLifecycleJob, job.id)
    assert persisted_project is not None
    assert persisted_project.lifecycle_status is ProjectLifecycleStatus.PROVISIONING
    assert persisted_job is not None
    assert persisted_job.current_step == "airflow_readiness"

    await provisioner.provision(_claimed(job))

    assert compose.calls == [
        "image",
        "validate",
        "init",
        "inspect",
        "services",
        "inspect",
        "image",
        "validate",
        "services",
        "inspect",
        "inspect",
    ]
    assert readiness.calls == 2
    async with factory() as session:
        persisted_project = await session.get(Project, project.id)
        persisted_job = await session.get(ProjectLifecycleJob, job.id)
    assert persisted_project is not None
    assert persisted_project.lifecycle_status is ProjectLifecycleStatus.READY
    assert persisted_job is not None
    assert persisted_job.current_step == "ready"

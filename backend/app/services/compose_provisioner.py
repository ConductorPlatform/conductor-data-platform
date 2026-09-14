"""Resumable, worker-only Docker Compose provisioning for project Airflow runtimes."""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.project import Project, ProjectLifecycleStatus
from app.models.project_deployment import ProjectDeployment
from app.models.git_config import GitConfig
from app.models.project_lifecycle_job import LifecycleJobStatus, ProjectLifecycleJob
from app.models.project_runtime_resource import ProjectRuntimeResource, RuntimeResourceKind
from app.services.crypto import decrypt_token
from app.services.lifecycle_errors import ForeignResourceConflictError, InvalidComposeError
from app.services.lifecycle_queue import ClaimedJob, JobOwnershipError
from app.services.project_database import ObservedDatabaseResource, ProjectDatabaseManager
from app.services.project_warehouse import ObservedWarehouseResource, ProjectWarehouseManager
from app.services.project_lifecycle import assert_transition
from app.services.git_dag_bundle import GitDagBundleSyncError, sync_git_dag_connection
from app.services.project_airflow_context import ProjectAirflowContext
from app.services.runtime_artifacts import (
    RuntimeArtifact,
    RuntimeArtifactSpec,
    RuntimeArtifactWriter,
    runtime_artifact_metadata,
)

_MANAGED_LABEL = "conductor.managed"
_PROJECT_LABEL = "conductor.project_id"
_TEMPLATE_LABEL = "conductor.template_version"
_REQUIRED_RUNTIME_SERVICES = frozenset(
    {
        "project-redis",
        "airflow-api-server",
        "airflow-scheduler",
        "airflow-dag-processor",
        "airflow-worker",
    }
)


class ComposeClient(Protocol):
    """Narrow Docker CLI boundary. Only the lifecycle worker instantiates this."""

    async def ensure_runtime_image(self) -> None: ...

    async def validate(self, artifact: RuntimeArtifact, deployment: ProjectDeployment) -> None: ...

    async def run_init(self, artifact: RuntimeArtifact, deployment: ProjectDeployment) -> None: ...

    async def start_services(self, artifact: RuntimeArtifact, deployment: ProjectDeployment) -> None: ...

    async def inspect_resources(
        self, artifact: RuntimeArtifact, deployment: ProjectDeployment
    ) -> list[ObservedComposeResource]: ...


@dataclass(frozen=True, slots=True)
class ObservedComposeResource:
    kind: RuntimeResourceKind
    logical_name: str
    provider_id: str | None
    provider_name: str
    observed_status: str
    labels: dict[str, str]


class AirflowReadinessChecker(Protocol):
    async def wait_ready(self, deployment: ProjectDeployment) -> None: ...


class SubprocessComposeClient:
    """Trusted fixed-argument Docker Compose client; it never accepts user commands."""

    def __init__(self, *, docker_executable: str = "docker", airflow_image: str = "conductor-airflow:latest") -> None:
        self._docker_executable = docker_executable
        self._airflow_image = airflow_image

    async def _run(self, *arguments: str) -> str:
        process = await asyncio.create_subprocess_exec(
            self._docker_executable,
            *arguments,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=os.environ | {"AIRFLOW_IMAGE": self._airflow_image},
        )
        stdout, stderr = await process.communicate()
        if process.returncode:
            message = stderr.decode(errors="replace").strip() or stdout.decode(errors="replace").strip()
            raise RuntimeError(f"Docker Compose command failed: {message}")
        return stdout.decode(errors="replace")

    def _base(self, artifact: RuntimeArtifact, deployment: ProjectDeployment) -> tuple[str, ...]:
        return (
            "compose",
            "--project-name",
            deployment.compose_project_name,
            "--env-file",
            str(artifact.env_path),
            "--file",
            str(artifact.compose_path),
        )

    async def ensure_runtime_image(self) -> None:
        await self._run("image", "inspect", self._airflow_image)

    async def validate(self, artifact: RuntimeArtifact, deployment: ProjectDeployment) -> None:
        output = await self._run(*self._base(artifact, deployment), "config", "--format", "json")
        try:
            rendered = json.loads(output)
        except json.JSONDecodeError as error:
            raise InvalidComposeError("Docker Compose did not return normalized JSON") from error
        if "airflow-api-server" not in rendered.get("services", {}):
            raise InvalidComposeError("Trusted Compose template has no Airflow API service")

    async def run_init(self, artifact: RuntimeArtifact, deployment: ProjectDeployment) -> None:
        await self._run(*self._base(artifact, deployment), "up", "--exit-code-from", "airflow-init", "airflow-init")

    async def start_services(self, artifact: RuntimeArtifact, deployment: ProjectDeployment) -> None:
        await self._run(
            *self._base(artifact, deployment),
            "up",
            "--detach",
            "project-redis",
            "airflow-api-server",
            "airflow-scheduler",
            "airflow-dag-processor",
            "airflow-worker",
        )

    async def inspect_resources(
        self, artifact: RuntimeArtifact, deployment: ProjectDeployment
    ) -> list[ObservedComposeResource]:
        containers = _json_list(
            await self._run(*self._base(artifact, deployment), "ps", "--all", "--format", "json")
        )
        resources: list[ObservedComposeResource] = []
        for container in containers:
            labels = _labels(container.get("Labels", {}))
            name = str(container.get("Name") or "")
            service = labels.get("com.docker.compose.service")
            if not name or not service:
                raise ForeignResourceConflictError("Compose container identity cannot be proven")
            _require_owned(labels, deployment)
            resources.append(
                ObservedComposeResource(
                    kind=RuntimeResourceKind.CONTAINER,
                    logical_name=service,
                    provider_id=str(container.get("ID") or "") or None,
                    provider_name=name,
                    observed_status=_container_observed_status(container),
                    labels=labels,
                )
            )

        for kind, logical_name, command in _expected_non_container_resources(deployment):
            inspected = _json_list(await self._run(*command))
            if len(inspected) != 1:
                raise ForeignResourceConflictError("Compose resource identity cannot be proven")
            raw = inspected[0]
            labels = _labels(raw.get("Labels", {}))
            _require_owned(labels, deployment)
            provider_name = str(raw.get("Name") or "")
            if not provider_name:
                raise ForeignResourceConflictError("Compose resource has no provider name")
            resources.append(
                ObservedComposeResource(
                    kind=kind,
                    logical_name=logical_name,
                    provider_id=str(raw.get("Id") or raw.get("ID") or "") or None,
                    provider_name=provider_name,
                    observed_status="present",
                    labels=labels,
                )
            )
        return resources


class HttpAirflowReadinessChecker:
    """Confirm an authenticated Airflow API response before publishing READY."""

    def __init__(self, *, timeout_seconds: float, poll_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds
        self._poll_seconds = poll_seconds

    async def wait_ready(self, deployment: ProjectDeployment) -> None:
        base_url = f"http://{_internal_airflow_alias(deployment.project_id)}:8080"
        credentials = {
            "username": deployment.airflow_integration_user,
            "password": decrypt_token(deployment.airflow_integration_password_encrypted),
        }
        deadline = asyncio.get_running_loop().time() + self._timeout_seconds
        async with httpx.AsyncClient(timeout=min(self._poll_seconds, 10.0)) as client:
            while True:
                try:
                    response = await client.post(f"{base_url}/auth/token", json=credentials)
                    access_token = response.json().get("access_token") if response.is_success else None
                    authenticated = await client.get(
                        f"{base_url}/api/v2/dags",
                        headers={"Authorization": f"Bearer {access_token}"},
                    ) if access_token else None
                    if authenticated is not None and authenticated.status_code == 200:
                        return
                except (httpx.HTTPError, ValueError):
                    pass
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError("Airflow did not become authenticated-ready before timeout")
                await asyncio.sleep(self._poll_seconds)


class ComposeProvisioner:
    """Convergent provision saga with durable steps and Postgres lease fencing."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        database_manager: ProjectDatabaseManager,
        artifact_writer: RuntimeArtifactWriter,
        compose_client: ComposeClient,
        readiness_checker: AirflowReadinessChecker,
        warehouse_manager: ProjectWarehouseManager | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._database_manager = database_manager
        self._warehouse_manager = warehouse_manager
        self._artifact_writer = artifact_writer
        self._compose_client = compose_client
        self._readiness_checker = readiness_checker

    async def provision(self, claimed: ClaimedJob) -> None:
        """Resume from observable state; each repeated step converges on one identity."""

        async with self._project_lock(claimed.project_id):
            project, deployment = await self._load_claimed_state(claimed)
            await self._set_step(claimed, "database_role")
            role = await self._database_manager.ensure_role(deployment)
            await self._record_database_resource(deployment, role)

            await self._set_step(claimed, "database")
            database = await self._database_manager.ensure_database(deployment)
            await self._record_database_resource(deployment, database)

            if self._warehouse_manager is not None:
                await self._set_step(claimed, "warehouse")
                for resource in await self._warehouse_manager.ensure_warehouse(deployment):
                    await self._record_warehouse_resource(deployment, resource)

            await self._set_step(claimed, "configuration")
            artifact = self._artifact_writer.render(_artifact_spec(project, deployment))
            await self._record_artifact(deployment, artifact)

            await self._set_step(claimed, "runtime_image")
            await self._compose_client.ensure_runtime_image()
            await self._compose_client.validate(artifact, deployment)

            await self._set_step(claimed, "init")
            if not await self._init_completed(deployment):
                await self._compose_client.run_init(artifact, deployment)
                await self._record_compose_resources(deployment, artifact)

            await self._set_step(claimed, "services")
            await self._compose_client.start_services(artifact, deployment)
            await self._record_compose_resources(deployment, artifact)

            await self._set_step(claimed, "airflow_readiness")
            await self._readiness_checker.wait_ready(deployment)
            resources = await self._record_compose_resources(deployment, artifact)
            self._require_required_services_running(resources)

            await self._set_step(claimed, "git_connection")
            await self._converge_git_connection(project, deployment)

            await self._publish_ready(claimed)

    async def _converge_git_connection(self, project: Project, deployment: ProjectDeployment) -> None:
        """Install an existing token config only after authenticated readiness."""

        async with self._session_factory() as session:
            config = (
                await session.execute(select(GitConfig).where(GitConfig.project_id == project.id))
            ).scalar_one_or_none()
            if config is None or config.auth_type != "token":
                return
            try:
                await sync_git_dag_connection(
                    context=ProjectAirflowContext(
                        project_id=project.id,
                        deployment_id=deployment.id,
                        deployment_generation=deployment.generation,
                        airflow_base_url=f"http://{_internal_airflow_alias(project.id)}:8080",
                        account_key="admin",
                    ),
                    config=config,
                    db=session,
                )
            except GitDagBundleSyncError as exc:
                raise ForeignResourceConflictError("Initial Git connection could not be converged") from exc

    @asynccontextmanager
    async def _project_lock(self, project_id: str):
        """Hold one Postgres advisory lock over the whole attempt, not each action."""

        async with self._session_factory() as session:
            dialect = session.bind.dialect.name if session.bind is not None else ""
            locked = False
            try:
                if dialect == "postgresql":
                    await session.execute(
                        text("SELECT pg_catalog.pg_advisory_lock(pg_catalog.hashtextextended(:key, 0))"),
                        {"key": f"conductor.project_provision:{project_id}"},
                    )
                    locked = True
                yield
            finally:
                if locked:
                    await session.execute(
                        text("SELECT pg_catalog.pg_advisory_unlock(pg_catalog.hashtextextended(:key, 0))"),
                        {"key": f"conductor.project_provision:{project_id}"},
                    )
                    await session.commit()

    async def _load_claimed_state(self, claimed: ClaimedJob) -> tuple[Project, ProjectDeployment]:
        async with self._session_factory() as session:
            job = await _locked_owned_job(session, claimed)
            project = await session.get(Project, claimed.project_id, with_for_update=True)
            deployment = (
                await session.execute(
                    select(ProjectDeployment).where(ProjectDeployment.project_id == claimed.project_id)
                )
            ).scalar_one_or_none()
            if project is None or deployment is None:
                raise ForeignResourceConflictError("Provisioning project has incomplete desired state")
            if project.lifecycle_status not in (
                ProjectLifecycleStatus.PROVISIONING,
                ProjectLifecycleStatus.PROVISION_FAILED,
            ):
                raise ForeignResourceConflictError("Provisioning project is not in a resumable lifecycle state")
            if job.project_id != project.id:
                raise JobOwnershipError("Lifecycle job project does not match claimed project")
            await session.commit()
            return project, deployment

    async def _set_step(self, claimed: ClaimedJob, step: str) -> None:
        async with self._session_factory() as session:
            job = await _locked_owned_job(session, claimed)
            job.current_step = step
            await session.commit()

    async def _record_database_resource(
        self, deployment: ProjectDeployment, resource: ObservedDatabaseResource
    ) -> None:
        kind = RuntimeResourceKind.DATABASE_ROLE if resource.kind == "role" else RuntimeResourceKind.DATABASE
        await self._upsert_resource(
            deployment,
            kind=kind,
            logical_name=resource.kind,
            provider_name=resource.name,
            observed_status="present",
            metadata={"owner": resource.owner} if resource.owner else {},
        )

    async def _record_warehouse_resource(
        self, deployment: ProjectDeployment, resource: ObservedWarehouseResource
    ) -> None:
        kind = (
            RuntimeResourceKind.DATABASE_ROLE
            if resource.kind == "role"
            else RuntimeResourceKind.DATABASE
        )
        await self._upsert_resource(
            deployment,
            kind=kind,
            logical_name=f"warehouse_{resource.kind}",
            provider_name=resource.name,
            observed_status="present",
            metadata={"owner": resource.owner} if resource.owner else {},
        )

    async def _record_artifact(self, deployment: ProjectDeployment, artifact: RuntimeArtifact) -> None:
        await self._upsert_resource(
            deployment,
            kind=RuntimeResourceKind.PROXY_ROUTE,
            logical_name="runtime_artifact",
            provider_name=str(artifact.compose_path),
            observed_status="rendered",
            metadata=runtime_artifact_metadata(artifact),
        )

    async def _record_compose_resources(
        self, deployment: ProjectDeployment, artifact: RuntimeArtifact
    ) -> list[ObservedComposeResource]:
        resources = await self._compose_client.inspect_resources(artifact, deployment)
        for resource in resources:
            await self._upsert_resource(
                deployment,
                kind=resource.kind,
                logical_name=resource.logical_name,
                provider_id=resource.provider_id,
                provider_name=resource.provider_name,
                observed_status=resource.observed_status,
                metadata={"labels": resource.labels},
            )
        await self._mark_missing_required_services(deployment, resources)
        return resources

    async def _mark_missing_required_services(
        self, deployment: ProjectDeployment, resources: list[ObservedComposeResource]
    ) -> None:
        observed_services = {
            resource.logical_name
            for resource in resources
            if resource.kind is RuntimeResourceKind.CONTAINER
        }
        missing_services = _REQUIRED_RUNTIME_SERVICES - observed_services
        if not missing_services:
            return
        async with self._session_factory() as session:
            persisted = (
                await session.execute(
                    select(ProjectRuntimeResource).where(
                        ProjectRuntimeResource.project_id == deployment.project_id,
                        ProjectRuntimeResource.generation == deployment.generation,
                        ProjectRuntimeResource.resource_kind == RuntimeResourceKind.CONTAINER,
                        ProjectRuntimeResource.logical_name.in_(missing_services),
                    )
                )
            ).scalars().all()
            for resource in persisted:
                resource.observed_status = "absent"
            await session.commit()

    @staticmethod
    def _require_required_services_running(resources: list[ObservedComposeResource]) -> None:
        statuses = {
            resource.logical_name: resource.observed_status.lower()
            for resource in resources
            if resource.kind is RuntimeResourceKind.CONTAINER
        }
        unavailable = sorted(
            service
            for service in _REQUIRED_RUNTIME_SERVICES
            if statuses.get(service) != "running"
        )
        if unavailable:
            raise RuntimeError(
                "Required Airflow runtime services are not running: " + ", ".join(unavailable)
            )

    async def _init_completed(self, deployment: ProjectDeployment) -> bool:
        async with self._session_factory() as session:
            resource = (
                await session.execute(
                    select(ProjectRuntimeResource).where(
                        ProjectRuntimeResource.project_id == deployment.project_id,
                        ProjectRuntimeResource.generation == deployment.generation,
                        ProjectRuntimeResource.logical_name == "airflow-init",
                        ProjectRuntimeResource.resource_kind == RuntimeResourceKind.CONTAINER,
                    )
                )
            ).scalar_one_or_none()
        # This row is written only after Docker Compose reports a successful
        # init command. A retry after a later failure therefore does not create
        # duplicate Airflow users or repeat the completed initialization.
        return resource is not None and resource.observed_status.lower() == "exited"

    async def _upsert_resource(
        self,
        deployment: ProjectDeployment,
        *,
        kind: RuntimeResourceKind,
        logical_name: str,
        provider_name: str,
        observed_status: str,
        metadata: dict[str, Any],
        provider_id: str | None = None,
    ) -> None:
        async with self._session_factory() as session:
            existing = (
                await session.execute(
                    select(ProjectRuntimeResource)
                    .where(
                        ProjectRuntimeResource.project_id == deployment.project_id,
                        ProjectRuntimeResource.generation == deployment.generation,
                        ProjectRuntimeResource.logical_name == logical_name,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if existing is None:
                session.add(
                    ProjectRuntimeResource(
                        project_id=deployment.project_id,
                        generation=deployment.generation,
                        resource_kind=kind,
                        logical_name=logical_name,
                        provider_id=provider_id,
                        provider_name=provider_name,
                        observed_status=observed_status,
                        metadata_json=metadata,
                    )
                )
            else:
                existing.resource_kind = kind
                existing.provider_id = provider_id
                existing.provider_name = provider_name
                existing.observed_status = observed_status
                existing.metadata_json = metadata
                existing.deleted_at = None
            await session.commit()

    async def _publish_ready(self, claimed: ClaimedJob) -> None:
        async with self._session_factory() as session:
            await _locked_owned_job(session, claimed)
            project = await session.get(Project, claimed.project_id, with_for_update=True)
            if project is None:
                raise JobOwnershipError("Lifecycle project no longer exists")
            if project.lifecycle_status is ProjectLifecycleStatus.PROVISION_FAILED:
                assert_transition(ProjectLifecycleStatus.PROVISION_FAILED, ProjectLifecycleStatus.PROVISIONING)
                project.lifecycle_status = ProjectLifecycleStatus.PROVISIONING
            assert_transition(project.lifecycle_status, ProjectLifecycleStatus.READY)
            project.lifecycle_status = ProjectLifecycleStatus.READY
            job = await session.get(ProjectLifecycleJob, claimed.id, with_for_update=True)
            assert job is not None
            job.current_step = "ready"
            await session.commit()


def build_compose_provisioner(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    database_manager: ProjectDatabaseManager,
    warehouse_manager: ProjectWarehouseManager,
    runtime_root: Path,
    readiness_timeout_seconds: float,
    readiness_poll_seconds: float,
    runtime_ingress_network: str = "conductor-runtime-ingress",
    airflow_image: str = "conductor-airflow:latest",
    airflow_database_host: str = "host.docker.internal",
    airflow_database_port: int = 5432,
    warehouse_host: str = "host.docker.internal",
    warehouse_port: int = 5433,
    runtime_secret_root: Path | None = None,
    runtime_artifact_root: Path | None = None,
) -> ComposeProvisioner:
    return ComposeProvisioner(
        session_factory,
        database_manager=database_manager,
        warehouse_manager=warehouse_manager,
        artifact_writer=RuntimeArtifactWriter(
            runtime_root=runtime_root,
            runtime_ingress_network=runtime_ingress_network,
            airflow_image=airflow_image,
            airflow_database_host=airflow_database_host,
            airflow_database_port=airflow_database_port,
            warehouse_host=warehouse_host,
            warehouse_port=warehouse_port,
            secret_root=runtime_secret_root,
            artifact_root=runtime_artifact_root,
        ),
        compose_client=SubprocessComposeClient(airflow_image=airflow_image),
        readiness_checker=HttpAirflowReadinessChecker(
            timeout_seconds=readiness_timeout_seconds,
            poll_seconds=readiness_poll_seconds,
        ),
    )


def _artifact_spec(project: Project, deployment: ProjectDeployment) -> RuntimeArtifactSpec:
    return RuntimeArtifactSpec(
        project_id=project.id,
        generation=deployment.generation,
        template_version=deployment.template_version,
        compose_project_name=deployment.compose_project_name,
        project_slug=project.slug,
        airflow_external_url=deployment.airflow_external_url,
        airflow_db_name=deployment.airflow_db_name,
        airflow_db_role=deployment.airflow_db_role,
        airflow_db_password=decrypt_token(deployment.airflow_db_password_encrypted),
        airflow_admin_user=deployment.airflow_admin_user,
        airflow_admin_password=decrypt_token(deployment.airflow_admin_password_encrypted),
        airflow_dev_user=deployment.airflow_dev_user,
        airflow_dev_password=decrypt_token(deployment.airflow_dev_password_encrypted),
        airflow_viewer_user=deployment.airflow_viewer_user,
        airflow_viewer_password=decrypt_token(deployment.airflow_viewer_password_encrypted),
        airflow_integration_user=deployment.airflow_integration_user,
        airflow_integration_password=decrypt_token(deployment.airflow_integration_password_encrypted),
        warehouse_db_name=_warehouse_value(deployment.warehouse_db_name, "database name"),
        warehouse_db_role=_warehouse_value(deployment.warehouse_db_role, "database role"),
        warehouse_db_password=decrypt_token(
            _warehouse_value(deployment.warehouse_db_password_encrypted, "database password")
        ),
        warehouse_schema=_warehouse_value(deployment.warehouse_schema, "schema"),
        parameters=deployment.parameters,
    )


def _warehouse_value(value: str | None, field: str) -> str:
    if not value:
        raise ForeignResourceConflictError(f"Warehouse {field} is not configured for this deployment")
    return value


async def _locked_owned_job(session: AsyncSession, claimed: ClaimedJob) -> ProjectLifecycleJob:
    job = (
        await session.execute(
            select(ProjectLifecycleJob).where(ProjectLifecycleJob.id == claimed.id).with_for_update()
        )
    ).scalar_one_or_none()
    now = datetime.now(UTC)
    if (
        job is None
        or job.project_id != claimed.project_id
        or job.status is not LifecycleJobStatus.RUNNING
        or job.locked_by != claimed.worker_id
        or job.attempt != claimed.attempt
        or job.lock_expires_at is None
        or job.lock_expires_at <= now
    ):
        raise JobOwnershipError("Lifecycle job lease is not owned by this worker")
    return job


def _internal_airflow_alias(project_id: str) -> str:
    return f"airflow-{project_id}"


def _json_list(output: str) -> list[dict[str, Any]]:
    try:
        decoded = json.loads(output)
    except json.JSONDecodeError:
        try:
            decoded = [json.loads(line) for line in output.splitlines() if line.strip()]
        except json.JSONDecodeError as error:
            raise ForeignResourceConflictError("Docker inspection returned invalid JSON") from error
    if isinstance(decoded, dict):
        return [decoded]
    if isinstance(decoded, list) and all(isinstance(item, dict) for item in decoded):
        return decoded
    raise ForeignResourceConflictError("Docker inspection returned an unexpected shape")


def _container_observed_status(container: dict[str, Any]) -> str:
    state = str(container.get("State") or "unknown").lower()
    health = str(container.get("Health") or "").lower()
    if state != "running":
        return state
    return "running" if not health or health == "healthy" else health


def _labels(value: object) -> dict[str, str]:
    if isinstance(value, dict):
        return {str(key): str(item) for key, item in value.items()}
    if isinstance(value, str):
        return dict(item.split("=", 1) for item in value.split(",") if "=" in item)
    raise ForeignResourceConflictError("Docker resource labels cannot be read")


def _require_owned(labels: dict[str, str], deployment: ProjectDeployment) -> None:
    if labels.get(_MANAGED_LABEL) != "true" or labels.get(_PROJECT_LABEL) != deployment.project_id:
        raise ForeignResourceConflictError("Docker resource exists without matching Conductor ownership")
    if labels.get(_TEMPLATE_LABEL) != deployment.template_version:
        raise ForeignResourceConflictError("Docker resource template ownership cannot be proven")


def _expected_non_container_resources(
    deployment: ProjectDeployment,
) -> tuple[tuple[RuntimeResourceKind, str, tuple[str, ...]], ...]:
    prefix = deployment.compose_project_name
    return (
        (RuntimeResourceKind.NETWORK, "default", ("network", "inspect", f"{prefix}_default")),
        (RuntimeResourceKind.VOLUME, "dags", ("volume", "inspect", f"{prefix}_dags")),
        (RuntimeResourceKind.VOLUME, "logs", ("volume", "inspect", f"{prefix}_logs")),
    )

"""Operation-to-runner registry for lifecycle workers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

from app.config import settings
from app.database import async_session_factory
from app.models.project_lifecycle_job import LifecycleOperation
from app.services.compose_provisioner import build_compose_provisioner
from app.services.lifecycle_errors import PermanentLifecycleError
from app.services.lifecycle_queue import ClaimedJob
from app.services.project_database import AsyncpgProjectDatabaseManager

LifecycleRunner = Callable[[ClaimedJob], Awaitable[None]]


class RunnerNotConfiguredError(PermanentLifecycleError):
    """No trusted runner has been registered for a lifecycle operation."""

    code = "RUNNER_NOT_CONFIGURED"


class LifecycleRunnerRegistry:
    """Explicit registry that enables only trusted operation implementations."""

    def __init__(self, runners: Mapping[LifecycleOperation, LifecycleRunner] | None = None) -> None:
        self._runners = dict(runners or {})

    def register(self, operation: LifecycleOperation, runner: LifecycleRunner) -> None:
        if operation in self._runners:
            raise ValueError(f"Runner already registered for {operation.value}")
        self._runners[operation] = runner

    def resolve(self, operation: LifecycleOperation) -> LifecycleRunner:
        try:
            return self._runners[operation]
        except KeyError as exc:
            raise RunnerNotConfiguredError(
                f"No lifecycle runner configured for operation {operation.value}"
            ) from exc


def build_default_registry() -> LifecycleRunnerRegistry:
    """Build the worker-only provision registry from trusted configuration."""

    maintenance_dsn = settings.lifecycle_maintenance_database_dsn or settings.database_url.replace(
        "postgresql+asyncpg://", "postgresql://", 1
    )
    provisioner = build_compose_provisioner(
        async_session_factory,
        database_manager=AsyncpgProjectDatabaseManager(maintenance_dsn),
        runtime_root=settings.lifecycle_runtime_root,
        readiness_timeout_seconds=settings.lifecycle_airflow_ready_timeout_seconds,
        readiness_poll_seconds=settings.lifecycle_airflow_ready_poll_seconds,
    )
    return LifecycleRunnerRegistry({LifecycleOperation.PROVISION: provisioner.provision})

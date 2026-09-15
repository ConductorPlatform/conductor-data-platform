from __future__ import annotations

from types import SimpleNamespace

import pytest

import app.services.lifecycle_runner as lifecycle_runner
from app.models.project_lifecycle_job import LifecycleOperation
from app.services.lifecycle_runner import WarehouseMaintenanceConfigurationError
from app.services.project_database import AsyncpgProjectDatabaseManager
from app.services.project_warehouse import ProjectWarehouseManager


def _settings(*, warehouse_dsn: str | None) -> SimpleNamespace:
    return SimpleNamespace(
        lifecycle_maintenance_database_dsn="postgresql://metadata-operator@metadata:5432/postgres",
        database_url="postgresql+asyncpg://conductor@metadata:5432/conductor",
        lifecycle_warehouse_maintenance_dsn=warehouse_dsn,
        lifecycle_runtime_root="/tmp/conductor-runtimes",
        lifecycle_runtime_secret_root="/tmp/conductor-runtime-secrets",
        lifecycle_runtime_artifact_root="/tmp/conductor-runtime-artifacts",
        lifecycle_runtime_ingress_network="conductor-runtime-ingress",
        lifecycle_airflow_image="conductor-airflow:test",
        lifecycle_airflow_database_host="metadata",
        lifecycle_airflow_database_port=5432,
        lifecycle_warehouse_host="warehouse",
        lifecycle_warehouse_port=5433,
        lifecycle_airflow_ready_timeout_seconds=120.0,
        lifecycle_airflow_ready_poll_seconds=2.0,
    )


def test_default_registry_refuses_metadata_database_as_warehouse(monkeypatch) -> None:
    monkeypatch.setattr(lifecycle_runner, "settings", _settings(warehouse_dsn=None))

    with pytest.raises(WarehouseMaintenanceConfigurationError) as error:
        lifecycle_runner.build_default_registry()

    assert error.value.code == "WAREHOUSE_MAINTENANCE_DSN_REQUIRED"


def test_default_registry_constructs_a_separate_fenced_warehouse_manager(monkeypatch) -> None:
    captured = {}

    class Provisioner:
        async def provision(self, _claimed) -> None:
            return None

    def build_provisioner(_session_factory, **kwargs):
        captured.update(kwargs)
        return Provisioner()

    warehouse_dsn = "postgresql://warehouse-operator@warehouse:5433/postgres"
    monkeypatch.setattr(lifecycle_runner, "settings", _settings(warehouse_dsn=warehouse_dsn))
    monkeypatch.setattr(lifecycle_runner, "build_compose_provisioner", build_provisioner)

    registry = lifecycle_runner.build_default_registry()

    assert registry.resolve(LifecycleOperation.PROVISION)
    assert isinstance(captured["database_manager"], AsyncpgProjectDatabaseManager)
    assert captured["database_manager"]._maintenance_dsn == "postgresql://metadata-operator@metadata:5432/postgres"
    assert isinstance(captured["warehouse_manager"], ProjectWarehouseManager)
    assert captured["warehouse_manager"]._maintenance_dsn == warehouse_dsn
    warehouse_database_manager = captured["warehouse_manager"]._database_manager
    assert isinstance(warehouse_database_manager, AsyncpgProjectDatabaseManager)
    assert warehouse_database_manager._resource_kind == "warehouse"

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from app.services.project_database import ObservedDatabaseResource
from app.services.project_warehouse import ProjectWarehouseManager

PROJECT_ID = "0123456789abcdef0123456789abcdef"
WAREHOUSE_NAME = f"conductor_warehouse_{PROJECT_ID}"
COMMENT = f"conductor.project_id={PROJECT_ID}"


class _DatabaseManager:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def ensure_role(self, deployment) -> ObservedDatabaseResource:
        self.calls.append("role")
        return ObservedDatabaseResource("role", deployment.airflow_db_role, deployment.project_id)

    async def ensure_database(self, deployment) -> ObservedDatabaseResource:
        self.calls.append("database")
        return ObservedDatabaseResource(
            "database", deployment.airflow_db_name, deployment.project_id, owner=deployment.airflow_db_role
        )


class _Connection:
    def __init__(self) -> None:
        self.rows = [None, {"owner": WAREHOUSE_NAME, "comment": COMMENT}]
        self.calls: list[tuple[str, str, tuple]] = []
        self.closed = False

    async def fetchrow(self, sql, *args):
        self.calls.append(("fetchrow", sql, args))
        return self.rows.pop(0)

    async def fetchval(self, sql, *args):
        self.calls.append(("fetchval", sql, args))
        return 'COMMENT ON SCHEMA "analytics" IS \'conductor.project_id=0123456789abcdef0123456789abcdef\''

    async def execute(self, sql, *args):
        self.calls.append(("execute", sql, args))
        return "OK"

    async def close(self):
        self.closed = True


def deployment(**overrides):
    values = {
        "project_id": PROJECT_ID,
        "warehouse_db_name": WAREHOUSE_NAME,
        "warehouse_db_role": WAREHOUSE_NAME,
        "warehouse_db_password_encrypted": "encrypted-warehouse-password",
        "warehouse_schema": "analytics",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.asyncio
async def test_warehouse_creates_only_deterministic_owned_resources_and_private_seed() -> None:
    connection = _Connection()
    connect = AsyncMock(return_value=connection)
    database = _DatabaseManager()
    manager = ProjectWarehouseManager(
        "postgresql://operator:operator-password@warehouse:5432/operator_db",
        connect=connect,
        database_manager=cast(Any, database),
    )

    resources = await manager.ensure_warehouse(cast(Any, deployment()))

    assert database.calls == ["role", "database"]
    connect.assert_awaited_once_with(f"postgresql://operator:operator-password@warehouse:5432/{WAREHOUSE_NAME}")
    assert [(resource.kind, resource.name, resource.owner) for resource in resources] == [
        ("role", WAREHOUSE_NAME, None),
        ("database", WAREHOUSE_NAME, WAREHOUSE_NAME),
        ("schema", "analytics", WAREHOUSE_NAME),
    ]
    statements = [sql for kind, sql, _ in connection.calls if kind == "execute"]
    assert any('CREATE SCHEMA "analytics" AUTHORIZATION' in sql for sql in statements)
    assert any("CREATE TABLE IF NOT EXISTS" in sql and "synthetic_orders" in sql for sql in statements)
    assert any("INSERT INTO" in sql and "ON CONFLICT" in sql for sql in statements)
    assert "RESET ROLE" in statements
    assert all("encrypted-warehouse-password" not in sql for sql in statements)
    assert connection.closed is True


@pytest.mark.asyncio
async def test_warehouse_rejects_another_projects_identity_before_connecting() -> None:
    connect = AsyncMock()
    manager = ProjectWarehouseManager(
        "postgresql://operator@warehouse:5432/operator_db",
        connect=connect,
        database_manager=cast(Any, _DatabaseManager()),
    )

    with pytest.raises(ValueError, match="identity"):
        await manager.ensure_warehouse(
            cast(Any, deployment(warehouse_db_name="conductor_warehouse_other"))
        )

    connect.assert_not_awaited()

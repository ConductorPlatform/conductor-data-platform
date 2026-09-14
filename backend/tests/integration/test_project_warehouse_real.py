from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import asyncpg
import pytest

from app.services.project_warehouse import ProjectWarehouseManager


MAINTENANCE_DSN = os.environ.get(
    "PROJECT_WAREHOUSE_TEST_MAINTENANCE_DSN",
    "postgresql://conductor:***@postgres:5432/postgres",
)


def deployment_for(project_id: str, ciphertext: str) -> SimpleNamespace:
    warehouse_name = f"conductor_warehouse_{project_id}"
    return SimpleNamespace(
        project_id=project_id,
        warehouse_db_name=warehouse_name,
        warehouse_db_role=warehouse_name,
        warehouse_db_password_encrypted=ciphertext,
        warehouse_schema="analytics",
    )


def quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


async def public_can_connect(connection: asyncpg.Connection, database_name: str) -> bool:
    return await connection.fetchval(
        """
        SELECT EXISTS(
            SELECT 1
            FROM pg_catalog.pg_database AS database
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(database.datacl, pg_catalog.acldefault('d', database.datdba))
            ) AS privilege
            WHERE database.datname = $1
              AND privilege.grantee = 0
              AND privilege.privilege_type = 'CONNECT'
        )
        """,
        database_name,
    )


async def cleanup_warehouse(connection: asyncpg.Connection, deployment: SimpleNamespace) -> None:
    expected_comment = f"conductor.project_id={deployment.project_id}"
    database = await connection.fetchrow(
        """
        SELECT database.oid,
               pg_catalog.pg_get_userbyid(database.datdba) AS owner,
               pg_catalog.shobj_description(database.oid, 'pg_database') AS comment
        FROM pg_catalog.pg_database AS database
        WHERE database.datname = $1
        """,
        deployment.warehouse_db_name,
    )
    if (
        database is not None
        and database["owner"] == deployment.warehouse_db_role
        and database["comment"] == expected_comment
    ):
        database_identifier = quote(deployment.warehouse_db_name)
        await connection.execute(f"REVOKE CONNECT ON DATABASE {database_identifier} FROM PUBLIC")
        await connection.execute(f"ALTER DATABASE {database_identifier} ALLOW_CONNECTIONS false")
        await connection.execute(
            """
            SELECT pg_catalog.pg_terminate_backend(activity.pid)
            FROM pg_catalog.pg_stat_activity AS activity
            WHERE activity.datname = $1
              AND activity.pid <> pg_catalog.pg_backend_pid()
            """,
            deployment.warehouse_db_name,
        )
        await connection.execute(f"DROP DATABASE {database_identifier}")

    role = await connection.fetchrow(
        """
        SELECT role.oid,
               pg_catalog.shobj_description(role.oid, 'pg_authid') AS comment
        FROM pg_catalog.pg_roles AS role
        WHERE role.rolname = $1
        """,
        deployment.warehouse_db_role,
    )
    if role is not None and role["comment"] == expected_comment:
        await connection.execute(f"DROP ROLE {quote(deployment.warehouse_db_role)}")


async def connect_as_project(
    deployment: SimpleNamespace, password: str, *, database_name: str | None = None
) -> asyncpg.Connection:
    return await asyncpg.connect(
        MAINTENANCE_DSN,
        user=deployment.warehouse_db_role,
        password=password,
        database=database_name or deployment.warehouse_db_name,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_default_warehouse_manager_isolates_real_postgresql_project_logins() -> None:
    project_a = deployment_for(uuid4().hex, "ciphertext-a")
    project_b = deployment_for(uuid4().hex, "ciphertext-b")
    passwords = {"ciphertext-a": uuid4().hex, "ciphertext-b": uuid4().hex}
    manager = ProjectWarehouseManager(MAINTENANCE_DSN, decrypt=passwords.__getitem__)
    operator = await asyncpg.connect(MAINTENANCE_DSN)
    try:
        resources_a = await manager.ensure_warehouse(cast(Any, project_a))
        resources_b = await manager.ensure_warehouse(cast(Any, project_b))
        assert await manager.ensure_warehouse(cast(Any, project_a)) == resources_a
        assert resources_b[1].name == project_b.warehouse_db_name
        assert await public_can_connect(operator, project_a.warehouse_db_name) is False
        assert await public_can_connect(operator, project_b.warehouse_db_name) is False

        project_a_connection = await connect_as_project(project_a, passwords["ciphertext-a"])
        try:
            assert await project_a_connection.fetchval(
                "SELECT count(*) FROM analytics.synthetic_orders"
            ) == 3
        finally:
            await project_a_connection.close()

        with pytest.raises(asyncpg.PostgresError) as denied:
            project_b_connection = await connect_as_project(
                project_b,
                passwords["ciphertext-b"],
                database_name=project_a.warehouse_db_name,
            )
            try:
                await project_b_connection.fetchval("SELECT count(*) FROM analytics.synthetic_orders")
            finally:
                await project_b_connection.close()
        assert denied.value.sqlstate in {"28000", "42501"}
    finally:
        try:
            await cleanup_warehouse(operator, project_b)
        finally:
            try:
                await cleanup_warehouse(operator, project_a)
            finally:
                await operator.close()

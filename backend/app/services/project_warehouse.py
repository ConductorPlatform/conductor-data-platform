"""Safe lifecycle management of isolated project PostgreSQL warehouses."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import re
from types import SimpleNamespace
from typing import AsyncIterator, Awaitable, Callable, Literal, cast
from urllib.parse import urlsplit, urlunsplit

import asyncpg

from app.models.project_deployment import ProjectDeployment
from app.services.lifecycle_errors import ForeignResourceConflictError
from app.services.project_database import AsyncpgProjectDatabaseManager, ProjectDatabaseManager

_PROJECT_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_SCHEMA_QUERY = """
    SELECT namespace.oid,
           pg_catalog.pg_get_userbyid(namespace.nspowner) AS owner,
           pg_catalog.obj_description(namespace.oid, 'pg_namespace') AS comment
    FROM pg_catalog.pg_namespace AS namespace
    WHERE namespace.nspname = $1
"""
Connect = Callable[[str], Awaitable[asyncpg.Connection]]


def _required(value: str | None, field: str) -> str:
    if not value:
        raise ValueError(f"Warehouse {field} must be configured")
    return value


@dataclass(frozen=True, slots=True)
class ObservedWarehouseResource:
    kind: Literal["role", "database", "schema"]
    name: str
    project_id: str
    owner: str | None = None


class ProjectWarehouseManager:
    """Provision only deterministic, Conductor-owned project warehouses.

    This deliberately reuses the fenced database/role implementation instead
    of treating the Airflow metadata database as a user-data warehouse.
    """

    def __init__(
        self,
        maintenance_dsn: str,
        *,
        connect: Connect = asyncpg.connect,
        database_manager: ProjectDatabaseManager | None = None,
    ) -> None:
        self._maintenance_dsn = maintenance_dsn
        self._connect = connect
        self._database_manager = database_manager or AsyncpgProjectDatabaseManager(
            maintenance_dsn, connect=connect
        )

    @staticmethod
    def _validate(deployment: ProjectDeployment) -> None:
        if not _PROJECT_ID_RE.fullmatch(deployment.project_id):
            raise ValueError("project_id must be exactly 32 lowercase hexadecimal characters")
        expected = f"conductor_warehouse_{deployment.project_id}"
        if deployment.warehouse_db_name != expected or deployment.warehouse_db_role != expected:
            raise ValueError("Warehouse database identity must match the immutable project identity")
        if not deployment.warehouse_db_password_encrypted:
            raise ValueError("Warehouse password must be configured")
        if deployment.warehouse_schema != "analytics":
            raise ValueError("Warehouse schema must be the supported analytics schema")

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    @staticmethod
    def _ownership_comment(project_id: str) -> str:
        return f"conductor.project_id={project_id}"

    def _database_deployment(self, deployment: ProjectDeployment) -> SimpleNamespace:
        self._validate(deployment)
        return SimpleNamespace(
            project_id=deployment.project_id,
            airflow_db_name=deployment.warehouse_db_name,
            airflow_db_role=deployment.warehouse_db_role,
            airflow_db_password_encrypted=deployment.warehouse_db_password_encrypted,
        )

    def _project_database_dsn(self, database_name: str) -> str:
        parsed = urlsplit(self._maintenance_dsn)
        if parsed.scheme not in {"postgresql", "postgres"} or not parsed.netloc:
            raise ValueError("Warehouse maintenance DSN must be a PostgreSQL connection URL")
        return urlunsplit((parsed.scheme, parsed.netloc, f"/{database_name}", "", ""))

    @asynccontextmanager
    async def _schema_connection(self, deployment: ProjectDeployment) -> AsyncIterator[asyncpg.Connection]:
        connection = await self._connect(
            self._project_database_dsn(_required(deployment.warehouse_db_name, "database name"))
        )
        locked = False
        try:
            await connection.execute(
                "SELECT pg_catalog.pg_advisory_lock(pg_catalog.hashtextextended($1, 0))",
                f"conductor.project_warehouse:{deployment.project_id}",
            )
            locked = True
            yield connection
        finally:
            try:
                if locked:
                    await connection.execute(
                        "SELECT pg_catalog.pg_advisory_unlock(pg_catalog.hashtextextended($1, 0))",
                        f"conductor.project_warehouse:{deployment.project_id}",
                    )
            finally:
                await connection.close()

    async def ensure_warehouse(
        self, deployment: ProjectDeployment
    ) -> tuple[ObservedWarehouseResource, ObservedWarehouseResource, ObservedWarehouseResource]:
        """Ensure one owned DB/login/schema and synthetic sample table per project."""
        proxy = self._database_deployment(deployment)
        role = await self._database_manager.ensure_role(cast(ProjectDeployment, proxy))
        database = await self._database_manager.ensure_database(cast(ProjectDeployment, proxy))
        comment = self._ownership_comment(deployment.project_id)
        database_name = _required(deployment.warehouse_db_name, "database name")
        role_name = _required(deployment.warehouse_db_role, "database role")
        schema_name = _required(deployment.warehouse_schema, "schema")
        schema_identifier = self._quote_identifier(schema_name)
        role_identifier = self._quote_identifier(role_name)

        async with self._schema_connection(deployment) as connection:
            schema = await connection.fetchrow(_SCHEMA_QUERY, schema_name)
            if schema is None:
                await connection.execute(
                    f"CREATE SCHEMA {schema_identifier} AUTHORIZATION {role_identifier}"
                )
                comment_sql = await connection.fetchval(
                    f"SELECT format('COMMENT ON SCHEMA {schema_identifier} IS %L', $1::text)", comment
                )
                await connection.execute(comment_sql)
                schema = await connection.fetchrow(_SCHEMA_QUERY, schema_name)
            if (
                schema is None
                or schema["owner"] != role_name
                or schema["comment"] != comment
            ):
                raise ForeignResourceConflictError(
                    "Warehouse schema exists without matching Conductor ownership"
                )
            # The seed is deterministic and remains private to the project's login role.
            await connection.execute(f"SET ROLE {role_identifier}")
            try:
                await connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {schema_identifier}.synthetic_orders (
                        order_id integer PRIMARY KEY,
                        customer_name text NOT NULL,
                        amount numeric(12, 2) NOT NULL,
                        ordered_at date NOT NULL
                    )
                    """
                )
                await connection.execute(
                    f"""
                    INSERT INTO {schema_identifier}.synthetic_orders
                        (order_id, customer_name, amount, ordered_at)
                    VALUES
                        (1, 'Ada', 19.99, DATE '2026-01-01'),
                        (2, 'Linus', 42.00, DATE '2026-01-02'),
                        (3, 'Grace', 8.50, DATE '2026-01-03')
                    ON CONFLICT (order_id) DO NOTHING
                    """
                )
            finally:
                await connection.execute("RESET ROLE")

        return (
            ObservedWarehouseResource("role", role.name, deployment.project_id),
            ObservedWarehouseResource(
                "database", database.name, deployment.project_id, owner=role_name
            ),
            ObservedWarehouseResource(
                "schema", schema_name, deployment.project_id, owner=role_name
            ),
        )

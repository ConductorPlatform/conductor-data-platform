"""Safe management of per-project Airflow PostgreSQL roles and databases."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import re
from typing import AsyncIterator, Awaitable, Callable, Literal, Protocol

import asyncpg

from app.models.project_deployment import ProjectDeployment
from app.services.crypto import decrypt_token
from app.services.lifecycle_errors import ForeignResourceConflictError


_PROJECT_ID_RE = re.compile(r"^[0-9a-f]{32}$")


@dataclass(frozen=True, slots=True)
class ObservedDatabaseResource:
    kind: Literal["role", "database"]
    name: str
    project_id: str
    owner: str | None = None


class ProjectDatabaseManager(Protocol):
    async def ensure_role(self, deployment: ProjectDeployment) -> ObservedDatabaseResource: ...

    async def ensure_database(self, deployment: ProjectDeployment) -> ObservedDatabaseResource: ...

    async def drop_database(self, deployment: ProjectDeployment) -> None: ...

    async def drop_role(self, deployment: ProjectDeployment) -> None: ...

    async def verify_absent(self, deployment: ProjectDeployment) -> bool: ...


Connect = Callable[[str], Awaitable[asyncpg.Connection]]
Decrypt = Callable[[str], str]


_ROLE_QUERY = """
    SELECT role.oid,
           pg_catalog.shobj_description(role.oid, 'pg_authid') AS comment
    FROM pg_catalog.pg_roles AS role
    WHERE role.rolname = $1
"""
_DATABASE_QUERY = """
    SELECT db.oid,
           pg_catalog.pg_get_userbyid(db.datdba) AS owner,
           pg_catalog.shobj_description(db.oid, 'pg_database') AS comment
    FROM pg_catalog.pg_database AS db
    WHERE db.datname = $1
"""


class AsyncpgProjectDatabaseManager:
    def __init__(
        self,
        maintenance_dsn: str,
        *,
        connect: Connect = asyncpg.connect,
        decrypt: Decrypt = decrypt_token,
        resource_kind: Literal["airflow", "warehouse"] = "airflow",
    ) -> None:
        if resource_kind not in {"airflow", "warehouse"}:
            raise ValueError("resource_kind must be airflow or warehouse")
        self._maintenance_dsn = maintenance_dsn
        self._connect = connect
        self._decrypt = decrypt
        self._resource_kind = resource_kind

    def _validate(self, deployment: ProjectDeployment) -> None:
        if not _PROJECT_ID_RE.fullmatch(deployment.project_id):
            raise ValueError("project_id must be exactly 32 lowercase hexadecimal characters")
        expected = f"conductor_{self._resource_kind}_{deployment.project_id}"
        for field in ("airflow_db_name", "airflow_db_role"):
            if getattr(deployment, field) != expected:
                raise ValueError(f"{field} must equal the deterministic project database identity")

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    @staticmethod
    def _ownership_comment(project_id: str) -> str:
        return f"conductor.project_id={project_id}"

    def _lock_identity(self, project_id: str) -> str:
        if self._resource_kind == "airflow":
            return f"conductor.project_database:{project_id}"
        return f"conductor.project_warehouse_database:{project_id}"

    @asynccontextmanager
    async def _fenced_connection(
        self, deployment: ProjectDeployment
    ) -> AsyncIterator[asyncpg.Connection]:
        connection = await self._connect(self._maintenance_dsn)
        locked = False
        try:
            await connection.execute(
                "SELECT pg_catalog.pg_advisory_lock(pg_catalog.hashtextextended($1, 0))",
                self._lock_identity(deployment.project_id),
            )
            locked = True
            yield connection
        finally:
            try:
                if locked:
                    await connection.execute(
                        "SELECT pg_catalog.pg_advisory_unlock("
                        "pg_catalog.hashtextextended($1, 0))",
                        self._lock_identity(deployment.project_id),
                    )
            finally:
                await connection.close()

    @staticmethod
    def _require_owned_role(row, expected_comment: str, *, message: str) -> None:
        if row is None or row["comment"] != expected_comment:
            raise ForeignResourceConflictError(message)

    @staticmethod
    def _require_owned_database(row, owner: str, expected_comment: str, *, message: str) -> None:
        if row is None or row["owner"] != owner or row["comment"] != expected_comment:
            raise ForeignResourceConflictError(message)

    @staticmethod
    def _same_role(left, right) -> bool:
        return (
            left is not None
            and right is not None
            and right["oid"] == left["oid"]
            and right["comment"] == left["comment"]
        )

    @staticmethod
    def _same_database(left, right) -> bool:
        return (
            left is not None
            and right is not None
            and right["oid"] == left["oid"]
            and right["owner"] == left["owner"]
            and right["comment"] == left["comment"]
        )

    async def ensure_role(self, deployment: ProjectDeployment) -> ObservedDatabaseResource:
        self._validate(deployment)
        expected_comment = self._ownership_comment(deployment.project_id)
        async with self._fenced_connection(deployment) as connection:
            role = await connection.fetchrow(_ROLE_QUERY, deployment.airflow_db_role)
            if role is not None:
                self._require_owned_role(
                    role,
                    expected_comment,
                    message="PostgreSQL role exists without matching Conductor ownership",
                )
            else:
                identifier = self._quote_identifier(deployment.airflow_db_role)
                password = self._decrypt(deployment.airflow_db_password_encrypted)
                async with connection.transaction():
                    create_sql = await connection.fetchval(
                        f"SELECT format('CREATE ROLE {identifier} LOGIN PASSWORD %L', $1::text)",
                        password,
                    )
                    await connection.execute(create_sql)
                    comment_sql = await connection.fetchval(
                        f"SELECT format('COMMENT ON ROLE {identifier} IS %L', $1::text)",
                        expected_comment,
                    )
                    await connection.execute(comment_sql)

            return ObservedDatabaseResource(
                kind="role",
                name=deployment.airflow_db_role,
                project_id=deployment.project_id,
            )

    async def ensure_database(self, deployment: ProjectDeployment) -> ObservedDatabaseResource:
        self._validate(deployment)
        expected_comment = self._ownership_comment(deployment.project_id)
        async with self._fenced_connection(deployment) as connection:
            role = await connection.fetchrow(_ROLE_QUERY, deployment.airflow_db_role)
            self._require_owned_role(
                role,
                expected_comment,
                message="PostgreSQL owner role is absent or lacks matching Conductor ownership",
            )

            database = await connection.fetchrow(_DATABASE_QUERY, deployment.airflow_db_name)
            if database is not None:
                self._require_owned_database(
                    database,
                    deployment.airflow_db_role,
                    expected_comment,
                    message="PostgreSQL database exists without matching Conductor ownership",
                )
            else:
                current_role = await connection.fetchrow(
                    _ROLE_QUERY, deployment.airflow_db_role
                )
                if not self._same_role(role, current_role):
                    raise ForeignResourceConflictError(
                        "PostgreSQL owner role changed before database creation"
                    )

                database_identifier = self._quote_identifier(deployment.airflow_db_name)
                role_identifier = self._quote_identifier(deployment.airflow_db_role)
                await connection.execute(
                    f"CREATE DATABASE {database_identifier} OWNER {role_identifier}"
                )
                created = await connection.fetchrow(_DATABASE_QUERY, deployment.airflow_db_name)
                if (
                    created is None
                    or created["owner"] != deployment.airflow_db_role
                    or created["comment"] is not None
                ):
                    raise ForeignResourceConflictError(
                        "Created PostgreSQL database identity could not be proven"
                    )

                try:
                    comment_sql = await connection.fetchval(
                        f"SELECT format('COMMENT ON DATABASE {database_identifier} IS %L', "
                        "$1::text)",
                        expected_comment,
                    )
                    await connection.execute(comment_sql)
                except BaseException:
                    current = await connection.fetchrow(
                        _DATABASE_QUERY, deployment.airflow_db_name
                    )
                    if self._same_database(created, current):
                        await connection.execute(f"DROP DATABASE {database_identifier}")
                    raise

                current = await connection.fetchrow(
                    _DATABASE_QUERY, deployment.airflow_db_name
                )
                if (
                    current is None
                    or current["oid"] != created["oid"]
                    or current["owner"] != deployment.airflow_db_role
                    or current["comment"] != expected_comment
                ):
                    if self._same_database(created, current):
                        await connection.execute(f"DROP DATABASE {database_identifier}")
                    raise ForeignResourceConflictError(
                        "Created PostgreSQL database ownership could not be proven after comment"
                    )

            # PostgreSQL grants CONNECT to PUBLIC by default. Explicitly revoke
            # it on every convergence so only the owning project role can open
            # this project-scoped database.
            database_identifier = self._quote_identifier(deployment.airflow_db_name)
            await connection.execute(f"REVOKE CONNECT ON DATABASE {database_identifier} FROM PUBLIC")

            return ObservedDatabaseResource(
                kind="database",
                name=deployment.airflow_db_name,
                project_id=deployment.project_id,
                owner=deployment.airflow_db_role,
            )

    async def drop_database(self, deployment: ProjectDeployment) -> None:
        self._validate(deployment)
        expected_comment = self._ownership_comment(deployment.project_id)
        async with self._fenced_connection(deployment) as connection:
            database = await connection.fetchrow(_DATABASE_QUERY, deployment.airflow_db_name)
            if database is None:
                return
            self._require_owned_database(
                database,
                deployment.airflow_db_role,
                expected_comment,
                message="PostgreSQL database exists without matching Conductor ownership",
            )

            identifier = self._quote_identifier(deployment.airflow_db_name)
            await connection.execute(f"REVOKE CONNECT ON DATABASE {identifier} FROM PUBLIC")
            await connection.execute(f"ALTER DATABASE {identifier} ALLOW_CONNECTIONS false")
            await connection.execute(
                """
                SELECT pg_catalog.pg_terminate_backend(activity.pid)
                FROM pg_catalog.pg_stat_activity AS activity
                WHERE activity.datname = $1
                  AND activity.pid <> pg_catalog.pg_backend_pid()
                """,
                deployment.airflow_db_name,
            )
            current = await connection.fetchrow(_DATABASE_QUERY, deployment.airflow_db_name)
            if not self._same_database(database, current):
                raise ForeignResourceConflictError(
                    "PostgreSQL database identity changed before final drop"
                )
            await connection.execute(f"DROP DATABASE {identifier}")

    async def drop_role(self, deployment: ProjectDeployment) -> None:
        self._validate(deployment)
        expected_comment = self._ownership_comment(deployment.project_id)
        async with self._fenced_connection(deployment) as connection:
            async with connection.transaction():
                role = await connection.fetchrow(_ROLE_QUERY, deployment.airflow_db_role)
                if role is None:
                    return
                self._require_owned_role(
                    role,
                    expected_comment,
                    message="PostgreSQL role exists without matching Conductor ownership",
                )
                current = await connection.fetchrow(_ROLE_QUERY, deployment.airflow_db_role)
                if not self._same_role(role, current):
                    raise ForeignResourceConflictError(
                        "PostgreSQL role identity changed before final drop"
                    )
                await connection.execute(
                    f"DROP ROLE {self._quote_identifier(deployment.airflow_db_role)}"
                )

    async def verify_absent(self, deployment: ProjectDeployment) -> bool:
        self._validate(deployment)
        expected_comment = self._ownership_comment(deployment.project_id)
        async with self._fenced_connection(deployment) as connection:
            database = await connection.fetchrow(_DATABASE_QUERY, deployment.airflow_db_name)
            role = await connection.fetchrow(_ROLE_QUERY, deployment.airflow_db_role)
            if database is not None:
                self._require_owned_database(
                    database,
                    deployment.airflow_db_role,
                    expected_comment,
                    message="PostgreSQL database exists without matching Conductor ownership",
                )
            if role is not None:
                self._require_owned_role(
                    role,
                    expected_comment,
                    message="PostgreSQL role exists without matching Conductor ownership",
                )
            return database is None and role is None

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services.lifecycle_errors import ForeignResourceConflictError
from app.services.project_database import AsyncpgProjectDatabaseManager, ObservedDatabaseResource


PROJECT_ID = "0123456789abcdef0123456789abcdef"
RESOURCE_NAME = f"conductor_airflow_{PROJECT_ID}"
COMMENT = f"conductor.project_id={PROJECT_ID}"


class FakeTransaction:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        self.connection.calls.append(("transaction", "BEGIN", ()))
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        outcome = "ROLLBACK" if exc_type is not None else "COMMIT"
        self.connection.calls.append(("transaction", outcome, ()))
        return False


class FakeConnection:
    def __init__(self, *, rows=None, formatted=None, fail_on=None):
        self.rows = list(rows or [])
        self.formatted = list(formatted or [])
        self.fail_on = fail_on
        self.calls = []
        self.closed = False

    def transaction(self):
        return FakeTransaction(self)

    async def fetchrow(self, sql, *args):
        self.calls.append(("fetchrow", sql, args))
        return self.rows.pop(0) if self.rows else None

    async def fetchval(self, sql, *args):
        self.calls.append(("fetchval", sql, args))
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("database operation failed")
        return self.formatted.pop(0)

    async def execute(self, sql, *args):
        self.calls.append(("execute", sql, args))
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("database operation failed")
        return "OK"

    async def close(self):
        self.calls.append(("close", "", ()))
        self.closed = True


def deployment(**overrides):
    values = {
        "project_id": PROJECT_ID,
        "airflow_db_name": RESOURCE_NAME,
        "airflow_db_role": RESOURCE_NAME,
        "airflow_db_password_encrypted": "encrypted-password",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def mutation_sql(connection):
    return [
        sql
        for kind, sql, _ in connection.calls
        if kind == "execute" and not sql.startswith("SELECT pg_catalog.pg_advisory_")
    ]


def assert_fenced(connection):
    assert "pg_advisory_lock" in connection.calls[0][1]
    assert connection.calls[0][2] == (f"conductor.project_database:{PROJECT_ID}",)
    assert "pg_advisory_unlock" in connection.calls[-2][1]
    assert connection.calls[-1][0] == "close"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("project_id", "A" * 32),
        ("project_id", "a" * 31),
        ("airflow_db_name", "postgres"),
        ("airflow_db_name", f"conductor_airflow_{'f' * 32}"),
        ("airflow_db_role", "valid_but_nondeterministic"),
    ],
)
async def test_rejects_invalid_or_nondeterministic_identity_before_connect(field, value):
    connect = AsyncMock()
    manager = AsyncpgProjectDatabaseManager("maintenance-dsn", connect=connect)

    with pytest.raises(ValueError):
        await manager.ensure_role(deployment(**{field: value}))

    connect.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_role_is_fenced_and_creates_role_and_comment_in_one_transaction():
    password = "p'ass; -- secret"
    connection = FakeConnection(
        rows=[None],
        formatted=[
            f'CREATE ROLE "{RESOURCE_NAME}" LOGIN PASSWORD \'server-quoted\'',
            f'COMMENT ON ROLE "{RESOURCE_NAME}" IS \'server-quoted\'',
        ],
    )
    manager = AsyncpgProjectDatabaseManager(
        "maintenance-dsn", connect=AsyncMock(return_value=connection), decrypt=lambda _: password
    )

    observed = await manager.ensure_role(deployment())

    assert observed == ObservedDatabaseResource("role", RESOURCE_NAME, PROJECT_ID)
    assert_fenced(connection)
    assert ("transaction", "BEGIN", ()) in connection.calls
    assert ("transaction", "COMMIT", ()) in connection.calls
    format_calls = [call for call in connection.calls if call[0] == "fetchval"]
    assert format_calls[0][2] == (password,)
    assert format_calls[1][2] == (COMMENT,)


@pytest.mark.asyncio
async def test_role_comment_failure_rolls_back_and_unlocks_before_close():
    connection = FakeConnection(
        rows=[None],
        formatted=[f'CREATE ROLE "{RESOURCE_NAME}" LOGIN', f'COMMENT ON ROLE "{RESOURCE_NAME}"'],
        fail_on="COMMENT ON ROLE",
    )
    manager = AsyncpgProjectDatabaseManager(
        "maintenance-dsn", connect=AsyncMock(return_value=connection), decrypt=lambda _: "secret"
    )

    with pytest.raises(RuntimeError):
        await manager.ensure_role(deployment())

    assert ("transaction", "ROLLBACK", ()) in connection.calls
    assert_fenced(connection)


@pytest.mark.asyncio
async def test_ensure_role_adopts_only_matching_comment_and_rejects_foreign_without_mutation():
    owned = FakeConnection(rows=[{"oid": 10, "comment": COMMENT}])
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=owned))
    assert await manager.ensure_role(deployment()) == ObservedDatabaseResource(
        "role", RESOURCE_NAME, PROJECT_ID
    )
    assert mutation_sql(owned) == []
    assert_fenced(owned)

    foreign = FakeConnection(rows=[{"oid": 10, "comment": None}])
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=foreign))
    with pytest.raises(ForeignResourceConflictError):
        await manager.ensure_role(deployment())
    assert mutation_sql(foreign) == []
    assert_fenced(foreign)


@pytest.mark.asyncio
async def test_ensure_database_revalidates_role_creates_and_comments_captured_database():
    connection = FakeConnection(
        rows=[
            {"oid": 11, "comment": COMMENT},
            None,
            {"oid": 11, "comment": COMMENT},
            {"oid": 22, "owner": RESOURCE_NAME, "comment": None},
            {"oid": 22, "owner": RESOURCE_NAME, "comment": COMMENT},
        ],
        formatted=[f'COMMENT ON DATABASE "{RESOURCE_NAME}" IS \'server-quoted\''],
    )
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=connection))

    observed = await manager.ensure_database(deployment())

    assert observed == ObservedDatabaseResource(
        "database", RESOURCE_NAME, PROJECT_ID, RESOURCE_NAME
    )
    mutations = mutation_sql(connection)
    assert mutations[0] == f'CREATE DATABASE "{RESOURCE_NAME}" OWNER "{RESOURCE_NAME}"'
    assert mutations[1].startswith(f'COMMENT ON DATABASE "{RESOURCE_NAME}" IS ')
    assert mutations[2] == f'REVOKE CONNECT ON DATABASE "{RESOURCE_NAME}" FROM PUBLIC'
    assert_fenced(connection)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("catalog_state", "expects_compensation"),
    [
        (None, False),
        ({"oid": 23, "owner": RESOURCE_NAME, "comment": COMMENT}, False),
        ({"oid": 22, "owner": "other", "comment": COMMENT}, False),
        ({"oid": 22, "owner": RESOURCE_NAME, "comment": None}, True),
        ({"oid": 22, "owner": RESOURCE_NAME, "comment": "wrong"}, False),
    ],
    ids=["absent", "replaced", "foreign-owned", "unmarked", "mismarked"],
)
async def test_database_comment_apparent_success_requires_exact_catalog_state(
    catalog_state, expects_compensation
):
    connection = FakeConnection(
        rows=[
            {"oid": 11, "comment": COMMENT},
            None,
            {"oid": 11, "comment": COMMENT},
            {"oid": 22, "owner": RESOURCE_NAME, "comment": None},
            catalog_state,
        ],
        formatted=[f'COMMENT ON DATABASE "{RESOURCE_NAME}" IS \'server-quoted\''],
    )
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=connection))

    with pytest.raises(ForeignResourceConflictError):
        await manager.ensure_database(deployment())

    drops = [sql for sql in mutation_sql(connection) if sql.startswith("DROP DATABASE")]
    assert drops == ([f'DROP DATABASE "{RESOURCE_NAME}"'] if expects_compensation else [])
    assert_fenced(connection)


@pytest.mark.asyncio
async def test_database_comment_failure_compensates_only_same_unowned_comment_identity():
    connection = FakeConnection(
        rows=[
            {"oid": 11, "comment": COMMENT},
            None,
            {"oid": 11, "comment": COMMENT},
            {"oid": 22, "owner": RESOURCE_NAME, "comment": None},
            {"oid": 22, "owner": RESOURCE_NAME, "comment": None},
        ],
        formatted=[f'COMMENT ON DATABASE "{RESOURCE_NAME}"'],
        fail_on="COMMENT ON DATABASE",
    )
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=connection))

    with pytest.raises(RuntimeError):
        await manager.ensure_database(deployment())

    mutations = mutation_sql(connection)
    assert mutations[-1] == f'DROP DATABASE "{RESOURCE_NAME}"'
    assert_fenced(connection)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "replacement",
    [
        {"oid": 23, "owner": RESOURCE_NAME, "comment": None},
        {"oid": 22, "owner": "other", "comment": None},
        {"oid": 22, "owner": RESOURCE_NAME, "comment": COMMENT},
    ],
)
async def test_database_comment_failure_never_compensates_changed_identity(replacement):
    connection = FakeConnection(
        rows=[
            {"oid": 11, "comment": COMMENT},
            None,
            {"oid": 11, "comment": COMMENT},
            {"oid": 22, "owner": RESOURCE_NAME, "comment": None},
            replacement,
        ],
        formatted=[f'COMMENT ON DATABASE "{RESOURCE_NAME}"'],
        fail_on="COMMENT ON DATABASE",
    )
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=connection))

    with pytest.raises(RuntimeError):
        await manager.ensure_database(deployment())

    assert not any(sql.startswith("DROP DATABASE") for sql in mutation_sql(connection))
    assert_fenced(connection)


@pytest.mark.asyncio
async def test_ensure_database_rejects_foreign_database_without_mutation():
    connection = FakeConnection(
        rows=[
            {"oid": 11, "comment": COMMENT},
            {"oid": 22, "owner": "other", "comment": COMMENT},
        ]
    )
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=connection))

    with pytest.raises(ForeignResourceConflictError):
        await manager.ensure_database(deployment())

    assert mutation_sql(connection) == []
    assert_fenced(connection)


@pytest.mark.asyncio
async def test_drop_database_revalidates_oid_owner_comment_after_termination_before_drop():
    row = {"oid": 22, "owner": RESOURCE_NAME, "comment": COMMENT}
    connection = FakeConnection(rows=[row, dict(row)])
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=connection))

    await manager.drop_database(deployment())

    mutations = mutation_sql(connection)
    assert "REVOKE CONNECT" in mutations[0]
    assert "ALLOW_CONNECTIONS false" in mutations[1]
    assert "pg_terminate_backend" in mutations[2]
    assert mutations[3] == f'DROP DATABASE "{RESOURCE_NAME}"'
    fetch_positions = [i for i, call in enumerate(connection.calls) if call[0] == "fetchrow"]
    drop_position = next(i for i, call in enumerate(connection.calls) if "DROP DATABASE" in call[1])
    assert fetch_positions[-1] == drop_position - 1
    assert_fenced(connection)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "replacement",
    [
        None,
        {"oid": 23, "owner": RESOURCE_NAME, "comment": COMMENT},
        {"oid": 22, "owner": "other", "comment": COMMENT},
        {"oid": 22, "owner": RESOURCE_NAME, "comment": None},
    ],
)
async def test_drop_database_changed_identity_before_final_ddl_prevents_drop(replacement):
    original = {"oid": 22, "owner": RESOURCE_NAME, "comment": COMMENT}
    connection = FakeConnection(rows=[original, replacement])
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=connection))

    with pytest.raises(ForeignResourceConflictError):
        await manager.drop_database(deployment())

    assert not any(sql.startswith("DROP DATABASE") for sql in mutation_sql(connection))
    assert_fenced(connection)


@pytest.mark.asyncio
async def test_drop_role_revalidates_in_transaction_and_rolls_back_on_changed_oid():
    original = {"oid": 31, "comment": COMMENT}
    changed = {"oid": 32, "comment": COMMENT}
    connection = FakeConnection(rows=[original, changed])
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=connection))

    with pytest.raises(ForeignResourceConflictError):
        await manager.drop_role(deployment())

    assert ("transaction", "ROLLBACK", ()) in connection.calls
    assert not any(sql.startswith("DROP ROLE") for sql in mutation_sql(connection))
    assert_fenced(connection)


@pytest.mark.asyncio
async def test_drop_role_owned_resource_is_checked_and_dropped_in_one_transaction():
    row = {"oid": 31, "comment": COMMENT}
    connection = FakeConnection(rows=[row, dict(row)])
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=connection))

    await manager.drop_role(deployment())

    assert mutation_sql(connection) == [f'DROP ROLE "{RESOURCE_NAME}"']
    assert ("transaction", "COMMIT", ()) in connection.calls
    assert_fenced(connection)


@pytest.mark.asyncio
async def test_missing_drop_and_verify_absent_are_idempotent_and_fenced():
    missing_database = FakeConnection(rows=[None])
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=missing_database))
    await manager.drop_database(deployment())
    assert mutation_sql(missing_database) == []
    assert_fenced(missing_database)

    missing_role = FakeConnection(rows=[None])
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=missing_role))
    await manager.drop_role(deployment())
    assert mutation_sql(missing_role) == []
    assert_fenced(missing_role)

    absent = FakeConnection(rows=[None, None])
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=absent))
    assert await manager.verify_absent(deployment()) is True
    assert_fenced(absent)


@pytest.mark.asyncio
async def test_unlock_and_close_are_failure_safe():
    connection = FakeConnection(rows=[None], fail_on="pg_advisory_unlock")
    manager = AsyncpgProjectDatabaseManager("dsn", connect=AsyncMock(return_value=connection))

    with pytest.raises(RuntimeError):
        await manager.drop_database(deployment())

    assert connection.closed is True
    assert connection.calls[-1][0] == "close"

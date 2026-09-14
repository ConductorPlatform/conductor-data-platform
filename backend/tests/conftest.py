from __future__ import annotations

import asyncio
import os

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.auth.jwt import create_access_token
from app.auth.password import hash_password
from app.database import get_db_session
from app.main import create_app
from app.models.base import Base
from app.models.role import Role
from app.models.user import User

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://conductor:conductor@postgres:5432/conductor_test",
)

DEFAULT_ROLES = (
    ("super_admin", "Full access"),
    ("project_admin", "Admin"),
    ("maintainer", "Maintainer"),
    ("developer", "Developer"),
    ("viewer", "Viewer"),
)


async def _seed_defaults(engine) -> None:
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        for name, description in DEFAULT_ROLES:
            session.add(Role(name=name, description=description, is_system=True))
        session.add(
            User(
                id="test-admin-001",
                email="admin@test.local",
                hashed_password=hash_password("admin"),
                display_name="Admin",
                is_active=True,
                is_admin=True,
            )
        )
        await session.commit()


async def _reset_test_database(engine) -> None:
    """Remove committed rows, including rows protected by audit triggers."""
    async with engine.begin() as connection:
        quote = connection.dialect.identifier_preparer.quote
        table_names = [quote(table.name) for table in reversed(Base.metadata.sorted_tables)]
        await connection.execute(text("SET LOCAL session_replication_role = replica"))
        await connection.execute(
            text(f"TRUNCATE TABLE {', '.join(table_names)} RESTART IDENTITY CASCADE")
        )
    await _seed_defaults(engine)


@pytest_asyncio.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def _engine():
    engine = create_async_engine(TEST_DATABASE_URL, echo=False, poolclass=NullPool)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def _init_db(_engine):
    # Import the package so every lifecycle table and DDL hook is registered.
    import app.models  # noqa: F401

    async with _engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    await _seed_defaults(_engine)


@pytest_asyncio.fixture(autouse=True)
async def _isolate_test_database(_engine, _init_db):
    """Give every test the same clean, seeded PostgreSQL state."""
    await _reset_test_database(_engine)
    yield
    await _reset_test_database(_engine)


@pytest_asyncio.fixture
async def db_session(_engine):
    """Real PostgreSQL session whose uncommitted work is isolated per test."""
    factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        try:
            yield session
        finally:
            await session.rollback()


def _make_override(engine):
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _override():
        async with factory() as session:
            try:
                yield session
            finally:
                await session.rollback()

    return _override


@pytest_asyncio.fixture
async def client(_engine):
    app = create_app()
    app.dependency_overrides[get_db_session] = _make_override(_engine)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def admin_client(_engine):
    token = create_access_token(
        user_id="test-admin-001", email="admin@test.local", is_admin=True
    )
    app = create_app()
    app.dependency_overrides[get_db_session] = _make_override(_engine)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        ac.headers["Authorization"] = f"Bearer {token}"
        yield ac

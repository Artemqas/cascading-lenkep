import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import app.db as db_module
from app.db import Base, get_session
from app.main import app
from app.models import Provider


@pytest_asyncio.fixture
async def db_session(monkeypatch):
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    # Background tasks (e.g. the cascade) look up app.db.session_factory at call
    # time, not via get_session — point that at this test's engine too, so they
    # never reach the real (unavailable-in-tests) Postgres engine.
    monkeypatch.setattr(db_module, "session_factory", session_factory)

    async with session_factory() as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def client(db_session):
    async def override_get_session():
        yield db_session

    app.dependency_overrides[get_session] = override_get_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def seed_providers(db_session):
    """Seeds the three mock providers, each with no manual priority (adaptive by
    default). Opt-in per test (not autouse) so other test files' own Provider
    seeding (e.g. with ids "a"/"b") never collides with these ids."""
    providers = [
        Provider(
            id="fast_sync", name="fast_sync", priority=None,
            supported_directions=["deposit", "withdraw"],
            supported_card_types=["visa", "mastercard", "other"],
            min_amount=0, max_amount=1_000_000, enabled=True,
        ),
        Provider(
            id="webhook_async", name="webhook_async", priority=None,
            supported_directions=["deposit", "withdraw"],
            supported_card_types=["visa", "mastercard", "other"],
            min_amount=0, max_amount=1_000_000, enabled=True,
        ),
        Provider(
            id="flaky", name="flaky", priority=None,
            supported_directions=["deposit", "withdraw"],
            supported_card_types=["visa", "mastercard", "other"],
            min_amount=0, max_amount=1_000_000, enabled=True,
        ),
    ]
    for provider in providers:
        db_session.add(provider)
    await db_session.commit()
    return providers

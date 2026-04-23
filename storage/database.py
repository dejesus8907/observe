"""Database engine, session factory, and base declarative model.

Startup wiring belongs here because database configuration is one of the first
places runtime drift becomes operational pain. A service that boots with stale
engine state is not "mostly fine"; it is lying to operators.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import event, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, sessionmaker, Session

from netobserv.config.settings import Settings, get_settings
from netobserv.observability.logging import get_logger


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""

    pass


async_engine: AsyncEngine | None = None
AsyncSessionLocal: async_sessionmaker[AsyncSession] | None = None
runtime_sync_engine = None
RuntimeSessionLocal: sessionmaker[Session] | None = None
logger = get_logger("storage.database")


def _build_engine(settings: Settings | None = None) -> AsyncEngine:
    """Create a fresh async SQLAlchemy engine for the supplied settings."""
    cfg = settings or get_settings()
    engine_kwargs: dict[str, Any] = {
        "echo": cfg.database_echo,
        "future": True,
    }
    if not cfg.is_sqlite:
        engine_kwargs["pool_size"] = cfg.database_pool_size
        engine_kwargs["max_overflow"] = cfg.database_max_overflow

    engine = create_async_engine(cfg.database_url, **engine_kwargs)

    if cfg.is_sqlite:
        sync_engine = engine.sync_engine

        @event.listens_for(sync_engine, "connect")
        def _set_sqlite_pragma(dbapi_connection: Any, connection_record: Any) -> None:
            del connection_record
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    return engine


def _to_runtime_sync_url(url: str) -> str:
    replacements = {
        "sqlite+aiosqlite://": "sqlite+pysqlite://",
        "postgresql+asyncpg://": "postgresql+psycopg://",
        "postgresql+psycopg_async://": "postgresql+psycopg://",
    }
    for src, dst in replacements.items():
        if url.startswith(src):
            return url.replace(src, dst, 1)
    return url

def configure_storage(settings: Settings | None = None) -> None:
    """Rebuild the engine/sessionmaker for the provided settings.

    Tests, CLI commands, and startup hooks need a single explicit place to swap
    database backends. Reusing an engine built from stale settings is a classic
    source of "works on my machine" garbage.
    """
    global async_engine, AsyncSessionLocal, runtime_sync_engine, RuntimeSessionLocal
    cfg = settings or get_settings()
    async_engine = _build_engine(cfg)
    AsyncSessionLocal = async_sessionmaker(
        bind=async_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )
    from sqlalchemy import create_engine
    runtime_sync_engine = create_engine(_to_runtime_sync_url(cfg.database_url), echo=cfg.database_echo, future=True)
    RuntimeSessionLocal = sessionmaker(bind=runtime_sync_engine, expire_on_commit=False, class_=Session)


configure_storage()


def _require_sessionmaker() -> async_sessionmaker[AsyncSession]:
    if AsyncSessionLocal is None:
        configure_storage()
    assert AsyncSessionLocal is not None
    return AsyncSessionLocal


def _require_engine() -> AsyncEngine:
    if async_engine is None:
        configure_storage()
    assert async_engine is not None
    return async_engine


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields a transaction-managed async session."""
    session_factory = _require_sessionmaker()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception as exc:
            logger.error("Rolling back DB session after exception", error=str(exc), exc_info=True)
            await session.rollback()
            raise


@asynccontextmanager
async def session_scope() -> AsyncGenerator[AsyncSession, None]:
    """Context manager for non-FastAPI callers that need transactional storage use."""
    session_factory = _require_sessionmaker()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception as exc:
            logger.error("Rolling back session scope after exception", error=str(exc), exc_info=True)
            await session.rollback()
            raise


async def check_db_connectivity() -> None:
    """Run a lightweight database connectivity probe."""
    engine = _require_engine()
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


async def init_db() -> None:
    """Create all tables. Production deployments should still use migrations."""
    engine = _require_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await init_runtime_db()
    await init_streaming_db()


async def close_db() -> None:
    """Dispose the current engine so shutdown and tests do not leak connections."""
    global async_engine, AsyncSessionLocal, runtime_sync_engine, RuntimeSessionLocal
    if async_engine is not None:
        await async_engine.dispose()
    if runtime_sync_engine is not None:
        runtime_sync_engine.dispose()
    async_engine = None
    AsyncSessionLocal = None
    runtime_sync_engine = None
    RuntimeSessionLocal = None


def get_runtime_session() -> Session:
    if RuntimeSessionLocal is None:
        configure_storage()
    assert RuntimeSessionLocal is not None
    return RuntimeSessionLocal()

async def init_runtime_db() -> None:
    if runtime_sync_engine is None:
        configure_storage()
    assert runtime_sync_engine is not None
    from netobserv.runtime.db_models import RuntimeBase
    RuntimeBase.metadata.create_all(bind=runtime_sync_engine)


async def init_streaming_db() -> None:
    """Create streaming SQL tables using the sync runtime engine."""
    if runtime_sync_engine is None:
        configure_storage()
    assert runtime_sync_engine is not None
    from netobserv.streaming.db_models import StreamingBase
    StreamingBase.metadata.create_all(bind=runtime_sync_engine)

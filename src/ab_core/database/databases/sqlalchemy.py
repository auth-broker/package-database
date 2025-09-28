"""SQLAlchemy database implementation."""

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from functools import cached_property
from typing import Literal, override

from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session, SQLModel, create_engine

from ..schema.database_type import DatabaseType
from .base import DatabaseBase


class SQLAlchemyDatabase(DatabaseBase[Session, AsyncSession]):
    """SQLAlchemy database implementation."""

    url: str
    type: Literal[DatabaseType.SQL_ALCHEMY] = DatabaseType.SQL_ALCHEMY

    # ---------- Engines ----------
    @cached_property
    def sync_engine(self) -> Engine:
        """Sync engine."""
        # No AUTOCOMMIT; let our context managers control transactions.
        return create_engine(self.url, echo=True)

    @cached_property
    def async_engine(self) -> AsyncEngine:
        """Async engine."""
        # No AUTOCOMMIT; let our context managers control transactions.
        return create_async_engine(self.url, echo=True)

    # ---------- Session Factories ----------
    @cached_property
    def sync_session_factory(self) -> sessionmaker:
        """Factory for sync sessions."""
        return sessionmaker(
            bind=self.sync_engine,
            class_=Session,
            expire_on_commit=False,
            autoflush=True,
            autocommit=False,
        )

    @cached_property
    def async_session_factory(self) -> sessionmaker:
        """Factory for async sessions."""
        return sessionmaker(
            bind=self.async_engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=True,
            autocommit=False,
        )

    # ---------- Context-Managed Sessions (with commit/rollback/close) ----------
    @override
    @contextmanager
    def sync_session(
        self,
        *,
        current_session: Session | None = None,
    ) -> Iterator[Session]:
        """Provide a sync session context."""
        if current_session is not None:
            # Caller manages lifecycle/transaction.
            yield current_session
        else:
            session: Session = self.sync_session_factory()
            with session.begin():
                yield session

    @override
    @asynccontextmanager
    async def async_session(
        self,
        *,
        current_session: AsyncSession | None = None,
    ) -> AsyncIterator[AsyncSession]:
        """Provide an async session context."""
        if current_session:
            # Caller manages lifecycle/transaction.
            yield current_session
        else:
            session: AsyncSession = self.async_session_factory()
            async with session.begin():
                yield session

    # ---------- Schema helpers ----------
    @override
    def sync_upgrade_db(self):
        """Create all tables in the database (synchronously)."""
        SQLModel.metadata.create_all(self.sync_engine)

    @override
    async def async_upgrade_db(self):
        """Create all tables in the database (asynchronously)."""
        async with self.async_engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)

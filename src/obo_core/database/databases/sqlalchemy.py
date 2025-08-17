from contextlib import asynccontextmanager, contextmanager
from functools import cached_property
from typing import AsyncIterator, Iterator, Literal, Optional, override

from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session, SQLModel, create_engine

from ..schema.database_type import DatabaseType
from .base import DatabaseBase


class SQLAlchemyDatabase(DatabaseBase[Session, AsyncSession]):
    url: str
    type: Literal[DatabaseType.SQL_ALCHEMY] = DatabaseType.SQL_ALCHEMY

    @cached_property
    def sync_engine(self) -> Engine:
        return create_engine(self.url, echo=True)

    @override
    @contextmanager
    def sync_session(
        self,
        *,
        current_session: Optional[Session] = None,
    ) -> Iterator[Session]:
        if current_session:
            yield current_session
        else:
            with Session(self.sync_engine) as session:
                yield session

    @cached_property
    def async_engine(self) -> AsyncEngine:
        return create_async_engine(self.url, echo=True)

    @override
    @asynccontextmanager
    async def async_session(
        self,
        *,
        current_session: Optional[AsyncSession] = None,
    ) -> AsyncIterator[AsyncSession]:
        if current_session:
            yield current_session
        else:
            async_session_factory = sessionmaker(
                self.async_engine, class_=AsyncSession, expire_on_commit=False
            )
            async with async_session_factory() as session:
                yield session

    @override
    def sync_upgrade_db(self):
        SQLModel.metadata.create_all(self.sync_engine)

    @override
    async def async_upgrade_db(self):
        async with self.async_engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)

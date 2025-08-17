from contextlib import asynccontextmanager, contextmanager
from typing import (
    AsyncIterator,
    Generator,
    Literal,
    Optional,
    override,
)

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import Session

from ..schema.database_type import DatabaseType
from .base import DatabaseBase


class TemplateSession(Session): ...


class TemplateAsyncSession(AsyncSession): ...


class TemplateDatabase(DatabaseBase[TemplateSession, TemplateAsyncSession]):
    type: Literal[DatabaseType.TEMPLATE] = DatabaseType.TEMPLATE

    @override
    @contextmanager
    def sync_session(
        self,
        *,
        current_session: Optional[TemplateSession] = None,
    ) -> Generator[TemplateSession, None, None]:
        raise NotImplementedError()

    @override
    @asynccontextmanager
    async def async_session(
        self,
        *,
        current_session: Optional[TemplateAsyncSession] = None,
    ) -> AsyncIterator[TemplateAsyncSession]:
        raise NotImplementedError()

    @override
    def sync_upgrade_db(self):
        raise NotImplementedError()

    @override
    async def async_upgrade_db(self):
        raise NotImplementedError()

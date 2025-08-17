from abc import ABC, abstractmethod
from typing import (
    AsyncContextManager,
    ContextManager,
    Generic,
    Optional,
    TypeVar,
)

from pydantic import BaseModel

SYNC_SESSION = TypeVar("SYNC_SESSION")
ASYNC_SESSION = TypeVar("ASYNC_SESSION")


class DatabaseBase(BaseModel, Generic[SYNC_SESSION, ASYNC_SESSION], ABC):
    @abstractmethod
    def sync_session(
        self,
        *,
        current_session: Optional[ASYNC_SESSION] = None,
    ) -> ContextManager[SYNC_SESSION]: ...

    @abstractmethod
    async def async_session(
        self,
        *,
        current_session: Optional[ASYNC_SESSION] = None,
    ) -> AsyncContextManager[ASYNC_SESSION]: ...

    @abstractmethod
    def sync_upgrade_db(self): ...

    @abstractmethod
    async def async_upgrade_db(self): ...

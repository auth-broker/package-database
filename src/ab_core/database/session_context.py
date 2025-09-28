"""Session context providers for database sessions."""

import logging
from typing import (
    Annotated,
)

from ab_core.dependency import Depends, inject, sentinel

from .databases import Database

logger = logging.getLogger(__name__)


@inject
def db_session_sync(
    db: Annotated[Database, Depends(Database, persist=True)] = sentinel(),
):
    """Provide a synchronous database session context."""
    with db.sync_session() as sync_session:
        yield sync_session


@inject
async def db_session_async(
    db: Annotated[Database, Depends(Database, persist=True)] = sentinel(),
):
    """Provide an asynchronous database session context."""
    async with db.async_session() as async_session:
        yield async_session

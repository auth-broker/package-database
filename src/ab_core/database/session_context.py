import logging
from contextlib import asynccontextmanager, contextmanager
from typing import (
    Annotated,
)

from ab_core.dependency import Depends, inject, sentinel

from .databases import Database

logger = logging.getLogger(__name__)


@inject
@contextmanager
def db_session_sync_cm(
    db: Annotated[Database, Depends(Database)] = sentinel(),
):
    with db.sync_session() as sync_session:
        try:
            yield sync_session
        except Exception as e:
            logger.debug(
                "An exception occurred, performing db rollback",
                exc_info=e,
            )
            sync_session.rollback()
            raise
        else:
            sync_session.commit()
        finally:
            sync_session.close()


@inject
@asynccontextmanager
async def db_session_async_cm(
    db: Annotated[Database, Depends(Database)] = sentinel(),
):
    async with db.async_session() as async_session:
        try:
            yield async_session
        except Exception as e:
            logger.debug(
                "An exception occurred, performing db rollback",
                exc_info=e,
            )
            await async_session.rollback()
            raise
        else:
            await async_session.commit()
        finally:
            await async_session.close()


# NOTE: below can be used as fastapi dependencies, since they don't
# have the context manager annotation


@inject
def db_session_sync(
    db: Annotated[Database, Depends(Database)] = sentinel(),
):
    with db_session_sync_cm(db) as sync_session:
        yield sync_session


@inject
async def db_session_async(
    db: Annotated[Database, Depends(Database)] = sentinel(),
):
    async with db_session_async_cm(db) as async_session:
        yield async_session

import pytest
from sqlalchemy import select
from tests.models import User

from obo_core.database.databases import Database
from obo_core.database.session_context import db_session_async_cm, db_session_sync_cm


def test_db_session_sync_commits(tmp_database_sync: Database):
    # ───── write inside the CM ────────────────────────────────
    with db_session_sync_cm(db=tmp_database_sync) as session:
        session.add(User(name="Charlie"))  # no explicit commit!
        # Session is open inside the block
        assert session.is_active

    # ───── verify commit occurred outside the CM ──────────────
    with tmp_database_sync.sync_session() as session:
        row = session.exec(select(User).where(User.name == "Charlie")).first()
        assert row is not None  # record persisted


def test_db_session_sync_rolls_back(tmp_database_sync: Database):
    with pytest.raises(RuntimeError):
        with db_session_sync_cm(db=tmp_database_sync) as session:
            session.add(User(name="Eve"))
            raise RuntimeError("boom")  # should trigger rollback

    # Row must **not** be there because CM rolled back
    with tmp_database_sync.sync_session() as session:
        row = session.exec(select(User).where(User.name == "Eve")).first()
        assert row is None


@pytest.mark.asyncio
async def test_db_session_async_commits(tmp_database_async: Database):
    async with db_session_async_cm(db=tmp_database_async) as session:
        session.add(User(name="Charlie"))

    async with tmp_database_async.async_session() as session:
        result = await session.execute(select(User).where(User.name == "Charlie"))
        assert result.first() is not None


@pytest.mark.asyncio
async def test_db_session_async_rolls_back(tmp_database_async: Database):
    with pytest.raises(RuntimeError):
        async with db_session_async_cm(db=tmp_database_async) as session:
            session.add(User(name="Eve"))
            raise RuntimeError("boom")

    async with tmp_database_async.async_session() as session:
        result = await session.execute(select(User).where(User.name == "Eve"))
        assert result.first() is None

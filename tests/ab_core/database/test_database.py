import pytest
from sqlalchemy import inspect, select
from tests.models import User

from ab_core.database.databases import Database


def test_database_sync(tmp_database_sync: Database):
    with tmp_database_sync.sync_session() as session:
        inspector = inspect(session.bind)
        tables = inspector.get_table_names()
        assert "user" in tables

        # Insert sample data
        user1 = User(name="Alice")
        user2 = User(name="Bob")
        session.add(user1)
        session.add(user2)
        session.commit()

    # Retrieve and validate inserted data
    with tmp_database_sync.sync_session() as session:
        result = session.exec(select(User)).all()
        assert len(result) == 2
        names = [user[0].name for user in result]
        assert "Alice" in names
        assert "Bob" in names


@pytest.mark.asyncio
async def test_database_async(tmp_database_async: Database):
    # Initially DB should be empty (sync check still fine for schema inspection)
    async with tmp_database_async.async_session() as session:
        async with session.bind.connect() as conn:

            def get_tables(sync_conn):
                inspector = inspect(sync_conn)
                return inspector.get_table_names()

            tables = await conn.run_sync(get_tables)
            assert "user" in tables

    # Insert sample data asynchronously
    async with tmp_database_async.async_session() as session:
        user1 = User(name="Alice")
        user2 = User(name="Bob")
        session.add_all([user1, user2])
        await session.commit()

    # Query asynchronously
    async with tmp_database_async.async_session() as session:
        result = await session.execute(select(User))
        users = result.all()

        assert len(users) == 2
        names = [user[0].name for user in users]
        assert "Alice" in names
        assert "Bob" in names

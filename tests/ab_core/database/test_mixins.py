from uuid import UUID

import pytest
from sqlalchemy import inspect, select
from sqlmodel import Field

from ab_core.database.mixins import CreatedAtMixin, DeletedMixin, IDMixin, UpdatedAtMixin


class FakeModel(IDMixin, CreatedAtMixin, UpdatedAtMixin, DeletedMixin, table=True):
    __tablename__ = "fake_models"
    extra_field: str = Field(default="test")


@pytest.mark.asyncio
async def test_base_db_model_integration(tmp_database_async):
    # ── Upgrade DB to create the FakeModel table ──────────────
    await tmp_database_async.async_upgrade_db()

    # ── Insert instance ───────────────────────────────────────
    async with tmp_database_async.async_session() as session:
        fake = FakeModel(extra_field="hello")
        session.add(fake)
        await session.commit()
        await session.refresh(fake)

        # Should have UUIDv7
        assert isinstance(fake.id, UUID)
        assert fake.id.version == 7

        # Should be not deleted by default
        assert fake.deleted is False
        assert fake.deleted_at is None

        original_updated_at = fake.updated_at

    # ── Update instance & check updated_at bumps ───────────────
    async with tmp_database_async.async_session() as session:
        result = await session.execute(select(FakeModel))
        db_fake = result.scalar_one()

        db_fake.extra_field = "updated!"
        await session.commit()
        await session.refresh(db_fake)

        assert db_fake.updated_at > original_updated_at

    # ── Soft delete instance & check deleted_at is set ────────
    async with tmp_database_async.async_session() as session:
        result = await session.execute(select(FakeModel))
        db_fake = result.scalar_one()

        db_fake.deleted = True
        await session.commit()
        await session.refresh(db_fake)

        assert db_fake.deleted is True
        assert db_fake.deleted_at is not None

    # ── Check schema via inspector ────────────────────────────
    async with tmp_database_async.async_session() as session:
        async with session.bind.connect() as conn:

            def get_columns(sync_conn):
                inspector = inspect(sync_conn)
                return inspector.get_columns("fake_models")

            columns = await conn.run_sync(get_columns)
            column_names = [col["name"] for col in columns]
            assert "id" in column_names
            assert "created_at" in column_names
            assert "updated_at" in column_names
            assert "deleted" in column_names
            assert "deleted_at" in column_names
            assert "extra_field" in column_names

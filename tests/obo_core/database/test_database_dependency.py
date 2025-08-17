import os
from typing import Annotated
from unittest.mock import patch

from pydantic import BaseModel

from obo_core.database.databases import Database, SQLAlchemyDatabase
from obo_core.dependency import Depends, inject


def test_database_dependency():
    with patch.dict(
        os.environ,
        {
            "DATABASE_TYPE": "SQL_ALCHEMY",
            "DATABASE_SQL_ALCHEMY_URL": "abc",
        },
        clear=False,
    ):
        # test function
        @inject
        def some_func(
            db: Annotated[Database, Depends(Database, persist=True)],
        ):
            return db

        db1 = some_func()
        assert isinstance(db1, SQLAlchemyDatabase)
        assert db1.url == "abc"

        # test class
        @inject
        class SomeClass(BaseModel):
            db: Annotated[Database, Depends(Database, persist=True)]

        db2 = SomeClass().db
        assert isinstance(db2, SQLAlchemyDatabase)
        assert db2.url == "abc"

        assert db1 is db2

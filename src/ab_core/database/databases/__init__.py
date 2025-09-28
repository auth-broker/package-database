from typing import Annotated, Union

from pydantic import Discriminator

from .sqlalchemy import SQLAlchemyDatabase
from .template import TemplateDatabase

Database = Annotated[SQLAlchemyDatabase | TemplateDatabase, Discriminator("type")]

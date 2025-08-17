from typing import Annotated, Union

from pydantic import Discriminator

from .sqlalchemy import SQLAlchemyDatabase
from .template import TemplateDatabase

Database = Annotated[Union[SQLAlchemyDatabase, TemplateDatabase], Discriminator("type")]

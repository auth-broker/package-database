from uuid import UUID

from sqlmodel import Field, SQLModel
from uuid_extensions import uuid7


class IDMixin(SQLModel):
    id: UUID = Field(default_factory=uuid7, primary_key=True, index=True)

from uuid import UUID

from sqlmodel import Field, SQLModel


class CreatedByMixin(SQLModel):
    created_by: UUID | None = Field(default=None, index=True)

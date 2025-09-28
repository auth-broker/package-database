from uuid import UUID

from sqlmodel import Field, SQLModel


class UpdatedByMixin(SQLModel):
    updated_by: UUID | None = Field(default=None, index=True)

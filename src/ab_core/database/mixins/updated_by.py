from typing import Optional
from uuid import UUID

from sqlmodel import Field, SQLModel


class UpdatedByMixin(SQLModel):
    updated_by: Optional[UUID] = Field(default=None, index=True)

from typing import Optional
from uuid import UUID

from sqlmodel import Field, SQLModel


class CreatedByMixin(SQLModel):
    created_by: Optional[UUID] = Field(default=None, index=True)

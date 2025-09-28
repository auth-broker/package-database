"""CreatedAtMixin for SQLModel models to automatically set created_at timestamp."""

from datetime import datetime

from sqlalchemy import Column, DateTime, func
from sqlmodel import Field, SQLModel


class CreatedAtMixin(SQLModel):
    """Mixin to add a created_at timestamp to a SQLModel model."""

    created_at: datetime = Field(
        sa_column=Column(
            DateTime(timezone=True),
            server_default=func.now(),
            nullable=False,
        ),
    )

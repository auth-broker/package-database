"""Mixin to add an "updated_at" timestamp column to SQLModel models."""

from datetime import datetime

from sqlalchemy import Column, DateTime, func
from sqlmodel import Field, SQLModel


class UpdatedAtMixin(SQLModel):
    """Mixin to add an updated_at timestamp to a SQLModel model."""

    updated_at: datetime = Field(
        sa_column=Column(
            DateTime(timezone=True),
            server_default=func.now(),
            onupdate=func.now(),
            nullable=False,
        ),
    )

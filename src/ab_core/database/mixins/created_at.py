from datetime import datetime, timezone

from sqlmodel import Field, SQLModel


class CreatedAtMixin(SQLModel):
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

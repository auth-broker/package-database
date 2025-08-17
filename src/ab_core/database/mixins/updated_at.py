from datetime import datetime, timezone

from sqlalchemy import event
from sqlalchemy.orm import Mapper
from sqlmodel import Field, SQLModel


class UpdatedAtMixin(SQLModel):
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


@event.listens_for(UpdatedAtMixin, "before_update", propagate=True)
def timestamp_updated(mapper: Mapper, connection, target: UpdatedAtMixin):
    target.updated_at = datetime.now(timezone.utc)

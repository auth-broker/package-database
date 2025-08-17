from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import event
from sqlalchemy.orm import Mapper
from sqlmodel import Field, SQLModel


class ArchivedMixin(SQLModel):
    archived: bool = Field(default=False)
    archived_at: Optional[datetime] = Field(default=None)


@event.listens_for(ArchivedMixin, "before_update", propagate=True)
def set_archived_at(mapper: Mapper, connection, target: ArchivedMixin):
    if target.archived and target.archived_at is None:
        target.archived_at = datetime.now(timezone.utc)

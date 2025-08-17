from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import event
from sqlalchemy.orm import Mapper
from sqlmodel import Field, SQLModel


class DeletedMixin(SQLModel):
    deleted: bool = Field(default=False)
    deleted_at: Optional[datetime] = Field(default=None)


@event.listens_for(DeletedMixin, "before_update", propagate=True)
def set_deleted_at(mapper: Mapper, connection, target: DeletedMixin):
    if target.deleted and target.deleted_at is None:
        target.deleted_at = datetime.now(timezone.utc)

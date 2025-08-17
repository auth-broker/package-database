from slugify import slugify
from sqlalchemy import event
from sqlalchemy.orm import Mapper
from sqlmodel import Field, SQLModel

from .name import NameMixin


class SlugMixin(NameMixin, SQLModel):
    slug: str = Field(index=True, unique=True)


@event.listens_for(SlugMixin, "before_insert", propagate=True)
@event.listens_for(SlugMixin, "before_update", propagate=True)
def auto_slug(mapper: Mapper, connection, target: SlugMixin):
    if not target.slug and target.name:
        target.slug = slugify(target.name)

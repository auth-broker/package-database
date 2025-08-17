from sqlmodel import Field, SQLModel


class NameMixin(SQLModel):
    name: str = Field(index=True)

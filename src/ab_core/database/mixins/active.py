from sqlmodel import Field, SQLModel


class ActiveMixin(SQLModel):
    is_active: bool = Field(default=True)

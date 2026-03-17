import uuid
from sqlalchemy import Column, String, DateTime
from sqlalchemy.dialects.postgresql import UUID, TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from app.database import Base


class Office(Base):
    __tablename__ = "offices"

    id = Column(UUID(as_uuid=True), primary_key=True,
                default=uuid.uuid4)
    name = Column(String(200), nullable=False)
    institution = Column(String(200), nullable=False)
    level = Column(String(50), nullable=True)
    territory_type = Column(String(50), nullable=True)
    created_at = Column(DateTime(timezone=True),
                        server_default=func.now())
    search_vector: Mapped[str | None] = mapped_column(
        TSVECTOR, nullable=True, index=False
    )

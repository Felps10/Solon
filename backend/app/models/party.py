import uuid
from sqlalchemy import Column, String, Date, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.database import Base


class Party(Base):
    __tablename__ = "parties"

    id = Column(UUID(as_uuid=True), primary_key=True,
                default=uuid.uuid4)
    name = Column(String(300), nullable=False)
    abbreviation = Column(String(30), nullable=False,
                          index=True)
    founded_date = Column(Date, nullable=True)
    dissolved_date = Column(Date, nullable=True)
    tse_code = Column(String(20), nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True),
                        server_default=func.now())

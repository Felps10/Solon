import uuid
from sqlalchemy import Column, String, Text, DateTime
from sqlalchemy.dialects.postgresql import UUID, TSTZRANGE
from sqlalchemy.sql import func
from app.database import Base


class InstitutionalContext(Base):
    __tablename__ = "institutional_contexts"

    id = Column(UUID(as_uuid=True), primary_key=True,
                default=uuid.uuid4)
    validity = Column(TSTZRANGE, nullable=False)
    regime_name = Column(String(200), nullable=False)
    regime_type = Column(String(100), nullable=True)
    constitution = Column(String(100), nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True),
                        server_default=func.now())

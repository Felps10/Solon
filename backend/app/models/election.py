import uuid
from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.database import Base


class Election(Base):
    __tablename__ = "elections"

    id = Column(UUID(as_uuid=True), primary_key=True,
                default=uuid.uuid4)
    year = Column(Integer, nullable=False, index=True)
    round = Column(Integer, default=1)
    election_type = Column(String(100), nullable=False)
    territory = Column(String(200), nullable=True)
    tse_election_id = Column(String(100), nullable=True)
    created_at = Column(DateTime(timezone=True),
                        server_default=func.now())

import uuid
from sqlalchemy import Column, String, Text, DateTime, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.database import Base


class Territory(Base):
    __tablename__ = "territories"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(300), nullable=False)
    territory_type = Column(String(30), nullable=False)
    tse_ue_name = Column(String(300), nullable=True)
    uf_code = Column(String(2), nullable=True)
    ibge_code = Column(String(10), nullable=True)
    parent_id = Column(UUID(as_uuid=True), ForeignKey("territories.id"),
                       nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False,
                        server_default=func.now())

    __table_args__ = (
        UniqueConstraint("name", "territory_type",
                         name="uq_territories_name_type"),
        Index("ix_territories_territory_type", "territory_type"),
        Index("ix_territories_tse_ue_name", "tse_ue_name"),
        Index("ix_territories_uf_code", "uf_code"),
    )

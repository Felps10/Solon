import uuid
from sqlalchemy import (
    Column, String, Date, Text,
    DateTime, Boolean, ForeignKey, Integer
)
from sqlalchemy.dialects.postgresql import UUID, TSTZRANGE
from sqlalchemy.sql import func
from app.database import Base


class Person(Base):
    __tablename__ = "people"

    id = Column(UUID(as_uuid=True), primary_key=True,
                default=uuid.uuid4)
    canonical_name = Column(String(300), nullable=False,
                            index=True)
    birth_date = Column(Date, nullable=True)
    death_date = Column(Date, nullable=True)
    gender = Column(String(20), nullable=True)
    bio_summary = Column(Text, nullable=True)
    cpdoc_id = Column(String(100), nullable=True, unique=True)
    tse_id = Column(String(100), nullable=True, unique=True)
    created_at = Column(DateTime(timezone=True),
                        server_default=func.now())
    updated_at = Column(DateTime(timezone=True),
                        onupdate=func.now())


class PartyAffiliation(Base):
    __tablename__ = "party_affiliations"

    id = Column(UUID(as_uuid=True), primary_key=True,
                default=uuid.uuid4)
    person_id = Column(UUID(as_uuid=True),
                       ForeignKey("people.id"),
                       nullable=False, index=True)
    party_id = Column(UUID(as_uuid=True),
                      ForeignKey("parties.id"),
                      nullable=False, index=True)
    validity = Column(TSTZRANGE, nullable=False)
    date_precision = Column(String(20), default="year")
    is_approximate = Column(Boolean, default=False)
    source_label = Column(String(200), nullable=True)
    confidence = Column(String(10), default="high")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True),
                        server_default=func.now())


class Mandate(Base):
    __tablename__ = "mandates"

    id = Column(UUID(as_uuid=True), primary_key=True,
                default=uuid.uuid4)
    person_id = Column(UUID(as_uuid=True),
                       ForeignKey("people.id"),
                       nullable=False, index=True)
    office_id = Column(UUID(as_uuid=True),
                       ForeignKey("offices.id"),
                       nullable=True)
    territory = Column(String(200), nullable=True)
    validity = Column(TSTZRANGE, nullable=False)
    date_precision = Column(String(20), default="year")
    is_approximate = Column(Boolean, default=False)
    interrupted = Column(Boolean, default=False)
    interruption_reason = Column(String(100), nullable=True)
    source_label = Column(String(200), nullable=True)
    confidence = Column(String(10), default="high")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True),
                        server_default=func.now())


class Candidacy(Base):
    __tablename__ = "candidacies"

    id = Column(UUID(as_uuid=True), primary_key=True,
                default=uuid.uuid4)
    person_id = Column(UUID(as_uuid=True),
                       ForeignKey("people.id"),
                       nullable=False, index=True)
    election_id = Column(UUID(as_uuid=True),
                         ForeignKey("elections.id"),
                         nullable=False)
    office_id = Column(UUID(as_uuid=True),
                       ForeignKey("offices.id"),
                       nullable=True)
    party_id = Column(UUID(as_uuid=True),
                      ForeignKey("parties.id"),
                      nullable=True)
    territory = Column(String(200), nullable=True)
    result = Column(String(30), nullable=True)
    vote_count = Column(Integer, nullable=True)
    validity = Column(TSTZRANGE, nullable=True)
    source_label = Column(String(200), nullable=True)
    confidence = Column(String(10), default="high")
    created_at = Column(DateTime(timezone=True),
                        server_default=func.now())

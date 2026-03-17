from __future__ import annotations
import uuid
from datetime import datetime
from typing import TYPE_CHECKING
from sqlalchemy import (
    Column, String, Date, Text,
    DateTime, Boolean, ForeignKey, Integer, UniqueConstraint, Index
)
from sqlalchemy.dialects.postgresql import UUID, TSTZRANGE, TSVECTOR
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.sql import func
from app.database import Base

if TYPE_CHECKING:
    from app.models.office import Office
    from app.models.election import Election
    from app.models.party import Party


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
    created_at = Column(DateTime(timezone=True),
                        server_default=func.now())
    updated_at = Column(DateTime(timezone=True),
                        onupdate=func.now())
    search_vector: Mapped[str | None] = mapped_column(
        TSVECTOR, nullable=True, index=False
    )

    external_ids: Mapped[list["PersonExternalId"]] = relationship(
        "PersonExternalId", back_populates="person",
        cascade="all, delete-orphan"
    )
    candidacies: Mapped[list["Candidacy"]] = relationship(
        "Candidacy", back_populates="person",
        cascade="all, delete-orphan"
    )
    mandates: Mapped[list["Mandate"]] = relationship(
        "Mandate", back_populates="person",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index(
            "ix_people_name_birthdate",
            func.immutable_unaccent(Column("canonical_name", String)),
            Column("birth_date", Date),
            unique=True,
            postgresql_where=Column("birth_date", Date).isnot(None),
        ),
    )


class PersonExternalId(Base):
    __tablename__ = "people_external_ids"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    person_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("people.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source: Mapped[str] = mapped_column(
        String(50), nullable=False
    )
    external_id: Mapped[str] = mapped_column(
        String(200), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("source", "external_id",
            name="uq_people_external_ids_source_external_id"),
    )

    person: Mapped["Person"] = relationship(
        "Person", back_populates="external_ids"
    )


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

    person: Mapped["Person"] = relationship("Person", back_populates="mandates")
    office: Mapped["Office"] = relationship("Office", foreign_keys=[office_id])


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
    nome_urna = Column(String(200), nullable=True)
    created_at = Column(DateTime(timezone=True),
                        server_default=func.now())

    person: Mapped["Person"] = relationship("Person", back_populates="candidacies")
    election: Mapped["Election"] = relationship("Election", foreign_keys=[election_id])
    office: Mapped["Office"] = relationship("Office", foreign_keys=[office_id])
    party: Mapped["Party"] = relationship("Party", foreign_keys=[party_id])

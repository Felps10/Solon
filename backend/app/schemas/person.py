from pydantic import BaseModel, UUID4
from datetime import date, datetime
from typing import Optional, Literal

DatePrecision = Literal[
    "exact", "month", "year",
    "decade", "approximate", "unknown"
]

Confidence = Literal["high", "medium", "low", "uncertain"]


class PersonBase(BaseModel):
    id: UUID4
    canonical_name: str
    birth_date: Optional[date]
    death_date: Optional[date]
    gender: Optional[str]
    bio_summary: Optional[str]

    class Config:
        from_attributes = True


class PartyAffiliationOut(BaseModel):
    id: UUID4
    party_id: UUID4
    date_precision: DatePrecision
    is_approximate: bool
    confidence: Confidence
    source_label: Optional[str]
    notes: Optional[str]

    class Config:
        from_attributes = True


class MandateOut(BaseModel):
    id: UUID4
    office_id: Optional[UUID4]
    territory: Optional[str]
    date_precision: DatePrecision
    is_approximate: bool
    interrupted: bool
    interruption_reason: Optional[str]
    confidence: Confidence
    source_label: Optional[str]
    notes: Optional[str]

    class Config:
        from_attributes = True


class CandidacyOut(BaseModel):
    id: UUID4
    election_id: UUID4
    office_id: Optional[UUID4]
    party_id: Optional[UUID4]
    territory: Optional[str]
    result: Optional[str]
    vote_count: Optional[int]
    confidence: Confidence
    source_label: Optional[str]

    class Config:
        from_attributes = True


class PersonProfileResponse(BaseModel):
    person: PersonBase
    snapshot_date: Optional[date]
    party_affiliations: list[PartyAffiliationOut]
    mandates: list[MandateOut]
    candidacies: list[CandidacyOut]

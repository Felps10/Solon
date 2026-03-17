from __future__ import annotations
from pydantic import BaseModel
from datetime import date, datetime
import uuid


class PersonSummary(BaseModel):
    id:             uuid.UUID
    canonical_name: str
    birth_date:     date | None
    gender:         str | None


class ExternalId(BaseModel):
    source:      str
    external_id: str


class CandidacySummary(BaseModel):
    id:            uuid.UUID
    election_year: int
    office_name:   str
    party_abbr:    str | None
    territory:     str | None
    result:        str | None
    vote_count:    int | None
    confidence:    str


class MandateSummary(BaseModel):
    id:                  uuid.UUID
    office_name:         str
    territory:           str | None
    validity_lower:      datetime | None
    validity_upper:      datetime | None
    interrupted:         bool
    interruption_reason: str | None
    confidence:          str


class PersonProfile(BaseModel):
    id:             uuid.UUID
    canonical_name: str
    birth_date:     date | None
    death_date:     date | None
    gender:         str | None
    bio_summary:    str | None
    external_ids:   list[ExternalId]
    candidacies:    list[CandidacySummary]
    mandates:       list[MandateSummary]

from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Annotated, Literal, Union
from datetime import date
import uuid

from app.schemas.common import PageMeta


class PersonHit(BaseModel):
    kind:           Literal["person"]
    id:             uuid.UUID
    canonical_name: str
    birth_date:     date | None
    gender:         str | None
    rank:           float


class PartyHit(BaseModel):
    kind:         Literal["party"]
    id:           uuid.UUID
    abbreviation: str
    name:         str
    rank:         float


class OfficeHit(BaseModel):
    kind:        Literal["office"]
    id:          uuid.UUID
    name:        str
    institution: str | None
    level:       str | None
    rank:        float


class CandidacyHit(BaseModel):
    kind:          Literal["candidacy"]
    id:            uuid.UUID
    person_id:     uuid.UUID
    person_name:   str
    election_year: int
    office_name:   str
    party_abbr:    str | None
    territory:     str | None
    result:        str | None
    rank:          float   # inherited from the matched person's rank


SearchHit = Annotated[
    Union[PersonHit, PartyHit, OfficeHit, CandidacyHit],
    Field(discriminator="kind"),
]


class FacetCounts(BaseModel):
    people:      int
    parties:     int
    offices:     int
    candidacies: int


class SearchResponse(BaseModel):
    query:  str
    facets: FacetCounts
    meta:   PageMeta
    hits:   list[SearchHit]

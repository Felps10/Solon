from __future__ import annotations
from pydantic import BaseModel
from app.schemas.people import CandidacySummary
import uuid


class CandidacyDetail(CandidacySummary):
    person_id:    uuid.UUID
    person_name:  str
    source_label: str | None

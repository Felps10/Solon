from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from sqlalchemy.orm import selectinload
from datetime import datetime
import uuid

from app.database import get_db
from app.models.person import Person, Candidacy, Mandate
from app.schemas.common import PaginatedResponse
from app.schemas.people import (
    PersonSummary, PersonProfile,
    ExternalId, CandidacySummary, MandateSummary,
)

router = APIRouter(prefix="/people", tags=["people"])


# ── GET /api/v1/people/search ────────────────────────────────────────

@router.get("/search", response_model=PaginatedResponse[PersonSummary])
async def search_people(
    q:         str = Query(..., min_length=2),
    page:      int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> PaginatedResponse[PersonSummary]:
    offset = (page - 1) * page_size

    count_stmt = text("""
        SELECT count(*)
        FROM people
        WHERE immutable_unaccent(canonical_name)
            ILIKE '%' || immutable_unaccent(:q) || '%'
    """)
    total = (await db.execute(count_stmt, {"q": q})).scalar_one()

    stmt = text("""
        SELECT id, canonical_name, birth_date, gender
        FROM people
        WHERE immutable_unaccent(canonical_name)
            ILIKE '%' || immutable_unaccent(:q) || '%'
        ORDER BY canonical_name
        LIMIT :limit OFFSET :offset
    """)
    rows = (
        await db.execute(stmt, {"q": q, "limit": page_size, "offset": offset})
    ).mappings().all()

    return PaginatedResponse[PersonSummary](
        total=total,
        page=page,
        page_size=page_size,
        has_next=(offset + page_size) < total,
        items=[PersonSummary(**r) for r in rows],
    )


# ── GET /api/v1/people/{person_id} ───────────────────────────────────

@router.get("/{person_id}", response_model=PersonProfile)
async def get_person(
    person_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> PersonProfile:
    stmt = (
        select(Person)
        .where(Person.id == person_id)
        .options(
            selectinload(Person.external_ids),
            selectinload(Person.candidacies)
                .selectinload(Candidacy.election),
            selectinload(Person.candidacies)
                .selectinload(Candidacy.office),
            selectinload(Person.candidacies)
                .selectinload(Candidacy.party),
            selectinload(Person.mandates)
                .selectinload(Mandate.office),
        )
    )
    result = await db.execute(stmt)
    person = result.scalar_one_or_none()
    if person is None:
        raise HTTPException(status_code=404, detail="Person not found")

    return PersonProfile(
        id=person.id,
        canonical_name=person.canonical_name,
        birth_date=person.birth_date,
        death_date=person.death_date,
        gender=person.gender,
        bio_summary=person.bio_summary,
        external_ids=[
            ExternalId(source=e.source, external_id=e.external_id)
            for e in person.external_ids
        ],
        candidacies=[
            CandidacySummary(
                id=c.id,
                election_year=c.election.year,
                office_name=c.office.name,
                party_abbr=c.party.abbreviation if c.party else None,
                territory=c.territory,
                result=c.result,
                vote_count=c.vote_count,
                confidence=c.confidence,
                nome_urna=c.nome_urna,
            )
            for c in sorted(
                person.candidacies,
                key=lambda x: x.election.year,
                reverse=True,
            )
        ],
        mandates=[
            MandateSummary(
                id=m.id,
                office_name=m.office.name,
                territory=m.territory,
                validity_lower=m.validity.lower if m.validity else None,
                validity_upper=m.validity.upper if m.validity else None,
                interrupted=m.interrupted,
                interruption_reason=m.interruption_reason,
                confidence=m.confidence,
            )
            for m in person.mandates
        ],
    )


# ── GET /api/v1/people/{person_id}/snapshot ──────────────────────────

@router.get("/{person_id}/snapshot")
async def get_snapshot(
    person_id: uuid.UUID,
    at: str = Query(
        ...,
        description="ISO-8601 datetime, e.g. 1998-10-03T00:00:00Z",
    ),
    db: AsyncSession = Depends(get_db),
):
    try:
        ts = datetime.fromisoformat(at.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Cannot parse 'at' as ISO-8601 datetime: {at!r}",
        )

    person_exists = (
        await db.execute(
            text("SELECT 1 FROM people WHERE id = :id"),
            {"id": str(person_id)},
        )
    ).scalar_one_or_none()
    if person_exists is None:
        raise HTTPException(status_code=404, detail="Person not found")

    cand_stmt = text("""
        SELECT
            c.id,
            e.year  AS election_year,
            o.name  AS office_name,
            p.abbreviation AS party_abbr,
            c.territory,
            c.result,
            c.vote_count,
            c.confidence,
            c.nome_urna
        FROM candidacies c
        JOIN elections e  ON e.id = c.election_id
        JOIN offices   o  ON o.id = c.office_id
        LEFT JOIN parties p ON p.id = c.party_id
        WHERE c.person_id = :person_id
          AND c.validity @> CAST(:ts AS timestamptz)
        ORDER BY e.year DESC
    """)
    cand_rows = (
        await db.execute(
            cand_stmt,
            {"person_id": str(person_id), "ts": ts},
        )
    ).mappings().all()

    mand_stmt = text("""
        SELECT
            m.id,
            o.name  AS office_name,
            m.territory,
            lower(m.validity) AS validity_lower,
            upper(m.validity) AS validity_upper,
            m.interrupted,
            m.interruption_reason,
            m.confidence
        FROM mandates m
        JOIN offices o ON o.id = m.office_id
        WHERE m.person_id = :person_id
          AND m.validity @> CAST(:ts AS timestamptz)
        ORDER BY lower(m.validity) DESC
    """)
    mand_rows = (
        await db.execute(
            mand_stmt,
            {"person_id": str(person_id), "ts": ts},
        )
    ).mappings().all()

    return {
        "person_id": person_id,
        "at": ts.isoformat(),
        "candidacies": [CandidacySummary(**r) for r in cand_rows],
        "mandates":    [MandateSummary(**r)    for r in mand_rows],
    }

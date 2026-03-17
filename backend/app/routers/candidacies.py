from __future__ import annotations
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.database import get_db
from app.models.person import Candidacy
from app.models.person import Person
from app.models.election import Election
from app.models.office import Office
from app.models.party import Party
from app.schemas.candidacies import CandidacyDetail
from app.schemas.common import PaginatedResponse

router = APIRouter(prefix="/candidacies", tags=["candidacies"])


@router.get("", response_model=PaginatedResponse[CandidacyDetail])
async def list_candidacies(
    year:      int | None = Query(default=None),
    office:    str | None = Query(default=None,
        description="Case-insensitive substring match on office name"),
    territory: str | None = Query(default=None,
        description="Case-insensitive substring match"),
    party:     str | None = Query(default=None,
        description="Case-insensitive substring match on party abbreviation"),
    result:    str | None = Query(default=None,
        description="Exact match: elected, defeated, renounced, annulled"),
    limit:     int        = Query(default=50, ge=1, le=200),
    offset:    int        = Query(default=0,  ge=0),
    db: AsyncSession      = Depends(get_db),
) -> PaginatedResponse[CandidacyDetail]:

    base = (
        select(
            Candidacy.id,
            Candidacy.person_id,
            Person.canonical_name.label("person_name"),
            Election.year.label("election_year"),
            Office.name.label("office_name"),
            Party.abbreviation.label("party_abbr"),
            Candidacy.territory,
            Candidacy.result,
            Candidacy.vote_count,
            Candidacy.confidence,
            Candidacy.source_label,
            Candidacy.nome_urna,
        )
        .join(Person,    Person.id    == Candidacy.person_id)
        .join(Election,  Election.id  == Candidacy.election_id)
        .join(Office,    Office.id    == Candidacy.office_id)
        .outerjoin(Party, Party.id   == Candidacy.party_id)
    )

    if year      is not None:
        base = base.where(Election.year == year)
    if office    is not None:
        base = base.where(Office.name.ilike(f"%{office}%"))
    if territory is not None:
        base = base.where(Candidacy.territory.ilike(f"%{territory}%"))
    if party     is not None:
        base = base.where(Party.abbreviation.ilike(f"%{party}%"))
    if result    is not None:
        base = base.where(Candidacy.result == result)

    count_stmt = select(func.count()).select_from(base.subquery())
    total = (await db.execute(count_stmt)).scalar_one()

    rows_stmt = (
        base
        .order_by(Election.year.desc(), Person.canonical_name.asc())
        .limit(limit)
        .offset(offset)
    )
    rows = (await db.execute(rows_stmt)).mappings().all()

    page = (offset // limit) + 1 if limit > 0 else 1

    return PaginatedResponse[CandidacyDetail](
        total=total,
        page=page,
        page_size=limit,
        has_next=(offset + limit) < total,
        items=[CandidacyDetail(**r) for r in rows],
    )

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.sql import text
from app.database import get_db
from app.models.person import (
    Person, PartyAffiliation, Mandate, Candidacy
)
from app.schemas.person import (
    PersonBase, PersonProfileResponse
)
from datetime import date, datetime, timezone
from typing import Optional
import uuid

router = APIRouter(prefix="/people", tags=["people"])


@router.get("/search", response_model=list[PersonBase])
async def search_people(
    q: str = Query(..., min_length=2, description="Name fragment to search"),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Person)
        .where(Person.canonical_name.ilike(f"%{q}%"))
        .order_by(Person.canonical_name)
        .limit(20)
    )
    return result.scalars().all()


@router.get("/{person_id}", response_model=PersonProfileResponse)
async def get_person(
    person_id: str,
    as_of: Optional[date] = Query(
        None,
        description="Return the person's state as of this date. "
                    "Omit for current data."
    ),
    db: AsyncSession = Depends(get_db),
):
    try:
        pid = uuid.UUID(person_id)
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail="Invalid person ID — must be a UUID"
        )

    result = await db.execute(
        select(Person).where(Person.id == pid)
    )
    person = result.scalar_one_or_none()
    if not person:
        raise HTTPException(
            status_code=404,
            detail="Person not found"
        )

    if as_of:
        ts = datetime.combine(
            as_of, datetime.min.time()
        ).replace(tzinfo=timezone.utc)

        affiliations = (await db.execute(
            select(PartyAffiliation)
            .where(PartyAffiliation.person_id == pid)
            .where(text("validity @> CAST(:ts AS timestamptz)")),
            {"ts": ts},
        )).scalars().all()

        mandates = (await db.execute(
            select(Mandate)
            .where(Mandate.person_id == pid)
            .where(text("validity @> CAST(:ts AS timestamptz)")),
            {"ts": ts},
        )).scalars().all()

    else:
        affiliations = (await db.execute(
            select(PartyAffiliation)
            .where(PartyAffiliation.person_id == pid)
        )).scalars().all()

        mandates = (await db.execute(
            select(Mandate)
            .where(Mandate.person_id == pid)
        )).scalars().all()

    candidacies = (await db.execute(
        select(Candidacy)
        .where(Candidacy.person_id == pid)
    )).scalars().all()

    return PersonProfileResponse(
        person=person,
        snapshot_date=as_of,
        party_affiliations=affiliations,
        mandates=mandates,
        candidacies=candidacies,
    )

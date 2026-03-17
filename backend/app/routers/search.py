from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.search import SearchService
from app.schemas.search import SearchResponse

router = APIRouter(prefix="/search", tags=["search"])

_VALID_KINDS = frozenset({"person", "party", "office", "candidacy"})


@router.get("", response_model=SearchResponse)
async def universal_search(
    q: str = Query(
        ...,
        min_length=2,
        description="Search query. Accent-insensitive. Prefix matching.",
    ),
    kinds: list[str] = Query(
        default=[],
        description=(
            "Restrict results to one or more entity types. "
            "Valid values: person, party, office, candidacy. "
            "Omit to search all types."
        ),
    ),
    page:      int = Query(default=1,  ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> SearchResponse:
    if kinds:
        invalid = set(kinds) - _VALID_KINDS
        if invalid:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Invalid kind value(s): {sorted(invalid)}. "
                    f"Must be one of: {sorted(_VALID_KINDS)}"
                ),
            )
    svc = SearchService(db)
    return await svc.search(
        q=q.strip(),
        kinds=list(kinds) if kinds else None,
        page=page,
        page_size=page_size,
    )

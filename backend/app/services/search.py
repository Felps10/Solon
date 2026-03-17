from __future__ import annotations
import re
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.schemas.search import (
    PersonHit, PartyHit, OfficeHit, CandidacyHit,
    FacetCounts, SearchResponse, SearchHit,
)
from app.schemas.common import PageMeta


class SearchService:

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    # ── public ──────────────────────────────────────────────────────

    async def search(
        self,
        q:         str,
        kinds:     list[str] | None,
        page:      int,
        page_size: int,
    ) -> SearchResponse:
        tsquery = self._make_tsquery(q)
        offset  = (page - 1) * page_size
        active  = set(kinds) if kinds else {"person", "party", "office", "candidacy"}

        # SQLAlchemy AsyncSession uses a single connection and does not
        # support concurrent operations — run sub-queries sequentially.
        people_hits  = await self._search_people(tsquery, q, page_size, offset) if "person" in active else []
        party_hits   = await self._search_parties(tsquery, page_size, offset)   if "party"  in active else []
        office_hits  = await self._search_offices(tsquery, page_size, offset)   if "office" in active else []
        cand_hits    = await self._search_candidacies(tsquery, q, page_size, offset) if "candidacy" in active else []
        facets       = await self._facet_counts(tsquery, q)

        all_hits: list[SearchHit] = (
            people_hits + party_hits + office_hits + cand_hits
        )
        all_hits.sort(key=lambda h: h.rank, reverse=True)
        all_hits = all_hits[:page_size]

        total = (
            facets.people + facets.parties +
            facets.offices + facets.candidacies
        )

        return SearchResponse(
            query=q,
            facets=facets,
            meta=PageMeta(
                page=page,
                page_size=page_size,
                total=total,
                has_next=(page * page_size) < total,
            ),
            hits=all_hits,
        )

    # ── private helpers ──────────────────────────────────────────────

    def _make_tsquery(self, q: str) -> str:
        """Convert raw user input into a safe prefix tsquery string.

        'lula'       -> 'lula:*'
        'lula silva' -> 'lula:* & silva:*'

        Strips characters that would cause to_tsquery() to throw a
        syntax error. The :* suffix enables prefix matching so partial
        names match (e.g. 'bol' matches 'Bolsonaro').
        """
        tokens = re.sub(r"[^\w\s]", "", q, flags=re.UNICODE).split()
        if not tokens:
            return "a:*"   # fallback that matches nothing useful
        return " & ".join(f"{t}:*" for t in tokens)

    async def _search_people(
        self, tsquery: str, q: str, limit: int, offset: int
    ) -> list[PersonHit]:
        stmt = text("""
            SELECT
                id,
                canonical_name,
                birth_date,
                gender,
                ts_rank(
                    search_vector,
                    to_tsquery('portuguese', immutable_unaccent(:tsquery))
                ) AS rank
            FROM people
            WHERE
                search_vector @@ to_tsquery('portuguese',
                    immutable_unaccent(:tsquery))
                OR immutable_unaccent(canonical_name)
                    ILIKE '%' || immutable_unaccent(:q) || '%'
            ORDER BY rank DESC
            LIMIT :limit OFFSET :offset
        """)
        rows = (
            await self.db.execute(
                stmt,
                {"tsquery": tsquery, "q": q,
                 "limit": limit, "offset": offset},
            )
        ).mappings().all()
        return [PersonHit(kind="person", **r) for r in rows]

    async def _search_parties(
        self, tsquery: str, limit: int, offset: int
    ) -> list[PartyHit]:
        stmt = text("""
            SELECT
                id,
                abbreviation,
                name,
                ts_rank(
                    search_vector,
                    to_tsquery('simple', immutable_unaccent(:tsquery))
                ) AS rank
            FROM parties
            WHERE
                search_vector @@ to_tsquery('simple',
                    immutable_unaccent(:tsquery))
            ORDER BY rank DESC
            LIMIT :limit OFFSET :offset
        """)
        rows = (
            await self.db.execute(
                stmt,
                {"tsquery": tsquery, "limit": limit, "offset": offset},
            )
        ).mappings().all()
        return [PartyHit(kind="party", **r) for r in rows]

    async def _search_offices(
        self, tsquery: str, limit: int, offset: int
    ) -> list[OfficeHit]:
        stmt = text("""
            SELECT
                id,
                name,
                institution,
                level,
                ts_rank(
                    search_vector,
                    to_tsquery('portuguese', immutable_unaccent(:tsquery))
                ) AS rank
            FROM offices
            WHERE
                search_vector @@ to_tsquery('portuguese',
                    immutable_unaccent(:tsquery))
            ORDER BY rank DESC
            LIMIT :limit OFFSET :offset
        """)
        rows = (
            await self.db.execute(
                stmt,
                {"tsquery": tsquery, "limit": limit, "offset": offset},
            )
        ).mappings().all()
        return [OfficeHit(kind="office", **r) for r in rows]

    async def _search_candidacies(
        self, tsquery: str, q: str, limit: int, offset: int
    ) -> list[CandidacyHit]:
        """Surface the most recent candidacy per matched person.

        Uses DISTINCT ON (person_id) ordered by election year DESC
        so each matched person contributes exactly one candidacy hit —
        their latest one. This avoids returning dozens of rows for a
        politician who ran 10 times.
        """
        stmt = text("""
            WITH matched_people AS (
                SELECT
                    id,
                    canonical_name,
                    ts_rank(
                        search_vector,
                        to_tsquery('portuguese', immutable_unaccent(:tsquery))
                    ) AS rank
                FROM people
                WHERE
                    search_vector @@ to_tsquery('portuguese',
                        immutable_unaccent(:tsquery))
                    OR immutable_unaccent(canonical_name)
                        ILIKE '%' || immutable_unaccent(:q) || '%'
                ORDER BY rank DESC
                LIMIT :limit OFFSET :offset
            )
            SELECT DISTINCT ON (c.person_id)
                c.id,
                c.person_id,
                mp.canonical_name  AS person_name,
                e.year             AS election_year,
                o.name             AS office_name,
                p.abbreviation     AS party_abbr,
                c.territory,
                c.result,
                mp.rank
            FROM candidacies    c
            JOIN matched_people mp ON mp.id = c.person_id
            JOIN elections      e  ON e.id  = c.election_id
            JOIN offices        o  ON o.id  = c.office_id
            LEFT JOIN parties   p  ON p.id  = c.party_id
            ORDER BY c.person_id, e.year DESC
        """)
        rows = (
            await self.db.execute(
                stmt,
                {"tsquery": tsquery, "q": q,
                 "limit": limit, "offset": offset},
            )
        ).mappings().all()
        return [CandidacyHit(kind="candidacy", **r) for r in rows]

    async def _facet_counts(
        self, tsquery: str, q: str
    ) -> FacetCounts:
        """Count total matches per entity type for the facet bar.

        Candidacy count mirrors people count because candidacies are
        found through people matches (one hit per person).
        """
        stmt = text("""
            SELECT
                (
                    SELECT count(*)
                    FROM people
                    WHERE
                        search_vector @@ to_tsquery('portuguese',
                            immutable_unaccent(:tsquery))
                        OR immutable_unaccent(canonical_name)
                            ILIKE '%' || immutable_unaccent(:q) || '%'
                ) AS people,
                (
                    SELECT count(*)
                    FROM parties
                    WHERE search_vector @@ to_tsquery('simple',
                        immutable_unaccent(:tsquery))
                ) AS parties,
                (
                    SELECT count(*)
                    FROM offices
                    WHERE search_vector @@ to_tsquery('portuguese',
                        immutable_unaccent(:tsquery))
                ) AS offices,
                (
                    SELECT count(*)
                    FROM people
                    WHERE
                        search_vector @@ to_tsquery('portuguese',
                            immutable_unaccent(:tsquery))
                        OR immutable_unaccent(canonical_name)
                            ILIKE '%' || immutable_unaccent(:q) || '%'
                ) AS candidacies
        """)
        row = (
            await self.db.execute(stmt, {"tsquery": tsquery, "q": q})
        ).mappings().one()
        return FacetCounts(**row)

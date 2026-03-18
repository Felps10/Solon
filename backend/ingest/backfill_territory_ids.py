"""
Backfill territory_id FK on candidacies and mandates.

Phase A — candidacies
    Matches on immutable_unaccent(territory) = immutable_unaccent(tse_ue_name).
    Covers both accented and accent-stripped TSE strings (e.g. "SÃO PAULO"
    and "SAO PAULO" both resolve to the same territories row).

Phase B — mandates
    Matches on territory = uf_code (exact; mandates already store 2-char codes).

Idempotent: the WHERE clause filters territory_id IS NULL on every run, so
re-running after a successful backfill updates 0 rows.

Usage:
    python -m ingest.backfill_territory_ids
"""

import logging
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

UNMATCHED_WARNING_THRESHOLD = 0.05  # 5 %

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

BACKFILL_CANDIDACIES_SQL = """
UPDATE candidacies c
SET territory_id = t.id
FROM territories t
WHERE public.immutable_unaccent(upper(c.territory))
    = public.immutable_unaccent(upper(t.tse_ue_name))
  AND c.territory_id IS NULL;
"""

BACKFILL_MANDATES_SQL = """
UPDATE mandates m
SET territory_id = t.id
FROM territories t
WHERE m.territory = t.uf_code
  AND m.territory_id IS NULL;
"""

COVERAGE_CANDIDACIES_SQL = """
SELECT
  COUNT(*)                                              AS total,
  COUNT(territory_id)                                   AS matched,
  COUNT(*) FILTER (WHERE territory_id IS NULL)          AS unmatched,
  ROUND(COUNT(territory_id) * 100.0 / COUNT(*), 2)     AS pct_matched
FROM candidacies;
"""

UNMATCHED_CANDIDACIES_SQL = """
SELECT territory, COUNT(*) AS n
FROM candidacies
WHERE territory_id IS NULL
GROUP BY territory
ORDER BY n DESC
LIMIT 50;
"""

COVERAGE_MANDATES_SQL = """
SELECT
  COUNT(*)                                              AS total,
  COUNT(territory_id)                                   AS matched,
  COUNT(*) FILTER (WHERE territory_id IS NULL)          AS unmatched,
  ROUND(COUNT(territory_id) * 100.0 / COUNT(*), 2)     AS pct_matched
FROM mandates;
"""

UNMATCHED_MANDATES_SQL = """
SELECT territory, COUNT(*) AS n
FROM mandates
WHERE territory_id IS NULL
GROUP BY territory
ORDER BY n DESC;
"""


def get_connection() -> psycopg2.extensions.connection:
    url = (
        settings.database_url
        .replace("postgresql+asyncpg://", "postgresql://")
        .replace("?ssl=require", "?sslmode=require")
        .replace("&ssl=require", "&sslmode=require")
    )
    return psycopg2.connect(url)


def main() -> None:
    conn = get_connection()
    try:
        with conn:
            cur = conn.cursor()

            # ----------------------------------------------------------------
            # Phase A — candidacies
            # ----------------------------------------------------------------
            log.info("Phase A: backfilling candidacies.territory_id …")
            cur.execute(BACKFILL_CANDIDACIES_SQL)
            cand_updated = cur.rowcount
            log.info("  %d rows updated", cand_updated)

            # ----------------------------------------------------------------
            # Phase B — mandates
            # ----------------------------------------------------------------
            log.info("Phase B: backfilling mandates.territory_id …")
            cur.execute(BACKFILL_MANDATES_SQL)
            mand_updated = cur.rowcount
            log.info("  %d rows updated", mand_updated)

            # ----------------------------------------------------------------
            # Phase C — coverage report
            # ----------------------------------------------------------------
            log.info("Phase C: coverage report")

            cur.execute(COVERAGE_CANDIDACIES_SQL)
            c_total, c_matched, c_unmatched, c_pct = cur.fetchone()

            cur.execute(UNMATCHED_CANDIDACIES_SQL)
            cand_unmatched_rows = cur.fetchall()

            cur.execute(COVERAGE_MANDATES_SQL)
            m_total, m_matched, m_unmatched, m_pct = cur.fetchone()

            cur.execute(UNMATCHED_MANDATES_SQL)
            mand_unmatched_rows = cur.fetchall()

            cur.close()

        # ----------------------------------------------------------------
        # Print summary
        # ----------------------------------------------------------------
        print()
        print("Backfill complete.")
        print(f"candidacies : {cand_updated:>7} updated")
        print(f"  matched   : {c_matched:>7} / {c_total} ({c_pct}%)")
        print(f"  unmatched : {c_unmatched}")
        print(f"mandates    : {mand_updated:>7} updated")
        print(f"  matched   : {m_matched:>7} / {m_total} ({m_pct}%)")
        print(f"  unmatched : {m_unmatched}")
        print("Second-run check: re-running this script will update 0 rows.")
        print()

        if cand_unmatched_rows:
            print("Unmatched candidacy territory strings (top 50):")
            print(f"  {'territory':<40} {'n':>8}")
            print("  " + "-" * 50)
            for territory, n in cand_unmatched_rows:
                print(f"  {territory:<40} {n:>8}")
            print()

        if mand_unmatched_rows:
            print("Unmatched mandate territory strings:")
            print(f"  {'territory':<10} {'n':>8}")
            print("  " + "-" * 20)
            for territory, n in mand_unmatched_rows:
                print(f"  {territory:<10} {n:>8}")
            print()

        # ----------------------------------------------------------------
        # Warning threshold
        # ----------------------------------------------------------------
        if c_total > 0 and (c_unmatched / c_total) > UNMATCHED_WARNING_THRESHOLD:
            log.warning(
                "WARNING: %.2f%% of candidacies rows are unmatched (threshold %.0f%%).",
                c_unmatched * 100.0 / c_total,
                UNMATCHED_WARNING_THRESHOLD * 100,
            )

        if m_total > 0 and (m_unmatched / m_total) > UNMATCHED_WARNING_THRESHOLD:
            log.warning(
                "WARNING: %.2f%% of mandates rows are unmatched (threshold %.0f%%).",
                m_unmatched * 100.0 / m_total,
                UNMATCHED_WARNING_THRESHOLD * 100,
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()

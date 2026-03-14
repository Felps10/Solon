"""
Deduplicate people records that are accent-encoding variants of the same name.

Finds all groups where unaccent(canonical_name) collides across multiple rows,
merges them into a single survivor (chosen by data completeness), re-points all
foreign keys, and deletes the losers — all inside one transaction.

Uses set-based SQL (CTEs + bulk UPDATE/DELETE) rather than row-by-row Python
so the entire operation completes in seconds even on millions of rows.

Safe to re-run: exits cleanly with "No duplicates found" if nothing remains.

Usage:
    python backend/ingest/deduplicate_people.py
"""

import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.settings import settings


def get_connection():
    url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    return psycopg2.connect(url)


FIND_DUPLICATES_SQL = """
SELECT COUNT(DISTINCT public.immutable_unaccent(canonical_name)) AS groups,
       COUNT(*) - COUNT(DISTINCT public.immutable_unaccent(canonical_name)) AS duplicates
FROM people
WHERE public.immutable_unaccent(canonical_name) IN (
    SELECT public.immutable_unaccent(canonical_name)
    FROM people
    GROUP BY public.immutable_unaccent(canonical_name)
    HAVING COUNT(*) > 1
)
"""

# Build a complete duplicate→survivor mapping in pure SQL.
#
# survivor_candidates ranks every person within their duplicate group by:
#   1. candidacy count DESC  (most linked records wins)
#   2. birth_date NOT NULL DESC  (real birth date preferred)
#   3. LENGTH(canonical_name) DESC  (more accented form preferred)
#   4. created_at ASC  (earliest record preferred)
# The top-ranked member (rank=1) becomes the survivor; all others are duplicates.
#
# dup_map produces (duplicate_id, survivor_id) pairs for all non-survivors.
BUILD_MAPPING_SQL = """
WITH survivor_candidates AS (
    SELECT
        p.id,
        public.immutable_unaccent(p.canonical_name) AS norm_name,
        ROW_NUMBER() OVER (
            PARTITION BY public.immutable_unaccent(p.canonical_name)
            ORDER BY
                COUNT(c.id) DESC,
                (p.birth_date IS NOT NULL) DESC,
                LENGTH(p.canonical_name) DESC,
                p.created_at ASC
        ) AS rn
    FROM people p
    LEFT JOIN candidacies c ON c.person_id = p.id
    WHERE public.immutable_unaccent(p.canonical_name) IN (
        SELECT public.immutable_unaccent(canonical_name)
        FROM people
        GROUP BY public.immutable_unaccent(canonical_name)
        HAVING COUNT(*) > 1
    )
    GROUP BY p.id, p.canonical_name, p.birth_date, p.created_at
),
survivors AS (
    SELECT id, norm_name FROM survivor_candidates WHERE rn = 1
),
duplicates AS (
    SELECT sc.id AS dup_id, s.id AS survivor_id
    FROM survivor_candidates sc
    JOIN survivors s ON s.norm_name = sc.norm_name
    WHERE sc.rn > 1
)
SELECT dup_id::text, survivor_id::text FROM duplicates
"""


def main():
    t0 = time.time()
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # ------------------------------------------------------------------
        # 1. Quick check — any duplicates at all?
        # ------------------------------------------------------------------
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT public.immutable_unaccent(canonical_name)
                FROM people
                GROUP BY public.immutable_unaccent(canonical_name)
                HAVING COUNT(*) > 1
            ) g
        """)
        group_count = cur.fetchone()[0]

        if group_count == 0:
            print("No duplicates found — database is clean")
            return

        print(f"Found {group_count:,} duplicate group(s). Building merge map...")

        # ------------------------------------------------------------------
        # 2. Build the full (duplicate_id → survivor_id) mapping in one query
        # ------------------------------------------------------------------
        cur.execute(BUILD_MAPPING_SQL)
        rows = cur.fetchall()
        # rows = list of (dup_id_str, survivor_id_str)

        total_duplicates = len(rows)
        print(f"Merging {total_duplicates:,} duplicate records...")

        if total_duplicates == 0:
            print("No duplicates found — database is clean")
            return

        # Build separate lists for bulk operations
        dup_ids    = [r[0] for r in rows]
        # Create a VALUES list for the mapping: (dup_id, survivor_id)
        # We'll use a temp table for bulk updates
        cur.execute("""
            CREATE TEMP TABLE _dup_map (dup_id uuid, survivor_id uuid) ON COMMIT DROP
        """)
        # Bulk insert the mapping
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO _dup_map (dup_id, survivor_id) VALUES %s",
            rows,
            template="(%s::uuid, %s::uuid)",
        )
        print("  Temp mapping table populated.")

        # ------------------------------------------------------------------
        # 3a. Bulk re-point candidacies
        # ------------------------------------------------------------------
        cur.execute("""
            UPDATE candidacies
            SET person_id = m.survivor_id
            FROM _dup_map m
            WHERE candidacies.person_id = m.dup_id
        """)
        cand_repointed = cur.rowcount
        print(f"  Candidacies re-pointed : {cand_repointed:,}")

        # ------------------------------------------------------------------
        # 3b. Bulk re-point party_affiliations
        # ------------------------------------------------------------------
        cur.execute("""
            UPDATE party_affiliations
            SET person_id = m.survivor_id
            FROM _dup_map m
            WHERE party_affiliations.person_id = m.dup_id
        """)
        aff_repointed = cur.rowcount
        print(f"  Affiliations re-pointed: {aff_repointed:,}")

        # ------------------------------------------------------------------
        # 3c. Bulk re-point mandates
        # ------------------------------------------------------------------
        cur.execute("""
            UPDATE mandates
            SET person_id = m.survivor_id
            FROM _dup_map m
            WHERE mandates.person_id = m.dup_id
        """)
        man_repointed = cur.rowcount
        print(f"  Mandates re-pointed    : {man_repointed:,}")

        # ------------------------------------------------------------------
        # 3d. Bulk delete duplicate people
        # ------------------------------------------------------------------
        cur.execute("""
            DELETE FROM people
            WHERE id IN (SELECT dup_id FROM _dup_map)
        """)
        deleted = cur.rowcount
        print(f"  People deleted         : {deleted:,}")

        # ------------------------------------------------------------------
        # 4. Commit atomically
        # ------------------------------------------------------------------
        conn.commit()

    except Exception:
        conn.rollback()
        print("ERROR: transaction rolled back — no changes made", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

    elapsed = time.time() - t0

    print(f"""
Deduplication complete
----------------------
  Duplicate groups found   : {group_count:,}
  Duplicate records merged : {total_duplicates:,}
  Records deleted          : {deleted:,}
  Candidacies re-pointed   : {cand_repointed:,}
  Affiliations re-pointed  : {aff_repointed:,}
  Mandates re-pointed      : {man_repointed:,}
  Time                     : {elapsed:.1f}s
""")


if __name__ == "__main__":
    main()

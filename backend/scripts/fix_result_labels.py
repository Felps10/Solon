"""
Fix result labels for candidacies ingested as 'unknown' due to
unmapped DS_SIT_TOT_TURNO values in older TSE CSV files.

Usage:
    cd backend
    source .venv/bin/activate
    python scripts/fix_result_labels.py --dry-run
    python scripts/fix_result_labels.py --year 2008
    python scripts/fix_result_labels.py
"""

import argparse
import io
import logging
import sys
import zipfile
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests

# --- path setup: same pattern as other ingest scripts ---
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.settings import settings
from ingest.tse_candidates import RESULT_MAP, CDN_URL, ELECTION_YEARS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BATCH_SIZE = 1000


def get_connection():
    import re
    url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    url = re.sub(r"[?&]ssl=require", "", url)
    return psycopg2.connect(url, sslmode="require")


def get_years_with_unknowns(cur) -> dict[int, int]:
    """
    Query DB for years that have at least one result = 'unknown'.
    Returns {year: count}.
    """
    cur.execute("""
        SELECT e.year, COUNT(*) as n
        FROM candidacies c
        JOIN elections e ON e.id = c.election_id
        WHERE c.result = 'unknown'
        GROUP BY e.year
        ORDER BY e.year
    """)
    return {row[0]: row[1] for row in cur.fetchall()}


def download_and_parse_csv(year: int) -> dict[str, str]:
    """
    Download consulta_cand_{year}.zip, parse the BRASIL CSV,
    return {sq_candidato: DS_SIT_TOT_TURNO} for all rows.
    Only reads the two needed columns — SQ_CANDIDATO and DS_SIT_TOT_TURNO.
    Uses the same CDN_URL and zip/CSV parsing pattern as tse_candidates.py.
    """
    import pandas as pd

    url = CDN_URL.format(year=year)
    log.info(f"[{year}] Downloading {url}")
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()
    log.info(f"[{year}] Downloaded {len(resp.content) / 1_048_576:.1f} MB")

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        brasil = [n for n in zf.namelist() if "BRASIL" in n.upper() and n.endswith(".csv")]
        if not brasil:
            # Some years use a flat filename without BRASIL in the name
            brasil = [n for n in zf.namelist() if n.endswith(".csv")]
        fname = brasil[0]
        log.info(f"[{year}] Parsing {fname}")
        with zf.open(fname) as f:
            df = pd.read_csv(
                f,
                sep=";",
                encoding="latin-1",
                dtype=str,
                low_memory=False,
                usecols=["SQ_CANDIDATO", "DS_SIT_TOT_TURNO"],
            )

    # Strip whitespace from both columns and return as dict
    df["SQ_CANDIDATO"] = df["SQ_CANDIDATO"].str.strip()
    df["DS_SIT_TOT_TURNO"] = df["DS_SIT_TOT_TURNO"].str.strip()
    return dict(zip(df["SQ_CANDIDATO"], df["DS_SIT_TOT_TURNO"]))


def build_updates(
    sq_to_raw: dict[str, str],
    db_unknowns: list[tuple[str, str]],  # [(candidacy_id, source_label), ...]
) -> list[tuple[str, str]]:
    """
    Cross-reference TSE CSV data against DB unknown rows.
    Returns [(new_result, candidacy_id)] for rows that can be corrected.
    Excludes rows where new_result would still be 'unknown'.

    source_label format: "TSE/consulta_cand_{YEAR}/{SQ_CANDIDATO}"
    Extract SQ_CANDIDATO as: source_label.split('/')[-1]
    """
    updates = []
    for candidacy_id, source_label in db_unknowns:
        if not source_label:
            continue
        sq = source_label.split("/")[-1]
        raw_value = sq_to_raw.get(sq)
        if raw_value is None:
            continue
        new_result = RESULT_MAP.get(raw_value, "unknown")
        if new_result == "unknown":
            continue
        updates.append((new_result, candidacy_id))
    return updates


def process_year(cur, year: int, sq_to_raw: dict[str, str], dry_run: bool) -> dict:
    """
    Load DB unknown rows for this year, build updates, apply them.
    Returns stats dict with keys:
      db_unknowns, csv_rows, updates_found, updated, still_unknown
    """
    # Load unknowns from DB for this year
    cur.execute("""
        SELECT c.id, c.source_label
        FROM candidacies c
        JOIN elections e ON e.id = c.election_id
        WHERE c.result = 'unknown'
          AND e.year = %s
    """, (year,))
    db_unknowns = [(str(row[0]), row[1]) for row in cur.fetchall()]

    updates = build_updates(sq_to_raw, db_unknowns)

    stats = {
        "db_unknowns": len(db_unknowns),
        "csv_rows": len(sq_to_raw),
        "updates_found": len(updates),
        "updated": 0,
        "still_unknown": len(db_unknowns) - len(updates),
    }

    if dry_run or not updates:
        return stats

    # Bulk update in batches
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i : i + BATCH_SIZE]
        psycopg2.extras.execute_batch(
            cur,
            "UPDATE candidacies SET result = %s WHERE id = %s::uuid",
            batch,
        )
        cur.connection.commit()
        log.info(f"[{year}] Committed batch {i // BATCH_SIZE + 1} ({len(batch)} rows)")

    stats["updated"] = len(updates)
    return stats


def main():
    parser = argparse.ArgumentParser(description="Fix unknown result labels")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count affected rows without updating")
    parser.add_argument("--year", type=int, default=None,
                        help="Process only this year")
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    years_with_unknowns = get_years_with_unknowns(cur)

    print("\nYears with unknown results in DB:")
    print(f"  {'Year':<6} {'Unknowns':>10}")
    print(f"  {'-'*6} {'-'*10}")
    for y, n in years_with_unknowns.items():
        print(f"  {y:<6} {n:>10,}")
    print()

    years_to_process = [args.year] if args.year else list(years_with_unknowns.keys())
    years_to_process = [y for y in years_to_process if y in years_with_unknowns]

    if not years_to_process:
        print("No years to process.")
        return

    total_updated = 0
    total_still_unknown = 0

    for year in years_to_process:
        log.info(f"[{year}] Downloading CSV...")
        sq_to_raw = download_and_parse_csv(year)
        log.info(f"[{year}] {len(sq_to_raw):,} candidates in CSV")

        stats = process_year(cur, year, sq_to_raw, dry_run=args.dry_run)

        mode = "DRY RUN" if args.dry_run else "UPDATED"
        log.info(
            f"[{year}] unknowns in DB: {stats['db_unknowns']:,} | "
            f"CSV rows: {stats['csv_rows']:,} | "
            f"updates found: {stats['updates_found']:,} | "
            f"{mode}: {stats['updates_found'] if args.dry_run else stats['updated']:,} | "
            f"still unknown: {stats['still_unknown']:,}"
        )
        total_updated += stats["updates_found"] if args.dry_run else stats["updated"]
        total_still_unknown += stats["still_unknown"]

    print(f"\n{'DRY RUN' if args.dry_run else 'DONE'}")
    print(f"  Total rows updated: {total_updated:,}")
    print(f"  Total still unknown: {total_still_unknown:,}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

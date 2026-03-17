"""
TSE CPF backfill — populates people_external_ids with source='TSE_CPF'.

For each TSE election year, reads NM_CANDIDATO, NR_CPF_CANDIDATO, and
DT_NASCIMENTO from the BRASIL CSV, matches each unique CPF to an existing
people record by (canonical_name, birth_date), and inserts a row into
people_external_ids.

Natural key for deduplication:
  - people_external_ids has UNIQUE(source, external_id), so re-runs are safe.
  - ON CONFLICT DO NOTHING makes every run fully idempotent.

Local cache:
  backend/data/raw/tse/consulta_cand_{year}/consulta_cand_{year}_BRASIL.csv
  If this file exists it is used directly (no download).
  If missing, the ZIP is downloaded, the BRASIL CSV extracted, and the ZIP
  deleted afterwards (extracted CSV kept for future re-runs).
"""

import argparse
import io
import logging
import sys
import uuid
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

# Make app importable when run as  python -m ingest.tse_cpf_backfill
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.settings import settings
from ingest.tse_candidates import (
    ELECTION_YEARS,
    CDN_URL,
    HIDDEN_VALUES,
    clean,
    normalize_name,
    parse_br_date,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw" / "tse"
BATCH_SIZE = 500
SOURCE = "TSE_CPF"

# CPF values that mean "not available" in TSE data
CPF_HIDDEN = HIDDEN_VALUES | {"00000000000", "0", "00000000191"}


# ---------------------------------------------------------------------------
# DB connection — handles both asyncpg and plain postgresql URLs, plus Neon SSL
# ---------------------------------------------------------------------------
def get_connection():
    url = (
        settings.database_url
        .replace("postgresql+asyncpg://", "postgresql://")
        .replace("?ssl=require", "?sslmode=require")
        .replace("&ssl=require", "&sslmode=require")
    )
    return psycopg2.connect(url)


# ---------------------------------------------------------------------------
# CPF helpers
# ---------------------------------------------------------------------------
def clean_cpf(raw) -> Optional[str]:
    """Normalize CPF: digits only, 11 chars, reject all-zero values."""
    s = clean(str(raw)) if raw is not None else ""
    digits = "".join(c for c in s if c.isdigit())
    if not digits:
        return None
    if digits in CPF_HIDDEN or set(digits) == {"0"}:
        return None
    # Pad to 11 digits (TSE sometimes drops leading zeros)
    return digits.zfill(11)


# ---------------------------------------------------------------------------
# Local cache / download
# ---------------------------------------------------------------------------
def get_brasil_csv(year: int) -> Optional[Path]:
    """
    Return path to the BRASIL CSV for *year*, downloading if needed.
    Returns None if the file cannot be obtained.
    """
    year_dir = DATA_DIR / f"consulta_cand_{year}"
    brasil_csv = year_dir / f"consulta_cand_{year}_BRASIL.csv"

    if brasil_csv.exists():
        log.info(f"[{year}] Using cached CSV: {brasil_csv}")
        return brasil_csv

    # Check if a previously-downloaded ZIP is sitting next to the year dir
    zip_path = DATA_DIR / f"consulta_cand_{year}.zip"
    if zip_path.exists():
        log.info(f"[{year}] Extracting BRASIL CSV from local ZIP: {zip_path}")
        return _extract_brasil(zip_path, year_dir, year, delete_zip=False)

    # Download from CDN
    url = CDN_URL.format(year=year)
    log.info(f"[{year}] Downloading {url}")
    try:
        resp = requests.get(url, timeout=180, stream=True)
        resp.raise_for_status()
    except requests.HTTPError as e:
        if resp.status_code == 404:
            log.warning(f"[{year}] ZIP not found on CDN (404) — skipping")
        else:
            log.warning(f"[{year}] HTTP error: {e} — skipping")
        return None
    except requests.RequestException as e:
        log.warning(f"[{year}] Download failed: {e} — skipping")
        return None

    log.info(f"[{year}] Downloaded {len(resp.content) / 1_048_576:.1f} MB")
    zip_bytes_path = DATA_DIR / f"consulta_cand_{year}.zip"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    zip_bytes_path.write_bytes(resp.content)

    result = _extract_brasil(zip_bytes_path, year_dir, year, delete_zip=True)
    return result


def _extract_brasil(zip_path: Path, year_dir: Path, year: int,
                    delete_zip: bool) -> Optional[Path]:
    """Extract the BRASIL CSV from *zip_path* into *year_dir*."""
    year_dir.mkdir(parents=True, exist_ok=True)
    brasil_csv = year_dir / f"consulta_cand_{year}_BRASIL.csv"
    try:
        with zipfile.ZipFile(zip_path) as zf:
            brasil_files = [
                n for n in zf.namelist()
                if "BRASIL" in n.upper() and n.endswith(".csv")
            ]
            if not brasil_files:
                log.error(
                    f"[{year}] No BRASIL CSV in ZIP. Contents: {zf.namelist()}"
                )
                return None
            entry = brasil_files[0]
            log.info(f"[{year}] Extracting {entry}")
            data = zf.read(entry)
            brasil_csv.write_bytes(data)
    except zipfile.BadZipFile as e:
        log.error(f"[{year}] Bad ZIP: {e}")
        return None

    if delete_zip:
        zip_path.unlink(missing_ok=True)
        log.info(f"[{year}] Deleted ZIP after extraction")

    return brasil_csv


# ---------------------------------------------------------------------------
# Bulk person lookup — one query per year instead of one per row
# ---------------------------------------------------------------------------
def bulk_lookup_people(cur, cpf_groups: "pd.DataFrame") -> dict:
    """
    Given a DataFrame with columns [cpf, norm_name, birth_date], return a
    dict mapping cpf → person_id for every CPF that matched a people row.

    Strategy: build two sets of candidates — those with a known birth_date
    and those without — then query each set in a single SQL statement using
    a temporary VALUES table joined against people.  Falls back to a chunked
    approach if the VALUES list is very large.
    """
    import pandas as pd

    cpf_to_person: dict = {}

    # Split into dated vs undated
    has_date = cpf_groups[cpf_groups["birth_date"].notna()].copy()
    no_date  = cpf_groups[cpf_groups["birth_date"].isna()].copy()

    # --- Dated candidates: match on (unaccent(name), birth_date) ---
    if not has_date.empty:
        # Build a VALUES list: (cpf, norm_name, birth_date)
        # Chunk to avoid overly large queries
        CHUNK = 5000
        for start in range(0, len(has_date), CHUNK):
            chunk = has_date.iloc[start:start + CHUNK]
            values_rows = [
                cur.mogrify("(%s, %s, %s::date)", (
                    row["cpf"], row["norm_name"], row["birth_date"]
                )).decode()
                for _, row in chunk.iterrows()
            ]
            values_sql = ", ".join(values_rows)
            cur.execute(
                f"""
                SELECT v.cpf, p.id
                FROM (VALUES {values_sql}) AS v(cpf, norm_name, birth_date)
                JOIN people p
                  ON public.immutable_unaccent(p.canonical_name)
                   = public.immutable_unaccent(v.norm_name)
                 AND p.birth_date = v.birth_date
                """
            )
            for cpf, person_id in cur.fetchall():
                cpf_to_person[cpf] = str(person_id)

    # --- Undated candidates: match on unaccent(name) where birth_date IS NULL ---
    if not no_date.empty:
        CHUNK = 5000
        for start in range(0, len(no_date), CHUNK):
            chunk = no_date.iloc[start:start + CHUNK]
            values_rows = [
                cur.mogrify("(%s, %s)", (row["cpf"], row["norm_name"])).decode()
                for _, row in chunk.iterrows()
            ]
            values_sql = ", ".join(values_rows)
            cur.execute(
                f"""
                SELECT v.cpf, p.id
                FROM (VALUES {values_sql}) AS v(cpf, norm_name)
                JOIN people p
                  ON public.immutable_unaccent(p.canonical_name)
                   = public.immutable_unaccent(v.norm_name)
                 AND p.birth_date IS NULL
                """
            )
            for cpf, person_id in cur.fetchall():
                cpf_to_person[cpf] = str(person_id)

    return cpf_to_person


# ---------------------------------------------------------------------------
# Per-year processing
# ---------------------------------------------------------------------------
def process_year(year: int, dry_run: bool) -> dict:
    stats = {
        "year": year,
        "csv_rows": 0,
        "unique_cpfs": 0,
        "matched": 0,
        "unmatched": 0,
        "skipped_conflict": 0,
    }

    # --- Obtain CSV ---
    csv_path = get_brasil_csv(year)
    if csv_path is None:
        log.warning(f"[{year}] No CSV available — skipping year")
        return stats

    # --- Read only the columns we need ---
    try:
        df = pd.read_csv(
            csv_path,
            sep=";",
            encoding="latin-1",
            dtype=str,
            low_memory=False,
            usecols=["NM_CANDIDATO", "NR_CPF_CANDIDATO", "DT_NASCIMENTO"],
        )
    except Exception as e:
        log.error(f"[{year}] Failed to parse CSV: {e} — skipping year")
        return stats

    stats["csv_rows"] = len(df)
    log.info(f"[{year}] {len(df):,} rows read from CSV")

    # --- Clean and filter ---
    df["cpf"] = df["NR_CPF_CANDIDATO"].apply(clean_cpf)
    df = df[df["cpf"].notna()].copy()

    df["norm_name"] = df["NM_CANDIDATO"].apply(normalize_name)
    df["birth_date"] = df["DT_NASCIMENTO"].apply(parse_br_date)

    # --- Deduplicate by CPF: one CPF → one person ---
    # Keep the most frequent (norm_name, birth_date) pair per CPF.
    # dropna=False ensures NaN values are counted, avoiding IndexError on all-NaN groups.
    cpf_groups = (
        df.groupby("cpf")[["norm_name", "birth_date"]]
        .agg(lambda s: s.value_counts(dropna=False).index[0])
        .reset_index()
    )
    stats["unique_cpfs"] = len(cpf_groups)
    log.info(f"[{year}] {len(cpf_groups):,} unique CPFs after dedup")

    # --- DB operations ---
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Pre-fetch CPFs already in people_external_ids (all at once)
        log.info(f"[{year}] Fetching existing CPFs from people_external_ids...")
        cur.execute(
            "SELECT external_id FROM people_external_ids WHERE source = %s",
            (SOURCE,),
        )
        existing_cpfs = {row[0] for row in cur.fetchall()}
        skippable = cpf_groups[cpf_groups["cpf"].isin(existing_cpfs)]
        stats["skipped_conflict"] = len(skippable)
        cpf_groups = cpf_groups[~cpf_groups["cpf"].isin(existing_cpfs)].copy()
        log.info(
            f"[{year}] {stats['skipped_conflict']:,} already in DB, "
            f"{len(cpf_groups):,} to match"
        )

        if cpf_groups.empty:
            cur.close()
            return stats

        # Bulk lookup: one round-trip per chunk of 5000
        log.info(f"[{year}] Bulk-matching {len(cpf_groups):,} CPFs against people...")
        cpf_to_person = bulk_lookup_people(cur, cpf_groups)
        stats["matched"] = len(cpf_to_person)
        stats["unmatched"] = len(cpf_groups) - stats["matched"]
        log.info(
            f"[{year}] Matched {stats['matched']:,} / {len(cpf_groups):,} CPFs"
        )

        # Insert matched rows — execute_values sends one multi-row INSERT per
        # batch (not one INSERT per row), keeping round-trips to ~6 for 28K rows.
        if not dry_run and cpf_to_person:
            insert_rows = [
                (str(uuid.uuid4()), person_id, SOURCE, cpf)
                for cpf, person_id in cpf_to_person.items()
            ]
            for i in range(0, len(insert_rows), BATCH_SIZE):
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO people_external_ids (id, person_id, source, external_id) "
                    "VALUES %s "
                    "ON CONFLICT (source, external_id) DO NOTHING",
                    insert_rows[i:i + BATCH_SIZE],
                )
            conn.commit()
            log.info(f"[{year}] Committed {len(insert_rows):,} inserts")

        cur.close()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def print_year_summary(s: dict, dry_run: bool):
    total = s["unique_cpfs"]
    matched = s["matched"]
    rate = (matched / total * 100) if total else 0.0
    tag = " [DRY RUN — no inserts]" if dry_run else ""
    print(
        f"\n[{s['year']}] CPF backfill summary{tag}\n"
        f"  CSV rows           : {s['csv_rows']:>10,}\n"
        f"  Unique CPFs        : {total:>10,}\n"
        f"  Matched            : {matched:>10,}  ({rate:.1f}%)\n"
        f"  Unmatched          : {s['unmatched']:>10,}\n"
        f"  Skipped (conflict) : {s['skipped_conflict']:>10,}\n"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Backfill TSE CPF data into people_external_ids"
    )
    parser.add_argument(
        "--year", type=int, default=None,
        help="Single election year to process. Omit to run all years.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Match CPFs to people but do not insert anything.",
    )
    args = parser.parse_args()

    years = [args.year] if args.year else ELECTION_YEARS
    dry_run = args.dry_run

    if dry_run:
        log.info("DRY RUN mode — no data will be written")

    all_stats = []
    for year in years:
        log.info(f"{'='*60}")
        log.info(f"Processing year {year}")
        log.info(f"{'='*60}")
        try:
            s = process_year(year, dry_run)
            all_stats.append(s)
            print_year_summary(s, dry_run)
        except Exception as e:
            log.error(f"[{year}] FAILED: {e}", exc_info=True)
            all_stats.append({"year": year, "csv_rows": 0, "unique_cpfs": 0,
                               "matched": 0, "unmatched": 0, "skipped_conflict": 0})

    # Global summary
    total_cpfs   = sum(s["unique_cpfs"] for s in all_stats)
    total_match  = sum(s["matched"]     for s in all_stats)
    total_skip   = sum(s["skipped_conflict"] for s in all_stats)
    overall_rate = (total_match / total_cpfs * 100) if total_cpfs else 0.0

    low_match = [
        s["year"] for s in all_stats
        if s["unique_cpfs"] > 0
        and (s["matched"] / s["unique_cpfs"]) < 0.70
    ]

    print("\n" + "=" * 60)
    print("CPF BACKFILL — OVERALL SUMMARY")
    print("=" * 60)
    print(f"  Years processed    : {len(all_stats)}")
    print(f"  Total unique CPFs  : {total_cpfs:,}")
    print(f"  Total matched      : {total_match:,}  ({overall_rate:.1f}%)")
    print(f"  Total skipped      : {total_skip:,}")
    if low_match:
        print(f"  Years < 70% match  : {low_match}")
    else:
        print("  All years ≥ 70% match rate")
    print("=" * 60)


if __name__ == "__main__":
    main()

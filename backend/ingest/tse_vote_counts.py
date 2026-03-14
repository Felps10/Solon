"""
TSE vote count ingestion.

Downloads votacao_candidato_munzona_{YEAR}.zip from the TSE CDN,
aggregates QT_VOTOS_NOMINAIS per SQ_CANDIDATO across all
municipality/zone rows, then bulk-updates candidacies.vote_count
by matching on source_label = 'TSE/consulta_cand_{YEAR}/{SQ_CANDIDATO}'.

Join Strategy A is used: SQ_CANDIDATO + year is an exact, unambiguous
match that requires no name fuzzing — the value is already embedded in
our source_label column.

Note: the BRASIL file inside the ZIP is 4+ GB uncompressed. This script
iterates individual state files (AC, AL, AM, …, TO) instead, which are
much smaller and cover the same data without duplication.

Usage:
    cd backend
    source .venv/bin/activate
    python -m ingest.tse_vote_counts --year 2022
    python -m ingest.tse_vote_counts        # all years
"""

import argparse
import io
import logging
import sys
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

ELECTION_YEARS = [
    1994, 1996, 1998, 2000, 2002, 2004, 2006, 2008,
    2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024,
]

CDN_URL = (
    "https://cdn.tse.jus.br/estatistica/sead/odsele/"
    "votacao_candidato_munzona/votacao_candidato_munzona_{year}.zip"
)

BATCH_SIZE = 5000


def get_connection():
    url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    return psycopg2.connect(url)


def download_zip(year: int) -> Optional[bytes]:
    url = CDN_URL.format(year=year)
    log.info(f"[{year}] Downloading {url}")
    try:
        resp = requests.get(url, timeout=300, stream=True)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            log.warning(f"[{year}] File not found on CDN (404) — skipping")
            return None
        raise
    except requests.RequestException as e:
        raise RuntimeError(f"[{year}] Download failed: {e}") from e
    data = resp.content
    log.info(f"[{year}] Downloaded {len(data) / 1_048_576:.1f} MB")
    return data


def aggregate_votes_from_zip(year: int, zip_bytes: bytes) -> dict[str, int]:
    """
    Open the ZIP and aggregate QT_VOTOS_NOMINAIS per SQ_CANDIDATO
    by streaming all per-state CSV files (skipping the BRASIL aggregate).
    Returns {sq_candidato_str: total_votes}.
    """
    votes: dict[str, int] = defaultdict(int)
    total_rows = 0

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        state_files = [
            n for n in zf.namelist()
            if n.endswith(".csv") and "BRASIL" not in n.upper()
        ]
        if not state_files:
            # Fallback: some years may only have BRASIL file
            state_files = [n for n in zf.namelist() if n.endswith(".csv")]

        log.info(f"[{year}] Processing {len(state_files)} state file(s)")

        for fname in state_files:
            with zf.open(fname) as f:
                raw = f.read()
            text = raw.decode("latin-1")
            lines = text.splitlines()
            if not lines:
                continue

            # Parse header once per file
            headers = [h.strip('"') for h in lines[0].split(";")]
            try:
                sq_idx  = headers.index("SQ_CANDIDATO")
                qt_idx  = headers.index("QT_VOTOS_NOMINAIS")
            except ValueError:
                log.warning(f"[{year}] Required columns missing in {fname} — skipping")
                continue

            file_rows = 0
            for line in lines[1:]:
                if not line.strip():
                    continue
                parts = line.split(";")
                if len(parts) <= max(sq_idx, qt_idx):
                    continue
                sq  = parts[sq_idx].strip('"').strip()
                raw_qt = parts[qt_idx].strip('"').strip()
                if not sq or not raw_qt:
                    continue
                try:
                    qt = int(raw_qt)
                except ValueError:
                    continue
                votes[sq] += qt
                file_rows += 1

            total_rows += file_rows

    log.info(
        f"[{year}] Aggregated {total_rows:,} zone-rows → "
        f"{len(votes):,} unique candidates"
    )
    return dict(votes)


def update_vote_counts(year: int, votes: dict[str, int]) -> dict:
    """
    Bulk-update candidacies.vote_count for the given year.
    Matches on source_label = 'TSE/consulta_cand_{year}/{sq}'.
    Returns stats dict.
    """
    source_prefix = f"TSE/consulta_cand_{year}/"

    conn = get_connection()
    try:
        cur = conn.cursor()

        # Load all (source_label, id) for this year so we can match in Python
        cur.execute(
            "SELECT source_label, id FROM candidacies WHERE source_label LIKE %s",
            (source_prefix + "%",),
        )
        db_rows = cur.fetchall()
        # Map sq_candidato_str → candidacy_id
        label_map: dict[str, str] = {}
        for label, cid in db_rows:
            sq = label[len(source_prefix):]  # strip 'TSE/consulta_cand_{year}/'
            label_map[sq] = str(cid)

        db_count = len(label_map)

        # Match TSE votes to DB candidacies
        updates = []   # (vote_count, candidacy_id)
        unmatched = 0
        for sq, total_votes in votes.items():
            cid = label_map.get(sq)
            if cid:
                updates.append((total_votes, cid))
            else:
                unmatched += 1

        matched   = len(updates)
        null_remaining = db_count - matched

        log.info(
            f"[{year}] {db_count:,} in DB | "
            f"{matched:,} matched | "
            f"{unmatched:,} TSE-only | "
            f"{null_remaining:,} will remain NULL"
        )

        # Bulk update in batches
        for i in range(0, len(updates), BATCH_SIZE):
            batch = updates[i : i + BATCH_SIZE]
            psycopg2.extras.execute_batch(
                cur,
                "UPDATE candidacies SET vote_count = %s WHERE id = %s::uuid",
                batch,
                page_size=BATCH_SIZE,
            )
        conn.commit()
        cur.close()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "tse_candidates": len(votes),
        "db_candidacies": db_count,
        "matched": matched,
        "unmatched_tse": unmatched,
        "null_remaining": null_remaining,
    }


def ingest_year(year: int) -> Optional[dict]:
    zip_bytes = download_zip(year)
    if zip_bytes is None:
        return None

    votes = aggregate_votes_from_zip(year, zip_bytes)
    if not votes:
        log.warning(f"[{year}] No vote data found in ZIP")
        return None

    stats = update_vote_counts(year, votes)
    log.info(
        f"[{year}] Done — {stats['matched']:,} candidacies updated, "
        f"{stats['null_remaining']:,} remain NULL"
    )
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Ingest TSE vote counts into memoria_politica"
    )
    parser.add_argument(
        "--year", type=int, default=None,
        help="Single election year to ingest. Omit to run all years.",
    )
    args = parser.parse_args()

    years = [args.year] if args.year else ELECTION_YEARS

    rows = []
    total_tse = total_matched = total_unmatched = total_null = 0

    for year in years:
        log.info(f"{'=' * 60}")
        log.info(f"Processing year {year}")
        log.info(f"{'=' * 60}")
        try:
            stats = ingest_year(year)
            if stats is None:
                rows.append((year, "SKIP", "-", "-", "-", "-"))
                continue
            total_tse      += stats["tse_candidates"]
            total_matched  += stats["matched"]
            total_unmatched += stats["unmatched_tse"]
            total_null     += stats["null_remaining"]
            rows.append((
                year,
                "OK",
                f"{stats['tse_candidates']:,}",
                f"{stats['matched']:,}",
                f"{stats['unmatched_tse']:,}",
                f"{stats['null_remaining']:,}",
            ))
        except Exception as e:
            log.error(f"[{year}] FAILED: {e}")
            rows.append((year, "ERROR", "-", "-", "-", str(e)[:40]))

    print("\n" + "=" * 80)
    print(f"{'Year':<6} {'Status':<7} {'TSE cands':>12} {'Matched':>10} {'Unmatched':>10} {'Null left':>10}")
    print("-" * 80)
    for r in rows:
        print(f"{r[0]:<6} {r[1]:<7} {r[2]:>12} {r[3]:>10} {r[4]:>10} {r[5]:>10}")
    print("-" * 80)
    print(
        f"{'TOTAL':<6} {'':7} {total_tse:>12,} {total_matched:>10,} "
        f"{total_unmatched:>10,} {total_null:>10,}"
    )
    print("=" * 80)


if __name__ == "__main__":
    main()

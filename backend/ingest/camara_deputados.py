"""
Câmara dos Deputados ingestion.

Fetches all deputados from legislatures 42-57 (1963–2027) via the Câmara
open-data API, matches them to existing people records, creates mandate
records, and stores CAMARA external IDs.

Person matching (in priority order):
  1. CPF — look up TSE_CPF row in people_external_ids (authoritative)
  2. name + birth_date — immutable_unaccent() match on people table
  3. Create a new person record if neither strategy finds a match

# DEDUPLICATION POLICY:
# Uses the same immutable_unaccent() normalisation as tse_candidates.py.
# CPF match is always preferred when available. Name+date match is a
# secondary fallback. New people created here may later be linked to
# TSE records via cpf_backfill. The first ingested canonical_name form
# is treated as canonical and is not overwritten.
"""

import argparse
import logging
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.settings import settings

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
API_BASE = "https://dadosabertos.camara.leg.br/api/v2"

LEGISLATURES: dict[int, tuple[str, str]] = {
    42: ("1963-02-01", "1967-01-31"),
    43: ("1967-02-01", "1971-01-31"),
    44: ("1971-02-01", "1975-01-31"),
    45: ("1975-02-01", "1979-01-31"),
    46: ("1979-02-01", "1983-01-31"),
    47: ("1983-02-01", "1987-01-31"),
    48: ("1987-02-01", "1991-01-31"),
    49: ("1991-02-01", "1995-01-31"),
    50: ("1995-02-01", "1999-01-31"),
    51: ("1999-02-01", "2003-01-31"),
    52: ("2003-02-01", "2007-01-31"),
    53: ("2007-02-01", "2011-01-31"),
    54: ("2011-02-01", "2015-01-31"),
    55: ("2015-02-01", "2019-01-31"),
    56: ("2019-02-01", "2023-01-31"),
    57: ("2023-02-01", "2027-01-31"),
}

BATCH_SIZE = 50      # commit every N deputados
API_DELAY = 0.3      # seconds between detail calls
MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_name(raw: str) -> str:
    """Uppercase + collapse whitespace — same logic as tse_candidates.py."""
    s = str(raw).strip() if raw else ""
    return " ".join(s.upper().split()) if s else ""


def parse_date(raw) -> Optional[str]:
    """Accept YYYY-MM-DD or DD/MM/YYYY, return YYYY-MM-DD. None on failure."""
    if not raw:
        return None
    s = str(raw).strip()
    if not s or s.lower() in {"null", "none", ""}:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def normalize_cpf(raw) -> Optional[str]:
    """Strip non-digits from CPF string. Returns None if result ≠ 11 digits."""
    if not raw:
        return None
    digits = "".join(c for c in str(raw) if c.isdigit())
    return digits if len(digits) == 11 else None


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def get_connection():
    """Prefer NEON_DATABASE_URL when set; fall back to DATABASE_URL."""
    raw = settings.neon_database_url or settings.database_url
    url = raw.replace("postgresql+asyncpg://", "postgresql://")
    return psycopg2.connect(url)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def api_get(url: str, params: dict = None) -> Optional[dict]:
    """GET with retry on 429/5xx. Returns None on permanent failure or 404."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = 2 ** attempt
                log.warning("HTTP %s at %s — retry %d/%d after %ds",
                            resp.status_code, url, attempt + 1, MAX_RETRIES, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            wait = 2 ** attempt
            log.warning("Request error: %s — retry %d/%d after %ds",
                        exc, attempt + 1, MAX_RETRIES, wait)
            time.sleep(wait)
    log.error("Permanently failed after %d retries: %s", MAX_RETRIES, url)
    return None


def fetch_legislature_list(leg_id: int) -> list[dict]:
    """Fetch all deputado list entries for a legislature (paginated)."""
    results: list[dict] = []
    page = 1
    while True:
        data = api_get(f"{API_BASE}/deputados", params={
            "idLegislatura": leg_id,
            "itens": 100,
            "pagina": page,
        })
        if not data or not data.get("dados"):
            break
        results.extend(data["dados"])
        links = data.get("links", [])
        if not any(lnk.get("rel") == "next" for lnk in links):
            break
        page += 1
    return results


def fetch_detail(camara_id: int) -> Optional[dict]:
    """Fetch /deputados/{id} detail. Returns the 'dados' dict or None."""
    data = api_get(f"{API_BASE}/deputados/{camara_id}")
    return data.get("dados") if data else None


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------

def find_person_by_cpf(cur, cpf: str) -> Optional[str]:
    cur.execute(
        "SELECT person_id FROM people_external_ids "
        "WHERE source = 'TSE_CPF' AND external_id = %s",
        (cpf,),
    )
    row = cur.fetchone()
    return str(row[0]) if row else None


def find_person_by_name_date(cur, name: str, birth_date: Optional[str]) -> Optional[str]:
    if birth_date:
        cur.execute(
            "SELECT id FROM people "
            "WHERE public.immutable_unaccent(canonical_name) "
            "    = public.immutable_unaccent(%(name)s) "
            "  AND birth_date = %(birth_date)s "
            "LIMIT 1",
            {"name": name, "birth_date": birth_date},
        )
    else:
        cur.execute(
            "SELECT id FROM people "
            "WHERE public.immutable_unaccent(canonical_name) "
            "    = public.immutable_unaccent(%(name)s) "
            "  AND birth_date IS NULL "
            "LIMIT 1",
            {"name": name},
        )
    row = cur.fetchone()
    return str(row[0]) if row else None


def create_person(cur, name: str, birth_date: Optional[str],
                  death_date: Optional[str], gender: Optional[str]) -> str:
    pid = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO people (id, canonical_name, birth_date, death_date, gender) "
        "VALUES (%s, %s, %s, %s, %s)",
        (pid, name, birth_date, death_date, gender),
    )
    return pid


def upsert_external_id(cur, person_id: str, source: str, external_id: str) -> None:
    cur.execute(
        "INSERT INTO people_external_ids (id, person_id, source, external_id) "
        "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
        (str(uuid.uuid4()), person_id, source, external_id),
    )


def mandate_exists(cur, person_id: str, source_label: str) -> bool:
    cur.execute(
        "SELECT id FROM mandates WHERE person_id = %s AND source_label = %s LIMIT 1",
        (person_id, source_label),
    )
    return cur.fetchone() is not None


def insert_mandate(cur, person_id: str, office_id: str, territory: str,
                   leg_start: str, leg_end: str, source_label: str) -> None:
    cur.execute(
        "INSERT INTO mandates "
        "  (id, person_id, office_id, territory, validity, "
        "   source_label, confidence, interrupted, is_approximate, date_precision) "
        "VALUES (%s, %s, %s, %s, "
        "        tstzrange(%s::timestamptz, %s::timestamptz, '[)'), "
        "        %s, %s, %s, %s, %s)",
        (
            str(uuid.uuid4()),
            person_id,
            office_id,
            territory,
            leg_start,
            leg_end,
            source_label,
            "high",
            False,
            False,
            "day",
        ),
    )


# ---------------------------------------------------------------------------
# Per-legislature ingestion
# ---------------------------------------------------------------------------

def ingest_legislature(leg_id: int, office_id: str, dry_run: bool) -> dict:
    leg_start, leg_end = LEGISLATURES[leg_id]

    log.info("[Leg %d] Fetching deputado list (%s → %s)…", leg_id, leg_start, leg_end)
    entries = fetch_legislature_list(leg_id)
    total_api = len(entries)
    log.info("[Leg %d] %d deputados in API", leg_id, total_api)

    stats = {
        "total_api": total_api,
        "matched_cpf": 0,
        "matched_name": 0,
        "created_people": 0,
        "mandates_inserted": 0,
        "mandates_skipped": 0,
        "errors": 0,
    }

    # Split entries into batches — each batch gets a fresh DB connection so
    # Neon's idle-connection timeout cannot fire during the API call loop.
    batches = [entries[i:i + BATCH_SIZE] for i in range(0, len(entries), BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches):
        # --- Fetch API details for the whole batch before touching DB ---
        batch_details: list[tuple[dict, Optional[dict]]] = []
        for entry in batch:
            time.sleep(API_DELAY)
            detail = fetch_detail(entry["id"])
            batch_details.append((entry, detail))

        if dry_run:
            # In dry-run, do matching with a single short-lived connection
            conn = get_connection()
            try:
                cur = conn.cursor()
                for entry, detail in batch_details:
                    camara_id = entry["id"]
                    if detail is None:
                        stats["errors"] += 1
                        continue
                    nome_civil = normalize_name(detail.get("nomeCivil") or "")
                    cpf = normalize_cpf(detail.get("cpf"))
                    birth_date = parse_date(detail.get("dataNascimento"))
                    if not nome_civil:
                        stats["errors"] += 1
                        continue
                    person_id = None
                    if cpf:
                        person_id = find_person_by_cpf(cur, cpf)
                        if person_id:
                            stats["matched_cpf"] += 1
                    if not person_id:
                        person_id = find_person_by_name_date(cur, nome_civil, birth_date)
                        if person_id:
                            stats["matched_name"] += 1
                    if not person_id:
                        stats["created_people"] += 1
                    stats["mandates_inserted"] += 1
                cur.close()
            finally:
                conn.close()
            continue

        # --- Real run: open a fresh connection, write, commit, close ---
        conn = get_connection()
        try:
            cur = conn.cursor()
            for entry, detail in batch_details:
                camara_id = entry["id"]
                sigla_uf = (entry.get("siglaUf") or "").strip()

                if detail is None:
                    log.warning("[Leg %d] camara_id=%s returned no detail, skipping",
                                leg_id, camara_id)
                    stats["errors"] += 1
                    continue

                nome_civil = normalize_name(detail.get("nomeCivil") or "")
                cpf = normalize_cpf(detail.get("cpf"))
                birth_date = parse_date(detail.get("dataNascimento"))
                death_date = parse_date(detail.get("dataFalecimento"))
                sexo = (detail.get("sexo") or "").upper()
                gender = "M" if sexo == "M" else ("F" if sexo == "F" else None)

                if not nome_civil:
                    log.warning("[Leg %d] camara_id=%s has empty nomeCivil, skipping",
                                leg_id, camara_id)
                    stats["errors"] += 1
                    continue

                # Person matching
                person_id: Optional[str] = None
                if cpf:
                    person_id = find_person_by_cpf(cur, cpf)
                    if person_id:
                        stats["matched_cpf"] += 1
                if not person_id:
                    person_id = find_person_by_name_date(cur, nome_civil, birth_date)
                    if person_id:
                        stats["matched_name"] += 1
                if not person_id:
                    stats["created_people"] += 1
                    person_id = create_person(cur, nome_civil, birth_date, death_date, gender)

                # External IDs
                upsert_external_id(cur, person_id, "CAMARA", str(camara_id))
                if cpf:
                    upsert_external_id(cur, person_id, "TSE_CPF", cpf)

                # Mandate
                source_label = f"CAMARA/legislatura/{leg_id}/{camara_id}"
                if mandate_exists(cur, person_id, source_label):
                    stats["mandates_skipped"] += 1
                else:
                    insert_mandate(
                        cur, person_id, office_id, sigla_uf,
                        leg_start, leg_end, source_label,
                    )
                    stats["mandates_inserted"] += 1

            conn.commit()
            cur.close()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        log.debug("[Leg %d] batch %d/%d committed", leg_id, batch_idx + 1, len(batches))

    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def get_office_id(name: str = "DEPUTADO FEDERAL") -> str:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM offices WHERE name = %s", (name,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Office '{name}' not found — run TSE ingestion first")
        return str(row[0])
    finally:
        conn.close()


def print_leg_summary(leg_id: int, stats: dict) -> None:
    leg_start, leg_end = LEGISLATURES[leg_id]
    total = stats["total_api"]
    matched = stats["matched_cpf"] + stats["matched_name"]
    match_rate = (matched / total * 100) if total else 0.0
    new_pct = (stats["created_people"] / total * 100) if total else 0.0
    print(f"\nLegislature {leg_id}  ({leg_start} → {leg_end})")
    print(f"  Deputados in API   : {total:>5,}")
    print(f"  Matched via CPF    : {stats['matched_cpf']:>5,}")
    print(f"  Matched via name   : {stats['matched_name']:>5,}")
    print(f"  New people created : {stats['created_people']:>5,}  ({new_pct:.1f}%)")
    print(f"  Match rate         : {match_rate:>5.1f}%")
    print(f"  Mandates inserted  : {stats['mandates_inserted']:>5,}")
    print(f"  Mandates skipped   : {stats['mandates_skipped']:>5,}")
    print(f"  Errors             : {stats['errors']:>5,}")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Câmara dos Deputados legislatures into Sólon"
    )
    parser.add_argument(
        "--legislature", type=int, default=None,
        help="Single legislature ID to process (42–57)",
    )
    parser.add_argument(
        "--start", type=int, default=42,
        help="Start from this legislature number (default: 42)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Match people and count operations without writing to DB",
    )
    args = parser.parse_args()

    if args.legislature is not None:
        if args.legislature not in LEGISLATURES:
            parser.error(f"--legislature must be between 42 and 57")
        leg_ids = [args.legislature]
    else:
        leg_ids = sorted(lid for lid in LEGISLATURES if lid >= args.start)

    dry = args.dry_run
    if dry:
        log.info("DRY RUN — no database writes will occur")

    office_id = get_office_id("DEPUTADO FEDERAL")
    log.info("DEPUTADO FEDERAL office_id = %s", office_id)

    all_stats: list[dict] = []
    t0 = time.time()

    for leg_id in leg_ids:
        log.info("\n%s", "=" * 60)
        log.info("Legislature %d", leg_id)
        log.info("%s", "=" * 60)
        try:
            stats = ingest_legislature(leg_id, office_id, dry_run=dry)
            all_stats.append({"leg": leg_id, "status": "ok", **stats})
            print_leg_summary(leg_id, stats)
        except Exception as exc:
            log.error("Legislature %d FAILED: %s", leg_id, exc, exc_info=True)
            all_stats.append({"leg": leg_id, "status": "error", "error": str(exc)})

    elapsed = time.time() - t0

    # Final summary table
    ok = [s for s in all_stats if s.get("status") == "ok"]
    hdr = f"{'Leg':<5}  {'Date range':<23}  {'API':>5}  {'CPF':>5}  " \
          f"{'Name':>5}  {'New':>5}  {'Mandates':>8}  {'Err':>4}"
    sep = "-" * len(hdr)

    print(f"\n{'=' * len(hdr)}")
    print("CÂMARA INGESTION SUMMARY")
    print(f"{'=' * len(hdr)}")
    print(hdr)
    print(sep)

    tot = {k: 0 for k in ("total_api", "matched_cpf", "matched_name",
                           "created_people", "mandates_inserted", "errors")}
    for s in all_stats:
        if s["status"] == "error":
            print(f"{s['leg']:<5}  {'ERROR: ' + s.get('error','')[:40]}")
            continue
        dates = f"{LEGISLATURES[s['leg']][0]} → {LEGISLATURES[s['leg']][1]}"
        print(
            f"{s['leg']:<5}  {dates:<23}  {s['total_api']:>5}  {s['matched_cpf']:>5}  "
            f"{s['matched_name']:>5}  {s['created_people']:>5}  "
            f"{s['mandates_inserted']:>8}  {s['errors']:>4}"
        )
        for k in tot:
            tot[k] += s.get(k, 0)

    print(sep)
    dates_range = "ALL LEGISLATURES"
    print(
        f"{'TOT':<5}  {dates_range:<23}  {tot['total_api']:>5}  {tot['matched_cpf']:>5}  "
        f"{tot['matched_name']:>5}  {tot['created_people']:>5}  "
        f"{tot['mandates_inserted']:>8}  {tot['errors']:>4}"
    )

    total_matched = tot["matched_cpf"] + tot["matched_name"]
    overall_rate = (total_matched / tot["total_api"] * 100) if tot["total_api"] else 0.0
    print(f"\n  Overall match rate : {overall_rate:.1f}%")
    print(f"  New people created : {tot['created_people']:,}")
    print(f"  Dry run            : {dry}")
    print(f"  Elapsed            : {elapsed:.0f}s")
    print("=" * len(hdr))


if __name__ == "__main__":
    main()

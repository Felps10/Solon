"""
TSE candidate data ingestion.

Downloads consulta_cand_{YEAR}_BRASIL.csv from the TSE CDN and upserts
into memoria_politica. Every inserted row is traceable to its source file.

Natural keys used for deduplication (no schema changes required):
  - Party:      abbreviation (SG_PARTIDO)
  - Election:   tse_election_id = "{CD_ELEICAO}_{NR_TURNO}"
  - Person:     canonical_name + birth_date (normalised NM_CANDIDATO + DT_NASCIMENTO)
                Falls back to name-only match when birth_date is None —
                some historical records have no birth date in the TSE source.
  - Candidacy:  source_label = "TSE/consulta_cand_{YEAR}/{SQ_CANDIDATO}"

Note: NR_VOTOS is not present in consulta_cand files — vote counts live
in a separate TSE results dataset and are left NULL here.

# DEDUPLICATION POLICY:
# Person lookup uses unaccent() normalisation to prevent
# accent-encoding duplicates across TSE election years.
# The primary dedup key is (immutable_unaccent(canonical_name), birth_date).
# When birth_date is None, the lookup falls back to name-only — this is
# a weaker match and a WARNING is logged to flag the ambiguity.
# When a match is found, the existing record is reused
# and the name is NOT updated — the first ingested form
# is treated as canonical until manually reviewed.
# See: backend/ingest/deduplicate_people.py for the
# one-time historical cleanup.
"""

import argparse
import io
import logging
import sys
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
from tqdm import tqdm
import pandas as pd

# Make app.settings importable when run as  python -m ingest.tse_candidates
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
ELECTION_YEARS = [
    1994, 1996, 1998, 2000, 2002, 2004, 2006, 2008,
    2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024,
]

# TSE documentation: 1994-1998 data was not fully centralised.
INCOMPLETE_YEARS = {1994, 1996, 1998}

CDN_URL = (
    "https://cdn.tse.jus.br/estatistica/sead/odsele/"
    "consulta_cand/consulta_cand_{year}.zip"
)

# DS_SIT_TOT_TURNO → Candidacy.result
RESULT_MAP = {
    "ELEITO": "elected",
    "ELEITO POR QP": "elected",
    "ELEITO POR MÉDIA": "elected",
    "MÉDIA": "elected",
    "NÃO ELEITO": "defeated",
    "NÃO ELEITO POR MÉDIA": "defeated",
    "SUPLENTE": "defeated",
    "2º TURNO": "defeated",
    "RENÚNCIA": "renounced",
    "RENÚNCIA/FALECIMENTO": "renounced",
    "CASSADO": "annulled",
    "CASSADO PELO TSE": "annulled",
    "CASSADO PELO TRE": "annulled",
    "ANULADO": "annulled",
    "INDEFERIDO": "annulled",
}

# DS_CARGO → (institution, level)
OFFICE_META = {
    "PRESIDENTE": ("Presidência da República", "federal"),
    "VICE-PRESIDENTE": ("Presidência da República", "federal"),
    "SENADOR": ("Senado Federal", "federal"),
    "DEPUTADO FEDERAL": ("Câmara dos Deputados", "federal"),
    "GOVERNADOR": ("Governo Estadual", "estadual"),
    "VICE-GOVERNADOR": ("Governo Estadual", "estadual"),
    "DEPUTADO ESTADUAL": ("Assembleia Legislativa", "estadual"),
    "DEPUTADO DISTRITAL": ("Câmara Legislativa do DF", "estadual"),
    "PREFEITO": ("Prefeitura Municipal", "municipal"),
    "VICE-PREFEITO": ("Prefeitura Municipal", "municipal"),
    "VEREADOR": ("Câmara Municipal", "municipal"),
}

HIDDEN_VALUES = {"NÃO DIVULGÁVEL", "#NULO", "-1", "-4", "nan", "NaN", ""}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_connection():
    """Synchronous psycopg2 connection derived from settings.DATABASE_URL."""
    url = (
        settings.database_url
        .replace("postgresql+asyncpg://", "postgresql://")
        .replace("ssl=require", "sslmode=require")
    )
    return psycopg2.connect(url)


def clean(value) -> str:
    """Return stripped string or empty string for hidden/null values."""
    s = str(value).strip() if value is not None else ""
    return "" if s in HIDDEN_VALUES else s


def normalize_name(raw: str) -> str:
    """Uppercase + collapse whitespace for name dedup."""
    s = clean(raw)
    return " ".join(s.upper().split()) if s else ""


def parse_br_date(raw) -> Optional[str]:
    """DD/MM/YYYY → YYYY-MM-DD. Returns None for hidden/invalid."""
    s = clean(str(raw))
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def map_result(raw) -> str:
    s = clean(str(raw)).upper()
    return RESULT_MAP.get(s, "unknown")


# ---------------------------------------------------------------------------
# DB helpers — all use in-memory caches to minimise round-trips
# ---------------------------------------------------------------------------

def get_or_create_party(cur, cache: dict, sg: str, nm: str) -> Optional[str]:
    sg = clean(sg)
    if not sg:
        return None
    if sg in cache:
        return cache[sg]
    cur.execute("SELECT id FROM parties WHERE abbreviation = %s", (sg,))
    row = cur.fetchone()
    if row:
        cache[sg] = str(row[0])
        return cache[sg]
    pid = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO parties (id, abbreviation, name) VALUES (%s, %s, %s)",
        (pid, sg, clean(nm) or sg),
    )
    cache[sg] = pid
    return pid


def get_or_create_office(cur, cache: dict, ds_cargo: str) -> Optional[str]:
    name = clean(ds_cargo).upper()
    if not name:
        return None
    if name in cache:
        return cache[name]
    cur.execute("SELECT id FROM offices WHERE name = %s", (name,))
    row = cur.fetchone()
    if row:
        cache[name] = str(row[0])
        return cache[name]
    meta = OFFICE_META.get(name, ("Outro", None))
    oid = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO offices (id, name, institution, level) VALUES (%s, %s, %s, %s)",
        (oid, name, meta[0], meta[1]),
    )
    cache[name] = oid
    return oid


def get_or_create_election(cur, cache: dict, year: int, cd_eleicao: str,
                            nr_turno: int, ds_eleicao: str, nm_ue: str) -> str:
    tse_id = f"{clean(cd_eleicao)}_{nr_turno}"
    if tse_id in cache:
        return cache[tse_id]
    cur.execute("SELECT id FROM elections WHERE tse_election_id = %s", (tse_id,))
    row = cur.fetchone()
    if row:
        cache[tse_id] = str(row[0])
        return cache[tse_id]
    eid = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO elections (id, year, round, election_type, territory, tse_election_id) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (eid, year, nr_turno, clean(ds_eleicao), clean(nm_ue), tse_id),
    )
    cache[tse_id] = eid
    return eid


def get_or_create_person(cur, cache: dict, nm_candidato: str,
                          birth_date: Optional[str], gender: Optional[str]) -> Optional[str]:
    norm = normalize_name(nm_candidato)
    if not norm:
        return None

    # Cache key is (name, birth_date) — two people with the same name but
    # different birth dates are distinct records.
    cache_key = (norm, birth_date)
    if cache_key in cache:
        return cache[cache_key]

    if birth_date is not None:
        # Primary path: match on both normalised name AND birth date.
        cur.execute(
            "SELECT id FROM people "
            "WHERE public.immutable_unaccent(canonical_name) = public.immutable_unaccent(%(name)s) "
            "AND birth_date = %(birth_date)s "
            "LIMIT 1",
            {"name": norm, "birth_date": birth_date},
        )
    else:
        # Fallback: birth_date unknown — match on name only.
        # This is a weaker match; log a warning so it can be reviewed.
        log.warning(
            "get_or_create_person: birth_date is None for %r — "
            "falling back to name-only match (ambiguous)",
            norm,
        )
        cur.execute(
            "SELECT id FROM people "
            "WHERE public.immutable_unaccent(canonical_name) = public.immutable_unaccent(%(name)s) "
            "AND birth_date IS NULL "
            "LIMIT 1",
            {"name": norm},
        )

    row = cur.fetchone()
    if row:
        cache[cache_key] = str(row[0])
        return cache[cache_key]

    pid = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO people (id, canonical_name, birth_date, gender) "
        "VALUES (%s, %s, %s, %s)",
        (pid, norm, birth_date, gender),
    )
    cache[cache_key] = pid
    return pid


# ---------------------------------------------------------------------------
# Per-year ingestion
# ---------------------------------------------------------------------------

def ingest_year(year: int) -> dict:
    url = CDN_URL.format(year=year)
    confidence = "medium" if year in INCOMPLETE_YEARS else "high"
    source_prefix = f"TSE/consulta_cand_{year}"

    if year in INCOMPLETE_YEARS:
        log.warning(
            f"[{year}] TSE data for {year} may be incomplete — "
            "source not fully centralised pre-1999 (TSE documentation)"
        )

    # --- Download ---
    log.info(f"[{year}] Downloading {url}")
    try:
        resp = requests.get(url, timeout=180, stream=True)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"[{year}] Download failed: {e}") from e

    zip_bytes = io.BytesIO(resp.content)
    log.info(f"[{year}] Downloaded {len(resp.content) / 1_048_576:.1f} MB")

    # --- Extract BRASIL file ---
    with zipfile.ZipFile(zip_bytes) as zf:
        brasil_files = [n for n in zf.namelist() if "BRASIL" in n.upper() and n.endswith(".csv")]
        if not brasil_files:
            raise FileNotFoundError(
                f"[{year}] No BRASIL CSV in ZIP. Contents: {zf.namelist()}"
            )
        brasil_file = brasil_files[0]
        log.info(f"[{year}] Parsing {brasil_file}")
        with zf.open(brasil_file) as f:
            df = pd.read_csv(f, sep=";", encoding="latin-1", dtype=str, low_memory=False)

    total_rows = len(df)
    log.info(f"[{year}] {total_rows:,} rows to process")

    # --- DB upsert ---
    stats = {
        "rows": total_rows,
        "inserted_people": 0,
        "inserted_parties": 0,
        "inserted_offices": 0,
        "inserted_elections": 0,
        "inserted_candidacies": 0,
        "skipped_candidacies": 0,
    }

    party_cache: dict = {}
    office_cache: dict = {}
    election_cache: dict = {}
    person_cache: dict = {}

    conn = get_connection()
    BATCH = 500
    nome_urna_updates: list = []  # (nome_urna_val, candidacy_label) pairs

    try:
        cur = conn.cursor()

        # Pre-load existing source_labels for this year into a set
        # to avoid one SELECT per row on re-run.
        cur.execute(
            "SELECT source_label FROM candidacies WHERE source_label LIKE %s",
            (f"{source_prefix}/%",),
        )
        existing_labels = {row[0] for row in cur.fetchall()}
        log.info(f"[{year}] {len(existing_labels):,} candidacies already in DB")

        batch_count = 0

        for _, row in tqdm(df.iterrows(), total=total_rows, desc=f"{year}", unit="row"):
            sq = clean(str(row.get("SQ_CANDIDATO", "")))
            candidacy_label = f"{source_prefix}/{sq}"
            nome_urna_val = clean(str(row.get("NM_URNA_CANDIDATO", ""))) or None

            if candidacy_label in existing_labels:
                # Queue the nome_urna update; flush in batches below.
                if nome_urna_val:
                    nome_urna_updates.append((candidacy_label, nome_urna_val))
                stats["skipped_candidacies"] += 1
                continue

            # Party
            party_before = len(party_cache)
            party_id = get_or_create_party(
                cur, party_cache,
                row.get("SG_PARTIDO", ""),
                row.get("NM_PARTIDO", ""),
            )
            if len(party_cache) > party_before:
                stats["inserted_parties"] += 1

            # Office
            office_before = len(office_cache)
            office_id = get_or_create_office(cur, office_cache, row.get("DS_CARGO", ""))
            if len(office_cache) > office_before:
                stats["inserted_offices"] += 1

            # Election
            election_before = len(election_cache)
            try:
                nr_turno = int(str(row.get("NR_TURNO", "1")).strip() or 1)
            except (ValueError, TypeError):
                nr_turno = 1
            election_id = get_or_create_election(
                cur, election_cache,
                year,
                row.get("CD_ELEICAO", ""),
                nr_turno,
                row.get("DS_ELEICAO", ""),
                row.get("NM_UE", ""),
            )
            if len(election_cache) > election_before:
                stats["inserted_elections"] += 1

            # Person
            person_before = len(person_cache)
            birth_date = parse_br_date(row.get("DT_NASCIMENTO"))
            gender_raw = clean(str(row.get("DS_GENERO", "")))
            gender = gender_raw.lower() if gender_raw else None
            person_id = get_or_create_person(
                cur, person_cache,
                row.get("NM_CANDIDATO", ""),
                birth_date,
                gender,
            )
            if len(person_cache) > person_before:
                stats["inserted_people"] += 1

            if not person_id or not election_id:
                stats["skipped_candidacies"] += 1
                continue

            # Candidacy
            territory = clean(str(row.get("NM_UE", "")))
            result = map_result(row.get("DS_SIT_TOT_TURNO", ""))

            cur.execute(
                "INSERT INTO candidacies "
                "(id, person_id, election_id, office_id, party_id, "
                " territory, result, source_label, confidence, nome_urna) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    str(uuid.uuid4()),
                    person_id,
                    election_id,
                    office_id,
                    party_id,
                    territory,
                    result,
                    candidacy_label,
                    confidence,
                    nome_urna_val,
                ),
            )
            stats["inserted_candidacies"] += 1
            existing_labels.add(candidacy_label)

            batch_count += 1
            if batch_count >= BATCH:
                conn.commit()
                batch_count = 0

        # Flush queued nome_urna updates via a temp table + single JOIN UPDATE.
        # This avoids repeated full-table scans when source_label lacks an index.
        if nome_urna_updates:
            cur.execute("""
                CREATE TEMP TABLE _nome_urna_updates (
                    source_label TEXT,
                    nome_urna    TEXT
                ) ON COMMIT DROP
            """)
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO _nome_urna_updates (source_label, nome_urna) VALUES %s",
                nome_urna_updates,
                page_size=1000,
            )
            cur.execute("""
                UPDATE candidacies c
                   SET nome_urna = u.nome_urna
                  FROM _nome_urna_updates u
                 WHERE c.source_label = u.source_label
                   AND c.nome_urna IS DISTINCT FROM u.nome_urna
            """)
            updated = cur.rowcount
            log.info(f"[{year}] nome_urna backfill: {updated:,} rows updated ({len(nome_urna_updates):,} queued)")

        conn.commit()
        cur.close()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ingest TSE candidate data into memoria_politica")
    parser.add_argument(
        "--year", type=int, default=None,
        help="Single election year to ingest. Omit to run all years.",
    )
    args = parser.parse_args()

    years = [args.year] if args.year else ELECTION_YEARS

    summary = []
    total_people = 0
    total_parties = 0
    total_offices = 0
    total_elections = 0
    total_candidacies = 0

    for year in years:
        log.info(f"{'='*60}")
        log.info(f"Processing year {year}")
        log.info(f"{'='*60}")
        try:
            stats = ingest_year(year)
            summary.append({"year": year, "status": "ok", "stats": stats})
            total_people += stats["inserted_people"]
            total_parties += stats["inserted_parties"]
            total_offices += stats["inserted_offices"]
            total_elections += stats["inserted_elections"]
            total_candidacies += stats["inserted_candidacies"]
            log.info(
                f"[{year}] Done — "
                f"{stats['inserted_candidacies']:,} candidacies inserted, "
                f"{stats['skipped_candidacies']:,} skipped, "
                f"{stats['inserted_people']:,} new people, "
                f"{stats['inserted_parties']:,} new parties"
            )
        except Exception as e:
            log.error(f"[{year}] FAILED: {e}")
            summary.append({"year": year, "status": "error", "error": str(e)})

    print("\n" + "=" * 60)
    print("INGESTION SUMMARY")
    print("=" * 60)
    print(f"  Years processed : {len([s for s in summary if s['status'] == 'ok'])}/{len(years)}")
    print(f"  People inserted : {total_people:,}")
    print(f"  Parties inserted: {total_parties:,}")
    print(f"  Offices inserted: {total_offices:,}")
    print(f"  Elections inserted: {total_elections:,}")
    print(f"  Candidacies inserted: {total_candidacies:,}")

    failures = [s for s in summary if s["status"] == "error"]
    if failures:
        print(f"\n  FAILURES ({len(failures)}):")
        for f in failures:
            print(f"    {f['year']}: {f['error']}")
    print("=" * 60)


if __name__ == "__main__":
    main()

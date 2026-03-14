# backend/data/

This directory is for local working files only. **Nothing here is committed to git.**

Raw CSVs, ZIP archives, and PDF leiame files downloaded from the TSE CDN are
ingestion-only artifacts. They are excluded by `.gitignore` and must never be
checked in — they are large (up to 500 MB per year), change on every TSE
publication cycle, and are fully reproducible by re-running the ingestion scripts.

---

## Data source

**Tribunal Superior Eleitoral (TSE) — Dados Abertos**
https://dados.tse.jus.br/dataset/candidatos

### Datasets used

| Dataset | CDN pattern | Description |
|---------|------------|-------------|
| `consulta_cand` | `consulta_cand_{YEAR}.zip` | Candidate registrations per election year |
| `votacao_candidato_munzona` | `votacao_candidato_munzona_{YEAR}.zip` | Vote counts per candidate per municipality/zone |

### Election years covered

1994, 1996, 1998, 2000, 2002, 2004, 2006, 2008,
2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024

---

## How to populate the database

The ingestion scripts download directly from the TSE CDN — no local files needed.

```bash
# Run full ingestion (all years) — takes ~30 min on first run
make ingest

# Or step by step:
cd backend && source .venv/bin/activate

# 1. Apply schema migrations
alembic upgrade head

# 2. Ingest all candidacies
python -m ingest.tse_candidates

# 3. Deduplicate people (accent-encoding variants)
python backend/ingest/deduplicate_people.py

# 4. Ingest vote counts
python -m ingest.tse_vote_counts
```

## How to restore from a database dump

If a dump exists in `backups/`:

```bash
make db-restore DUMP_FILE=backups/solon_20260313.sql.gz
```

This is faster than re-ingesting 16 years of TSE data from scratch.

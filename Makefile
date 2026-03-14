.PHONY: dev migrate migration downgrade tables shell

dev:
	cd backend && source .venv/bin/activate && \
	uvicorn app.main:app --reload --reload-exclude .venv --port 8000

migrate:
	cd backend && source .venv/bin/activate && \
	alembic upgrade head

migration:
	cd backend && source .venv/bin/activate && \
	alembic revision --autogenerate -m "$(msg)"

downgrade:
	cd backend && source .venv/bin/activate && \
	alembic downgrade -1

tables:
	psql -U mpb_user -d memoria_politica -c "\dt"

shell:
	psql -U mpb_user -d memoria_politica

db-dump:
	mkdir -p backups
	pg_dump -U mpb_user -Fc memoria_politica \
	  | gzip > backups/solon_$$(date +%Y%m%d).sql.gz
	@echo "Dump written to backups/solon_$$(date +%Y%m%d).sql.gz"

db-restore:
	@test -n "$(DUMP_FILE)" || (echo "Usage: make db-restore DUMP_FILE=backups/solon_YYYYMMDD.sql.gz" && exit 1)
	gunzip -c $(DUMP_FILE) | pg_restore -U mpb_user -d memoria_politica --clean --if-exists

ingest:
	@echo ""
	@echo "Ingestion pipeline for Sólon — Memória Política Brasileira"
	@echo "-----------------------------------------------------------"
	@echo "Data source: https://dados.tse.jus.br/dataset/candidatos"
	@echo ""
	@echo "The ingestion scripts download directly from the TSE CDN."
	@echo "No local data files are required — files are fetched on demand."
	@echo ""
	@echo "  Ingest all candidacies (1994–2024):"
	@echo "    cd backend && source .venv/bin/activate"
	@echo "    python -m ingest.tse_candidates"
	@echo ""
	@echo "  Ingest vote counts (1994–2024):"
	@echo "    python -m ingest.tse_vote_counts"
	@echo ""
	@echo "  Ingest a single year:"
	@echo "    python -m ingest.tse_candidates --year 2022"
	@echo "    python -m ingest.tse_vote_counts --year 2022"
	@echo ""
	@echo "  Deduplicate people after first ingest:"
	@echo "    python backend/ingest/deduplicate_people.py"
	@echo ""

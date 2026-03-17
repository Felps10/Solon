"""add_nome_urna_to_candidacies

Revision ID: f1a2b3c4d5e6
Revises: e3f1a2b4c5d6
Create Date: 2026-03-17 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f1a2b3c4d5e6'
down_revision: Union[str, Sequence[str], None] = 'e3f1a2b4c5d6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── nome_urna column ─────────────────────────────────────────────
    op.execute("""
        ALTER TABLE candidacies ADD COLUMN IF NOT EXISTS nome_urna VARCHAR(200)
    """)

    # ── search_vector column for candidacies (powered by nome_urna) ──
    op.execute("""
        ALTER TABLE candidacies ADD COLUMN IF NOT EXISTS search_vector tsvector
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_candidacies_search_vector
            ON candidacies USING gin(search_vector)
    """)

    # ── trigger: keep search_vector in sync with nome_urna ───────────
    op.execute("""
        CREATE OR REPLACE FUNCTION candidacies_search_vector_update()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            NEW.search_vector :=
                CASE WHEN NEW.nome_urna IS NOT NULL AND NEW.nome_urna <> '' THEN
                    setweight(
                        to_tsvector('portuguese',
                            immutable_unaccent(NEW.nome_urna)),
                        'A'
                    )
                ELSE NULL END;
            RETURN NEW;
        END;
        $$
    """)

    op.execute("DROP TRIGGER IF EXISTS trg_candidacies_search_vector ON candidacies")

    op.execute("""
        CREATE TRIGGER trg_candidacies_search_vector
            BEFORE INSERT OR UPDATE OF nome_urna
            ON candidacies
            FOR EACH ROW EXECUTE FUNCTION candidacies_search_vector_update()
    """)

    # ── backfill: compute search_vector for any existing rows ─────────
    # At migration time nome_urna is newly added (all NULL), so this
    # sets search_vector to NULL for all existing rows. After re-ingest
    # sets nome_urna, the trigger above will populate search_vector
    # automatically on each UPDATE.
    op.execute("""
        UPDATE candidacies
        SET search_vector =
            CASE WHEN nome_urna IS NOT NULL AND nome_urna <> '' THEN
                setweight(
                    to_tsvector('portuguese', immutable_unaccent(nome_urna)),
                    'A'
                )
            ELSE NULL END
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_candidacies_search_vector ON candidacies")
    op.execute("DROP FUNCTION IF EXISTS candidacies_search_vector_update()")
    op.execute("DROP INDEX IF EXISTS ix_candidacies_search_vector")
    op.execute("ALTER TABLE candidacies DROP COLUMN IF EXISTS search_vector")
    op.execute("ALTER TABLE candidacies DROP COLUMN IF EXISTS nome_urna")

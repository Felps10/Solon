"""add_search_tsvector_columns

Revision ID: e3f1a2b4c5d6
Revises: b9744991aacd
Create Date: 2026-03-17 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e3f1a2b4c5d6'
down_revision: Union[str, Sequence[str], None] = 'a328b488b5b4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── people ────────────────────────────────────────────────────────
    op.execute("""
        ALTER TABLE people ADD COLUMN IF NOT EXISTS search_vector tsvector
    """)

    op.execute("""
        UPDATE people
        SET search_vector =
            setweight(
                to_tsvector('portuguese',
                    coalesce(immutable_unaccent(canonical_name), '')),
                'A'
            )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_people_search_vector
            ON people USING gin(search_vector)
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION people_search_vector_update()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            NEW.search_vector :=
                setweight(
                    to_tsvector('portuguese',
                        coalesce(immutable_unaccent(NEW.canonical_name), '')),
                    'A'
                );
            RETURN NEW;
        END;
        $$
    """)

    op.execute("DROP TRIGGER IF EXISTS trg_people_search_vector ON people")

    op.execute("""
        CREATE TRIGGER trg_people_search_vector
            BEFORE INSERT OR UPDATE OF canonical_name
            ON people
            FOR EACH ROW EXECUTE FUNCTION people_search_vector_update()
    """)

    # ── parties ───────────────────────────────────────────────────────
    op.execute("""
        ALTER TABLE parties ADD COLUMN IF NOT EXISTS search_vector tsvector
    """)

    op.execute("""
        UPDATE parties
        SET search_vector =
            setweight(
                to_tsvector('simple',
                    coalesce(immutable_unaccent(abbreviation), '')),
                'A'
            ) ||
            setweight(
                to_tsvector('simple',
                    coalesce(immutable_unaccent(name), '')),
                'B'
            )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_parties_search_vector
            ON parties USING gin(search_vector)
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION parties_search_vector_update()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            NEW.search_vector :=
                setweight(
                    to_tsvector('simple',
                        coalesce(immutable_unaccent(NEW.abbreviation), '')),
                    'A'
                ) ||
                setweight(
                    to_tsvector('simple',
                        coalesce(immutable_unaccent(NEW.name), '')),
                    'B'
                );
            RETURN NEW;
        END;
        $$
    """)

    op.execute("DROP TRIGGER IF EXISTS trg_parties_search_vector ON parties")

    op.execute("""
        CREATE TRIGGER trg_parties_search_vector
            BEFORE INSERT OR UPDATE OF abbreviation, name
            ON parties
            FOR EACH ROW EXECUTE FUNCTION parties_search_vector_update()
    """)

    # ── offices ───────────────────────────────────────────────────────
    op.execute("""
        ALTER TABLE offices ADD COLUMN IF NOT EXISTS search_vector tsvector
    """)

    op.execute("""
        UPDATE offices
        SET search_vector =
            setweight(
                to_tsvector('portuguese',
                    coalesce(immutable_unaccent(name), '')),
                'A'
            ) ||
            setweight(
                to_tsvector('portuguese',
                    coalesce(immutable_unaccent(coalesce(institution, '')), '')),
                'B'
            )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_offices_search_vector
            ON offices USING gin(search_vector)
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION offices_search_vector_update()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            NEW.search_vector :=
                setweight(
                    to_tsvector('portuguese',
                        coalesce(immutable_unaccent(NEW.name), '')),
                    'A'
                ) ||
                setweight(
                    to_tsvector('portuguese',
                        coalesce(immutable_unaccent(coalesce(NEW.institution, '')), '')),
                    'B'
                );
            RETURN NEW;
        END;
        $$
    """)

    op.execute("DROP TRIGGER IF EXISTS trg_offices_search_vector ON offices")

    op.execute("""
        CREATE TRIGGER trg_offices_search_vector
            BEFORE INSERT OR UPDATE OF name, institution
            ON offices
            FOR EACH ROW EXECUTE FUNCTION offices_search_vector_update()
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_offices_search_vector ON offices")
    op.execute("DROP FUNCTION IF EXISTS offices_search_vector_update()")
    op.execute("DROP INDEX IF EXISTS ix_offices_search_vector")
    op.execute("ALTER TABLE offices DROP COLUMN IF EXISTS search_vector")

    op.execute("DROP TRIGGER IF EXISTS trg_parties_search_vector ON parties")
    op.execute("DROP FUNCTION IF EXISTS parties_search_vector_update()")
    op.execute("DROP INDEX IF EXISTS ix_parties_search_vector")
    op.execute("ALTER TABLE parties DROP COLUMN IF EXISTS search_vector")

    op.execute("DROP TRIGGER IF EXISTS trg_people_search_vector ON people")
    op.execute("DROP FUNCTION IF EXISTS people_search_vector_update()")
    op.execute("DROP INDEX IF EXISTS ix_people_search_vector")
    op.execute("ALTER TABLE people DROP COLUMN IF EXISTS search_vector")

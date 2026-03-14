"""add unaccent extension and name index

Revision ID: 25d5bc92fd1a
Revises: 5c5bc2d55945
Create Date: 2026-03-13 07:55:30.964880

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '25d5bc92fd1a'
down_revision: Union[str, Sequence[str], None] = '5c5bc2d55945'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS unaccent SCHEMA public")
    # unaccent() is STABLE, not IMMUTABLE, so it can't be used directly in an
    # index expression. PL/pgSQL is used instead of SQL language because SQL
    # functions are inlined by PostgreSQL at index-creation time, which causes
    # the extension objects to be unresolvable within the same transaction.
    # PL/pgSQL blocks are never inlined, so resolution happens at call time.
    op.execute("""
        CREATE OR REPLACE FUNCTION public.immutable_unaccent(text)
        RETURNS text AS $$
        BEGIN
            RETURN public.unaccent($1);
        END;
        $$ LANGUAGE plpgsql IMMUTABLE STRICT
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_people_canonical_name_unaccent
        ON people (public.immutable_unaccent(canonical_name))
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_people_canonical_name_unaccent")
    op.execute("DROP FUNCTION IF EXISTS immutable_unaccent(text)")
    op.execute("DROP EXTENSION IF EXISTS unaccent")

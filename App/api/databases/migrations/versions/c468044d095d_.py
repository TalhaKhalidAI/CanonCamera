"""empty message

Revision ID: c468044d095d
Revises: 7c06030f5e89
Create Date: 2026-04-13 09:50:01.762292

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c468044d095d'
down_revision: Union[str, Sequence[str], None] = '7c06030f5e89'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass

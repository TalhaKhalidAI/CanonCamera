"""added all tables

Revision ID: 7687b6ea3c93
Revises: c468044d095d
Create Date: 2026-04-13 09:52:23.906033

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7687b6ea3c93'
down_revision: Union[str, Sequence[str], None] = 'c468044d095d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass

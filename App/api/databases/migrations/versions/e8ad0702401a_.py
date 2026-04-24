"""empty message

Revision ID: e8ad0702401a
Revises: d0ae760068d0
Create Date: 2026-04-24 16:17:00.702763

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e8ad0702401a'
down_revision: Union[str, Sequence[str], None] = 'd0ae760068d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass

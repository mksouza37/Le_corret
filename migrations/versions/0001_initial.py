"""Initial migration"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic
revision = '0001'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table('user',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('email', sa.String(length=120), nullable=False),
        sa.Column('password_hash', sa.String(length=128), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('email')
    )

    op.create_table('subscription',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('cpf', sa.String(length=14), nullable=False),
        sa.Column('valid_until', sa.DateTime(), nullable=False),
        sa.Column('stripe_id', sa.String(length=100), nullable=True),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['user.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

def downgrade():
    op.drop_table('subscription')
    op.drop_table('user')

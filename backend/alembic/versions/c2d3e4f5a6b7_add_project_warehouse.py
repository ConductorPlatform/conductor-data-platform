"""add isolated per-project warehouse desired state

Revision ID: c2d3e4f5a6b7
Revises: b1c2d3e4f5a6
Create Date: 2026-09-15 00:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c2d3e4f5a6b7"
down_revision: Union[str, None] = "b1c2d3e4f5a6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Nullable intentionally preserves legacy lifecycle diagnostics.  New
    # project creation writes all values atomically before provisioning starts.
    op.add_column("project_deployments", sa.Column("warehouse_db_name", sa.String(63), nullable=True))
    op.add_column("project_deployments", sa.Column("warehouse_db_role", sa.String(63), nullable=True))
    op.add_column(
        "project_deployments", sa.Column("warehouse_db_password_encrypted", sa.Text(), nullable=True)
    )
    op.add_column("project_deployments", sa.Column("warehouse_schema", sa.String(63), nullable=True))
    op.create_unique_constraint(
        "uq_project_deployments_warehouse_db_name", "project_deployments", ["warehouse_db_name"]
    )
    op.create_unique_constraint(
        "uq_project_deployments_warehouse_db_role", "project_deployments", ["warehouse_db_role"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_project_deployments_warehouse_db_role", "project_deployments", type_="unique")
    op.drop_constraint("uq_project_deployments_warehouse_db_name", "project_deployments", type_="unique")
    op.drop_column("project_deployments", "warehouse_schema")
    op.drop_column("project_deployments", "warehouse_db_password_encrypted")
    op.drop_column("project_deployments", "warehouse_db_role")
    op.drop_column("project_deployments", "warehouse_db_name")

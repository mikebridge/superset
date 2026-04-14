# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""add_version_history_tables

Create shadow version tables for SQLAlchemy-Continuum versioning of
Dashboard, Slice, and SqlaTable models, plus the shared transaction
log table. Also creates version tables for association/junction tables
that Continuum tracks automatically.

Revision ID: 06d6cae6d409
Revises: ce6bd21901ab
Create Date: 2026-04-14 16:19:39.437300

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

import sqlalchemy_utils

# revision identifiers, used by Alembic.
revision = "06d6cae6d409"
down_revision = "ce6bd21901ab"
branch_labels = None
depends_on = None

# All version tables created by this migration
VERSION_TABLES = [
    "transaction",
    "dashboards_version",
    "slices_version",
    "tables_version",
    # Junction table version tables (auto-tracked by Continuum)
    "dashboard_roles_version",
    "dashboard_slices_version",
    "dashboard_user_version",
    "slice_user_version",
    "sqlatable_user_version",
    "rls_filter_tables_version",
]


def upgrade():
    # Transaction log table (shared by all versioned models)
    op.create_table(
        "transaction",
        sa.Column("issued_at", sa.DateTime(), nullable=True),
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("remote_addr", sa.String(length=50), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_transaction_issued_at"), "transaction", ["issued_at"], unique=False
    )

    # --- Dashboard version table ---
    op.create_table(
        "dashboards_version",
        sa.Column(
            "uuid",
            sqlalchemy_utils.types.uuid.UUIDType(),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("created_on", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("changed_on", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column(
            "dashboard_title",
            sa.String(length=500),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "position_json",
            sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("description", sa.Text(), autoincrement=False, nullable=True),
        sa.Column(
            "css",
            sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("theme_id", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("certified_by", sa.Text(), autoincrement=False, nullable=True),
        sa.Column(
            "certification_details", sa.Text(), autoincrement=False, nullable=True
        ),
        sa.Column(
            "json_metadata",
            sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "slug", sa.String(length=255), autoincrement=False, nullable=True
        ),
        sa.Column("published", sa.Boolean(), autoincrement=False, nullable=True),
        sa.Column(
            "is_managed_externally",
            sa.Boolean(),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("external_url", sa.Text(), autoincrement=False, nullable=True),
        sa.Column("created_by_fk", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("changed_by_fk", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column(
            "transaction_id",
            sa.BigInteger(),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("end_transaction_id", sa.BigInteger(), nullable=True),
        sa.Column("operation_type", sa.SmallInteger(), nullable=False),
        sa.PrimaryKeyConstraint("id", "transaction_id"),
    )
    op.create_index(
        op.f("ix_dashboards_version_end_transaction_id"),
        "dashboards_version",
        ["end_transaction_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_dashboards_version_operation_type"),
        "dashboards_version",
        ["operation_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_dashboards_version_transaction_id"),
        "dashboards_version",
        ["transaction_id"],
        unique=False,
    )

    # --- Slice (chart) version table ---
    op.create_table(
        "slices_version",
        sa.Column(
            "uuid",
            sqlalchemy_utils.types.uuid.UUIDType(),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("created_on", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("changed_on", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column(
            "slice_name",
            sa.String(length=250),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("datasource_id", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column(
            "datasource_type",
            sa.String(length=200),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "datasource_name",
            sa.String(length=2000),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "viz_type", sa.String(length=250), autoincrement=False, nullable=True
        ),
        sa.Column(
            "params",
            sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("description", sa.Text(), autoincrement=False, nullable=True),
        sa.Column(
            "cache_timeout", sa.Integer(), autoincrement=False, nullable=True
        ),
        sa.Column(
            "perm", sa.String(length=2000), autoincrement=False, nullable=True
        ),
        sa.Column(
            "schema_perm",
            sa.String(length=1000),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "catalog_perm",
            sa.String(length=1000),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "last_saved_at", sa.DateTime(), autoincrement=False, nullable=True
        ),
        sa.Column(
            "last_saved_by_fk", sa.Integer(), autoincrement=False, nullable=True
        ),
        sa.Column("certified_by", sa.Text(), autoincrement=False, nullable=True),
        sa.Column(
            "certification_details", sa.Text(), autoincrement=False, nullable=True
        ),
        sa.Column(
            "is_managed_externally",
            sa.Boolean(),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("external_url", sa.Text(), autoincrement=False, nullable=True),
        sa.Column("created_by_fk", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("changed_by_fk", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column(
            "transaction_id",
            sa.BigInteger(),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("end_transaction_id", sa.BigInteger(), nullable=True),
        sa.Column("operation_type", sa.SmallInteger(), nullable=False),
        sa.PrimaryKeyConstraint("id", "transaction_id"),
    )
    op.create_index(
        op.f("ix_slices_version_end_transaction_id"),
        "slices_version",
        ["end_transaction_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_slices_version_operation_type"),
        "slices_version",
        ["operation_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_slices_version_transaction_id"),
        "slices_version",
        ["transaction_id"],
        unique=False,
    )

    # --- SqlaTable (dataset) version table ---
    op.create_table(
        "tables_version",
        sa.Column(
            "uuid",
            sqlalchemy_utils.types.uuid.UUIDType(),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("created_on", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("changed_on", sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column("cache_timeout", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("description", sa.Text(), autoincrement=False, nullable=True),
        sa.Column(
            "default_endpoint", sa.Text(), autoincrement=False, nullable=True
        ),
        sa.Column("is_featured", sa.Boolean(), autoincrement=False, nullable=True),
        sa.Column(
            "filter_select_enabled",
            sa.Boolean(),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("offset", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column(
            "params",
            sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "perm", sa.String(length=1000), autoincrement=False, nullable=True
        ),
        sa.Column(
            "schema_perm",
            sa.String(length=1000),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "catalog_perm",
            sa.String(length=1000),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "is_managed_externally",
            sa.Boolean(),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("external_url", sa.Text(), autoincrement=False, nullable=True),
        sa.Column(
            "table_name",
            sa.String(length=250),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "main_dttm_col", sa.String(length=250), autoincrement=False, nullable=True
        ),
        sa.Column(
            "currency_code_column",
            sa.String(length=250),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("database_id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column(
            "fetch_values_predicate", sa.Text(), autoincrement=False, nullable=True
        ),
        sa.Column(
            "schema", sa.String(length=255), autoincrement=False, nullable=True
        ),
        sa.Column(
            "catalog", sa.String(length=256), autoincrement=False, nullable=True
        ),
        sa.Column("sql", sa.Text(), autoincrement=False, nullable=True),
        sa.Column(
            "is_sqllab_view",
            sa.Boolean(),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("template_params", sa.Text(), autoincrement=False, nullable=True),
        sa.Column("extra", sa.Text(), autoincrement=False, nullable=True),
        sa.Column(
            "normalize_columns",
            sa.Boolean(),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "always_filter_main_dttm",
            sa.Boolean(),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("folders", sa.JSON(), autoincrement=False, nullable=True),
        sa.Column("created_by_fk", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("changed_by_fk", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column(
            "transaction_id",
            sa.BigInteger(),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("end_transaction_id", sa.BigInteger(), nullable=True),
        sa.Column("operation_type", sa.SmallInteger(), nullable=False),
        sa.PrimaryKeyConstraint("id", "transaction_id"),
    )
    op.create_index(
        op.f("ix_tables_version_end_transaction_id"),
        "tables_version",
        ["end_transaction_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_tables_version_operation_type"),
        "tables_version",
        ["operation_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_tables_version_transaction_id"),
        "tables_version",
        ["transaction_id"],
        unique=False,
    )

    # --- Junction table version tables (auto-tracked by Continuum) ---
    # Note: id is nullable because Continuum may not include it when
    # versioning association table changes. PK is (id, transaction_id)
    # but id defaults to 0 when not provided.
    _JUNCTION_TABLES = {
        "dashboard_roles_version": [
            sa.Column("id", sa.Integer(), autoincrement=False, nullable=True, server_default="0"),
            sa.Column("dashboard_id", sa.Integer(), autoincrement=False, nullable=True),
            sa.Column("role_id", sa.Integer(), autoincrement=False, nullable=True),
        ],
        "dashboard_slices_version": [
            sa.Column("id", sa.Integer(), autoincrement=False, nullable=True, server_default="0"),
            sa.Column("dashboard_id", sa.Integer(), autoincrement=False, nullable=True),
            sa.Column("slice_id", sa.Integer(), autoincrement=False, nullable=True),
        ],
        "dashboard_user_version": [
            sa.Column("id", sa.Integer(), autoincrement=False, nullable=True, server_default="0"),
            sa.Column("user_id", sa.Integer(), autoincrement=False, nullable=True),
            sa.Column("dashboard_id", sa.Integer(), autoincrement=False, nullable=True),
        ],
        "slice_user_version": [
            sa.Column("id", sa.Integer(), autoincrement=False, nullable=True, server_default="0"),
            sa.Column("user_id", sa.Integer(), autoincrement=False, nullable=True),
            sa.Column("slice_id", sa.Integer(), autoincrement=False, nullable=True),
        ],
        "sqlatable_user_version": [
            sa.Column("id", sa.Integer(), autoincrement=False, nullable=True, server_default="0"),
            sa.Column("user_id", sa.Integer(), autoincrement=False, nullable=True),
            sa.Column("table_id", sa.Integer(), autoincrement=False, nullable=True),
        ],
        "rls_filter_tables_version": [
            sa.Column("id", sa.Integer(), autoincrement=False, nullable=True, server_default="0"),
            sa.Column("table_id", sa.Integer(), autoincrement=False, nullable=True),
            sa.Column("rls_filter_id", sa.Integer(), autoincrement=False, nullable=True),
        ],
    }

    for table_name, columns in _JUNCTION_TABLES.items():
        op.create_table(
            table_name,
            *columns,
            sa.Column(
                "transaction_id",
                sa.BigInteger(),
                autoincrement=False,
                nullable=False,
            ),
            sa.Column("end_transaction_id", sa.BigInteger(), nullable=True),
            sa.Column("operation_type", sa.SmallInteger(), nullable=False),
            sa.PrimaryKeyConstraint("transaction_id"),
        )
        op.create_index(
            op.f(f"ix_{table_name}_end_transaction_id"),
            table_name,
            ["end_transaction_id"],
            unique=False,
        )
        op.create_index(
            op.f(f"ix_{table_name}_operation_type"),
            table_name,
            ["operation_type"],
            unique=False,
        )
        op.create_index(
            op.f(f"ix_{table_name}_transaction_id"),
            table_name,
            ["transaction_id"],
            unique=False,
        )


def downgrade():
    # Drop in reverse order; indexes are dropped automatically with their tables
    for table_name in reversed(VERSION_TABLES):
        op.drop_table(table_name)

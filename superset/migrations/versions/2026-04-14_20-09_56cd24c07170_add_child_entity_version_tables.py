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
"""add_child_entity_version_tables

Create version tables for TableColumn and SqlMetric so dataset
versioning captures child entities (columns and metrics) alongside
the parent dataset. Uses Continuum's validity strategy for
point-in-time queries.

Revision ID: 56cd24c07170
Revises: 06d6cae6d409
Create Date: 2026-04-14 20:09:00.000000

"""

import sqlalchemy as sa
import sqlalchemy_utils
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "56cd24c07170"
down_revision = "06d6cae6d409"
branch_labels = None
depends_on = None


def upgrade():
    # --- TableColumn version table ---
    op.create_table(
        "table_columns_version",
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
            "column_name",
            sa.String(length=255),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "verbose_name",
            sa.String(length=1024),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("is_active", sa.Boolean(), autoincrement=False, nullable=True),
        sa.Column("type", sa.Text(), autoincrement=False, nullable=True),
        sa.Column(
            "advanced_data_type",
            sa.String(length=255),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("groupby", sa.Boolean(), autoincrement=False, nullable=True),
        sa.Column("filterable", sa.Boolean(), autoincrement=False, nullable=True),
        sa.Column(
            "description",
            sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("table_id", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column("is_dttm", sa.Boolean(), autoincrement=False, nullable=True),
        sa.Column(
            "expression",
            sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "python_date_format",
            sa.String(length=255),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "datetime_format",
            sa.String(length=100),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column("extra", sa.Text(), autoincrement=False, nullable=True),
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
        op.f("ix_table_columns_version_end_transaction_id"),
        "table_columns_version",
        ["end_transaction_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_table_columns_version_operation_type"),
        "table_columns_version",
        ["operation_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_table_columns_version_transaction_id"),
        "table_columns_version",
        ["transaction_id"],
        unique=False,
    )

    # --- SqlMetric version table ---
    op.create_table(
        "sql_metrics_version",
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
            "metric_name",
            sa.String(length=255),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "verbose_name",
            sa.String(length=1024),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "metric_type",
            sa.String(length=32),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "description",
            sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "d3format", sa.String(length=128), autoincrement=False, nullable=True
        ),
        sa.Column("currency", sa.Text(), autoincrement=False, nullable=True),
        sa.Column("warning_text", sa.Text(), autoincrement=False, nullable=True),
        sa.Column("table_id", sa.Integer(), autoincrement=False, nullable=True),
        sa.Column(
            "expression",
            sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("extra", sa.Text(), autoincrement=False, nullable=True),
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
        op.f("ix_sql_metrics_version_end_transaction_id"),
        "sql_metrics_version",
        ["end_transaction_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_sql_metrics_version_operation_type"),
        "sql_metrics_version",
        ["operation_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_sql_metrics_version_transaction_id"),
        "sql_metrics_version",
        ["transaction_id"],
        unique=False,
    )


def downgrade():
    op.drop_table("sql_metrics_version")
    op.drop_table("table_columns_version")

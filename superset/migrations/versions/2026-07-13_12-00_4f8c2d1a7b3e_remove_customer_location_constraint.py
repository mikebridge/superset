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
"""Remove the legacy dataset customer-location constraint.

Revision ID: 4f8c2d1a7b3e
Revises: e7d93a524ff6
Create Date: 2026-07-13 12:00:00.000000
"""

from alembic import op
from migration_utils import create_unique_constraint, drop_unique_constraint
from sqlalchemy.engine.reflection import Inspector

from superset.utils.core import generic_find_uq_constraint_name

revision = "4f8c2d1a7b3e"
down_revision = "e7d93a524ff6"

TABLE_NAME = "tables"
LEGACY_CONSTRAINT_NAME = "_customer_location_uc"
LEGACY_COLUMNS = ["database_id", "schema", "table_name"]


def upgrade() -> None:
    """Remove the legacy constraint when it is present."""
    inspector = Inspector.from_engine(op.get_bind())
    if constraint_name := generic_find_uq_constraint_name(
        TABLE_NAME,
        LEGACY_COLUMNS,
        inspector,
    ):
        drop_unique_constraint(op, constraint_name, TABLE_NAME)


def downgrade() -> None:
    """Restore the legacy constraint."""
    create_unique_constraint(
        op,
        LEGACY_CONSTRAINT_NAME,
        TABLE_NAME,
        LEGACY_COLUMNS,
    )

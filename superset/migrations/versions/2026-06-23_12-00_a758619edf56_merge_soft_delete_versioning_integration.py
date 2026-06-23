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
"""merge soft-delete + versioning integration heads

Unifies the divergent Alembic heads brought together on the
integration branch into a single head. All four descend from the
single master head 78a40c08b4be:

  - 7c4a8d09ca37  charts soft-delete (slices.deleted_at)
  - 9e1f3b8c4d2a  dashboards soft-delete (dashboards.deleted_at)
  - 3a8e6f2c1b95  datasets soft-delete (tables.deleted_at)
  - d3b9a1f6c204  version-history (version_transaction issued_at index)

This is a no-op merge revision; it introduces no schema changes.

Revision ID: a758619edf56
Revises: 7c4a8d09ca37, 9e1f3b8c4d2a, 3a8e6f2c1b95, d3b9a1f6c204
Create Date: 2026-06-23 12:00:00.000000

"""

# revision identifiers, used by Alembic.
revision = "a758619edf56"
down_revision = (
    "7c4a8d09ca37",
    "9e1f3b8c4d2a",
    "3a8e6f2c1b95",
    "d3b9a1f6c204",
)


def upgrade():
    pass


def downgrade():
    pass

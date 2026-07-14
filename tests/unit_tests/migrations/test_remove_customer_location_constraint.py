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
"""Regression tests for the SC-112173 corrective migration."""

from collections.abc import Callable
from importlib import import_module

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import (
    Column,
    create_engine,
    insert,
    inspect,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

migration = import_module(
    "superset.migrations.versions."
    "2026-07-13_12-00_4f8c2d1a7b3e_remove_customer_location_constraint"
)

TABLE_NAME = "tables"
LEGACY_CONSTRAINT_NAME = "renamed_legacy_location_constraint"
UNRELATED_CONSTRAINT_NAME = "uq_tables_catalog_location"
LEGACY_COLUMNS = frozenset({"database_id", "schema", "table_name"})
UNRELATED_COLUMNS = frozenset({"database_id", "catalog", "schema", "table_name"})


def _create_engine(*, include_legacy_constraint: bool = True) -> Engine:
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    constraints = [
        UniqueConstraint(
            "database_id",
            "catalog",
            "schema",
            "table_name",
            name=UNRELATED_CONSTRAINT_NAME,
        )
    ]
    if include_legacy_constraint:
        constraints.append(
            UniqueConstraint(
                "schema",
                "table_name",
                "database_id",
                name=LEGACY_CONSTRAINT_NAME,
            )
        )

    Table(
        TABLE_NAME,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("database_id", Integer, nullable=False),
        Column("catalog", String(250)),
        Column("schema", String(250)),
        Column("table_name", String(250), nullable=False),
        *constraints,
    )
    metadata.create_all(engine)
    return engine


@pytest.fixture
def engine() -> Engine:
    """Create a minimal metadata table with legacy and catalog-aware rules."""
    return _create_engine()


def _run_migration(engine: Engine, operation: Callable[[], None]) -> None:
    with engine.connect() as connection:
        context = MigrationContext.configure(connection)
        with Operations.context(context):
            operation()


def _unique_constraints(engine: Engine) -> dict[str, frozenset[str]]:
    return {
        constraint["name"]: frozenset(constraint["column_names"])
        for constraint in inspect(engine).get_unique_constraints(TABLE_NAME)
        if constraint["name"] is not None
    }


def test_upgrade_removes_renamed_reordered_legacy_constraint(
    engine: Engine,
) -> None:
    _run_migration(engine, migration.upgrade)

    constraints = _unique_constraints(engine)
    assert LEGACY_COLUMNS not in constraints.values()
    assert constraints[UNRELATED_CONSTRAINT_NAME] == UNRELATED_COLUMNS


def test_upgrade_is_safe_when_legacy_constraint_is_absent() -> None:
    engine = _create_engine(include_legacy_constraint=False)

    _run_migration(engine, migration.upgrade)

    assert _unique_constraints(engine) == {UNRELATED_CONSTRAINT_NAME: UNRELATED_COLUMNS}


def test_upgrade_can_be_repeated(engine: Engine) -> None:
    _run_migration(engine, migration.upgrade)
    _run_migration(engine, migration.upgrade)

    assert _unique_constraints(engine) == {UNRELATED_CONSTRAINT_NAME: UNRELATED_COLUMNS}


def test_downgrade_restores_legacy_constraint(engine: Engine) -> None:
    _run_migration(engine, migration.upgrade)
    _run_migration(engine, migration.downgrade)

    constraints = _unique_constraints(engine)
    assert constraints["_customer_location_uc"] == LEGACY_COLUMNS
    assert constraints[UNRELATED_CONSTRAINT_NAME] == UNRELATED_COLUMNS


def test_downgrade_fails_when_catalog_distinct_rows_conflict(
    engine: Engine,
) -> None:
    _run_migration(engine, migration.upgrade)
    tables = Table(TABLE_NAME, MetaData(), autoload_with=engine)
    with engine.begin() as connection:
        connection.execute(
            insert(tables),
            [
                {
                    "id": 1,
                    "database_id": 7,
                    "catalog": "catalog_a",
                    "schema": "analytics",
                    "table_name": "events",
                },
                {
                    "id": 2,
                    "database_id": 7,
                    "catalog": "catalog_b",
                    "schema": "analytics",
                    "table_name": "events",
                },
            ],
        )

    with pytest.raises(IntegrityError):
        _run_migration(engine, migration.downgrade)


def test_upgrade_allows_catalog_distinct_rows_and_preserves_catalog_rule(
    engine: Engine,
) -> None:
    _run_migration(engine, migration.upgrade)
    tables = Table(TABLE_NAME, MetaData(), autoload_with=engine)
    catalog_distinct_rows = [
        {
            "id": 1,
            "database_id": 7,
            "catalog": "catalog_a",
            "schema": "analytics",
            "table_name": "events",
        },
        {
            "id": 2,
            "database_id": 7,
            "catalog": "catalog_b",
            "schema": "analytics",
            "table_name": "events",
        },
    ]
    with engine.begin() as connection:
        connection.execute(insert(tables), catalog_distinct_rows)

    with pytest.raises(IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                insert(tables),
                {
                    "id": 3,
                    "database_id": 7,
                    "catalog": "catalog_a",
                    "schema": "analytics",
                    "table_name": "events",
                },
            )

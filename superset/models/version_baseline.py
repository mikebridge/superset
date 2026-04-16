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
"""Baseline version capture for pre-migration entities.

Registers a ``before_flush`` listener that inserts a snapshot of the
pre-edit state into the version table the first time a legacy entity
is modified after versioning was enabled.  This produces two version
rows on the first save: the baseline (operation_type=0) and the edit
recorded by Continuum (operation_type=1).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import sqlalchemy as sa
from flask import has_request_context, request
from sqlalchemy.orm import Session

from superset import db

logger = logging.getLogger(__name__)

# In-process cache of (version_table_name, entity_id) tuples that have
# been confirmed to already have version rows or have had a baseline
# inserted.  Avoids repeated DB lookups for the same entity.
_baseline_cache: set[tuple[str, int]] = set()

# Model classes eligible for baseline capture.  Populated at
# registration time to avoid repeated hasattr checks during flush.
_VERSIONED_MODELS: set[type] = set()

# Continuum metadata columns that exist in version tables but not in
# the source entity table.
_CONTINUUM_COLUMNS = frozenset(
    {"transaction_id", "end_transaction_id", "operation_type"}
)


def _get_entity_column_values(
    obj: Any,
    version_table: sa.Table,
    excluded: set[str],
) -> dict[str, Any]:
    """Read current column values from an ORM object for all columns
    present in the version table, excluding Continuum metadata and any
    columns in the model's ``__versioned__["exclude"]`` list.
    """
    import uuid as _uuid

    # Use the committed (pre-edit) state so the baseline captures the
    # values BEFORE the current edit, not the pending changes.
    committed = sa.inspect(obj).committed_state

    values: dict[str, Any] = {}
    for col in version_table.columns:
        if col.name in _CONTINUUM_COLUMNS:
            continue
        if col.name in excluded:
            continue
        try:
            # committed_state only contains columns that have been
            # modified; for unmodified columns, fall back to the
            # current attribute value (which is the same as committed).
            if col.name in committed:
                val = committed[col.name]
            else:
                val = getattr(obj, col.name)
            # Convert UUID objects to strings for Core SQL compatibility
            # with reflected table columns.
            if isinstance(val, _uuid.UUID):
                val = str(val)
            values[col.name] = val
        except AttributeError:
            pass
    return values


def _insert_entity_baseline(
    conn: sa.engine.Connection,
    obj: Any,
    version_table: sa.Table,
    txn_id: int,
    excluded: set[str],
) -> None:
    """Insert a baseline version row for a single entity."""
    values = _get_entity_column_values(obj, version_table, excluded)
    values["transaction_id"] = txn_id
    values["end_transaction_id"] = None
    values["operation_type"] = 0  # INSERT
    conn.execute(version_table.insert().values(**values))


def _insert_children_baseline(
    conn: sa.engine.Connection,
    obj: Any,
    txn_id: int,
    children_relations: list[str],
) -> None:
    """Insert baseline version rows for one-to-many child entities."""
    from sqlalchemy_continuum import version_class

    mapper = obj.__class__.__mapper__

    for rel_name in children_relations:
        prop = mapper.get_property(rel_name)
        child_cls = prop.mapper.class_
        child_version_cls = version_class(child_cls)
        child_version_table = child_version_cls.__table__
        child_excluded = set(
            getattr(child_cls, "__versioned__", {}).get("exclude", [])
        )

        for child in getattr(obj, rel_name, []):
            values = _get_entity_column_values(
                child, child_version_table, child_excluded
            )
            values["transaction_id"] = txn_id
            values["end_transaction_id"] = None
            values["operation_type"] = 0
            conn.execute(child_version_table.insert().values(**values))


def _insert_m2m_baseline(
    conn: sa.engine.Connection,
    obj: Any,
    txn_id: int,
    m2m_relations: list[tuple[str, str]],
) -> None:
    """Insert baseline version rows for many-to-many association rows."""
    mapper = obj.__class__.__mapper__

    for rel_name, related_fk in m2m_relations:
        prop = mapper.get_property(rel_name)
        secondary = prop.secondary

        # Find the FK column pointing to the entity's table
        entity_fk = None
        for fk in secondary.foreign_keys:
            if fk.column.table == obj.__class__.__table__:
                entity_fk = fk.parent.name
                break

        if entity_fk is None:
            continue

        version_table_name = f"{secondary.name}_version"
        version_table = db.metadata.tables.get(version_table_name)
        if version_table is None:
            continue

        # Query current association rows
        current_rows = conn.execute(
            secondary.select().where(secondary.c[entity_fk] == obj.id)
        ).fetchall()

        for row in current_rows:
            values = {col.name: row._mapping[col.name] for col in secondary.columns}
            values["transaction_id"] = txn_id
            values["end_transaction_id"] = None
            values["operation_type"] = 0
            conn.execute(version_table.insert().values(**values))


def _has_existing_versions(
    conn: sa.engine.Connection,
    version_table: sa.Table,
    entity_id: int,
) -> bool:
    """Check whether any version row exists for the given entity."""
    result = conn.execute(
        sa.select(sa.literal(1))
        .select_from(version_table)
        .where(version_table.c.id == entity_id)
        .limit(1)
    ).scalar()
    return result is not None


def _before_flush_baseline(
    session: Session,
    flush_context: Any,
    instances: Any,
) -> None:
    """Insert baseline version rows for dirty entities with no prior versions."""
    if not session.dirty:
        return

    from superset.daos.version import (
        VERSIONED_CHILDREN_RELATIONS,
        VERSIONED_M2M_RELATIONS,
    )

    needs_baseline: list[tuple[Any, sa.Table, set[str]]] = []

    for obj in list(session.dirty):
        cls = type(obj)
        if cls not in _VERSIONED_MODELS:
            continue
        if not session.is_modified(obj, include_collections=False):
            continue

        version_table_name = f"{cls.__tablename__}_version"
        entity_id = obj.id
        cache_key = (version_table_name, entity_id)

        if cache_key in _baseline_cache:
            continue

        version_table = db.metadata.tables.get(version_table_name)
        if version_table is None:
            try:
                version_table = sa.Table(
                    version_table_name,
                    db.metadata,
                    autoload_with=db.engine,
                )
            except sa.exc.NoSuchTableError:
                _baseline_cache.add(cache_key)
                continue

        conn = session.connection()
        has_versions = _has_existing_versions(conn, version_table, entity_id)

        _baseline_cache.add(cache_key)

        if has_versions:
            continue

        excluded = set(
            getattr(cls, "__versioned__", {}).get("exclude", [])
        )
        needs_baseline.append((obj, version_table, excluded))

    if not needs_baseline:
        return

    conn = session.connection()

    # Create a single transaction record for all baselines in this flush
    txn_table = db.metadata.tables.get("transaction")
    if txn_table is None:
        try:
            txn_table = sa.Table(
                "transaction", db.metadata, autoload_with=db.engine
            )
        except sa.exc.NoSuchTableError:
            logger.warning(
                "Transaction table not found; skipping baseline capture"
            )
            return

    remote_addr = (
        request.remote_addr if has_request_context() else None
    )
    result = conn.execute(
        txn_table.insert().values(
            issued_at=datetime.utcnow(),
            remote_addr=remote_addr,
        )
    )
    baseline_txn_id = result.inserted_primary_key[0]

    for obj, version_table, excluded in needs_baseline:
        cls = type(obj)
        model_name = cls.__name__

        _insert_entity_baseline(conn, obj, version_table, baseline_txn_id, excluded)

        children = VERSIONED_CHILDREN_RELATIONS.get(model_name, [])
        if children:
            _insert_children_baseline(conn, obj, baseline_txn_id, children)

        m2m = VERSIONED_M2M_RELATIONS.get(model_name, [])
        if m2m:
            _insert_m2m_baseline(conn, obj, baseline_txn_id, m2m)

    logger.info(
        "Created baseline versions for %d entities (txn=%d)",
        len(needs_baseline),
        baseline_txn_id,
    )


def register_baseline_listener(session_factory: Any) -> None:
    """Register the before_flush baseline listener.

    Call this during app initialization after models have been imported.
    """
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    _VERSIONED_MODELS.update({Dashboard, Slice, SqlaTable})

    # Register on sa.orm.Session (the base class) with insert=True so
    # our listener runs BEFORE Continuum's before_flush listener.
    # Continuum also registers on sa.orm.Session; without insert=True
    # our listener would run second and get a higher transaction_id,
    # making the baseline appear newer than the edit.
    sa.event.listen(
        sa.orm.Session, "before_flush", _before_flush_baseline, insert=True
    )

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
"""before_flush listener that captures a baseline version (version 0) for entities
being updated for the first time after the versioning migration.

VERSIONED_MODELS is populated at app startup by the initialisation code after
make_versioned() has run and all versioned model classes have been defined.
"""

import logging
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Populated at app startup (superset/initialization/__init__.py) before
# register_baseline_listener() is called.
VERSIONED_MODELS: list[type] = []


def _get_user_id() -> Optional[int]:
    """Return the current Flask user's PK, or None outside a request context."""
    try:
        from flask_login import current_user  # pylint: disable=import-outside-toplevel

        if current_user.is_authenticated:
            return int(current_user.id)
    except Exception:  # pylint: disable=broad-except  # noqa: S110
        pass
    return None


def _insert_baseline_row(
    session: Session, obj: Any, version_table: sa.Table
) -> Optional[int]:
    """Insert a synthetic baseline row capturing the pre-edit DB state of *obj*.

    Creates a version_transaction entry and an operation_type=0 version row.
    All writes use the session's existing connection so they share the same
    database transaction as the triggering flush.

    Returns the allocated ``transaction_id`` so the caller can baseline child
    collections under the same tx (see :func:`_insert_child_baseline_rows`),
    or ``None`` when the entity has no live row.
    """
    from sqlalchemy_continuum import (
        versioning_manager,  # pylint: disable=import-outside-toplevel
    )

    main_table = type(obj).__table__
    conn = session.connection()

    # Read the persisted (pre-edit) state of the entity.
    row = (
        conn.execute(sa.select(main_table).where(main_table.c.id == obj.id))
        .mappings()
        .first()
    )
    if row is None:
        return None

    # Insert a version_transaction row for the baseline.
    #
    # ``issued_at`` and ``user_id`` are sourced from the entity's audit fields
    # (``changed_on`` / ``changed_by_fk``, falling back to ``created_on`` /
    # ``created_by_fk`` if the row was never edited), so the baseline reads
    # in the version-history UI as "this is the state at the time of the
    # last pre-versioning edit, by that user." Using ``now()`` and the
    # current user would have made the baseline look chronologically newer
    # than subsequent edits and attributed historical content to the user
    # who happened to trigger the first save under versioning.
    baseline_issued_at = (
        row.get("changed_on") or row.get("created_on") or sa.func.now()
    )
    baseline_user_id = row.get("changed_by_fk") or row.get("created_by_fk")
    tx_table = versioning_manager.transaction_cls.__table__
    result = conn.execute(
        tx_table.insert().values(
            issued_at=baseline_issued_at,
            user_id=baseline_user_id,
            remote_addr=None,
        )
    )
    tx_id = result.inserted_primary_key[0]

    # Build version row using Column objects as keys to avoid name/key mismatches
    # (string-based values(**dict) raises "Unconsumed column names" when a Column's
    # .key differs from its .name, which can happen with Continuum-generated tables).
    meta_col_names = {"transaction_id", "end_transaction_id", "operation_type"}
    col_values: dict[Any, Any] = {}
    for col in version_table.columns:
        if col.name in meta_col_names:
            continue
        if col.name in row:
            col_values[col] = row[col.name]

    col_values[version_table.c.transaction_id] = tx_id
    col_values[version_table.c.end_transaction_id] = None
    col_values[version_table.c.operation_type] = 0

    conn.execute(version_table.insert().values(col_values))
    return tx_id


def _insert_child_baseline_rows(
    session: Session,
    parent_obj: Any,
    child_table: sa.Table,
    child_version_table: sa.Table,
    fk_column_name: str,
    tx_id: int,
) -> None:
    """Synthesize ``operation_type=0`` shadow rows for every live child of
    *parent_obj* under transaction id *tx_id*.

    Parallels :func:`_insert_baseline_row` but iterates over child rows. Used
    to give Continuum's ``Reverter`` baseline data for children of pre-existing
    parents (children that predate this commit have no shadow rows otherwise,
    so Reverter would treat them as "deleted at the target tx" and try to
    remove them on revert — the ADR-004 Failure 1 reproduction scenario).

    :param child_table: the live child SQLAlchemy ``Table`` (e.g.
        ``TableColumn.__table__`` or the bare ``dashboard_slices`` association)
    :param child_version_table: the corresponding Continuum shadow ``Table``
    :param fk_column_name: column on *child_table* that points to the parent
        (e.g. ``"table_id"`` for ``TableColumn``, ``"dashboard_id"`` for
        ``dashboard_slices``)
    """
    conn = session.connection()
    fk_col = getattr(child_table.c, fk_column_name)

    rows = (
        conn.execute(sa.select(child_table).where(fk_col == parent_obj.id))
        .mappings()
        .all()
    )
    # SPIKE TRACE: log a snapshot of what we read from DB. If these values
    # are post-edit, an earlier silent flush already pushed the UPDATE to
    # the DB before our baseline listener got to read pre-edit state.
    sample_keys = ("id", "column_name", "expression", "metric_name")
    sample = [
        {k: r.get(k) for k in sample_keys if k in r} for r in rows
    ]
    logger.info(
        "baseline_listener: read %d rows from %s for parent id=%s sample=%s",
        len(rows),
        child_table.name,
        parent_obj.id,
        sample,
    )
    if not rows:
        return

    meta_col_names = {"transaction_id", "end_transaction_id", "operation_type"}
    for row in rows:
        col_values: dict[Any, Any] = {}
        for col in child_version_table.columns:
            if col.name in meta_col_names:
                continue
            if col.name in row:
                col_values[col] = row[col.name]
        col_values[child_version_table.c.transaction_id] = tx_id
        col_values[child_version_table.c.end_transaction_id] = None
        col_values[child_version_table.c.operation_type] = 0
        conn.execute(child_version_table.insert().values(col_values))


def _baseline_children_for_parent(
    session: Session, parent_obj: Any, tx_id: int
) -> None:
    """Baseline a parent's child collections under the parent's baseline tx.

    Dispatches based on the parent class. SqlaTable gets TableColumn and
    SqlMetric children versioned via Continuum shadows; Dashboard gets the
    dashboard_slices M2M association versioned via its auto-created
    ``dashboard_slices_version`` shadow.

    Errors are swallowed (with logging) — a child-baseline failure should not
    block the parent baseline.
    """
    # pylint: disable=import-outside-toplevel
    from sqlalchemy_continuum import version_class

    parent_name = type(parent_obj).__name__
    try:
        if parent_name == "SqlaTable":
            from superset.connectors.sqla.models import SqlMetric, TableColumn

            for child_cls in (TableColumn, SqlMetric):
                _insert_child_baseline_rows(
                    session,
                    parent_obj,
                    child_cls.__table__,
                    version_class(child_cls).__table__,
                    "table_id",
                    tx_id,
                )
        elif parent_name == "Dashboard":
            # dashboard_slices is an M2M association, not a model class.
            # Fetch the live and shadow tables directly from metadata.
            metadata = type(parent_obj).__table__.metadata
            live_tbl = metadata.tables.get("dashboard_slices")
            shadow_tbl = metadata.tables.get("dashboard_slices_version")
            if live_tbl is not None and shadow_tbl is not None:
                _insert_child_baseline_rows(
                    session,
                    parent_obj,
                    live_tbl,
                    shadow_tbl,
                    "dashboard_id",
                    tx_id,
                )
    except Exception:  # pylint: disable=broad-except
        logger.exception(
            "baseline_listener: failed to baseline children of %s id=%s",
            parent_name,
            getattr(parent_obj, "id", None),
        )


def register_baseline_listener() -> None:
    """Attach the before_flush listener that captures baseline versions.

    Call this after VERSIONED_MODELS has been populated and make_versioned() has run.
    """
    from superset.extensions import db  # pylint: disable=import-outside-toplevel

    # Child → parent registry: when a dirty child of a known type appears in
    # session.dirty/new/deleted, walk to its parent and baseline the parent
    # (+ siblings) under the SAME flush so pre-edit child values land in the
    # baseline shadow rows. Without this, edits that only touch child rows
    # produce a "silent" flush A (just TableColumn) followed by flush B
    # (SqlaTable.changed_on); flush B reads children from DB AFTER flush A
    # already pushed UPDATEs, capturing post-edit state. See spike trace
    # 2026-04-30 21:38:44 in spike-continuum-restore.md.
    def _child_to_parent_registry() -> dict[type, tuple[str, type]]:
        # pylint: disable=import-outside-toplevel
        from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn

        return {
            TableColumn: ("table", SqlaTable),
            SqlMetric: ("table", SqlaTable),
        }

    # insert=True prepends us in the listener chain so we run BEFORE
    # Continuum's before_flush. Continuum's pending Transaction object
    # (added in its own before_flush) would otherwise get a lower
    # auto-increment tx_id than our direct-SQL baseline insert, placing the
    # baseline row after the update in version_number order. Prepending
    # ensures our baseline's tx_id comes first.
    @event.listens_for(db.session, "before_flush", insert=True)
    def capture_baseline(session: Session, flush_context: Any, instances: Any) -> None:
        if not VERSIONED_MODELS:
            return

        # SPIKE TRACE: log every flush including the full dirty/new/deleted
        # composition so we can spot silent flushes that update children
        # before the parent's baseline listener fires.
        dirty_versioned = [
            o for o in session.dirty if type(o) in VERSIONED_MODELS
        ]
        new_versioned = [o for o in session.new if type(o) in VERSIONED_MODELS]
        all_dirty_types = sorted(
            {type(o).__name__ for o in session.dirty}
        )
        all_new_types = sorted({type(o).__name__ for o in session.new})
        all_deleted_types = sorted({type(o).__name__ for o in session.deleted})
        logger.info(
            "baseline_listener: flush triggered. "
            "dirty_versioned=%s new_versioned=%s "
            "total_new=%d (types=%s) total_dirty=%d (types=%s) "
            "total_deleted=%d (types=%s)",
            [(type(o).__name__, getattr(o, "id", None)) for o in dirty_versioned],
            [(type(o).__name__, getattr(o, "id", None)) for o in new_versioned],
            len(session.new),
            all_new_types,
            len(session.dirty),
            all_dirty_types,
            len(session.deleted),
            all_deleted_types,
        )

        # Build the set of parents to baseline: explicit dirty versioned
        # parents PLUS parents reachable from dirty/new/deleted children.
        parents_to_check: dict[int, Any] = {}  # id(obj) → obj (dedupe by identity)
        for obj in list(session.dirty) + list(session.new) + list(session.deleted):
            if type(obj) in VERSIONED_MODELS:
                parents_to_check[id(obj)] = obj
                continue
            child_map = _child_to_parent_registry()
            entry = child_map.get(type(obj))
            if entry is None:
                continue
            parent_attr, parent_cls = entry
            parent = getattr(obj, parent_attr, None)
            if parent is not None and type(parent) is parent_cls:  # noqa: E721
                parents_to_check[id(parent)] = parent

        for obj in parents_to_check.values():
            if type(obj) not in VERSIONED_MODELS:
                continue

            try:
                from sqlalchemy_continuum import (
                    version_class,  # pylint: disable=import-outside-toplevel
                )

                ver_cls = version_class(type(obj))
                version_table = ver_cls.__table__
            except Exception:  # pylint: disable=broad-except  # noqa: S112
                continue

            try:
                with session.no_autoflush:
                    count = (
                        session.connection()
                        .execute(
                            sa.select(sa.func.count())
                            .select_from(version_table)
                            .where(version_table.c.id == obj.id)
                        )
                        .scalar()
                    )
            except OperationalError:
                # Version table does not yet exist (migration not yet applied).
                continue
            except Exception:  # pylint: disable=broad-except
                logger.exception(
                    "baseline_listener: unexpected error checking version count "
                    "for %s id=%s",
                    type(obj).__name__,
                    getattr(obj, "id", None),
                )
                continue

            logger.info(
                "baseline_listener: %s id=%s shadow_count=%d → %s",
                type(obj).__name__,
                getattr(obj, "id", None),
                count,
                "FIRING baseline" if count == 0 else "skip (already has shadows)",
            )

            if count == 0:
                try:
                    # no_autoflush here too: prevents ``session.connection()``
                    # inside ``_insert_baseline_row`` from triggering a
                    # flush of Continuum's pending Transaction object
                    # before our direct-SQL insert grabs its tx_id.
                    with session.no_autoflush:
                        tx_id = _insert_baseline_row(session, obj, version_table)
                        if tx_id is not None:
                            # SPIKE: also baseline the parent's child collections
                            # so Continuum's Reverter has data to roll back to
                            # for children that predate versioning.
                            _baseline_children_for_parent(session, obj, tx_id)
                            logger.info(
                                "baseline_listener: inserted baseline tx_id=%s "
                                "for %s id=%s",
                                tx_id,
                                type(obj).__name__,
                                getattr(obj, "id", None),
                            )
                except Exception:  # pylint: disable=broad-except
                    logger.exception(
                        "baseline_listener: failed to insert baseline for %s id=%s",
                        type(obj).__name__,
                        getattr(obj, "id", None),
                    )

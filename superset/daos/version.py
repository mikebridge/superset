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
"""DAO for entity version history (SQLAlchemy-Continuum)."""

from __future__ import annotations

import logging
from typing import Any

from flask_appbuilder import Model
from sqlalchemy_continuum import transaction_class, version_class

from superset import db

logger = logging.getLogger(__name__)

# Fields excluded from restore — AuditMixinNullable sets these
# automatically and they should reflect the restoring user, not the
# historical snapshot.
RESTORE_EXCLUDE_FIELDS = frozenset(
    {
        "created_on",
        "created_by_fk",
        "changed_on",
        "changed_by_fk",
    }
)

# Continuum operation_type integers → human-readable strings
OPERATION_TYPE_MAP = {
    0: "insert",
    1: "update",
    2: "delete",
}


# Map of parent model name → list of relationship names whose children
# should be reverted alongside the parent via Continuum's Reverter.
VERSIONED_CHILDREN_RELATIONS: dict[str, list[str]] = {
    "SqlaTable": ["columns", "metrics"],
}

# Map of parent model name → list of (relationship_name, related_fk_column)
# tuples for many-to-many relationships that should be restored alongside
# the parent.  The related_fk_column is the column in the association table
# that points to the related entity (e.g. slice_id in dashboard_slices).
VERSIONED_M2M_RELATIONS: dict[str, list[tuple[str, str]]] = {
    "Dashboard": [("slices", "slice_id")],
}


class VersionDAO:
    """Data access layer for entity version history."""

    @staticmethod
    def list_versions(
        model_cls: type[Model],
        entity_id: int,
        page: int = 0,
        page_size: int = 25,
    ) -> dict[str, Any]:
        """Return paginated version history for an entity.

        Returns a dict with ``count`` (total) and ``result`` (list of
        version summary dicts).
        """
        VersionCls = version_class(model_cls)  # noqa: N806
        TransactionCls = transaction_class(model_cls)  # noqa: N806

        base_query = db.session.query(VersionCls).filter(VersionCls.id == entity_id)

        total = base_query.count()

        versions = (
            base_query.order_by(VersionCls.transaction_id.desc())
            .offset(page * page_size)
            .limit(page_size)
            .all()
        )

        result = []
        for v in versions:
            txn = db.session.query(TransactionCls).get(v.transaction_id)
            result.append(
                {
                    "version_number": v.transaction_id,
                    "changed_on": txn.issued_at if txn else None,
                    "changed_by_fk": v.changed_by_fk
                    if hasattr(v, "changed_by_fk")
                    else None,
                    "operation_type": OPERATION_TYPE_MAP.get(
                        v.operation_type, "unknown"
                    ),
                    "is_current": v.end_transaction_id is None,
                }
            )

        return {"count": total, "result": result}

    @staticmethod
    def get_version(
        model_cls: type[Model],
        entity_id: int,
        version_number: int,
    ) -> dict[str, Any] | None:
        """Return a single version snapshot.

        ``version_number`` corresponds to ``transaction_id``.
        Returns ``None`` if the version does not exist.
        """
        VersionCls = version_class(model_cls)  # noqa: N806
        TransactionCls = transaction_class(model_cls)  # noqa: N806

        version = (
            db.session.query(VersionCls)
            .filter(
                VersionCls.id == entity_id,
                VersionCls.transaction_id == version_number,
            )
            .one_or_none()
        )

        if version is None:
            return None

        txn = db.session.query(TransactionCls).get(version.transaction_id)

        # Build snapshot from all entity columns (exclude Continuum metadata)
        continuum_columns = {
            "transaction_id",
            "end_transaction_id",
            "operation_type",
        }
        snapshot = {}
        for col in VersionCls.__table__.columns:
            if col.name not in continuum_columns:
                try:
                    snapshot[col.name] = getattr(version, col.name)
                except AttributeError:
                    # Column exists in table but not as a Python attribute
                    # (e.g., inherited columns). Read from the raw row.
                    pass

        return {
            "version_number": version.transaction_id,
            "changed_on": txn.issued_at if txn else None,
            "changed_by_fk": version.changed_by_fk
            if hasattr(version, "changed_by_fk")
            else None,
            "operation_type": OPERATION_TYPE_MAP.get(version.operation_type, "unknown"),
            "snapshot": snapshot,
        }

    @staticmethod
    def restore_version(
        model_cls: type[Model],
        entity_id: int,
        version_number: int,
    ) -> Model | None:
        """Restore an entity to a previous version.

        Uses Continuum's ``revert()`` for parent entity properties, then
        manually replaces child entities (columns, metrics) by deleting
        current children and reverting each child version. This is needed
        because Continuum's Reverter inserts children without removing
        existing ones, causing unique constraint violations.

        Returns the restored entity, or ``None`` if the version or entity
        does not exist.
        """
        VersionCls = version_class(model_cls)  # noqa: N806

        version_obj = (
            db.session.query(VersionCls)
            .filter(
                VersionCls.id == entity_id,
                VersionCls.transaction_id == version_number,
            )
            .one_or_none()
        )

        if version_obj is None:
            return None

        entity = db.session.query(model_cls).get(entity_id)
        if entity is None:
            return None

        # Restore children first (if configured) via direct SQL
        # before touching the parent — avoids cascade conflicts
        relations = VERSIONED_CHILDREN_RELATIONS.get(
            model_cls.__name__, []
        )
        if relations:
            VersionDAO._restore_children(entity, version_number, relations)

        # Apply parent properties manually (skip audit fields and PK)
        version_data = VersionDAO.get_version(
            model_cls, entity_id, version_number
        )
        if version_data:
            for field, value in version_data["snapshot"].items():
                if field in RESTORE_EXCLUDE_FIELDS:
                    continue
                if field == "id":
                    continue
                if hasattr(entity, field):
                    setattr(entity, field, value)

        # Restore M2M relationships (e.g. dashboard↔charts)
        m2m_relations = VERSIONED_M2M_RELATIONS.get(model_cls.__name__, [])
        if m2m_relations:
            VersionDAO._restore_m2m_relationships(
                entity, version_number, m2m_relations
            )

        # Clear derived columns that are excluded from versioning so
        # they get regenerated from the restored state on next access.
        for col_name in getattr(model_cls, "__versioned__", {}).get(
            "exclude", []
        ):
            if hasattr(entity, col_name):
                setattr(entity, col_name, None)

        return entity

    @staticmethod
    def _restore_children(
        entity: Model,
        target_txn: int,
        relations: list[str],
    ) -> None:
        """Replace current children with their state at target_txn.

        For each relationship, deletes all current children via direct
        SQL (bypassing ORM cascades), then uses Continuum's revert on
        each child version that was active at the target transaction.
        """
        from sqlalchemy import or_

        mapper = entity.__class__.__mapper__

        with db.session.no_autoflush:
            for rel_name in relations:
                prop = mapper.get_property(rel_name)
                child_cls = prop.mapper.class_
                child_version_cls = version_class(child_cls)

                # Determine FK column name from the relationship
                fk_column = list(prop.local_remote_pairs)[0][1].name

                # Find child versions active at target transaction.
                # override_columns saves delete+re-insert children
                # with new IDs, so the same column_name can appear
                # under multiple IDs with overlapping validity.
                # Deduplicate by keeping only the row with the highest
                # (id, transaction_id) per result set.
                from sqlalchemy import func

                fk_version_col = getattr(child_version_cls, fk_column)

                # Get all active child versions at target txn
                active_versions = (
                    db.session.query(child_version_cls)
                    .filter(
                        fk_version_col == entity.id,
                        child_version_cls.transaction_id <= target_txn,
                        or_(
                            child_version_cls.end_transaction_id.is_(None),
                            child_version_cls.end_transaction_id
                            > target_txn,
                        ),
                    )
                    .order_by(
                        child_version_cls.id.desc(),
                        child_version_cls.transaction_id.desc(),
                    )
                    .all()
                )

                # Deduplicate: for columns use column_name, for
                # metrics use metric_name. Fall back to keeping all.
                # Skip DELETE operations (operation_type=2) — those
                # represent children that were removed at that point.
                seen = set()
                children_at_txn = []
                for v in active_versions:
                    if v.operation_type == 2:  # DELETE
                        continue
                    key = (
                        getattr(v, "column_name", None)
                        or getattr(v, "metric_name", None)
                        or v.id
                    )
                    if key not in seen:
                        seen.add(key)
                        children_at_txn.append(v)

                if not children_at_txn:
                    continue

                # Expunge current children from ORM session to prevent
                # cascade re-insertion on flush
                current_children = getattr(entity, rel_name, [])
                for child in list(current_children):
                    if child in db.session:
                        db.session.expunge(child)

                # Delete current children via direct SQL
                child_table = child_cls.__table__
                db.session.execute(
                    child_table.delete().where(
                        child_table.c[fk_column] == entity.id
                    )
                )
                # Expire the relationship so ORM reloads on next access
                db.session.expire(entity, [rel_name])

                # Recreate children from version data via direct SQL
                # (bypasses ORM/Continuum to avoid autoflush conflicts)
                continuum_cols = {
                    "transaction_id",
                    "end_transaction_id",
                    "operation_type",
                }
                for child_version in children_at_txn:
                    values = {}
                    for col in child_version_cls.__table__.columns:
                        if col.name in continuum_cols:
                            continue
                        if col.name in RESTORE_EXCLUDE_FIELDS:
                            continue
                        if col.name == "id":
                            continue
                        try:
                            values[col.name] = getattr(
                                child_version, col.name
                            )
                        except AttributeError:
                            pass
                    values[fk_column] = entity.id
                    db.session.execute(
                        child_table.insert().values(**values)
                    )

    @staticmethod
    def _restore_m2m_relationships(
        entity: Model,
        target_txn: int,
        m2m_config: list[tuple[str, str]],
    ) -> None:
        """Restore many-to-many relationships to their state at target_txn.

        Starts with the current set of related IDs, then replays tracked
        changes to compute the target state.  Related entities whose
        association pre-dates Continuum tracking are preserved (no INSERT
        record exists, so we assume they were present).  Related entities
        that have been hard-deleted are silently skipped.
        """
        from sqlalchemy import Table

        mapper = entity.__class__.__mapper__

        for rel_name, related_fk in m2m_config:
            prop = mapper.get_property(rel_name)
            related_cls = prop.mapper.class_
            secondary = prop.secondary

            # Find the FK column pointing to the entity's table
            entity_fk = None
            for fk in secondary.foreign_keys:
                if fk.column.table == entity.__class__.__table__:
                    entity_fk = fk.parent.name
                    break

            if entity_fk is None:
                continue

            # Reflect the version table from the database
            version_table_name = f"{secondary.name}_version"
            version_table = db.metadata.tables.get(version_table_name)
            if version_table is None:
                version_table = Table(
                    version_table_name,
                    db.metadata,
                    autoload_with=db.engine,
                )

            # Start with the current set of related IDs.  IDs that
            # pre-date Continuum tracking have no version rows, so
            # starting from the current set preserves them.
            current_ids = {
                r.id for r in getattr(entity, rel_name, [])
            }

            # Fetch version rows AFTER target_txn — these are changes
            # we need to undo.  Ordered newest-first for dedup.
            changes_after = db.session.execute(
                version_table.select()
                .where(version_table.c[entity_fk] == entity.id)
                .where(version_table.c.transaction_id > target_txn)
                .order_by(version_table.c.transaction_id.desc())
            ).fetchall()

            if not changes_after:
                # No changes after target — relationship is already
                # in the target state.
                continue

            # Deduplicate: keep only the most recent change per ID
            # after the target transaction.
            seen: set[int] = set()
            for row in changes_after:
                rid = row._mapping[related_fk]
                if rid in seen:
                    continue
                seen.add(rid)
                op = row._mapping["operation_type"]
                if op == 0:
                    # INSERT after target → was not present at target
                    current_ids.discard(rid)
                elif op == 2:
                    # DELETE after target → was present at target
                    current_ids.add(rid)

            # Verify related entities still exist
            if current_ids:
                existing = db.session.query(related_cls.id).filter(
                    related_cls.id.in_(current_ids)
                ).all()
                valid_ids = {r[0] for r in existing}
            else:
                valid_ids = set()

            # Assign via ORM so Continuum tracks the changes
            if valid_ids:
                related_objects = (
                    db.session.query(related_cls)
                    .filter(related_cls.id.in_(valid_ids))
                    .all()
                )
            else:
                related_objects = []

            setattr(entity, rel_name, related_objects)

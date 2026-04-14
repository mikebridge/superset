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
from sqlalchemy import or_
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


# Map of parent model → list of (child_model, FK column name on the child)
# for models whose children should be restored alongside the parent.
VERSIONED_CHILDREN: dict[str, list[tuple[str, str]]] = {
    "SqlaTable": [
        ("superset.connectors.sqla.models.TableColumn", "table_id"),
        ("superset.connectors.sqla.models.SqlMetric", "table_id"),
    ],
}


def _import_class(dotted_path: str) -> type:
    """Import a class from a dotted module path."""
    module_path, class_name = dotted_path.rsplit(".", 1)
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, class_name)


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

        Applies the snapshot columns (excluding audit fields) as a normal
        UPDATE so Continuum captures the restore as a new version.
        For models with versioned children (e.g., SqlaTable → TableColumn,
        SqlMetric), also restores children to their state at the target
        transaction using Continuum's validity range.
        Returns the restored entity, or ``None`` if the version or entity
        does not exist.
        """
        version_data = VersionDAO.get_version(model_cls, entity_id, version_number)
        if version_data is None:
            return None

        entity = db.session.query(model_cls).get(entity_id)
        if entity is None:
            return None

        snapshot = version_data["snapshot"]

        # Apply snapshot fields, skipping audit fields and the PK
        for field, value in snapshot.items():
            if field in RESTORE_EXCLUDE_FIELDS:
                continue
            if field == "id":
                continue
            if hasattr(entity, field):
                setattr(entity, field, value)

        # Restore versioned children if configured
        VersionDAO._restore_children(model_cls, entity_id, version_number)

        return entity

    @staticmethod
    def _restore_children(
        model_cls: type[Model],
        entity_id: int,
        target_txn: int,
    ) -> None:
        """Restore child entities to their state at the target transaction.

        Uses Continuum's validity range: a child version was active at
        ``target_txn`` if ``transaction_id <= target_txn`` and
        ``(end_transaction_id IS NULL OR end_transaction_id > target_txn)``.
        """
        class_name = model_cls.__name__
        children_config = VERSIONED_CHILDREN.get(class_name)
        if not children_config:
            return

        for child_path, fk_column in children_config:
            child_cls = _import_class(child_path)
            child_version_cls = version_class(child_cls)

            # Find child versions active at the target transaction
            fk_version_col = getattr(child_version_cls, fk_column)
            children_at_txn = (
                db.session.query(child_version_cls)
                .filter(
                    fk_version_col == entity_id,
                    child_version_cls.transaction_id <= target_txn,
                    or_(
                        child_version_cls.end_transaction_id.is_(None),
                        child_version_cls.end_transaction_id > target_txn,
                    ),
                )
                .all()
            )

            if not children_at_txn:
                # No child versions at this transaction — either the
                # version predates child versioning or the entity had
                # no children. Leave current children unchanged.
                continue

            # Delete current children and flush before inserting to avoid
            # unique constraint violations from autoflush
            fk_col = getattr(child_cls, fk_column)
            db.session.query(child_cls).filter(fk_col == entity_id).delete(
                synchronize_session="fetch"
            )
            db.session.flush()

            # Recreate from version data
            continuum_cols = {
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            }
            for child_version in children_at_txn:
                child = child_cls()
                for col in child_version_cls.__table__.columns:
                    if col.name in continuum_cols:
                        continue
                    if col.name in RESTORE_EXCLUDE_FIELDS:
                        continue
                    if col.name == "id":
                        continue
                    try:
                        value = getattr(child_version, col.name)
                        setattr(child, col.name, value)
                    except AttributeError:
                        pass
                setattr(child, fk_column, entity_id)
                db.session.add(child)

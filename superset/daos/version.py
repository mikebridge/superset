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

        Uses Continuum's built-in ``revert()`` method which handles both
        the parent entity and any versioned child relationships (e.g.,
        SqlaTable → columns, metrics). The revert creates a new version
        in the history chain (non-destructive).

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

        # Check entity still exists
        entity = db.session.query(model_cls).get(entity_id)
        if entity is None:
            return None

        # Use Continuum's Reverter which handles parent + child relations
        relations = VERSIONED_CHILDREN_RELATIONS.get(
            model_cls.__name__, []
        )
        version_obj.revert(relations=relations)

        return entity

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
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _clear_baseline_cache() -> None:
    """Reset the module-level cache between tests."""
    from superset.models.version_baseline import _baseline_cache

    _baseline_cache.clear()


def test_baseline_noop_when_session_not_dirty(
    app_context: None,
) -> None:
    from superset.models.version_baseline import _before_flush_baseline

    session = MagicMock()
    session.dirty = []

    _before_flush_baseline(session, MagicMock(), None)

    session.connection.assert_not_called()


@patch("superset.models.version_baseline.db")
def test_baseline_skips_non_versioned_models(
    mock_db: MagicMock,
    app_context: None,
) -> None:
    from superset.models.version_baseline import _before_flush_baseline

    obj = MagicMock()
    obj.__class__ = type("NotVersioned", (), {})
    session = MagicMock()
    session.dirty = [obj]

    _before_flush_baseline(session, MagicMock(), None)

    session.connection.assert_not_called()


@patch("superset.models.version_baseline._has_existing_versions", return_value=True)
@patch("superset.models.version_baseline.db")
def test_baseline_skips_entity_with_existing_versions(
    mock_db: MagicMock,
    mock_has_versions: MagicMock,
    app_context: None,
) -> None:
    from superset.models.version_baseline import (
        _baseline_cache,
        _before_flush_baseline,
        _VERSIONED_MODELS,
    )

    class FakeModel:
        __tablename__ = "dashboards"
        __versioned__ = {}
        id = 1

    _VERSIONED_MODELS.add(FakeModel)

    obj = FakeModel()
    session = MagicMock()
    session.dirty = [obj]
    session.is_modified.return_value = True

    version_table = MagicMock()
    mock_db.metadata.tables.get.return_value = version_table

    _before_flush_baseline(session, MagicMock(), None)

    assert ("dashboards_version", 1) in _baseline_cache
    mock_has_versions.assert_called_once()

    _VERSIONED_MODELS.discard(FakeModel)


@patch("superset.models.version_baseline._insert_entity_baseline")
@patch("superset.models.version_baseline._has_existing_versions", return_value=False)
@patch("superset.models.version_baseline.db")
def test_baseline_inserts_for_entity_with_no_versions(
    mock_db: MagicMock,
    mock_has_versions: MagicMock,
    mock_insert_baseline: MagicMock,
    app_context: None,
) -> None:
    from superset.models.version_baseline import (
        _baseline_cache,
        _before_flush_baseline,
        _VERSIONED_MODELS,
    )

    class FakeModel:
        __tablename__ = "slices"
        __versioned__ = {"exclude": ["query_context"]}
        __name__ = "Slice"
        id = 42

    _VERSIONED_MODELS.add(FakeModel)

    obj = FakeModel()
    session = MagicMock()
    session.dirty = [obj]
    session.is_modified.return_value = True

    version_table = MagicMock()
    txn_table = MagicMock()
    mock_db.metadata.tables.get.side_effect = lambda name: {
        "slices_version": version_table,
        "transaction": txn_table,
    }.get(name)

    conn = session.connection.return_value
    conn.execute.return_value.inserted_primary_key = [999]

    _before_flush_baseline(session, MagicMock(), None)

    assert ("slices_version", 42) in _baseline_cache
    mock_insert_baseline.assert_called_once_with(
        conn, obj, version_table, 999, {"query_context"}
    )

    _VERSIONED_MODELS.discard(FakeModel)


@patch("superset.models.version_baseline.db")
def test_baseline_cache_prevents_repeated_checks(
    mock_db: MagicMock,
    app_context: None,
) -> None:
    from superset.models.version_baseline import (
        _baseline_cache,
        _before_flush_baseline,
        _VERSIONED_MODELS,
    )

    class FakeModel:
        __tablename__ = "dashboards"
        __versioned__ = {}
        id = 7

    _VERSIONED_MODELS.add(FakeModel)
    _baseline_cache.add(("dashboards_version", 7))

    obj = FakeModel()
    session = MagicMock()
    session.dirty = [obj]
    session.is_modified.return_value = True

    _before_flush_baseline(session, MagicMock(), None)

    session.connection.assert_not_called()

    _VERSIONED_MODELS.discard(FakeModel)

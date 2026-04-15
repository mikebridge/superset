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


def _make_column(name: str) -> MagicMock:
    col = MagicMock()
    col.name = name
    return col


def _make_version(
    entity_id: int = 1,
    transaction_id: int = 100,
    operation_type: int = 1,
    end_transaction_id: int | None = None,
    changed_by_fk: int | None = 42,
    extra_attrs: dict | None = None,
) -> MagicMock:
    v = MagicMock()
    v.id = entity_id
    v.transaction_id = transaction_id
    v.operation_type = operation_type
    v.end_transaction_id = end_transaction_id
    v.changed_by_fk = changed_by_fk
    if extra_attrs:
        for k, val in extra_attrs.items():
            setattr(v, k, val)
    return v


def _make_txn(issued_at: str = "2025-01-01T00:00:00") -> MagicMock:
    txn = MagicMock()
    txn.issued_at = issued_at
    return txn


@patch("superset.daos.version.db")
@patch("superset.daos.version.version_class")
@patch("superset.daos.version.transaction_class")
def test_list_versions_returns_paginated_results(
    mock_transaction_class: MagicMock,
    mock_version_class: MagicMock,
    mock_db: MagicMock,
    app_context: None,
) -> None:
    from superset.daos.version import VersionDAO

    model_cls = MagicMock()
    version_cls = MagicMock()
    transaction_cls = MagicMock()
    mock_version_class.return_value = version_cls
    mock_transaction_class.return_value = transaction_cls

    v1 = _make_version(transaction_id=101, operation_type=1)
    v2 = _make_version(transaction_id=100, operation_type=0)

    query = mock_db.session.query.return_value
    filtered = query.filter.return_value
    filtered.count.return_value = 2
    ordered = filtered.order_by.return_value
    offset = ordered.offset.return_value
    limited = offset.limit.return_value
    limited.all.return_value = [v1, v2]

    txn1 = _make_txn("2025-01-02T00:00:00")
    txn2 = _make_txn("2025-01-01T00:00:00")

    def get_txn(tid: int) -> MagicMock:
        return {101: txn1, 100: txn2}[tid]

    txn_query = MagicMock()
    mock_db.session.query.side_effect = [query, txn_query, txn_query]
    txn_query.get.side_effect = get_txn

    # Reset side_effect so each call to db.session.query works
    mock_db.session.query.reset_mock(side_effect=True)
    mock_db.session.query.side_effect = None

    # Re-setup: first call returns version query, subsequent calls return txn query
    call_count = 0

    def query_side_effect(cls: MagicMock) -> MagicMock:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return query
        return txn_query

    mock_db.session.query.side_effect = query_side_effect

    result = VersionDAO.list_versions(model_cls, entity_id=1, page=0, page_size=25)

    assert result["count"] == 2
    assert len(result["result"]) == 2
    assert result["result"][0]["version_number"] == 101
    assert result["result"][1]["version_number"] == 100


@patch("superset.daos.version.db")
@patch("superset.daos.version.version_class")
@patch("superset.daos.version.transaction_class")
def test_list_versions_empty(
    mock_transaction_class: MagicMock,
    mock_version_class: MagicMock,
    mock_db: MagicMock,
    app_context: None,
) -> None:
    from superset.daos.version import VersionDAO

    model_cls = MagicMock()
    mock_version_class.return_value = MagicMock()
    mock_transaction_class.return_value = MagicMock()

    query = mock_db.session.query.return_value
    filtered = query.filter.return_value
    filtered.count.return_value = 0
    ordered = filtered.order_by.return_value
    offset = ordered.offset.return_value
    limited = offset.limit.return_value
    limited.all.return_value = []

    result = VersionDAO.list_versions(model_cls, entity_id=999, page=0, page_size=25)

    assert result["count"] == 0
    assert result["result"] == []


@patch("superset.daos.version.db")
@patch("superset.daos.version.version_class")
@patch("superset.daos.version.transaction_class")
def test_list_versions_maps_operation_type(
    mock_transaction_class: MagicMock,
    mock_version_class: MagicMock,
    mock_db: MagicMock,
    app_context: None,
) -> None:
    from superset.daos.version import VersionDAO

    model_cls = MagicMock()
    version_cls = MagicMock()
    transaction_cls = MagicMock()
    mock_version_class.return_value = version_cls
    mock_transaction_class.return_value = transaction_cls

    v_insert = _make_version(transaction_id=1, operation_type=0)
    v_update = _make_version(transaction_id=2, operation_type=1)
    v_delete = _make_version(transaction_id=3, operation_type=2)

    query = mock_db.session.query.return_value
    filtered = query.filter.return_value
    filtered.count.return_value = 3
    ordered = filtered.order_by.return_value
    offset = ordered.offset.return_value
    limited = offset.limit.return_value
    limited.all.return_value = [v_insert, v_update, v_delete]

    txn_query = MagicMock()
    txn_query.get.return_value = _make_txn()

    call_count = 0

    def query_side_effect(cls: MagicMock) -> MagicMock:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return query
        return txn_query

    mock_db.session.query.side_effect = query_side_effect

    result = VersionDAO.list_versions(model_cls, entity_id=1, page=0, page_size=25)

    assert result["result"][0]["operation_type"] == "insert"
    assert result["result"][1]["operation_type"] == "update"
    assert result["result"][2]["operation_type"] == "delete"


@patch("superset.daos.version.db")
@patch("superset.daos.version.version_class")
@patch("superset.daos.version.transaction_class")
def test_get_version_returns_snapshot(
    mock_transaction_class: MagicMock,
    mock_version_class: MagicMock,
    mock_db: MagicMock,
    app_context: None,
) -> None:
    from superset.daos.version import VersionDAO

    model_cls = MagicMock()
    version_cls = MagicMock()
    transaction_cls = MagicMock()
    mock_version_class.return_value = version_cls
    mock_transaction_class.return_value = transaction_cls

    version = _make_version(
        entity_id=1,
        transaction_id=100,
        operation_type=1,
        extra_attrs={"slice_name": "My Chart", "viz_type": "bar"},
    )

    # Set up table columns — entity columns + continuum metadata
    entity_columns = [
        _make_column("id"),
        _make_column("slice_name"),
        _make_column("viz_type"),
    ]
    continuum_columns = [
        _make_column("transaction_id"),
        _make_column("end_transaction_id"),
        _make_column("operation_type"),
    ]
    version_cls.__table__ = MagicMock()
    version_cls.__table__.columns = entity_columns + continuum_columns

    query = mock_db.session.query.return_value
    filtered = query.filter.return_value
    filtered.one_or_none.return_value = version

    txn = _make_txn("2025-06-01T12:00:00")
    txn_query = MagicMock()
    txn_query.get.return_value = txn

    call_count = 0

    def query_side_effect(cls: MagicMock) -> MagicMock:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return query
        return txn_query

    mock_db.session.query.side_effect = query_side_effect

    result = VersionDAO.get_version(model_cls, entity_id=1, version_number=100)

    assert result is not None
    assert result["version_number"] == 100
    assert result["changed_on"] == "2025-06-01T12:00:00"
    assert result["operation_type"] == "update"
    # Snapshot should include entity columns but NOT continuum metadata
    assert "id" in result["snapshot"]
    assert "slice_name" in result["snapshot"]
    assert "viz_type" in result["snapshot"]
    assert "transaction_id" not in result["snapshot"]
    assert "end_transaction_id" not in result["snapshot"]
    assert "operation_type" not in result["snapshot"]


@patch("superset.daos.version.db")
@patch("superset.daos.version.version_class")
@patch("superset.daos.version.transaction_class")
def test_get_version_returns_none_for_missing(
    mock_transaction_class: MagicMock,
    mock_version_class: MagicMock,
    mock_db: MagicMock,
    app_context: None,
) -> None:
    from superset.daos.version import VersionDAO

    model_cls = MagicMock()
    mock_version_class.return_value = MagicMock()
    mock_transaction_class.return_value = MagicMock()

    query = mock_db.session.query.return_value
    filtered = query.filter.return_value
    filtered.one_or_none.return_value = None

    result = VersionDAO.get_version(model_cls, entity_id=1, version_number=999)

    assert result is None


@patch("superset.daos.version.version_class")
@patch("superset.daos.version.db")
@patch("superset.daos.version.VersionDAO.get_version")
def test_restore_version_applies_snapshot(
    mock_get_version: MagicMock,
    mock_db: MagicMock,
    mock_version_class: MagicMock,
    app_context: None,
) -> None:
    from superset.daos.version import RESTORE_EXCLUDE_FIELDS, VersionDAO

    model_cls = MagicMock()
    model_cls.__name__ = "Slice"
    model_cls.__versioned__ = {"exclude": ["query_context"]}
    version_cls = MagicMock()
    version_obj = MagicMock()
    mock_version_class.return_value = version_cls
    mock_db.session.query.return_value.filter.return_value.one_or_none.return_value = (
        version_obj
    )
    mock_get_version.return_value = {
        "version_number": 100,
        "changed_on": "2025-06-01T12:00:00",
        "changed_by_fk": 42,
        "operation_type": "update",
        "snapshot": {
            "id": 1,
            "slice_name": "Old Chart Name",
            "viz_type": "bar",
            "created_on": "2025-01-01T00:00:00",
            "created_by_fk": 10,
            "changed_on": "2025-06-01T12:00:00",
            "changed_by_fk": 42,
        },
    }

    # Use a simple namespace object so we can track which attrs get set
    class FakeEntity:
        pass

    entity = FakeEntity()
    entity.id = 1  # type: ignore
    entity.slice_name = "Current Name"  # type: ignore
    entity.viz_type = "line"  # type: ignore
    entity.query_context = '{"stale": true}'  # type: ignore
    entity.created_on = "original"  # type: ignore
    entity.created_by_fk = 99  # type: ignore
    entity.changed_on = "original"  # type: ignore
    entity.changed_by_fk = 99  # type: ignore
    mock_db.session.query.return_value.get.return_value = entity

    result = VersionDAO.restore_version(model_cls, entity_id=1, version_number=100)

    assert result is entity

    # Verify snapshot fields were applied (excluding audit fields and id)
    assert entity.slice_name == "Old Chart Name"  # type: ignore
    assert entity.viz_type == "bar"  # type: ignore

    # Derived columns listed in __versioned__["exclude"] should be cleared
    assert entity.query_context is None  # type: ignore

    # Audit fields and id should NOT have been overwritten
    for excluded in RESTORE_EXCLUDE_FIELDS:
        assert getattr(entity, excluded) == (
            "original" if excluded in ("created_on", "changed_on") else 99
        )


@patch("superset.daos.version.version_class")
@patch("superset.daos.version.db")
def test_restore_version_returns_none_for_missing_version(
    mock_db: MagicMock,
    mock_version_class: MagicMock,
    app_context: None,
) -> None:
    from superset.daos.version import VersionDAO

    model_cls = MagicMock()
    mock_version_class.return_value = MagicMock()
    mock_db.session.query.return_value.filter.return_value.one_or_none.return_value = (
        None
    )

    result = VersionDAO.restore_version(model_cls, entity_id=1, version_number=999)

    assert result is None


@patch("superset.daos.version.version_class")
@patch("superset.daos.version.db")
def test_restore_version_returns_none_for_missing_entity(
    mock_db: MagicMock,
    mock_version_class: MagicMock,
    app_context: None,
) -> None:
    from superset.daos.version import VersionDAO

    model_cls = MagicMock()
    mock_version_class.return_value = MagicMock()
    mock_db.session.query.return_value.filter.return_value.one_or_none.return_value = (
        MagicMock()
    )
    mock_db.session.query.return_value.get.return_value = None

    result = VersionDAO.restore_version(model_cls, entity_id=1, version_number=100)

    assert result is None

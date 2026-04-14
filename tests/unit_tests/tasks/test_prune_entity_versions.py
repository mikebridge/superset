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
"""Unit tests for prune_entity_versions Celery task."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_prune_deletes_oldest_excess_versions(app_context: None) -> None:
    """Entities over the limit have their oldest versions pruned."""
    from superset.tasks.scheduler import prune_entity_versions

    mock_version_cls = MagicMock()
    mock_version_cls.id = MagicMock()
    mock_version_cls.transaction_id = MagicMock()

    # Entity 1 has 25 versions (5 over limit of 20)
    over_limit_rows = [(1, 25)]
    oldest_txn_ids = [(t,) for t in range(1, 6)]  # txn IDs 1-5

    with (
        patch("sqlalchemy_continuum.version_class") as mock_vc,
        patch("superset.extensions.db") as mock_db,
        patch(
            "superset.tasks.scheduler.current_app",
            new_callable=MagicMock,
        ) as mock_app,
    ):
        mock_app.config.get.side_effect = lambda k, d=None: (
            20 if k == "VERSION_HISTORY_MAX_VERSIONS" else 100
        )
        mock_app.config.__getitem__ = MagicMock(return_value=MagicMock())

        mock_vc.return_value = mock_version_cls

        # Chain: query().group_by().having().limit().all()
        group_query = MagicMock()
        group_query.group_by.return_value = group_query
        group_query.having.return_value = group_query
        group_query.limit.return_value = group_query
        group_query.all.return_value = over_limit_rows

        # Chain: query().filter().order_by().limit().all()
        oldest_query = MagicMock()
        oldest_query.filter.return_value = oldest_query
        oldest_query.order_by.return_value = oldest_query
        oldest_query.limit.return_value = oldest_query
        oldest_query.all.return_value = oldest_txn_ids

        # Chain: query().filter().delete()
        delete_query = MagicMock()
        delete_query.filter.return_value = delete_query

        call_count = [0]

        def query_side_effect(*args, **kwargs):
            call_count[0] += 1
            # First call per model: group_by query (over-limit check)
            # Second: oldest txn query
            # Third: delete query
            idx = (call_count[0] - 1) % 3
            if idx == 0:
                return group_query
            if idx == 1:
                return oldest_query
            return delete_query

        mock_db.session.query.side_effect = query_side_effect

        prune_entity_versions()

        # Verify delete was called (3 models × 1 entity each)
        assert delete_query.delete.call_count == 3


def test_prune_no_op_when_within_limit(app_context: None) -> None:
    """No deletions when all entities are within the version limit."""
    from superset.tasks.scheduler import prune_entity_versions

    mock_version_cls = MagicMock()
    mock_version_cls.id = MagicMock()
    mock_version_cls.transaction_id = MagicMock()

    with (
        patch("sqlalchemy_continuum.version_class") as mock_vc,
        patch("superset.extensions.db") as mock_db,
        patch(
            "superset.tasks.scheduler.current_app",
            new_callable=MagicMock,
        ) as mock_app,
    ):
        mock_app.config.get.side_effect = lambda k, d=None: (
            20 if k == "VERSION_HISTORY_MAX_VERSIONS" else 100
        )
        mock_app.config.__getitem__ = MagicMock(return_value=MagicMock())

        mock_vc.return_value = mock_version_cls

        # No entities over limit
        group_query = MagicMock()
        group_query.group_by.return_value = group_query
        group_query.having.return_value = group_query
        group_query.limit.return_value = group_query
        group_query.all.return_value = []

        mock_db.session.query.return_value = group_query

        prune_entity_versions()

        # Commit called 3 times (once per model) but no deletes
        assert mock_db.session.commit.call_count == 3


def test_prune_reads_config_values(app_context: None) -> None:
    """Task reads VERSION_HISTORY_MAX_VERSIONS from config."""
    from superset.tasks.scheduler import prune_entity_versions

    mock_version_cls = MagicMock()
    mock_version_cls.id = MagicMock()
    mock_version_cls.transaction_id = MagicMock()

    with (
        patch("sqlalchemy_continuum.version_class") as mock_vc,
        patch("superset.extensions.db") as mock_db,
        patch(
            "superset.tasks.scheduler.current_app",
            new_callable=MagicMock,
        ) as mock_app,
    ):
        mock_app.config.get.side_effect = lambda k, d=None: {
            "VERSION_HISTORY_MAX_VERSIONS": 15,
            "VERSION_PRUNE_BATCH_SIZE": 50,
        }.get(k, d)
        mock_app.config.__getitem__ = MagicMock(return_value=MagicMock())

        mock_vc.return_value = mock_version_cls

        group_query = MagicMock()
        group_query.group_by.return_value = group_query
        group_query.having.return_value = group_query
        group_query.limit.return_value = group_query
        group_query.all.return_value = []

        mock_db.session.query.return_value = group_query

        prune_entity_versions()

        # Verify config was read
        mock_app.config.get.assert_any_call("VERSION_HISTORY_MAX_VERSIONS", 20)
        mock_app.config.get.assert_any_call("VERSION_PRUNE_BATCH_SIZE", 100)

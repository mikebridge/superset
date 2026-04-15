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
"""Integration tests for dataset version history (restore)."""

from __future__ import annotations

import uuid

from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn
from superset.daos.version import VersionDAO
from superset.extensions import db
from superset.utils import json
from superset.utils.database import get_example_database
from tests.integration_tests.base_tests import SupersetTestCase
from tests.integration_tests.constants import ADMIN_USERNAME


class TestDatasetVersionHistory(SupersetTestCase):
    """Tests for dataset version listing and restore."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _create_test_dataset(
        self,
        description: str = "",
    ) -> SqlaTable:
        """Create a throwaway SqlaTable with columns and a metric."""
        suffix = uuid.uuid4().hex[:8]
        table = SqlaTable(
            table_name=f"test_version_{suffix}",
            database=get_example_database(),
            schema="",
            description=description,
        )
        table.columns = [
            TableColumn(column_name="col_a", type="VARCHAR(255)"),
            TableColumn(column_name="col_b", type="VARCHAR(255)"),
            TableColumn(column_name="col_c", type="VARCHAR(255)"),
        ]
        table.metrics = [
            SqlMetric(
                metric_name="metric_a",
                expression="COUNT(*)",
            ),
        ]
        db.session.add(table)
        db.session.commit()
        return table

    def _hard_delete_dataset(self, dataset_id: int) -> None:
        """Hard-delete a dataset and its children."""
        if (dataset := db.session.query(SqlaTable).get(dataset_id)) is not None:
            db.session.delete(dataset)
            db.session.commit()

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_list_versions_empty(self) -> None:
        """A freshly created dataset has exactly 1 version (the INSERT)."""
        dataset = self._create_test_dataset()
        try:
            self.login(ADMIN_USERNAME)
            uri = f"/api/v1/dataset/{dataset.id}/versions/"
            rv = self.client.get(uri)
            assert rv.status_code == 200
            data = json.loads(rv.data)
            assert data["count"] == 1
        finally:
            self._hard_delete_dataset(dataset.id)

    def test_list_versions_after_update(self) -> None:
        """Updating a dataset creates a second version."""
        dataset = self._create_test_dataset()
        try:
            # Perform an update to generate a second version
            dataset.description = "updated description"
            db.session.commit()

            self.login(ADMIN_USERNAME)
            uri = f"/api/v1/dataset/{dataset.id}/versions/"
            rv = self.client.get(uri)
            assert rv.status_code == 200
            data = json.loads(rv.data)
            assert data["count"] == 2
        finally:
            self._hard_delete_dataset(dataset.id)

    def test_restore_parent_properties(self) -> None:
        """Restoring to the first version reverts parent description."""
        dataset = self._create_test_dataset(description="original")
        try:
            # Get version 1 transaction id
            versions_before = VersionDAO.list_versions(SqlaTable, dataset.id)
            assert versions_before["count"] == 1
            first_version = versions_before["result"][0]["version_number"]

            # Update description
            dataset.description = "changed"
            db.session.commit()

            # Restore to original version
            restored = VersionDAO.restore_version(SqlaTable, dataset.id, first_version)
            assert restored is not None
            db.session.commit()
            db.session.expire_all()

            # Verify description is back to the original
            refreshed = db.session.query(SqlaTable).get(dataset.id)
            assert refreshed is not None
            assert refreshed.description == "original"
        finally:
            self._hard_delete_dataset(dataset.id)

    def test_restore_nested_children(self) -> None:
        """Restoring reverts dataset columns and metrics to the target version."""
        dataset = self._create_test_dataset(description="original")
        try:
            versions_before = VersionDAO.list_versions(SqlaTable, dataset.id)
            first_version = versions_before["result"][0]["version_number"]

            dataset.description = "changed"
            dataset.columns[0].verbose_name = "Column A"
            dataset.columns.pop()
            dataset.columns.append(
                TableColumn(column_name="col_d", type="VARCHAR(255)")
            )
            dataset.metrics[0].expression = "SUM(1)"
            dataset.metrics.append(
                SqlMetric(metric_name="metric_b", expression="AVG(1)")
            )
            db.session.commit()

            restored = VersionDAO.restore_version(SqlaTable, dataset.id, first_version)
            assert restored is not None
            db.session.commit()
            db.session.expire_all()

            refreshed = db.session.query(SqlaTable).get(dataset.id)
            assert refreshed is not None
            assert refreshed.description == "original"
            assert sorted(column.column_name for column in refreshed.columns) == [
                "col_a",
                "col_b",
                "col_c",
            ]
            assert [
                (metric.metric_name, metric.expression) for metric in refreshed.metrics
            ] == [("metric_a", "COUNT(*)")]
        finally:
            self._hard_delete_dataset(dataset.id)

    def test_restore_creates_new_version(self) -> None:
        """Restoring creates a new version entry (count goes from 2 to 3)."""
        dataset = self._create_test_dataset(description="v1 desc")
        try:
            versions_v1 = VersionDAO.list_versions(SqlaTable, dataset.id)
            first_version = versions_v1["result"][0]["version_number"]

            dataset.description = "v2 desc"
            db.session.commit()

            versions_v2 = VersionDAO.list_versions(SqlaTable, dataset.id)
            assert versions_v2["count"] == 2

            # Restore to first version — this update creates version 3
            restored = VersionDAO.restore_version(SqlaTable, dataset.id, first_version)
            assert restored is not None
            db.session.commit()
            db.session.expire_all()

            versions_v3 = VersionDAO.list_versions(SqlaTable, dataset.id)
            assert versions_v3["count"] == 3
        finally:
            self._hard_delete_dataset(dataset.id)

    def test_restore_nonexistent_version_returns_404(self) -> None:
        """POST restore with a non-existent version number returns 404."""
        dataset = self._create_test_dataset()
        try:
            self.login(ADMIN_USERNAME)
            uri = f"/api/v1/dataset/{dataset.id}/versions/99999/restore"
            rv = self.client.post(uri)
            assert rv.status_code == 404
        finally:
            self._hard_delete_dataset(dataset.id)

    def test_restore_nonexistent_dataset_returns_404(self) -> None:
        """POST restore with a non-existent dataset pk returns 404."""
        self.login(ADMIN_USERNAME)
        uri = "/api/v1/dataset/99999/versions/1/restore"
        rv = self.client.post(uri)
        assert rv.status_code == 404


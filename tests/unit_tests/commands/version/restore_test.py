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


@patch("superset.commands.version.restore.VersionDAO")
@patch("superset.commands.version.restore.security_manager", new_callable=MagicMock)
@patch("superset.commands.version.restore.db")
def test_restore_success(
    mock_db: MagicMock,
    mock_sm: MagicMock,
    mock_dao: MagicMock,
    app_context: None,
) -> None:
    from superset.commands.version.restore import RestoreVersionCommand

    model_cls = MagicMock()
    entity = MagicMock()
    restored_entity = MagicMock()

    mock_db.session.query.return_value.get.return_value = entity
    mock_dao.get_version.return_value = {"version_number": 5, "snapshot": {}}
    mock_dao.restore_version.return_value = restored_entity
    mock_sm.is_admin.return_value = False

    cmd = RestoreVersionCommand(model_cls, entity_id=1, version_number=5)
    result = cmd.run()

    assert result is restored_entity
    mock_sm.raise_for_ownership.assert_called_once_with(entity)


@patch("superset.commands.version.restore.VersionDAO")
@patch("superset.commands.version.restore.security_manager", new_callable=MagicMock)
@patch("superset.commands.version.restore.db")
def test_restore_entity_not_found(
    mock_db: MagicMock,
    mock_sm: MagicMock,
    mock_dao: MagicMock,
    app_context: None,
) -> None:
    from superset.commands.version.exceptions import VersionNotFoundError
    from superset.commands.version.restore import RestoreVersionCommand

    model_cls = MagicMock()
    mock_db.session.query.return_value.get.return_value = None

    cmd = RestoreVersionCommand(model_cls, entity_id=999, version_number=5)
    with pytest.raises(VersionNotFoundError):
        cmd.run()


@patch("superset.commands.version.restore.VersionDAO")
@patch("superset.commands.version.restore.security_manager", new_callable=MagicMock)
@patch("superset.commands.version.restore.db")
def test_restore_version_not_found(
    mock_db: MagicMock,
    mock_sm: MagicMock,
    mock_dao: MagicMock,
    app_context: None,
) -> None:
    from superset.commands.version.exceptions import VersionNotFoundError
    from superset.commands.version.restore import RestoreVersionCommand

    model_cls = MagicMock()
    entity = MagicMock()
    mock_db.session.query.return_value.get.return_value = entity
    mock_dao.get_version.return_value = None

    cmd = RestoreVersionCommand(model_cls, entity_id=1, version_number=999)
    with pytest.raises(VersionNotFoundError):
        cmd.run()


@patch("superset.commands.version.restore.VersionDAO")
@patch("superset.commands.version.restore.security_manager", new_callable=MagicMock)
@patch("superset.commands.version.restore.db")
def test_restore_forbidden_non_owner(
    mock_db: MagicMock,
    mock_sm: MagicMock,
    mock_dao: MagicMock,
    app_context: None,
) -> None:
    from superset.commands.version.exceptions import VersionForbiddenError
    from superset.commands.version.restore import RestoreVersionCommand
    from superset.exceptions import SupersetSecurityException

    model_cls = MagicMock()
    entity = MagicMock()
    mock_db.session.query.return_value.get.return_value = entity
    mock_dao.get_version.return_value = {"version_number": 5, "snapshot": {}}
    mock_sm.is_admin.return_value = False

    def raise_security(*args: object, **kwargs: object) -> None:
        raise SupersetSecurityException(MagicMock())

    mock_sm.raise_for_ownership.side_effect = raise_security

    cmd = RestoreVersionCommand(model_cls, entity_id=1, version_number=5)
    with pytest.raises(VersionForbiddenError):
        cmd.run()


@patch("superset.commands.version.restore.VersionDAO")
@patch("superset.commands.version.restore.security_manager", new_callable=MagicMock)
@patch("superset.commands.version.restore.db")
def test_restore_admin_bypasses_ownership(
    mock_db: MagicMock,
    mock_sm: MagicMock,
    mock_dao: MagicMock,
    app_context: None,
) -> None:
    from superset.commands.version.restore import RestoreVersionCommand

    model_cls = MagicMock()
    entity = MagicMock()
    restored_entity = MagicMock()

    mock_db.session.query.return_value.get.return_value = entity
    mock_dao.get_version.return_value = {"version_number": 5, "snapshot": {}}
    mock_dao.restore_version.return_value = restored_entity
    mock_sm.is_admin.return_value = True

    cmd = RestoreVersionCommand(model_cls, entity_id=1, version_number=5)
    result = cmd.run()

    assert result is restored_entity
    mock_sm.raise_for_ownership.assert_not_called()

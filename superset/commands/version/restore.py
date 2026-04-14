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
"""Command to restore an entity to a previous version."""

from __future__ import annotations

import logging
from functools import partial
from typing import Any

from flask_appbuilder import Model

from superset import db, security_manager
from superset.commands.base import BaseCommand
from superset.commands.version.exceptions import (
    VersionForbiddenError,
    VersionNotFoundError,
    VersionRestoreFailedError,
)
from superset.daos.version import VersionDAO
from superset.exceptions import SupersetSecurityException
from superset.utils.decorators import on_error, transaction

logger = logging.getLogger(__name__)


class RestoreVersionCommand(BaseCommand):
    """Restore an entity to a previous version snapshot."""

    def __init__(
        self,
        model_cls: type[Model],
        entity_id: int,
        version_number: int,
    ) -> None:
        self._model_cls = model_cls
        self._entity_id = entity_id
        self._version_number = version_number
        self._entity: Any | None = None

    @transaction(on_error=partial(on_error, reraise=VersionRestoreFailedError))
    def run(self) -> Model:
        self.validate()
        restored = VersionDAO.restore_version(
            self._model_cls, self._entity_id, self._version_number
        )
        if restored is None:
            raise VersionNotFoundError()
        return restored

    def validate(self) -> None:
        # Check entity exists
        self._entity = db.session.query(self._model_cls).get(self._entity_id)
        if self._entity is None:
            raise VersionNotFoundError()

        # Check version exists
        version = VersionDAO.get_version(
            self._model_cls, self._entity_id, self._version_number
        )
        if version is None:
            raise VersionNotFoundError()

        # Permission check — editors can restore, admins bypass ownership
        if not security_manager.is_admin():
            try:
                security_manager.raise_for_ownership(self._entity)
            except SupersetSecurityException as ex:
                raise VersionForbiddenError() from ex

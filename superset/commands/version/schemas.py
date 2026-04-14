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
"""Marshmallow schemas for version history API responses."""

from marshmallow import fields, Schema


class VersionListItemSchema(Schema):
    """Schema for a single item in a version list response."""

    version_number = fields.Integer()
    changed_on = fields.DateTime()
    changed_by_fk = fields.Integer(allow_none=True)
    operation_type = fields.String()
    is_current = fields.Boolean()


class VersionListResponseSchema(Schema):
    """Schema for the paginated version list response."""

    count = fields.Integer()
    result = fields.List(fields.Nested(VersionListItemSchema))


class VersionDetailResponseSchema(Schema):
    """Schema for a single version detail response."""

    version_number = fields.Integer()
    changed_on = fields.DateTime()
    changed_by_fk = fields.Integer(allow_none=True)
    operation_type = fields.String()
    snapshot = fields.Dict()


class VersionRestoreResponseSchema(Schema):
    """Schema for the restore response."""

    message = fields.String()

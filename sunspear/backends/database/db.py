"""
Copyright 2016 Numan Sachwani <numan856@gmail.com>

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""
from __future__ import absolute_import

import calendar
import copy
import datetime
import uuid

from dateutil.parser import parse
from dateutil import tz

from sqlalchemy import create_engine, sql
from sqlalchemy.pool import QueuePool
from sunspear.activitystreams.models import Activity, Model, Object
from sunspear.backends.base import SUB_ACTIVITY_MAP, BaseBackend
from sunspear.exceptions import (SunspearOperationNotSupportedException,
                                 SunspearValidationException)

from . import schema

DB_OBJ_FIELD_MAPPING = {
    'id': 'id',
    'objectType': 'object_type',
    'displayName': 'display_name',
    'content': 'content',
    'published': 'published',
    'image': 'image',
}


class DatabaseBackend(BaseBackend):

    def __init__(self, db_connection_string=None, verbose=False, poolsize=10,
                 max_overflow=5, **kwargs):
        self._engine = create_engine(db_connection_string, echo=verbose, poolclass=QueuePool,
                                     pool_size=poolsize, max_overflow=max_overflow)

    @property
    def engine(self):
        return self._engine

    def _get_connection(self):
        return self.engine.connect()

    def create_tables(self):
        schema.metadata.create_all(self.engine)

    def drop_tables(self):
        schema.metadata.drop_all(self.engine)

    def clear_all(self):
        self.drop_tables()
        self.create_tables()

    def clear_all_objects(self):
        raise SunspearOperationNotSupportedException()

    def clear_all_activities(self):
        self.engine.execute(schema.tables['activities'].delete())

    def obj_exists(self, obj, **kwargs):
        obj_id = self._extract_id(obj)
        objs_db_table = schema.tables['objects']

        return self.engine.execute(sql.select([sql.exists().where(objs_db_table.c.id == obj_id)]))

    def obj_create(self, obj, **kwargs):
        obj = Object(obj, backend=self)

        obj.validate()
        obj_dict = obj.get_parsed_dict()

        obj_db_schema_dict = self._obj_dict_to_db_schema(obj_dict)

        self.engine.execute(schema.tables['objects'].insert(), [obj_db_schema_dict])

        return obj_dict

    def _obj_dict_to_db_schema(self, obj):
        # we make a copy because we will be mutating the dict.
        # we will map official fields to db fields, and put the rest in `other_data`
        obj_copy = copy.deepcopy(obj)
        schema_dict = {}

        for obj_field, db_schema_field in DB_OBJ_FIELD_MAPPING.items():
            if obj_field in obj_copy:
                data = obj_copy.pop(obj_field)

                # SQLAlchemy requires datetime fields to be datetime instances
                if obj_field in Model._datetime_fields:
                    data = self._get_db_compatiable_date_string(data)

                schema_dict[db_schema_field] = data

        # all standard fields should no longer be part of the dictionary
        if obj_copy:
            schema_dict['other_data'] = obj_copy

        return schema_dict

    def _get_datetime_obj(self, datetime_instance):
        if isinstance(datetime_instance, basestring):
            datetime_instance = parse(datetime_instance)
        utctimezone = tz.tzutc()

        # Assume UTC if we don't have a timezone
        if datetime_instance.tzinfo is None:
            datetime_instance.replace(tzinfo=utctimezone)
        # If we do have a timezone, convert it to UTC
        elif datetime.tzinfo != utctimezone:
            datetime_instance.astimezone(utctimezone)

        return datetime_instance

    def _get_db_compatiable_date_string(self, datetime_instance):
        datetime_instance = self._get_datetime_obj(datetime_instance)

        return datetime_instance.strftime('%Y-%m-%d %H:%M:%S')

    def get_new_id(self):
        """
        Generates a new unique ID. The default implementation uses uuid1 to
        generate a unique ID.

        :return: a new id
        """
        return uuid.uuid1().hex

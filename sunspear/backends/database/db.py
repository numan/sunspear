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
from __future__ import absolute_import, unicode_literals

import calendar
import copy
import datetime
import json
import uuid

import six
from dateutil import tz
from dateutil.parser import parse
from sqlalchemy import create_engine, sql
from sqlalchemy.pool import QueuePool
from sunspear.activitystreams.models import Activity, Model, Object
from sunspear.backends.base import SUB_ACTIVITY_MAP, BaseBackend
from sunspear.exceptions import (SunspearDuplicateEntryException,
                                 SunspearOperationNotSupportedException,
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

DB_ACTIVITY_FIELD_MAPPING = {
    'id': 'id',
    'verb': 'verb',
    'actor': 'actor',
    'object': 'object',
    'target': 'target',
    'author': 'author',
    'generator': 'generator',
    'provider': 'provider',
    'content': 'content',
    'published': 'published',
    'updated': 'updated',
    'icon': 'icon',
}

DICT_FIELDS = Activity._media_fields + Object._media_fields + Activity._object_fields + ['other_data',]


class DatabaseBackend(BaseBackend):

    def __init__(self, db_connection_string=None, verbose=False, poolsize=10,
                 max_overflow=5, **kwargs):
        self._engine = create_engine(db_connection_string, echo=verbose, poolclass=QueuePool,
                                     pool_size=poolsize, max_overflow=max_overflow, convert_unicode=True)

    @property
    def engine(self):
        return self._engine

    @property
    def activities_table(self):
        return schema.tables['activities']

    @property
    def objects_table(self):
        return schema.tables['objects']

    @property
    def likes_table(self):
        return schema.tables['likes']

    @property
    def replies_table(self):
        return schema.tables['replies']

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
        self.engine.execute(self.activities_table.delete())

    def obj_create(self, obj, **kwargs):
        obj_dict = self._get_parsed_and_validated_obj_dict(obj)
        obj_db_schema_dict = self._obj_dict_to_db_schema(obj_dict)

        self.engine.execute(self.objects_table.insert(), [obj_db_schema_dict])

        return obj_dict

    def obj_exists(self, obj, **kwargs):
        obj_id = self._extract_id(obj)
        objs_db_table = self.objects_table

        return self.engine.execute(sql.select([sql.exists().where(objs_db_table.c.id == obj_id)])).scalar()

    def obj_update(self, obj, **kwargs):
        obj_dict = self._get_parsed_and_validated_obj_dict(obj)
        obj_id = self._extract_id(obj_dict)
        obj_db_schema_dict = self._obj_dict_to_db_schema(obj_dict)

        self.engine.execute(
            self.objects_table.update().where(self.objects_table.c.id == obj_id).values(**obj_db_schema_dict))

    def obj_get(self, obj, **kwargs):
        """
        Given a list of object ids, returns a list of objects
        """
        if not obj:
            return obj

        obj_ids = [self._extract_id(o) for o in obj]
        s = self._get_select_multiple_objects_query(obj_ids)

        results = self.engine.execute(s).fetchall()
        results = map(self._db_schema_to_obj_dict, results)

        return results

    def obj_delete(self, obj, **kwargs):
        obj_id = self._extract_id(obj)

        stmt = self.objects_table.delete().where(self.objects_table.c.id == obj_id)
        self.engine.execute(stmt)

    def activity_exists(self, activity, **kwargs):
        activity_id = self._extract_id(activity)
        activities_db_table = self.activities_table

        return self.engine.execute(sql.select([sql.exists().where(activities_db_table.c.id == activity_id)])).scalar()

    def activity_create(self, activity, **kwargs):
        """
        Creates an activity. This assumes the activity is already dehydrated (ie has refrences
        to the objects and not the actual objects itself)
        """
        activity = Activity(activity, backend=self)

        activity.validate()
        activity_dict = activity.get_parsed_dict()

        activity_db_schema_dict = self._activity_dict_to_db_schema(activity_dict)

        self.engine.execute(self.activities_table.insert(), [activity_db_schema_dict])

        return self.get_activity(activity_dict)

    def _extract_activity_obj_key(self, obj_or_value):
        activity_obj = None

        if isinstance(obj_or_value, dict):
            activity_obj_id = self._extract_id(obj_or_value)
            activity_obj = obj_or_value
        else:
            activity_obj_id = obj_or_value

        return activity_obj, activity_obj_id

    def create_activity(self, activity, **kwargs):
        activity_id = self._resolve_activity_id(activity, **kwargs)
        activity['id'] = activity_id

        activity_copy = copy.copy(activity)

        activity_objs = {}
        ids_of_objs_with_no_dict = []

        audience_targeting_fields = Activity._direct_audience_targeting_fields + Activity._indirect_audience_targeting_fields

        for key, value in activity_copy.items():
            if key in Activity._object_fields:
                activity_obj, activity_obj_id = self._extract_activity_obj_key(value)
                if activity_obj:
                    activity_objs[activity_obj_id] = activity_obj
                    activity[key] = activity_obj_id
                else:
                    ids_of_objs_with_no_dict.append(activity_obj_id)

            if key in audience_targeting_fields and value:
                activity_audience_targeting_objs = []
                for activity_obj_or_value in value:
                    activity_obj, activity_obj_id = self._extract_activity_obj_key(activity_obj_or_value)
                    if activity_obj:
                        activity_objs[activity_obj_id] = activity_obj
                        activity_audience_targeting_objs.append(activity_obj_id)
                    else:
                        ids_of_objs_with_no_dict.append(activity_obj_id)
                activity[key] = activity_audience_targeting_objs

        # For all of the objects in the activity, find out which ones actually already have existing
        # objects in the database
        obj_ids = self._flatten([ids_of_objs_with_no_dict, activity_objs.keys()])

        s = self._get_select_multiple_objects_query(obj_ids)
        results = self.engine.execute(s).fetchall()
        results = self._flatten(results)

        objs_need_to_be_inserted = []
        objs_need_to_be_updated = []

        for obj_id, obj in activity_objs.items():
            parsed_validated_schema_dict = self._get_parsed_and_validated_obj_dict(obj)
            parsed_validated_schema_dict = self._obj_dict_to_db_schema(parsed_validated_schema_dict)
            if obj_id not in results:
                objs_need_to_be_inserted.append(parsed_validated_schema_dict)
            else:
                objs_need_to_be_updated.append(parsed_validated_schema_dict)

        # Upsert all objects for the activity
        with self.engine.begin() as connection:
            if objs_need_to_be_inserted:
                connection.execute(self.objects_table.insert(), objs_need_to_be_inserted)
            for obj in objs_need_to_be_updated:
                connection.execute(
                    self.objects_table.update().where(self.objects_table.c.id == self._extract_id(obj)).values(**obj))

        return_val = self.activity_create(activity, **kwargs)

        return return_val

    def activity_get(self, activity_ids, **kwargs):
        activity_ids = self._listify(activity_ids)
        activities = self._get_raw_activities(activity_ids, **kwargs)
        activities = self.hydrate_activities(activities)

        return activities

    def sub_activity_create(self, activity, actor, content, extra={}, sub_activity_verb="", published=None, **kwargs):
        object_type = kwargs.get('object_type', sub_activity_verb)
        sub_activity_model = self.get_sub_activity_model(sub_activity_verb)
        sub_activity_attribute = self.get_sub_activity_attribute(sub_activity_verb)

        activity_id = self._extract_id(activity)
        raw_activity = self._get_raw_activities([activity_id])[0]
        activity_model = Activity(raw_activity, backend=self)

        sub_activity_table = getattr(self, '{}_table'.format(sub_activity_attribute))

        sub_activity, original_activity = activity_model\
            .get_parsed_sub_activity_dict(
                actor=actor, content=content, verb=sub_activity_verb,
                object_type=object_type, collection=sub_activity_attribute,
                activity_class=sub_activity_model, published=published, extra=extra)

        sub_activity = self.create_activity(sub_activity)[0]
        sub_activity_db_schema = self._convert_sub_activity_to_db_schema(sub_activity, original_activity)
        self.engine.execute(sub_activity_table.insert(), [sub_activity_db_schema])

        return sub_activity, original_activity

    def hydrate_activities(self, activities):
        """
        Takes a raw list of activities returned from riak and replace keys with contain ids for riak objects with actual riak object
        TODO: This can probably be refactored out of the riak backend once everything like
        sub activities and shared with fields are implemented
        """
        # collect a list of unique object ids. We only iterate through the fields that we know
        # for sure are objects. User is responsible for hydrating all other fields.
        object_ids = set()
        for activity in activities:
            object_ids.update(self._extract_object_keys(activity))

        # Get the objects for the ids we have collected
        objects = self.get_obj(object_ids)
        objects_dict = dict(((obj["id"], obj,) for obj in objects))

        # replace the object ids with the hydrated objects
        for activity in activities:
            activity = self._dehydrate_object_keys(activity, objects_dict)

        return activities

    def get_new_id(self):
        """
        Generates a new unique ID. The default implementation uses uuid1 to
        generate a unique ID.

        :return: a new id
        """
        return uuid.uuid1().hex

    def _get_raw_activities(self, activity_ids, **kwargs):
        activity_ids = map(self._extract_id, activity_ids)
        if not activity_ids:
            return []

        s = self._get_select_multiple_activities_query(activity_ids)
        activities = self.engine.execute(s).fetchall()
        activities = [self._db_schema_to_activity_dict(activity) for activity in activities]

        return activities

    def _convert_sub_activity_to_db_schema(self, sub_activity, activity):
        # Find all the fields in the sub activity that aren't part of the standard activity object
        converted_subactivity = self._activity_dict_to_db_schema(sub_activity)
        other_data = converted_subactivity.get('other_data')
        sub_activity = {
            'id': sub_activity['id'],
            'in_reply_to': activity['id'],
            'actor': sub_activity['actor']['id'],
            'published': self._get_db_compatiable_date_string(sub_activity['published']),
            'updated': self._get_db_compatiable_date_string(sub_activity['published']),
            'content': sub_activity['object']['content'],
        }
        if other_data:
            sub_activity['other_data'] = other_data
        return sub_activity

    def _convert_to_db_schema(self, obj, field_mapping):
        # we make a copy because we will be mutating the dict.
        # we will map official fields to db fields, and put the rest in `other_data`
        obj_copy = copy.deepcopy(obj)
        schema_dict = {}

        for obj_field, db_schema_field in field_mapping.items():
            if obj_field in obj_copy:
                data = obj_copy.pop(obj_field)

                # SQLAlchemy requires datetime fields to be datetime strings
                if obj_field in Model._datetime_fields:
                    data = self._get_datetime_obj(data)
                    data = self._get_db_compatiable_date_string(data)

                schema_dict[db_schema_field] = data

        # all standard fields should no longer be part of the dictionary
        if obj_copy:
            schema_dict['other_data'] = obj_copy

        return schema_dict

    def _need_to_parse_json(self, schema_field_name, data):
        if schema_field_name in DICT_FIELDS and isinstance(data, six.string_types) and data:
            # TODO: This seems hacky. Is there a better way to do this?
            if '{' in data or '[' in data:
                return True
        return False

    def _convert_to_activity_stream_schema(self, schema_dict, field_mapping):
        # we make a copy because we will be mutating the dict.
        # we will map official fields to db fields, and put the rest in `other_data`
        obj_dict = {}

        for obj_field, db_schema_field in field_mapping.items():
            if db_schema_field in schema_dict:
                data = schema_dict[db_schema_field]
                if self._need_to_parse_json(db_schema_field, data):
                    data = json.loads(data)

                # SQLAlchemy requires datetime fields to be datetime instances
                if obj_field in Model._datetime_fields:
                    data = self._get_datetime_obj(data)
                    data = '{}Z'.format(data.isoformat())

            obj_dict[obj_field] = data

        if 'other_data' in schema_dict and schema_dict['other_data'] is not None:
            other_data = schema_dict['other_data']
            if self._need_to_parse_json('other_data', other_data):
                other_data = json.loads(other_data)
            obj_dict.update(other_data)

        return obj_dict

    def _obj_dict_to_db_schema(self, obj):
        return self._convert_to_db_schema(obj, DB_OBJ_FIELD_MAPPING)

    def _activity_dict_to_db_schema(self, activity):
        return self._convert_to_db_schema(activity, DB_ACTIVITY_FIELD_MAPPING)

    def _db_schema_to_obj_dict(self, obj):
        return self._convert_to_activity_stream_schema(obj, DB_OBJ_FIELD_MAPPING)

    def _db_schema_to_activity_dict(self, activity):
        return self._convert_to_activity_stream_schema(activity, DB_ACTIVITY_FIELD_MAPPING)

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

    def _flatten(self, list_of_lists):
        return [item for sublist in list_of_lists for item in sublist]

    def _get_parsed_and_validated_obj_dict(self, obj):
        obj = Object(obj, backend=self)

        obj.validate()
        obj_dict = obj.get_parsed_dict()

        return obj_dict

    def _get_select_multiple_objects_query(self, obj_ids):
        s = sql.select(['*']).where(self.objects_table.c.id.in_(obj_ids))
        return s

    def _get_select_multiple_activities_query(self, activity_ids):
        s = sql.select(['*']).where(self.activities_table.c.id.in_(activity_ids))
        return s

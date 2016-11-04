from __future__ import absolute_import

from nose.tools import ok_, eq_, raises
from sqlalchemy import create_engine, sql

from sunspear.backends.database.db import *
from sunspear.exceptions import SunspearOperationNotSupportedException
from sunspear.backends.database import schema
from sunspear.activitystreams.models import Model

import copy
import os
import datetime


DB_CONNECTION_STRING = os.environ.get('DB_CONNECTION_STRING', 'mysql://root:@localhost')
DB_TYPE = os.environ.get('DB_TYPE', 'mysql')
DB_USER = os.environ.get('DB_USER', 'root')
DB_PASS = os.environ.get('DB_PASSWORD', '')
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = int(os.environ.get('DB_PORT', 3306))

DB_NAME = os.environ.get('DB_NAME', 'sunspear_test_database')


class TestDatabaseBackend(object):
    @classmethod
    def setUpClass(cls):
        database_name = DB_NAME
        cls._setup_db(database_name)
        database_connection_string = cls.get_connection_string_with_database(database_name)

        cls._backend = DatabaseBackend(db_connection_string=database_connection_string, verbose=False)
        cls._backend.drop_tables()
        cls._engine = cls._backend.engine
        cls.now = datetime.datetime.utcnow()

    @classmethod
    def tearDownClass(cls):
        database_name = DB_NAME
        cls._cleanup_db(database_name)

    @classmethod
    def get_connection_string(cls):
        return '{0}://{1}:{2}@{3}:{4}'.format(DB_TYPE, DB_USER, DB_PASS, DB_HOST, DB_PORT)

    @classmethod
    def get_connection_string_with_database(cls, database_name):
        return '{0}/{1}'.format(cls.get_connection_string(), database_name)

    @classmethod
    def _cleanup_db(cls, db_name):
        connection_string = cls.get_connection_string()

        # This engine just used to query for list of databases
        engine = create_engine(connection_string)

        engine.execute("DROP DATABASE {};".format(db_name))

    @classmethod
    def _setup_db(cls, db_name):
        connection_string = cls.get_connection_string()

        # This engine just used to query for list of databases
        engine = create_engine(connection_string)
        conn = engine.connect()

        # Query for existing databases
        existing_databases = conn.execute("SHOW DATABASES;")
        # Results are a list of single item tuples, so unpack each tuple
        existing_databases = [d[0] for d in existing_databases]

        # Create database if not exists
        if db_name not in existing_databases:
            conn.execute("CREATE DATABASE {0}".format(db_name))
            print("Created database {0}".format(db_name))

        conn.close()

    def setUp(self):
        self._backend.create_tables()
        self._setup_objs()
        self._setup_activities()

    def tearDown(self):
        self._backend.drop_tables()

    def _setup_objs(self):
        self.test_objs = [{
            'id': 'AxsdSG244BfduiIZ',
            'objectType': u'use\u0403',
            'displayName': u'\u019duman S',
            'content': u'Foo bar!\u03ee',
            'published': self._datetime_to_string(self.now),
            'image': {
                'url': 'https://www.google.com/cool_image.png',
                'displayName': u'Cool \u0268mage',
                'width': '500px',
                'height': '500px'
            },
            'foo': 'bar',
            'baz': u'go\u0298',
            'zoo': {'zee': 12, 'tim': {'zde': u'\u0268\u0298'}}
        }]

        self.test_obj = self.test_objs[0]

    def _setup_activities(self):
        self.test_activities = [{
            'id': 'WvgYP43bfg64fsdDHt3',
            'verb': 'join',
            'actor': 'user:1',
            'object': 'recognition:1',
            'target': 'badge:2',
            'author': 'user:435',
            'generator': 'mobile:phone:android',
            'provider': 'mobile:phone:android',
            'content': 'foo baz',
            'published': self.now,
            'updated': self.now,
            'icon': {
                'url': 'https://www.google.com/cool_image.png',
                'displayName': u'Cool \u0268mage',
                'width': '500px',
                'height': '500px'
            },
            'foo': 'bar',
            'baz': u'go\u0298',
            'zoo': {'zee': 12, 'tim': {'zde': u'\u0268\u0298'}},
        }]

        self.test_objs_for_activities = [{
            'id': 'user:1',
            'objectType': u'use\u0403',
            'displayName': u'\u019duman S1',
            'content': u'Foo bar!\u03ee',
            'published': self._datetime_to_string(self.now),
            'image': {
                'url': 'https://www.google.com/cool_image.png',
                'displayName': u'Cool \u0268mage',
                'width': '500px',
                'height': '500px'
            },
            'foo': 'bar',
            'baz': u'go\u0298',
            'zoo': {'zee': 12, 'tim': {'zde': u'\u0268\u0298'}}
        }, {
            'id': 'recognition:1',
            'objectType': u'use\u0403',
            'displayName': u'\u019dRecognitionBadge',
            'content': u'Good Work on everything\u03ee',
            'published': self._datetime_to_string(self.now),
            'image': {
                'url': 'https://www.google.com/cool_image.png',
                'displayName': u'Cool \u0268mage',
                'width': '500px',
                'height': '500px'
            },
            'foo': 'bar',
            'baz': u'go\u0298',
            'zoo': {'zee': 12, 'tim': {'zde': u'\u0268\u0298'}}
        }, {
            'id': 'badge:2',
            'objectType': u'use\u0403',
            'displayName': u'\u019dAwesomeness',
            'content': u'Just for being awesome\u03ee',
            'published': self._datetime_to_string(self.now),
            'image': {
                'url': 'https://www.google.com/cool_image.png',
                'displayName': u'Cool \u0268mage',
                'width': '500px',
                'height': '500px'
            },
            'foo': 'bar',
            'baz': u'go\u0298',
            'zoo': {'zee': 12, 'tim': {'zde': u'\u0268\u0298'}}
        }, {
            'id': 'user:435',
            'objectType': u'use\u0403',
            'displayName': u'\u019duman S435',
            'content': u'Foo bar!\u03ee',
            'published': self._datetime_to_string(self.now),
            'image': {
                'url': 'https://www.google.com/cool_image.png',
                'displayName': u'Cool \u0268mage',
                'width': '500px',
                'height': '500px'
            },
            'foo': 'bar',
            'baz': u'go\u0298',
            'zoo': {'zee': 12, 'tim': {'zde': u'\u0268\u0298'}}
        }, {
            'id': 'mobile:phone:android',
            'objectType': u'androidmobilephone\u0403',
            'displayName': u'\u019dobile Phone Android',
            'content': u'Foo bar!\u03ee',
            'published': self._datetime_to_string(self.now),
            'image': {
                'url': 'https://www.google.com/cool_image.png',
                'displayName': u'Cool \u0268mage',
                'width': '500px',
                'height': '500px'
            },
            'foo': 'bar',
            'baz': u'go\u0298',
            'zoo': {'zee': 12, 'tim': {'zde': u'\u0268\u0298'}}
        }]

        self.test_activity = self.test_activities[0]

        self.hydrated_test_activity = self._build_hydrated_activity(self.test_activity, self.test_objs_for_activities)

    def _build_hydrated_activity(self, dehydrated_activity, objs):
        hydrated_activity = copy.deepcopy(dehydrated_activity)
        for obj_field in Model._object_fields:
            if obj_field in hydrated_activity:
                obj_id = hydrated_activity[obj_field]
                obj = [obj for obj in objs if obj['id'] == obj_id][0]
                hydrated_activity[obj_field] = obj

        return hydrated_activity

    @raises(SunspearOperationNotSupportedException)
    def test_sample_test(self):
        self._backend.clear_all_objects()

    def test__obj_dict_to_db_schema(self):
        obj_dict = self.test_obj
        obj_dict_copy = copy.deepcopy(obj_dict)

        db_schema_dict = self._backend._obj_dict_to_db_schema(obj_dict)

        # Confirm the original dict was not modified
        eq_(obj_dict, obj_dict_copy)

        for obj_field, db_schema_field in DB_OBJ_FIELD_MAPPING.items():
            data = obj_dict[obj_field]
            if obj_field in Model._datetime_fields:
                data = self._backend._get_db_compatiable_date_string(data)

            eq_(data, db_schema_dict[db_schema_field])
            # Remove all "supported" fields. What we have left should be what went to `other_data`
            obj_dict_copy.pop(obj_field)

        # Everything was placed in other_data
        eq_(obj_dict_copy, db_schema_dict['other_data'])

    def test_obj_create(self):
        self._backend.obj_create(self.test_obj)

        obj_exists = self._engine.execute(sql.select([sql.exists().where(schema.tables['objects'].c.id == self.test_obj['id'])]))

        ok_(obj_exists)

    def test_obj_exists(self):
        db_obj = self._backend._obj_dict_to_db_schema(self.test_obj)

        objects_table = schema.tables['objects']

        self._engine.execute(objects_table.insert(), [
            db_obj
        ])

        ok_(self._backend.obj_exists(self.test_obj))

    def test_activity_exists(self):
        db_activity = self._backend._activity_dict_to_db_schema(self.test_activity)
        db_objs = map(self._backend._obj_dict_to_db_schema, self.test_objs_for_activities)

        activities_table = schema.tables['activities']
        objects_table = schema.tables['objects']

        self._engine.execute(objects_table.insert(), db_objs)

        self._engine.execute(activities_table.insert(), [
            db_activity
        ])

        ok_(self._backend.activity_exists(self.test_activity))

    def test_activity_create(self):
        db_objs = map(self._backend._obj_dict_to_db_schema, self.test_objs_for_activities)

        objects_table = schema.tables['objects']
        self._engine.execute(objects_table.insert(), db_objs)

        self._backend.activity_create(self.test_activity)

        ok_(self._backend.activity_exists(self.test_activity))

    def _datetime_to_db_compatibal_str(self, datetime_instance):
        return datetime_instance.strftime('%Y-%m-%d %H:%M:%S')

    def _datetime_to_string(self, datetime_instance):
        return datetime_instance.strftime('%Y-%m-%dT%H:%M:%S') + "Z"

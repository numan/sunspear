from __future__ import absolute_import, division, print_function

import datetime

import six
from mock import MagicMock
from nose.tools import eq_, ok_, raises

from sunspear.activitystreams.models import Activity, MediaLink, Model, Object
from sunspear.exceptions import SunspearValidationException


class TestActivityModel(object):
    def test_initialize(self):
        act = Activity({"id": 5, "verb": "post", \
            "actor": {"objectType": "actor", "id": 1232, "published": "today"}, \
            "target": {"objectType": "target", "id": 4325, "published": "today"}, \
            "object": {"objectType": "something", "id": 4353, "published": "today"},
            "icon": {'url': "http://example.org/something"}}, backend=MagicMock())

        act_dict = act._dict
        ok_(isinstance(act_dict['actor'], dict))
        ok_(isinstance(act_dict['target'], dict))
        ok_(isinstance(act_dict['object'], dict))

        ok_(isinstance(act_dict['icon'], dict))

        ok_('replies' in act_dict)
        ok_('likes' in act_dict)

        eq_(act_dict['id'], str(5))

    def test_initialize_with_audience_targeting(self):
        act = Activity({
                'to': [{'objectType': 'user1', 'id': 'user:id:1'}, {'objectType': 'user2', 'id': 'user:id:2'}],
                'bto': [{'objectType': 'user3', 'id': 'user:id:3'}, {'objectType': 'user4', 'id': 'user:id:4'}, {'objectType': 'user5', 'id': 'user:id:5'}],
                'cc': [{'objectType': 'user6', 'id': 'user:id:6'}],
                'bcc': [],
            }, backend=MagicMock())

        act_dict = act._dict

        eq_(len(act_dict['to']), 2)
        eq_(len(act_dict['bto']), 3)
        eq_(len(act_dict['cc']), 1)
        eq_(len(act_dict['bcc']), 0)

    def test_parse_data(self):

        act = Activity({"id": 5, "verb": "post", \
            "actor": {"objectType": "actor", "id": 1232, "published": '2012-07-05T12:00:00Z'}, \
            "target": {"objectType": "target", "id": 4325, "published": '2012-07-05T12:00:00Z'}, \
            "object": {"objectType": "something", "id": 4353, "published": '2012-07-05T12:00:00Z'},
            "icon": {'url': "http://example.org/something"}}, backend=MagicMock())

        act_dict = act.parse_data(act._dict)

        eq_({
            'target': {'objectType': 'target', 'id': 4325, 'published': '2012-07-05T12:00:00Z'},
            'object': {'objectType': 'something', 'id': 4353, 'published': '2012-07-05T12:00:00Z'},
            'actor': {'objectType': 'actor', 'id': 1232, 'published': '2012-07-05T12:00:00Z'},
            'verb': 'post',
            'id': '5',
            'icon': {'url': 'http://example.org/something'}
        }, act_dict)

    def test_parse_data_with_audience_targeting(self):
        act = Activity({
                'id': '4213',
                'to': [{'objectType': 'user1', 'id': 'user:id:1', 'published': '2012-07-05T12:00:00Z'}, {'objectType': 'user2', 'id': 'user:id:2', 'published': '2012-07-05T12:00:00Z'}],
                'bto': [{'objectType': 'user3', 'id': 'user:id:3', 'published': '2012-07-05T12:00:00Z'}, {'objectType': 'user4', 'id': 'user:id:4', 'published': '2012-07-05T12:00:00Z'}, {'objectType': 'user5', 'id': 'user:id:5', 'published': '2012-07-05T12:00:00Z'}],
                'cc': [{'objectType': 'user6', 'id': 'user:id:6', 'published': '2012-07-05T12:00:00Z'}],
                'bcc': [],
            }, backend=MagicMock())

        act_dict = act.parse_data(act._dict)
        eq_({
            'cc': [{'published': '2012-07-05T12:00:00Z', 'id': 'user:id:6', 'objectType': 'user6'}],
            'bcc': [],
            'to': [{'published': '2012-07-05T12:00:00Z', 'id': 'user:id:1', 'objectType': 'user1'}, {'published': '2012-07-05T12:00:00Z', 'id': 'user:id:2', 'objectType': 'user2'}],
            'bto': [{'published': '2012-07-05T12:00:00Z', 'id': 'user:id:3', 'objectType': 'user3'}, {'published': '2012-07-05T12:00:00Z', 'id': 'user:id:4', 'objectType': 'user4'}, {'published': '2012-07-05T12:00:00Z', 'id': 'user:id:5', 'objectType': 'user5'}], 'id': '4213'
        }, act_dict)


class TestActivity(object):

    def test_required_fields_all_there(self):
        Activity({"id": 5, "verb": "post", \
            "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
            "object": {"objectType": "something", "id": 4353, "published": "today"}}, backend=MagicMock()).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_actor(self):
        Activity({"id": 5, "title": "Stream Item", "verb": "post", \
            "object": {"objectType": "something", "id": 4353, "published": "today"}}, backend=MagicMock()).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_object(self):
        Activity({"id": 5, "title": "Stream Item", "verb": "post", \
            "actor": {"objectType": "something", "id": 1232, "published": "today"}}, backend=MagicMock()).validate()

    def test_required_fields_no_id(self):
        act = Activity({"title": "Stream Item", "verb": "post", \
            "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
            "object": {"objectType": "something", "id": 4353, "published": "today"}}, backend=MagicMock())
        act.validate()
        ok_(act.get_dict()["id"])

    @raises(SunspearValidationException)
    def test_required_fields_no_verb(self):
        Activity({"id": 5, "title": "Stream Item", \
            "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
            "object": {"objectType": "something", "id": 4353, "published": "today"}}, backend=MagicMock()).validate()

    def test_disallowed_field_published(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "published": "today", \
                "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
                "object": {"objectType": "something", "id": 4353, "published": "today"}}, backend=MagicMock()).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: published")

    def test_disallowed_field_updated(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "updated": "today", \
                "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
                "object": {"objectType": "something", "id": 4353, "published": "today"}}, backend=MagicMock()).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: updated")

    def test_disallowed_field_to(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "to": [], \
                "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
                "object": {"objectType": "something", "id": 4353, "published": "today"}}, backend=MagicMock()).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: to")

    def test_disallowed_field_bto(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "bto": [], \
                "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
                "object": {"objectType": "something", "id": 4353, "published": "today"}}, backend=MagicMock()).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: bto")

    def test_disallowed_field_cc(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "cc": [], \
                "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
                "object": {"objectType": "something", "id": 4353, "published": "today"}}, backend=MagicMock()).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: cc")

    def test_disallowed_field_bcc(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "bcc": [], \
                "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
                "object": {"objectType": "something", "id": 4353, "published": "today"}}, backend=MagicMock()).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: bcc")


class TestMediaLink(object):
    def test_required_fields_all_there(self):
        MediaLink({"url": "http://cdn.fake.com/static/img/clown.png"}, backend=MagicMock()).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_url(self):
        MediaLink({}, backend=MagicMock()).validate()


class TestObject(object):
    def test_required_fields_all_there(self):
        Object({"objectType": "something", "id": 1232, "published": "today"}, backend=MagicMock()).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_object_type(self):
        Object({"id": 1232, "published": "today"}, backend=MagicMock()).validate()

    def test_required_fields_no_id_generates_id(self):
        obj = Object({"objectType": "something", "published": "today"}, backend=MagicMock())
        obj.validate()
        ok_(obj._dict['id'])

    @raises(SunspearValidationException)
    def test_required_fields_published(self):
        Object({"objectType": "something", "id": 1232}, backend=MagicMock()).validate()


class TestModelMethods(object):
    def test_parse_data_published_date(self):
        d = datetime.datetime.now()

        obj = Model({"displayName": "something", "id": 1232, \
            "published": d, "updated": d}, backend=MagicMock())
        parsed_dict = obj.parse_data(obj.get_dict())
        eq_(parsed_dict["published"], d.strftime('%Y-%m-%dT%H:%M:%S') + "Z")

    def test_parse_data_updated_date(self):
        d = datetime.datetime.now()

        obj = Model({"displayName": "something", "id": 1232, \
            "published": d, "updated": d}, backend=MagicMock())
        parsed_dict = obj.parse_data(obj.get_dict())
        eq_(parsed_dict["updated"], d.strftime('%Y-%m-%dT%H:%M:%S') + "Z")

    def test__set_defaults(self):
        obj = Model({}, backend=MagicMock())
        obj_dict = obj._set_defaults({'id': 12})

        ok_(isinstance(obj_dict.get('id'), six.string_types))

    def test__set_defaults_no_id_does_not_fail(self):
        obj = Model({}, backend=MagicMock())
        data = {'foo': 'bar', 'baz': 'bee'}
        obj_dict = obj._set_defaults(data)

        eq_(obj_dict, data)

    def test__parse_date(self):
        obj = Model({}, backend=MagicMock())
        d = datetime.datetime.utcnow()
        eq_(obj._parse_date(d), d.strftime('%Y-%m-%dT%H:%M:%S') + "Z")

        #badly formatted string date
        ok_(isinstance(obj._parse_date(date="qwerty"), six.string_types))

        #no date passed
        ok_(isinstance(obj._parse_date(date=None), six.string_types))

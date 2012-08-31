from __future__ import absolute_import

from nose.tools import ok_, eq_, raises, set_trace

from mock import MagicMock

from sunspear.activitystreams.models import Activity, MediaLink, Object
from sunspear.exceptions import SunspearValidationException

import datetime


class TestActivityModel(object):
    def test_initialize(self):
        act = Activity({"id": 5, "verb": "post", \
            "actor": {"objectType": "actor", "id": 1232, "published": "today"}, \
            "target": {"objectType": "target", "id": 4325, "published": "today"}, \
            "object": {"objectType": "something", "id": 4353, "published": "today"}}, objects_bucket=MagicMock())

        act_dict = act._dict
        ok_(isinstance(act_dict['actor'], Object))
        ok_(isinstance(act_dict['target'], Object))
        ok_(isinstance(act_dict['object'], Object))


class TestActivity(object):

    def test_required_fields_all_there(self):
        Activity({"id": 5, "verb": "post", \
            "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
            "object": {"objectType": "something", "id": 4353, "published": "today"}}, objects_bucket=MagicMock()).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_actor(self):
        Activity({"id": 5, "title": "Stream Item", "verb": "post", \
            "object": {"objectType": "something", "id": 4353, "published": "today"}}, objects_bucket=MagicMock()).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_object(self):
        Activity({"id": 5, "title": "Stream Item", "verb": "post", \
            "actor": {"objectType": "something", "id": 1232, "published": "today"}}, objects_bucket=MagicMock()).validate()

    def test_required_fields_no_id(self):
        act = Activity({"title": "Stream Item", "verb": "post", \
            "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
            "object": {"objectType": "something", "id": 4353, "published": "today"}}, objects_bucket=MagicMock())
        act.validate()
        ok_(act.get_dict()["id"])

    @raises(SunspearValidationException)
    def test_required_fields_no_verb(self):
        Activity({"id": 5, "title": "Stream Item", \
            "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
            "object": {"objectType": "something", "id": 4353, "published": "today"}}, objects_bucket=MagicMock()).validate()

    def test_disallowed_field_published(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "published": "today", \
                "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
                "object": {"objectType": "something", "id": 4353, "published": "today"}}, objects_bucket=MagicMock()).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: published")

    def test_disallowed_field_updated(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "updated": "today", \
                "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
                "object": {"objectType": "something", "id": 4353, "published": "today"}}, objects_bucket=MagicMock()).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: updated")

    def test_disallowed_field_to(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "to": [], \
                "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
                "object": {"objectType": "something", "id": 4353, "published": "today"}}, objects_bucket=MagicMock()).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: to")

    def test_disallowed_field_bto(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "bto": [], \
                "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
                "object": {"objectType": "something", "id": 4353, "published": "today"}}, objects_bucket=MagicMock()).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: bto")

    def test_disallowed_field_cc(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "cc": [], \
                "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
                "object": {"objectType": "something", "id": 4353, "published": "today"}}, objects_bucket=MagicMock()).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: cc")

    def test_disallowed_field_bcc(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "bcc": [], \
                "actor": {"objectType": "something", "id": 1232, "published": "today"}, \
                "object": {"objectType": "something", "id": 4353, "published": "today"}}, objects_bucket=MagicMock()).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: bcc")


class TestMediaLink(object):
    def test_required_fields_all_there(self):
        MediaLink({"url": "http://cdn.fake.com/static/img/clown.png"}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_url(self):
        MediaLink({}).validate()


class TestObject(object):
    def test_required_fields_all_there(self):
        Object({"objectType": "something", "id": 1232, "published": "today"}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_object_type(self):
        Object({"id": 1232, "published": "today"}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_id(self):
        Object({"objectType": "something", "published": "today"}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_published(self):
        Object({"objectType": "something", "id": 1232}).validate()


class TestModelMethods(object):

    def test_parse_data_published_date(self):
        d = datetime.datetime.now()

        obj = Object({"displayName": "something", "id": 1232, \
            "published": d, "updated": d})
        parsed_dict = obj.parse_data(obj.get_dict())
        eq_(parsed_dict["published"], d.strftime('%Y-%m-%dT%H:%M:%S') + "Z")

    def test_parse_data_updated_date(self):
        d = datetime.datetime.now()

        obj = Object({"displayName": "something", "id": 1232, \
            "published": d, "updated": d})
        parsed_dict = obj.parse_data(obj.get_dict())
        eq_(parsed_dict["updated"], d.strftime('%Y-%m-%dT%H:%M:%S') + "Z")

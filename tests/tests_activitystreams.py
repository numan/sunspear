from __future__ import absolute_import

from nose.tools import ok_, eq_, raises, set_trace, assert_raises

from sunspear.activitystreams.models import Activity, MediaLink, Object
from sunspear.exceptions import SunspearValidationException

import datetime


class TestActivity(object):
    def test_required_fields_all_there(self):
        Activity({"id": 5, "title": "Stream Item", "verb": "post", \
            "actor": {"displayName": "something", "id": 1232, "published": "today"}, \
            "object": {"displayName": "something", "id": 4353, "published": "today"}}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_actor(self):
        Activity({"id": 5, "title": "Stream Item", "verb": "post", \
            "object": {"displayName": "something", "id": 4353, "published": "today"}}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_object(self):
        Activity({"id": 5, "title": "Stream Item", "verb": "post", \
            "actor": {"displayName": "something", "id": 1232, "published": "today"}}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_id(self):
        Activity({"published": "today", "title": "Stream Item", "verb": "post", \
            "actor": {"displayName": "something", "id": 1232, "published": "today"}, \
            "object": {"displayName": "something", "id": 4353, "published": "today"}}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_title(self):
        Activity({"id": 5, "verb": "post", \
            "actor": {"displayName": "something", "id": 1232, "published": "today"}, \
            "object": {"displayName": "something", "id": 4353, "published": "today"}}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_verb(self):
        Activity({"id": 5, "title": "Stream Item", \
            "actor": {"displayName": "something", "id": 1232, "published": "today"}, \
            "object": {"displayName": "something", "id": 4353, "published": "today"}}).validate()

    def test_disallowed_field_published(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "published": "today", \
                "actor": {"displayName": "something", "id": 1232, "published": "today"}, \
                "object": {"displayName": "something", "id": 4353, "published": "today"}}).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: published")

    def test_disallowed_field_updated(self):
        try:
            Activity({"id": 5, "title": "Stream Item", "verb": "post", "updated": "today", \
                "actor": {"displayName": "something", "id": 1232, "published": "today"}, \
                "object": {"displayName": "something", "id": 4353, "published": "today"}}).validate()
        except SunspearValidationException as e:
            ok_(isinstance(e, SunspearValidationException))
            eq_(e.message, "Reserved field name used: updated")


class TestMediaLink(object):
    def test_required_fields_all_there(self):
        MediaLink({"url": "http://cdn.fake.com/static/img/clown.png"}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_url(self):
        MediaLink().validate()


class TestObject(object):
    def test_required_fields_all_there(self):
        Object({"displayName": "something", "id": 1232, "published": "today"}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_display_name(self):
        Object({"id": 1232, "published": "today"}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_no_id(self):
        Object({"displayName": "something", "published": "today"}).validate()

    @raises(SunspearValidationException)
    def test_required_fields_published(self):
        Object({"displayName": "something", "id": 1232}).validate()


class TestModelMethods(object):

    def test_parse_data_published_date(self):
        d = datetime.datetime.now()

        obj = Object({"displayName": "something", "id": 1232, \
            "published": d, "updated": d})
        parsed_dict = obj.parse_data(obj.get_dict())
        eq_(parsed_dict["published"], d.strftime('%Y-%m-%dT%H:%M:%S') + "Z")
        eq_(parsed_dict["updated"], d.strftime('%Y-%m-%dT%H:%M:%S') + "Z")

from __future__ import absolute_import

from nose.tools import ok_, eq_, raises, set_trace

from sunspear.backends import RiakBackend
from sunspear.exceptions import SunspearValidationException, SunspearNotFoundException

import datetime


class TestRiakBackend(object):
    def setUp(self):
        self._backend = RiakBackend({
            'hosts': [{'port': 8094}, {'port': 8093}, {'port': 8092}, {'port': 8091}]
        })
        self._riak_client = self._backend._get_riak_client()

    def test_create_stream(self):
        stream_name = "user1:timeline"
        stream = self._backend.create_stream(stream_name)
        stream_data = stream.get_data()

        saved_stream = self._backend._streams.get(stream.get_key())
        saved_stream_dict = saved_stream.get_data()

        eq_(stream_data, saved_stream_dict)
        saved_stream.delete()

    def test_create_object(self):
        self._backend._objects.get('1234').delete()

        actstream_obj = self._backend.create_object({"displayName": "Hello", "id": 1234, "published": datetime.datetime.utcnow()})

        ok_(actstream_obj.get_key())
        saved_obj = self._backend._objects.get(actstream_obj.get_key())
        saved_obj_dict = saved_obj.get_data()

        eq_(actstream_obj.get_data(), saved_obj_dict)
        actstream_obj.delete()

    def test_create_activity(self):
        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"displayName": "something", "id": actor_id, "published": published_time}
        obj = {"displayName": "something", "id": object_id, "published": published_time}

        act_obj = self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
        act_obj_dict = act_obj.get_data()

        eq_(act_obj_dict['actor'], actor_id)
        eq_(act_obj_dict['object'], object_id)

        actor.update({'published': published_time.strftime('%Y-%m-%dT%H:%M:%S') + "Z"})
        obj.update({'published': published_time.strftime('%Y-%m-%dT%H:%M:%S') + "Z"})
        eq_(self._backend._objects.get(actor_id).get_data(), actor)
        eq_(self._backend._objects.get(object_id).get_data(), obj)

    def test_create_activity_maintains_extra_keys(self):
        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"bar": "baz", "displayName": "something", "id": actor_id, "published": published_time}
        obj = {"foo": "bar", "displayName": "something", "id": object_id, "published": published_time}
        other = {
            "stuff": "this"
        }

        act_obj = self._backend.create_activity({"id": 5, "title": "Stream Item",
            "verb": "post",
            "actor": actor,
            "object": obj,
            "other": other
        })
        act_obj_dict = act_obj.get_data()

        eq_(act_obj_dict['actor'], actor_id)
        eq_(act_obj_dict['object'], object_id)

        actor.update({'published': published_time.strftime('%Y-%m-%dT%H:%M:%S') + "Z"})
        obj.update({'published': published_time.strftime('%Y-%m-%dT%H:%M:%S') + "Z"})
        eq_(self._backend._objects.get(actor_id).get_data(), actor)
        eq_(self._backend._objects.get(object_id).get_data(), obj)
        eq_(act_obj_dict["other"], other)

    def test_create_activity_with_actor_already_exists(self):
        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"displayName": "something", "id": actor_id, "published": '2012-07-05T12:00:00Z'}
        obj = {"displayName": "something", "id": object_id, "published": published_time}

        self._backend._objects.new(key=actor["id"]).set_data(actor).store()
        try:
            self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
            ok_(False)
        except SunspearValidationException:
            ok_(True)

        ok_(not self._backend._objects.get(obj["id"]).exists())

    def test_create_activity_with_object_already_exists(self):
        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"displayName": "something", "id": actor_id, "published": published_time}
        obj = {"displayName": "something", "id": object_id, "published": '2012-07-05T12:00:00Z'}

        self._backend._objects.new(key=obj["id"]).set_data(obj).store()
        try:
            self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
            ok_(False)
        except SunspearValidationException:
            ok_(True)

        ok_(not self._backend._objects.get(actor["id"]).exists())

    def test_group_by_aggregator(self):
        data_dict = {
            'a': 1,
            'b': 2,
            'c': {
                'd': 3,
                'e': 4
            }
        }
        expected = [1, 2, 4]
        actual = self._backend._group_by_aggregator(group_by_attributes=['a', 'b', 'a.c.f', 'c.e'])(data_dict)
        eq_(expected, actual)

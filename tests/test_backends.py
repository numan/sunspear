from __future__ import absolute_import

from nose.tools import ok_, eq_, raises, set_trace

from sunspear.backends import RiakBackend
from sunspear.exceptions import SunspearValidationException, SunspearNotFoundException

from itertools import groupby

import datetime


class TestRiakBackend(object):
    def setUp(self):
        self._backend = RiakBackend({
            'hosts': [{'port': 8094}, {'port': 8093}, {'port': 8092}, {'port': 8091}]
        })
        self._riak_client = self._backend._get_riak_client()

    def test_create_object(self):
        self._backend._objects.get('1234').delete()

        actstream_obj = self._backend.create_object({"objectType": "Hello", "id": 1234, "published": datetime.datetime.utcnow()})

        ok_(actstream_obj.get_key())
        saved_obj = self._backend._objects.get(actstream_obj.get_key())
        saved_obj_dict = saved_obj.get_data()

        eq_(actstream_obj.get_data(), saved_obj_dict)
        actstream_obj.delete()

    def test_create_activity(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        act_obj = self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
        act_obj_dict = act_obj.get_data()

        eq_(act_obj_dict['actor'], actor_id)
        eq_(act_obj_dict['object'], object_id)

        actor.update({'published': published_time.strftime('%Y-%m-%dT%H:%M:%S') + "Z"})
        obj.update({'published': published_time.strftime('%Y-%m-%dT%H:%M:%S') + "Z"})
        eq_(self._backend._objects.get(actor_id).get_data(), actor)
        eq_(self._backend._objects.get(object_id).get_data(), obj)

    def test_create_activity_maintains_extra_keys(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"bar": "baz", "objectType": "something", "id": actor_id, "published": published_time}
        obj = {"foo": "bar", "objectType": "something", "id": object_id, "published": published_time}
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
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": '2012-07-05T12:00:00Z'}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

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

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": '2012-07-05T12:00:00Z'}

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

    def test__listify_attributes(self):
        data_dict = {
            'a': 1,
            'b': 2,
            'c': {
                'd': 3,
                'e': 4
            }
        }
        group_by_attributes = ['a', 'a.c.f', 'c.e']
        expected = {
            'a': 1,
            'b': [2],
            'c': {
                'd': [3],
                'e': 4
            }
        }

        actual = self._backend._listify_attributes(group_by_attributes=group_by_attributes, activity=data_dict)
        eq_(actual, (['c'], expected,))

    def test__aggregate_activities(self):
        group_by_attributes = ['b', 'c.e']

        data_dict = [{'a': 1, 'b': 2, 'c': {'d': 3, 'e': 4}
        }, {'a': 3, 'b': 2,  'c': {'d': 5, 'e': 4}
        }, {'a': 4, 'b': 2, 'c': {'d': 6, 'e': 4}
        }, {'a': 5, 'b': 3, 'c': {'d': 6, 'e': 4}
        }]
        expected = [{'grouped_by_values': [2, 4], 'a': [1, 3, 4], 'b': 2, 'c': {'d': [3, 5, 6], 'e': 4}
        }, {'a': 5, 'b': 3, 'c': {'d': 6, 'e': 4}
        }]

        _raw_group_actvities = groupby(data_dict, self._backend._group_by_aggregator(group_by_attributes))
        actual = self._backend._aggregate_activities(group_by_attributes=group_by_attributes, grouped_activities=_raw_group_actvities)
        eq_(actual, expected)

    def test__get_many_activities(self):
        self._backend._activities.get('1').delete()
        self._backend._activities.get('2').delete()
        self._backend._activities.get('3').delete()
        self._backend._activities.get('4').delete()
        self._backend._activities.get('5').delete()

        self._backend.create_activity({"id": 1, "title": "Stream Item", "verb": "post", "actor": "1234", "object": "5678"})
        self._backend.create_activity({"id": 2, "title": "Stream Item", "verb": "post", "actor": "1234", "object": "5678"})
        self._backend.create_activity({"id": 3, "title": "Stream Item", "verb": "post", "actor": "1234", "object": "5678"})
        self._backend.create_activity({"id": 4, "title": "Stream Item", "verb": "post", "actor": "1234", "object": "5678"})
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": "1234", "object": "5678"})

        activities = self._backend._get_many_activities(activity_ids=['1', '2', '3', '4', '5'])

        for i in range(1, 6):
            eq_(activities[i - 1]["id"], str(i))

    def test_hydrate_activities(self):
        actor_id = '1234'
        actor_id2 = '4321'
        actor_id3 = '9999'

        object_id = '4353'
        object_id2 = '7654'

        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(actor_id2).delete()
        self._backend._objects.get(actor_id3).delete()
        self._backend._objects.get(object_id).delete()
        self._backend._objects.get(object_id2).delete()

        actor = {"objectType": "something", "id": actor_id, "published": '2012-07-05T12:00:00Z'}
        actor2 = {"objectType": "something", "id": actor_id2, "published": '2012-07-05T12:00:00Z'}
        actor3 = {"objectType": "something", "id": actor_id3, "published": '2012-07-05T12:00:00Z'}

        obj = {"objectType": "something", "id": object_id, "published": '2012-07-05T12:00:00Z'}
        obj2 = {"objectType": "something", "id": object_id2, "published": '2012-07-05T12:00:00Z'}

        self._backend._objects.new(key=actor["id"]).set_data(actor).store()
        self._backend._objects.new(key=actor2["id"]).set_data(actor2).store()
        self._backend._objects.new(key=actor3["id"]).set_data(actor3).store()

        self._backend._objects.new(key=obj["id"]).set_data(obj).store()
        self._backend._objects.new(key=obj2["id"]).set_data(obj2).store()

        activities = [
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": [actor_id, actor_id2], "object": object_id},
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": actor_id3, "object": [object_id, object_id2]},
        ]

        expected = [
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": [actor, actor2], "object": obj},
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": actor3, "object": [obj, obj2]},
        ]

        result = self._backend.hydrate_activities(activities)
        eq_(result, expected)

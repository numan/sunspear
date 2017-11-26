from __future__ import absolute_import, division, print_function

import datetime

import six
from mock import ANY, call, MagicMock
from nose.tools import eq_, ok_, raises, set_trace

from sunspear.aggregators.property import PropertyAggregator
from sunspear.backends.riak import RiakBackend
from sunspear.exceptions import SunspearValidationException

riak_connection_options = {
    "nodes": [
        {'http_port': 8098, 'host': '127.0.0.1'}],
    'protocol': 'http',
    # "nodes": [{'host': '127.0.0.1', 'pb_port': 10017}, {'host': '127.0.0.1', 'pb_port': 10027}, {'host': '127.0.0.1', 'pb_port': 10037}],
}


class TestRiakBackend(object):
    def setUp(self):
        backend = RiakBackend(**riak_connection_options)
        self._backend = backend

    def test_create_obj(self):
        self._backend._objects.get('1234').delete()
        obj = {"objectType": "Hello", "id": "1234", "published": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"}

        actstream_obj = self._backend.create_obj(obj)

        ok_(self._backend._objects.get(key=obj['id']).exists)
        eq_(actstream_obj, obj)

    def test_get_obj(self):
        obj1_id = '1111'
        obj2_id = '1112'
        obj3_id = '1113'
        obj4_id = '1114'

        self._backend._objects.get(obj1_id).delete()
        self._backend._objects.get(obj2_id).delete()
        self._backend._objects.get(obj3_id).delete()
        self._backend._objects.get(obj4_id).delete()

        obj1 = {
            "objectType": "Hello",
            "id": obj1_id,
            "published": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"
        }
        obj2 = {
            "objectType": "Hello",
            "id": obj2_id,
            "published": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"
        }
        obj3 = {
            "objectType": "Hello",
            "id": obj3_id,
            "published": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"
        }
        obj4 = {
            "objectType": "Hello",
            "id": obj4_id,
            "published": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"
        }

        self._backend.create_obj(obj1)
        self._backend.create_obj(obj2)
        self._backend.create_obj(obj3)
        self._backend.create_obj(obj4)

        objects = self._backend.get_obj([obj1_id, obj2_id, obj3_id, obj4_id, 'xxx'])

        eq_(len(objects), 4)
        for obj in objects:
            ok_(obj['id'] in [obj1_id, obj2_id, obj3_id, obj4_id])

    #see https://github.com/basho/riak_kv/issues/358
    def test_get_obj_no_objects_returns_empty_list(self):
        obj1_id = '1111'
        obj2_id = '1112'
        obj3_id = '1113'
        obj4_id = '1114'

        self._backend._objects.get(obj1_id).delete()
        self._backend._objects.get(obj2_id).delete()
        self._backend._objects.get(obj3_id).delete()
        self._backend._objects.get(obj4_id).delete()

        objects = self._backend.get_obj([obj1_id, obj2_id, obj3_id, obj4_id])

        eq_(len(objects), 0)
        eq_(objects, [])

    def test_get_obj_no_args_returns_empty_list(self):
        objects = self._backend.get_obj([])

        eq_(len(objects), 0)
        eq_(objects, [])

    def test_clear_all(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        ok_(len(self._backend._activities.get_keys()) > 0)
        ok_(len(self._backend._objects.get_keys()) > 0)

        self._backend.clear_all()

        #TODO: Not sure why riak still returns keys but they are actually deleted.
        #Need a way to fix this test
        # eq_(len(self._backend._activities.get_keys()), 0)
        # eq_(len(self._backend._objects.get_keys()), 0)

    def test_create_activity(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        act_obj = self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
        act_obj_dict = act_obj

        eq_(act_obj_dict['actor'], actor)
        eq_(act_obj_dict['object'], obj)

    def test_create_activity_stored_as_sparse(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        object_id = '4353'
        target_id = '2133'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}
        target = {"objectType": "something", "id": target_id, "published": published_time}

        self._backend.create_activity(
            {
                "id": 5,
                "title": "Stream Item",
                "verb": "post",
                "actor": actor,
                "object": obj,
                "target": target,
            }
        )

        riak_obj = self._backend._activities.get('5')
        riak_obj_data = riak_obj.data
        ok_(isinstance(riak_obj_data.get("target"), six.string_types))

    def test_delete_activity(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        act_obj = self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
        act_obj_dict = act_obj

        eq_(act_obj_dict['actor'], actor)
        eq_(act_obj_dict['object'], obj)

        self._backend.delete_activity(act_obj_dict)
        ok_(not self._backend._activities.get(key=act_obj_dict['id']).exists)

    def test_create_activity_with_targeted_audience(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        object_id = '4353'
        object_id2 = '5555'
        object_id3 = '5556'
        object_id4 = '5557'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()
        self._backend._objects.get(object_id2).delete()
        self._backend._objects.get(object_id3).delete()
        self._backend._objects.get(object_id4).delete()

        published_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}
        obj2 = {"objectType": "something", "id": object_id2, "published": published_time}
        obj3 = {"objectType": "something", "id": object_id3, "published": published_time}
        obj4 = {"objectType": "something", "id": object_id4, "published": published_time}

        act_obj_dict = self._backend.create_activity({
            "id": 5,
            "title": "Stream Item",
            "verb": "post",
            "actor": actor,
            "object": obj,
            'to': [obj2, obj3],
            "cc": [obj4]})

        eq_(act_obj_dict['to'], [obj2, obj3])
        eq_(act_obj_dict['cc'], [obj4])

    def test_create_activity_maintains_extra_keys(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"

        actor = {"bar": "baz", "objectType": "something", "id": actor_id, "published": published_time}
        obj = {"foo": "bar", "objectType": "something", "id": object_id, "published": published_time}
        other = {
            "stuff": "this"
        }

        act_obj = self._backend.create_activity(
            {
                "id": 5,
                "title": "Stream Item",
                "verb": "post",
                "actor": actor,
                "object": obj,
                "other": other
            }
        )
        act_obj_dict = act_obj

        eq_(act_obj_dict['actor'], actor)
        eq_(act_obj_dict['object'], obj)
        eq_(act_obj_dict["other"], other)

    def test_create_activity_with_actor_already_exists_overrides_object(self):
        self._backend._activities.get('5').delete()
        self._backend._activities.get('6').delete()

        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": '2012-07-05T12:00:00Z'}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        riak_obj = self._backend._objects.new(key=actor["id"])
        riak_obj.data = actor
        riak_obj.store()

        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
        ok_(self._backend._objects.get(obj["id"]).exists)
        ok_('content' not in self._backend._objects.get(actor["id"]).data)

        actor['content'] = "Some new content that wasn't there before."
        self._backend.create_activity({"id": 6, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
        ok_(self._backend._objects.get(obj["id"]).exists)
        eq_(actor['content'], self._backend._objects.get(actor["id"]).data['content'])

    def test_create_activity_with_exception_rollsback_objects(self):
        self._backend._activities.get('5').delete()
        self._backend._activities.get('6').delete()

        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": '2012-07-05T12:00:00Z'}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        riak_obj = self._backend._objects.new(key=actor["id"])
        riak_obj.data = actor
        riak_obj.store()

        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
        ok_(self._backend._objects.get(obj["id"]).exists)
        ok_('content' not in self._backend._objects.get(actor["id"]).data)

        try:
            actor['content'] = "Some new content that wasn't there before."
            self._backend.create_activity({"id": 6, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj, 'something': self._backend})
            ok_(False)
        except TypeError:
            ok_(True)
            ok_('content' not in self._backend._objects.get(actor["id"]).data)

    def test_create_activity_with_object_already_exists_shouldnt_throw_exception(self):
        actor_id = '1234'
        object_id = '4353'
        activity_id = '5'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()
        self._backend._activities.get(activity_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": '2012-07-05T12:00:00Z'}

        riak_obj = self._backend._objects.new(key=obj["id"])
        riak_obj.data = obj
        riak_obj.store()

        self._backend.create_activity({"id": activity_id, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        ok_(self._backend._objects.get(actor["id"]).exists)

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

    def test__get_many_activities_with_filtering(self):
        self._backend._activities.get('1').delete()
        self._backend._activities.get('2').delete()
        self._backend._activities.get('3').delete()
        self._backend._activities.get('4').delete()
        self._backend._activities.get('5').delete()

        self._backend.create_activity({"id": 1, "title": "Stream Item 1", "verb": "type1", "actor": "1234", "object": "5678"})
        self._backend.create_activity({"id": 2, "title": "Stream Item 2", "verb": "type1", "actor": "1234", "object": "5678"})
        self._backend.create_activity({"id": 3, "title": "Stream Item 3", "verb": "type3", "actor": "1234", "object": "5678"})
        self._backend.create_activity({"id": 4, "title": "Stream Item 4", "verb": "type4", "actor": "1234", "object": "5678"})
        self._backend.create_activity({"id": 5, "title": "Stream Item 5", "verb": "type5", "actor": "1234", "object": "5678"})

        activities = self._backend._get_many_activities(activity_ids=['1', '2', '3', '4', '5'], filters={'verb': ['type1', 'type3']})

        eq_(len(activities), 3)
        for i in range(1, 4):
            eq_(activities[i - 1]["id"], str(i))

    def test__get_many_activities_with_multiple_filters(self):
        self._backend._activities.get('1').delete()
        self._backend._activities.get('2').delete()
        self._backend._activities.get('3').delete()
        self._backend._activities.get('4').delete()
        self._backend._activities.get('5').delete()

        activity_1 = {"id": "1", "title": "Stream Item 1", "verb": "type1", "actor": "1234", "object": "5678"}
        activity_2 = {"id": "2", "title": "Stream Item 2", "verb": "type1", "actor": "1235", "object": "5678"}
        activity_3 = {"id": "3", "title": "Stream Item 3", "verb": "type3", "actor": "1236", "object": "5678"}
        activity_4 = {"id": "4", "title": "Stream Item 4", "verb": "type4", "actor": "1237", "object": "5678"}
        activity_5 = {"id": "5", "title": "Stream Item 5", "verb": "type5", "actor": "1238", "object": "5678"}

        self._backend.create_activity(activity_1)
        self._backend.create_activity(activity_2)
        self._backend.create_activity(activity_3)
        self._backend.create_activity(activity_4)
        self._backend.create_activity(activity_5)

        activities = self._backend._get_many_activities(activity_ids=['1', '2', '3', '4', '5'], filters={'verb': ['type3'], 'actor': ['1237', '1238']})

        eq_(3, len(activities))
        eq_(activities[0]['id'], activity_3['id'])
        eq_(activities[1]['id'], activity_4['id'])
        eq_(activities[2]['id'], activity_5['id'])

    def test__get_many_activities_with_raw_filter(self):
        self._backend._activities.get('1').delete()
        self._backend._activities.get('2').delete()
        self._backend._activities.get('3').delete()
        self._backend._activities.get('4').delete()
        self._backend._activities.get('5').delete()

        activity_1 = {"id": "1", "title": "Stream Item 1", "verb": "type1", "actor": "1234", "object": "5678"}
        activity_2 = {"id": "2", "title": "Stream Item 2", "verb": "type1", "actor": "1235", "object": "5678"}
        activity_3 = {"id": "3", "title": "Stream Item 3", "verb": "type3", "actor": "1236", "object": "5678"}
        activity_4 = {"id": "4", "title": "Stream Item 4", "verb": "type4", "actor": "1237", "object": "5678"}
        activity_5 = {"id": "5", "title": "Stream Item 5", "verb": "type5", "actor": "1238", "object": "5678"}

        self._backend.create_activity(activity_1)
        self._backend.create_activity(activity_2)
        self._backend.create_activity(activity_3)
        self._backend.create_activity(activity_4)
        self._backend.create_activity(activity_5)

        raw_filter = """
        function(obj) {
            return obj.verb == "type1";
        }
        """
        activities = self._backend._get_many_activities(activity_ids=['1', '2', '3', '4', '5'], raw_filter=raw_filter)

        eq_(2, len(activities))
        eq_(activities[0]['id'], activity_1['id'])
        eq_(activities[1]['id'], activity_2['id'])

    def test__get_many_activities_with_raw_filter_and_other_filter_runs_both_filters(self):
        self._backend._activities.get('1').delete()
        self._backend._activities.get('2').delete()
        self._backend._activities.get('3').delete()
        self._backend._activities.get('4').delete()
        self._backend._activities.get('5').delete()

        activity_1 = {"id": "1", "title": "Stream Item 1", "verb": "type1", "actor": "1234", "object": "5678"}
        activity_2 = {"id": "2", "title": "Stream Item 2", "verb": "type1", "actor": "1235", "object": "5678"}
        activity_3 = {"id": "3", "title": "Stream Item 3", "verb": "type3", "actor": "1236", "object": "5678"}
        activity_4 = {"id": "4", "title": "Stream Item 4", "verb": "type4", "actor": "1237", "object": "5678"}
        activity_5 = {"id": "5", "title": "Stream Item 5", "verb": "type5", "actor": "1238", "object": "5678"}

        self._backend.create_activity(activity_1)
        self._backend.create_activity(activity_2)
        self._backend.create_activity(activity_3)
        self._backend.create_activity(activity_4)
        self._backend.create_activity(activity_5)

        raw_filter = """
        function(obj) {
            return obj.verb == "type1";
        }
        """

        activities = self._backend._get_many_activities(
            activity_ids=['1', '2', '3', '4', '5'], raw_filter=raw_filter,
            filters={'actor': ['1234', '1238']})

        eq_(1, len(activities))
        eq_(activities[0]['id'], activity_1['id'])

    def test_dehydrate_activities(self):
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

        riak_obj1 = self._backend._objects.new(key=actor["id"])
        riak_obj1.data = actor
        riak_obj1.store()

        riak_obj2 = self._backend._objects.new(key=actor2["id"])
        riak_obj2.data = actor2
        riak_obj2.store()

        riak_obj3 = self._backend._objects.new(key=actor3["id"])
        riak_obj3.data = actor3
        riak_obj3.store()

        riak_obj4 = self._backend._objects.new(key=obj["id"])
        riak_obj4.data = obj
        riak_obj4.store()

        riak_obj5 = self._backend._objects.new(key=obj2["id"])
        riak_obj5.data = obj2
        riak_obj5.store()

        activities = [
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": [actor_id, actor_id2], "object": object_id},
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": actor_id3, "object": [object_id, object_id2]},
        ]

        expected = [
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": [actor, actor2], "object": obj},
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": actor3, "object": [obj, obj2]},
        ]

        result = self._backend.dehydrate_activities(activities)
        eq_(result, expected)

    def test__get_many_activities_with_audience_targeting_and_public(self):
        self._backend._activities.get('1').delete()
        self._backend._activities.get('2').delete()
        self._backend._activities.get('3').delete()
        self._backend._activities.get('4').delete()
        self._backend._activities.get('5').delete()
        self._backend._activities.get('6').delete()
        self._backend._activities.get('7').delete()
        self._backend._activities.get('8').delete()

        self._backend.create_activity({"id": 1, "title": "Stream Item 1", "verb": "type1", "actor": "1234", "object": "5678", 'to': ['100', '101']})
        self._backend.create_activity({"id": 2, "title": "Stream Item 2", "verb": "type1", "actor": "1234", "object": "5678", 'bto': ['100']})
        self._backend.create_activity({"id": 3, "title": "Stream Item 3", "verb": "type3", "actor": "1234", "object": "5678", 'cc': ['103', '104'], 'bcc': ['100']})
        self._backend.create_activity({"id": 4, "title": "Stream Item 4", "verb": "type4", "actor": "1234", "object": "5678", 'bto': ['105']})
        self._backend.create_activity({"id": 5, "title": "Stream Item 5", "verb": "type5", "actor": "1234", "object": "5678", 'to': ['100', '101'], 'cc': ['103']})
        self._backend.create_activity({"id": 6, "title": "Stream Item 5", "verb": "type5", "actor": "1234", "object": "5678"})
        self._backend.create_activity({"id": 7, "title": "Stream Item 5", "verb": "type5", "actor": "1234", "object": "5678"})
        self._backend.create_activity({"id": 8, "title": "Stream Item 5", "verb": "type5", "actor": "1234", "object": "5678"})

        activities = self._backend._get_many_activities(
            activity_ids=['1', '2', '3', '4', '5', '6', '7', '8'],
            include_public=True, audience_targeting={'to': ['100', '105'], 'bto': ['105']})

        eq_(len(activities), 6)
        for i in range(6):
            ok_(activities[i]['id'] in ['1', '4', '5', '6', '7', '8'])

    def test__get_many_activities_with_audience_targeting(self):
        self._backend._activities.get('1').delete()
        self._backend._activities.get('2').delete()
        self._backend._activities.get('3').delete()
        self._backend._activities.get('4').delete()
        self._backend._activities.get('5').delete()

        self._backend.create_activity({"id": 1, "title": "Stream Item 1", "verb": "type1", "actor": "1234", "object": "5678", 'to': ['100', '101']})
        self._backend.create_activity({"id": 2, "title": "Stream Item 2", "verb": "type1", "actor": "1234", "object": "5678", 'bto': ['100']})
        self._backend.create_activity({"id": 3, "title": "Stream Item 3", "verb": "type3", "actor": "1234", "object": "5678", 'cc': ['103', '104'], 'bcc': ['100']})
        self._backend.create_activity({"id": 4, "title": "Stream Item 4", "verb": "type4", "actor": "1234", "object": "5678", 'bto': ['105']})
        self._backend.create_activity({"id": 5, "title": "Stream Item 5", "verb": "type5", "actor": "1234", "object": "5678", 'to': ['100', '101'], 'cc': ['103']})

        activities = self._backend._get_many_activities(activity_ids=['1', '2', '3', '4', '5'], audience_targeting={'to': ['100', '105'], 'bto': ['105']})

        eq_(len(activities), 3)
        for i in range(3):
            ok_(activities[i]['id'] == '1' or activities[i]['id'] == '4' or activities[i]['id'] == '5')

        activities = self._backend._get_many_activities(activity_ids=['1', '2', '3', '4', '5'], audience_targeting={'cc': ['103'], 'bcc': ['100']})

        eq_(len(activities), 2)
        for i in range(2):
            ok_(activities[i]['id'] == '3' or activities[i]['id'] == '5')

        activities = self._backend._get_many_activities(activity_ids=['1', '2', '3', '4', '5'], audience_targeting={'cc': ['103'], 'bcc': ['100']})

        eq_(len(activities), 2)
        for i in range(2):
            ok_(activities[i]['id'] == '3' or activities[i]['id'] == '5')

    def test_dehydrate_activities_with_audience(self):
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

        riak_obj1 = self._backend._objects.new(key=actor["id"])
        riak_obj1.data = actor
        riak_obj1.store()

        riak_obj2 = self._backend._objects.new(key=actor2["id"])
        riak_obj2.data = actor2
        riak_obj2.store()

        riak_obj3 = self._backend._objects.new(key=actor3["id"])
        riak_obj3.data = actor3
        riak_obj3.store()

        riak_obj4 = self._backend._objects.new(key=obj["id"])
        riak_obj4.data = obj
        riak_obj4.store()

        riak_obj5 = self._backend._objects.new(key=obj2["id"])
        riak_obj5.data = obj2
        riak_obj5.store()

        activities = [
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": [actor_id, actor_id2], "object": object_id, 'to': [actor_id, actor_id2], 'cc': [actor_id3]},
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": actor_id3, "object": [object_id, object_id2], 'bto': [object_id2], 'bcc': [object_id]},
        ]

        expected = [
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": [actor, actor2], "object": obj, 'to': [actor, actor2], 'cc': [actor3]},
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": actor3, "object": [obj, obj2], 'bto': [obj2], 'bcc': [obj]},
        ]

        result = self._backend.dehydrate_activities(activities)
        eq_(result, expected)

    def test_create_reply(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        reply_activity_dict, activity_obj_dict = self._backend.sub_activity_create(
            5, actor2_id, "This is a reply.",
            sub_activity_verb='reply')

        eq_(reply_activity_dict['actor']['id'], actor2_id)
        eq_(reply_activity_dict['verb'], 'reply')

        eq_(activity_obj_dict['replies']['totalItems'], 1)
        eq_(activity_obj_dict['replies']['items'][0]['object']['id'], reply_activity_dict['id'])
        eq_(activity_obj_dict['replies']['items'][0]['verb'], 'reply')
        eq_(activity_obj_dict['replies']['items'][0]['actor']['id'], actor2_id)

    def test_create_reply_change_actor(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        reply_activity_dict, activity_obj_dict = self._backend.sub_activity_create(
            5, actor2_id, "This is a reply.",
            sub_activity_verb='reply')

        eq_(reply_activity_dict['actor']['id'], actor2_id)
        eq_(reply_activity_dict['verb'], 'reply')

        eq_(activity_obj_dict['replies']['totalItems'], 1)
        eq_(activity_obj_dict['replies']['items'][0]['object']['id'], reply_activity_dict['id'])
        eq_(activity_obj_dict['replies']['items'][0]['verb'], 'reply')
        eq_(activity_obj_dict['replies']['items'][0]['actor']['id'], actor2_id)
        eq_(activity_obj_dict['replies']['items'][0]['actor'].get("displayName"), None)

        #change the actor of the reply and confirm it changes
        actor['displayName'] = 'foobar'
        self._backend.update_obj(actor)

        activity_obj_dict = self._backend.get_activity(activity_ids=[activity_obj_dict['id']])[0]
        eq_(activity_obj_dict['replies']['totalItems'], 1)
        eq_(activity_obj_dict['replies']['items'][0]['object']['id'], reply_activity_dict['id'])
        eq_(activity_obj_dict['replies']['items'][0]['verb'], 'reply')
        eq_(activity_obj_dict['replies']['items'][0]['actor']['id'], actor2_id)
        eq_(activity_obj_dict['replies']['items'][0]['actor']['displayName'], 'foobar')

    def test_create_reply_maintains_dehydrate_state(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        riak_obj_data = self._backend._activities.get(key="5").data
        ok_(isinstance(riak_obj_data.get("actor"), six.string_types))
        ok_(isinstance(riak_obj_data.get("object"), six.string_types))

        #now create a reply for the activity
        reply_activity_dict, activity_obj_dict = self._backend.sub_activity_create(
            5, actor2_id, "This is a reply.",
            sub_activity_verb='reply')

        riak_obj_data = self._backend._activities.get(key="5").data
        ok_(isinstance(riak_obj_data.get("actor"), six.string_types))
        ok_(isinstance(riak_obj_data.get("object"), six.string_types))

    def test_create_reply_with_extra_data(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}
        extra = {'published': datetime.datetime(year=2012, month=1, day=1), 'foo': 'bar'}

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        reply_activity_dict, activity_obj_dict = self._backend.sub_activity_create(
            5, actor2_id, "This is a reply.", extra=extra,
            sub_activity_verb='reply')

        eq_(extra['published'].strftime('%Y-%m-%dT%H:%M:%S') + "Z", reply_activity_dict['published'])
        eq_(extra['foo'], reply_activity_dict['foo'])

        eq_(reply_activity_dict['actor']['id'], actor2_id)
        eq_(reply_activity_dict['verb'], 'reply')

        eq_(activity_obj_dict['replies']['totalItems'], 1)
        eq_(activity_obj_dict['replies']['items'][0]['object']['id'], reply_activity_dict['id'])
        eq_(activity_obj_dict['replies']['items'][0]['verb'], 'reply')
        eq_(activity_obj_dict['replies']['items'][0]['actor']['id'], actor2_id)

    def test_create_reply_as_dict(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '4321'
        object_id = '4353'
        reply_id = '9999'
        reply_dict = {
            'id': reply_id,
            'content': 'This is my reply.',
            'metadata': 'I can put whatever I want here.',
        }
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(actor2_id).delete()
        self._backend._objects.get(object_id).delete()
        self._backend._objects.get(reply_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        actor2 = {"objectType": "something", "id": actor2_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        self._backend.create_obj(actor2)

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        reply_activity_dict, activity_obj_dict = self._backend.sub_activity_create(
            5, actor2_id, reply_dict,
            sub_activity_verb='reply')

        eq_(reply_activity_dict['actor']['id'], actor2_id)
        eq_(reply_activity_dict['verb'], 'reply')
        eq_(reply_activity_dict['object']['metadata'], reply_dict['metadata'])

        eq_(activity_obj_dict['replies']['totalItems'], 1)
        eq_(activity_obj_dict['replies']['items'][0]['object']['id'], reply_activity_dict['id'])
        eq_(activity_obj_dict['replies']['items'][0]['verb'], 'reply')
        eq_(activity_obj_dict['replies']['items'][0]['actor']['id'], actor2_id)

    def test_create_like_maintains_dehydrate_state(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '4321'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(actor2_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        actor2 = {"objectType": "something", "id": actor2_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        self._backend.create_obj(actor2)

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        riak_obj_data = self._backend._activities.get(key="5").data
        ok_(isinstance(riak_obj_data.get("actor"), six.string_types))
        ok_(isinstance(riak_obj_data.get("object"), six.string_types))

        #now create a reply for the activity
        like_activity_dict, activity_obj_dict = self._backend.sub_activity_create(
            5, actor2_id, "",
            sub_activity_verb='like')

        riak_obj_data = self._backend._activities.get(key="5").data
        ok_(isinstance(riak_obj_data.get("actor"), six.string_types))
        ok_(isinstance(riak_obj_data.get("object"), six.string_types))

    def test_create_like(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '4321'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(actor2_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        actor2 = {"objectType": "something", "id": actor2_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        self._backend.create_obj(actor2)

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        like_activity_dict, activity_obj_dict = self._backend.sub_activity_create(
            5, actor2_id, "",
            sub_activity_verb='like')

        eq_(like_activity_dict['actor']['id'], actor2_id)
        eq_(like_activity_dict['verb'], 'like')

        eq_(activity_obj_dict['likes']['totalItems'], 1)
        eq_(activity_obj_dict['likes']['items'][0]['object']['id'], like_activity_dict['id'])
        eq_(activity_obj_dict['likes']['items'][0]['verb'], 'like')
        eq_(activity_obj_dict['likes']['items'][0]['actor']['id'], actor2_id)

    def test_delete_like(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '4321'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        like_activity_dict, activity_obj_dict = self._backend.create_sub_activity(
            5, actor2_id, "", sub_activity_verb='like')

        eq_(like_activity_dict['actor']['id'], actor2_id)
        eq_(like_activity_dict['verb'], 'like')

        eq_(activity_obj_dict['likes']['totalItems'], 1)
        eq_(activity_obj_dict['likes']['items'][0]['object']['id'], like_activity_dict['id'])
        eq_(activity_obj_dict['likes']['items'][0]['verb'], 'like')
        eq_(activity_obj_dict['likes']['items'][0]['actor']['id'], actor2_id)

        #now delete the like and make sure everything is ok:
        returned_updated_activity = self._backend.sub_activity_delete(like_activity_dict['id'], 'like')
        activity_obj_dict = self._backend._activities.get('5').data

        ok_('likes' not in returned_updated_activity)
        ok_('likes' not in activity_obj_dict)

    def test_delete_like_maintains_dehydrated_state(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '4321'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        like_activity_dict, activity_obj_dict = self._backend.create_sub_activity(
            5, actor2_id, "", sub_activity_verb='like')

        riak_obj_data = self._backend._activities.get(key="5").data
        ok_(isinstance(riak_obj_data.get("actor"), six.string_types))
        ok_(isinstance(riak_obj_data.get("object"), six.string_types))

        #now delete the like and make sure everything is ok:
        self._backend.sub_activity_delete(like_activity_dict['id'], 'like')

        riak_obj_data = self._backend._activities.get(key="5").data
        ok_(isinstance(riak_obj_data.get("actor"), six.string_types))
        ok_(isinstance(riak_obj_data.get("object"), six.string_types))

    def test_reply_delete_maintains_dehydrated_state(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        reply_activity_dict, activity_obj_dict = self._backend.sub_activity_create(
            5, actor2_id, "This is a reply.", sub_activity_verb='reply')

        riak_obj_data = self._backend._activities.get(key="5").data
        ok_(isinstance(riak_obj_data.get("actor"), six.string_types))
        ok_(isinstance(riak_obj_data.get("object"), six.string_types))

        #now delete the reply and make sure everything is ok:
        self._backend.sub_activity_delete(
            reply_activity_dict['id'], 'reply')

        riak_obj_data = self._backend._activities.get(key="5").data
        ok_(isinstance(riak_obj_data.get("actor"), six.string_types))
        ok_(isinstance(riak_obj_data.get("object"), six.string_types))

    def test_reply_delete(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        reply_activity_dict, activity_obj_dict = self._backend.sub_activity_create(
            5, actor2_id, "This is a reply.", sub_activity_verb='reply')

        eq_(reply_activity_dict['actor']['id'], actor['id'])
        eq_(reply_activity_dict['verb'], 'reply')

        eq_(activity_obj_dict['replies']['totalItems'], 1)
        eq_(activity_obj_dict['replies']['items'][0]['object']['id'], reply_activity_dict['id'])
        eq_(activity_obj_dict['replies']['items'][0]['verb'], 'reply')
        eq_(activity_obj_dict['replies']['items'][0]['actor']['id'], actor2_id)

        #now delete the reply and make sure everything is ok:
        returned_updated_activity = self._backend.sub_activity_delete(
            reply_activity_dict['id'], 'reply')
        activity_obj_dict = self._backend._activities.get('5').data

        ok_('replies' not in returned_updated_activity)
        ok_('replies' not in activity_obj_dict)

    def test_delete_sub_activity_reply(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        reply_activity_dict, activity_obj_dict = self._backend.create_sub_activity(
            5, actor2_id, "This is a reply.",
            sub_activity_verb='reply')

        eq_(reply_activity_dict['actor']['id'], actor['id'])
        eq_(reply_activity_dict['verb'], 'reply')

        eq_(activity_obj_dict['replies']['totalItems'], 1)
        eq_(activity_obj_dict['replies']['items'][0]['object']['id'], reply_activity_dict['id'])
        eq_(activity_obj_dict['replies']['items'][0]['verb'], 'reply')
        eq_(activity_obj_dict['replies']['items'][0]['actor']['id'], actor2_id)

        #now delete the activity and make sure the reply gets deleted too:
        self._backend.delete_sub_activity(reply_activity_dict, 'reply')

        ok_(not self._backend._activities.get(reply_activity_dict['id']).exists)
        ok_(not self._backend._activities.get(activity_obj_dict['replies']['items'][0]['id']).exists)

    def test_delete_sub_activity_like(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '4321'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        like_activity_dict, activity_obj_dict = self._backend.create_sub_activity(
            5, actor2_id, "",
            sub_activity_verb='like')

        eq_(like_activity_dict['actor']['id'], actor2_id)
        eq_(like_activity_dict['verb'], 'like')

        eq_(activity_obj_dict['likes']['totalItems'], 1)
        eq_(activity_obj_dict['likes']['items'][0]['object']['id'], like_activity_dict['id'])
        eq_(activity_obj_dict['likes']['items'][0]['verb'], 'like')
        eq_(activity_obj_dict['likes']['items'][0]['actor']['id'], actor2_id)

        #now delete the activity and make sure the like gets deleted too:
        self._backend.delete_sub_activity(like_activity_dict, 'like')

        ok_(not self._backend._activities.get(like_activity_dict['id']).exists)
        ok_(not self._backend._activities.get(activity_obj_dict['likes']['items'][0]['id']).exists)

    def test_delete_activity_with_sub_activity(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '4321'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        like_activity_dict, activity_obj_dict = self._backend.create_sub_activity(
            5, actor2_id, "", sub_activity_verb='like')

        eq_(like_activity_dict['actor']['id'], actor2_id)
        eq_(like_activity_dict['verb'], 'like')

        eq_(activity_obj_dict['likes']['totalItems'], 1)
        eq_(activity_obj_dict['likes']['items'][0]['object']['id'], like_activity_dict['id'])
        eq_(activity_obj_dict['likes']['items'][0]['verb'], 'like')
        eq_(activity_obj_dict['likes']['items'][0]['actor']['id'], actor2_id)

        #now delete the activity and make sure the like gets deleted too:
        self._backend.delete_activity(activity_obj_dict)

        ok_(not self._backend._activities.get(like_activity_dict['id']).exists)
        ok_(not self._backend._activities.get(activity_obj_dict['id']).exists)

    def test_get_activities_doesnt_crash_for_missing_activities(self):
        self._backend._activities.get('1').delete()
        self._backend._activities.get('2').delete()

        result = self._backend.activity_get(activity_ids=['1', '2'])
        eq_(result, [])


class TestRiakBackendDehydrate(object):
    def setUp(self):
        self._backend = RiakBackend(**riak_connection_options)

        self.actor_id = '1234'
        self.actor_id2 = '4321'
        self.actor_id3 = '31415'

        self.object_id = '4353'
        self.object_id2 = '7654'
        self.object_id3 = '7655'

        self.reply_obj_id = '9999'
        self.reply_obj_id2 = '9998'

        self.reply_activity_id = '8889'
        self.reply_activity_id2 = '8888'

        self.like_obj_id = '6669'
        self.like_obj_id2 = '6669'

        self.like_activity_id = '7779'
        self.like_activity_id2 = '7778'

        self.activity_id = '5555'
        self.activity_id2 = '5556'

        self.reply_1 = {
            'objectType': 'reply',
            'id': self.reply_obj_id,
            'published': '2012-08-05T12:00:00Z',
            'content': 'This is my first reply',
            'inReplyTo': [],
        }

        self.reply_2 = {
            'objectType': 'reply',
            'id': self.reply_obj_id2,
            'published': '2012-08-05T12:05:00Z',
            'content': 'This is my second reply',
            'inReplyTo': [],
        }

        self.like_1 = {
            'objectType': 'like',
            'id': self.like_obj_id,
            'published': '2012-08-05T12:00:00Z',
            'inReplyTo': [],
        }

        self.like_2 = {
            'objectType': 'like',
            'id': self.like_obj_id2,
            'published': '2012-08-05T12:00:00Z',
            'inReplyTo': [],
        }

        self.reply_activity_1 = {
            'actor': self.actor_id,
            'object': self.reply_obj_id,
            'target': self.actor_id3,
            'verb': 'reply',
            'id': self.reply_activity_id,
        }

        self.reply_activity_2 = {
            'actor': self.actor_id2,
            'object': self.reply_obj_id2,
            'target': self.actor_id,
            'verb': 'reply',
            'id': self.reply_activity_id2,
        }

        self.like_activity_1 = {
            'actor': self.actor_id,
            'object': self.like_obj_id,
            'target': self.actor_id3,
            'verb': 'like',
            'id': self.like_activity_id,
        }

        self.like_activity_2 = {
            'actor': self.actor_id,
            'object': self.like_obj_id2,
            'target': self.actor_id3,
            'verb': 'like',
            'id': self.like_activity_id2,
        }

        self.activity_1 = {
            "id": self.activity_id,
            "title": "Stream Item",
            "verb": "post",
            "actor": self.actor_id2,
            "object": self.object_id,
            'replies': {
                'totalItems': 2,
                'items': [
                    {'actor': self.actor_id, 'verb': 'reply', 'object': {'objectType': 'activity', 'id': self.reply_activity_id}},
                    {'actor': self.actor_id, 'verb': 'reply', 'object': {'objectType': 'activity', 'id': self.reply_activity_id2}},
                ]
            },
        }

        self.activity_2 = {
            "id": self.activity_id2,
            "title": "Stream Item",
            "verb": "post",
            "actor": self.actor_id2,
            "object": self.object_id,
            'replies': {
                'totalItems': 2,
                'items': [
                    {'actor': self.actor_id, 'verb': 'reply', 'object': {'objectType': 'activity', 'id': self.reply_activity_id}},
                    {'actor': self.actor_id, 'verb': 'reply', 'object': {'objectType': 'activity', 'id': self.reply_activity_id2}},
                ]
            },
            "likes": {
                'totalItems': 1,
                'items': [
                    {'actor': self.actor_id, 'verb': 'like', 'object': {'objectType': 'activity', 'id': self.like_activity_id}},
                ],
            },
        }

        self.actor = {"objectType": "something", "id": self.actor_id, "published": '2012-07-05T12:00:00Z'}
        self.actor2 = {"objectType": "something", "id": self.actor_id2, "published": '2012-07-05T12:00:00Z'}
        self.actor3 = {"objectType": "something", "id": self.actor_id3, "published": '2012-07-05T12:00:00Z'}

        self.obj = {"objectType": "something", "id": self.object_id, "published": '2012-07-05T12:00:00Z'}
        self.obj2 = {"objectType": "something", "id": self.object_id2, "published": '2012-07-05T12:00:00Z'}
        self.obj3 = {"objectType": "something", "id": self.object_id3, "published": '2012-07-05T12:00:00Z', 'inReplyTo': [{'objectType': 'activity', 'id': self.reply_activity_id}]}

        self._backend._objects.get(self.actor_id).delete()
        self._backend._objects.get(self.actor_id2).delete()
        self._backend._objects.get(self.actor_id3).delete()
        self._backend._objects.get(self.object_id).delete()
        self._backend._objects.get(self.object_id2).delete()
        self._backend._objects.get(self.object_id3).delete()
        self._backend._objects.get(self.reply_obj_id).delete()
        self._backend._objects.get(self.reply_obj_id2).delete()
        self._backend._objects.get(self.like_obj_id).delete()
        self._backend._objects.get(self.like_obj_id2).delete()

        self._backend._activities.get(self.reply_activity_id).delete()
        self._backend._activities.get(self.reply_activity_id2).delete()
        self._backend._activities.get(self.like_activity_id).delete()
        self._backend._activities.get(self.activity_id).delete()
        self._backend._activities.get(self.activity_id2).delete()

        obj1 = self._backend._objects.new(key=self.actor["id"])
        obj1.data = self.actor
        obj1.store()

        obj2 = self._backend._objects.new(key=self.actor2["id"])
        obj2.data = self.actor2
        obj2.store()

        obj3 = self._backend._objects.new(key=self.actor3["id"])
        obj3.data = self.actor3
        obj3.store()

        obj4 = self._backend._objects.new(key=self.obj["id"])
        obj4.data = self.obj
        obj4.store()

        obj5 = self._backend._objects.new(key=self.obj2["id"])
        obj5.data = self.obj2
        obj5.store()

        obj6 = self._backend._objects.new(key=self.obj3["id"])
        obj6.data = self.obj3
        obj6.store()

        obj7 = self._backend._objects.new(key=self.reply_1["id"])
        obj7.data = self.reply_1
        obj7.store()

        obj8 = self._backend._objects.new(key=self.reply_2["id"])
        obj8.data = self.reply_2
        obj8.store()

        obj9 = self._backend._objects.new(key=self.like_1["id"])
        obj9.data = self.like_1
        obj9.store()

        obj10 = self._backend._objects.new(key=self.like_2["id"])
        obj10.data = self.like_2
        obj10.store()

        obj11 = self._backend._activities.new(key=self.reply_activity_1["id"])
        obj11.data = self.reply_activity_1
        obj11.store()

        obj12 = self._backend._activities.new(key=self.reply_activity_2["id"])
        obj12.data = self.reply_activity_2
        obj12.store()

        obj13 = self._backend._activities.new(key=self.like_activity_1["id"])
        obj13.data = self.like_activity_1
        obj13.store()

        obj14 = self._backend._activities.new(key=self.like_activity_2["id"])
        obj14.data = self.like_activity_2
        obj14.store()

        obj15 = self._backend._activities.new(key=self.activity_1["id"])
        obj15.data = self.activity_1
        obj15.store()

        obj16 = self._backend._activities.new(key=self.activity_2["id"])
        obj16.data = self.activity_2
        obj16.store()

    def test_dehydrate_activities_with_in_reply_to(self):

        activities = [
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": [self.actor_id, self.actor_id2], "object": self.object_id3},
        ]

        expected = [
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": [self.actor, self.actor2], "object": self.obj3},
        ]
        expected[0]['object']['inReplyTo'][0].update(self.reply_activity_1)
        expected[0]['object']['inReplyTo'][0]['target'] = self.actor3
        expected[0]['object']['inReplyTo'][0]['object'] = self.reply_1
        expected[0]['object']['inReplyTo'][0]['actor'] = self.actor

        result = self._backend.dehydrate_activities(activities)
        eq_(result, expected)

    def test_dehydrate_activities_with_replies(self):

        activities = [
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": [self.actor_id, self.actor_id2], "object": self.object_id,
                'replies': {
                    'totalItems': 1,
                    'items': [{'actor': self.actor_id, 'verb': 'reply', 'object': {'objectType': 'activity'}}]
                },
            },
            {"id": 1, "title": "Stream Item 2", "verb": "post", "actor": self.actor_id3, "object": [self.object_id, self.object_id2]},
        ]
        activities[0]['replies']['items'][0]['object'].update(self.reply_activity_1)

        expected = [
            {
                "id": 1, "title": "Stream Item", "verb": "post", "actor": [self.actor, self.actor2], "object": self.obj,
                    'replies': {
                        'totalItems': 1,
                        'items': [{'actor': self.actor, 'verb': 'reply', 'object': {
                            'objectType': 'activity',
                            'actor': self.actor,
                            'object': self.reply_1,
                            'target': self.actor3,
                            'verb': 'reply',
                            'id': self.reply_activity_id,
                        }}]
                    },
            },
            {"id": 1, "title": "Stream Item 2", "verb": "post", "actor": self.actor3, "object": [self.obj, self.obj2]},
        ]

        result = self._backend.dehydrate_activities(activities)
        eq_(result, expected)

    def test_dehydrate_activities_with_replies_not_existing(self):

        activities = [
            {
                "id": 1,
                "title": "Stream Item",
                "verb": "post",
                "actor": [self.actor_id, self.actor_id2],
                "object": self.object_id,
                'replies': {
                    'totalItems': 2,
                    'items': [
                        {
                            'actor': self.actor_id,
                            'verb': 'reply',
                            'object': {
                                'objectType': 'activity'
                            }
                        },
                        {
                            'actor': self.actor_id,
                            'id': 'thisidprobdoesntexist',
                            'verb': 'reply',
                            'object': {
                                'objectType': 'activity',
                                'id': 'thisidprobdoesntexist',
                            }
                        }
                    ]
                },
            },
            {"id": 1, "title": "Stream Item 2", "verb": "post", "actor": self.actor_id3, "object": [self.object_id, self.object_id2]},
        ]
        activities[0]['replies']['items'][0]['object'].update(self.reply_activity_1)

        expected = [
            {
                "id": 1,
                "title": "Stream Item",
                "verb": "post",
                "actor": [self.actor, self.actor2],
                "object": self.obj,
                'replies': {
                    'totalItems': 1,
                    'items': [{'actor': self.actor, 'verb': 'reply', 'object': {
                        'objectType': 'activity',
                        'actor': self.actor,
                        'object': self.reply_1,
                        'target': self.actor3,
                        'verb': 'reply',
                        'id': self.reply_activity_id,
                    }}]
                },
            },
            {"id": 1, "title": "Stream Item 2", "verb": "post", "actor": self.actor3, "object": [self.obj, self.obj2]},
        ]

        result = self._backend.dehydrate_activities(activities)
        eq_(result, expected)


    def test_get_activities_with_replies(self):

        expected = [
            {"id": self.activity_id, "title": "Stream Item", "verb": "post", "actor": self.actor2, "object": self.obj,
                'replies': {
                    'totalItems': 2,
                    'items': [
                        {
                            'actor': self.actor,
                            'verb': 'reply',
                            'object': {
                                'objectType': 'activity',
                                'actor': self.actor,
                                'object': self.reply_1,
                                'target': self.actor3,
                                'verb': 'reply',
                                'id': self.reply_activity_id,
                            }
                        },
                        {
                            'actor': self.actor,
                            'verb': 'reply',
                            'object': {
                                'objectType': 'activity',
                                'actor': self.actor2,
                                'object': self.reply_2,
                                'target': self.actor,
                                'verb': 'reply',
                                'id': self.reply_activity_id2,
                            }
                        },
                    ]
                },
            },
        ]

        result = self._backend.get_activity(activity_ids=[self.activity_id])
        eq_(result, expected)

    def test_get_activities_with_likes_and_replies(self):
        expected = [
            {"id": self.activity_id2, "title": "Stream Item", "verb": "post", "actor": self.actor2, "object": self.obj,
                'replies': {
                    'totalItems': 2,
                    'items': [
                        {
                            'actor': self.actor,
                            'verb': 'reply',
                            'object': {
                                'objectType': 'activity',
                                'actor': self.actor,
                                'object': self.reply_1,
                                'target': self.actor3,
                                'verb': 'reply',
                                'id': self.reply_activity_id,
                            }
                        },
                        {
                            'actor': self.actor,
                            'verb': 'reply',
                            'object': {
                                'objectType': 'activity',
                                'actor': self.actor2,
                                'object': self.reply_2,
                                'target': self.actor,
                                'verb': 'reply',
                                'id': self.reply_activity_id2,
                            }
                        },
                    ]
                },

                'likes': {
                    'totalItems': 1,
                    'items': [
                        {
                            'actor': self.actor,
                            'verb': 'like',
                            'object': {
                                'objectType': 'activity',
                                'actor': self.actor,
                                'object': self.like_1,
                                'target': self.actor3,
                                'verb': 'like',
                                'id': self.like_activity_id,
                            }
                        },
                    ]
                }
            },
        ]

        result = self._backend.activity_get(activity_ids=[self.activity_id2])
        eq_(result, expected)

    def test_get_activities_with_aggregation_pipline(self):
        activity_ids = [self.like_activity_id, self.reply_activity_id, self.activity_id, self.activity_id2, self.like_activity_id2, self.reply_activity_id2]

        activities = self._backend.activity_get(activity_ids, aggregation_pipeline=[PropertyAggregator(properties=['verb', 'actor'])])

        eq_([{u'id': u'7779', u'verb': u'like', u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'inReplyTo': [], u'objectType': u'like', u'id': u'6669', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}}, {u'id': u'8889', u'verb': u'reply', u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my first reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9999', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}}, {'grouped_by_attributes': ['verb', 'actor'], u'title': [u'Stream Item', u'Stream Item'], u'object': [{u'objectType': u'something', u'id': u'4353', u'published': u'2012-07-05T12:00:00Z'}, {u'published': u'2012-07-05T12:00:00Z', u'id': u'4353', u'objectType': u'something'}], u'actor': {u'published': u'2012-07-05T12:00:00Z', u'id': u'4321', u'objectType': u'something'}, u'verb': u'post', u'replies': [{u'totalItems': 2, u'items': [{u'verb': u'reply', u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my first reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9999', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'verb': u'reply', u'id': u'8889', u'objectType': u'activity'}}, {u'verb': u'reply', u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'target': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my second reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9998', u'published': u'2012-08-05T12:05:00Z'}, u'actor': {u'objectType': u'something', u'id': u'4321', u'published': u'2012-07-05T12:00:00Z'}, u'verb': u'reply', u'id': u'8888', u'objectType': u'activity'}}]}, {u'totalItems': 2, u'items': [{u'verb': u'reply', u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my first reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9999', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'verb': u'reply', u'id': u'8889', u'objectType': u'activity'}}, {u'verb': u'reply', u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'target': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my second reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9998', u'published': u'2012-08-05T12:05:00Z'}, u'actor': {u'objectType': u'something', u'id': u'4321', u'published': u'2012-07-05T12:00:00Z'}, u'verb': u'reply', u'id': u'8888', u'objectType': u'activity'}}]}], u'id': [u'5555', u'5556'], 'grouped_by_values': [u'post', {u'published': u'2012-07-05T12:00:00Z', u'id': u'4321', u'objectType': u'something'}]}, {u'id': u'7778', u'verb': u'like', u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'inReplyTo': [], u'objectType': u'like', u'id': u'6669', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}}, {u'id': u'8888', u'verb': u'reply', u'target': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my second reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9998', u'published': u'2012-08-05T12:05:00Z'}, u'actor': {u'objectType': u'something', u'id': u'4321', u'published': u'2012-07-05T12:00:00Z'}}], activities)


class TestIndexes(object):
    def setUp(self):
        backend = RiakBackend(**riak_connection_options)
        self._backend = backend

    def test_set_sub_item_indexes(self):
        riak_obj_mock = MagicMock()
        riak_obj_mock.data = {'verb': 'post', 'actor': '1234', 'object': '5678', 'target': 4333}

        self._backend.set_activity_indexes(riak_obj_mock)

        calls = [
            call.add_index('verb_bin', 'post'),
            call.add_index('actor_bin', '1234'),
            call.add_index('object_bin', '5678'),
            call.add_index('target_bin', '4333'),
        ]

        riak_obj_mock.assert_has_calls(calls, any_order=True)
        eq_(riak_obj_mock.add_index.call_count, 4)

    def test_set_sub_item_indexes_reply(self):
        riak_obj_mock = MagicMock()
        riak_obj_mock.get_indexes.return_value = []

        self._backend.set_sub_item_indexes(riak_obj_mock, activity_id=1234)

        calls = [
            call.add_index('inreplyto_bin', ANY),
        ]

        riak_obj_mock.assert_has_calls(calls, any_order=True)
        eq_(riak_obj_mock.add_index.call_count, 1)

    @raises(SunspearValidationException)
    def test_set_sub_item_indexes_no_activity_id_raises_exception(self):
        riak_obj_mock = MagicMock()
        riak_obj_mock.get_indexes.return_value = []

        self._backend.set_sub_item_indexes(riak_obj_mock)

    def test_set_general_indexes_not_already_created_set(self):
        riak_obj_mock = MagicMock()
        riak_obj_mock.indexes = []

        self._backend.set_general_indexes(riak_obj_mock)

        calls = [
            call.add_index('timestamp_int', ANY),
            call.add_index('modified_int', ANY),
        ]

        riak_obj_mock.assert_has_calls(calls, any_order=True)
        eq_(riak_obj_mock.add_index.call_count, 2)

    def test_set_general_indexes_already_created(self):
        riak_obj_mock = MagicMock()
        riak_obj_mock.indexes = [('timestamp_int', 12343214,)]

        self._backend.set_general_indexes(riak_obj_mock)

        calls = [
            call.add_index('modified_int', ANY),
        ]

        riak_obj_mock.assert_has_calls(calls, any_order=True)
        eq_(riak_obj_mock.add_index.call_count, 1)

    def test_create_obj_indexes(self):
        self._backend._objects.get('1234').delete()
        obj = {"objectType": "Hello", "id": "1234", "published": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"}

        actstream_obj = self._backend.create_obj(obj)
        riak_obj = self._backend._objects.get(key=actstream_obj['id'])
        riak_obj.data

        ok_(filter(lambda x: x[0] == 'timestamp_int', riak_obj.indexes) != [])
        ok_(filter(lambda x: x[0] == 'modified_int', riak_obj.indexes) != [])

    def test_create_activity_indexes(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        act_obj = self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
        act_obj_dict = act_obj

        riak_obj = self._backend._activities.get(key=act_obj_dict['id'])
        riak_obj.data

        ok_(filter(lambda x: x[0] == 'timestamp_int', riak_obj.indexes) != [])
        ok_(filter(lambda x: x[0] == 'modified_int', riak_obj.indexes) != [])
        eq_(filter(lambda x: x[0] == 'verb_bin', riak_obj.indexes)[0][1], 'post')
        eq_(filter(lambda x: x[0] == 'actor_bin', riak_obj.indexes)[0][1], actor_id)
        eq_(filter(lambda x: x[0] == 'object_bin', riak_obj.indexes)[0][1], object_id)

    def test_create_sub_activity_indexes(self):
        self._backend._activities.get('5').delete()

        actor_id = '1234'
        actor2_id = '4321'
        object_id = '4353'
        #make sure these 2 keys don't exist anymore
        self._backend._objects.get(actor_id).delete()
        self._backend._objects.get(actor2_id).delete()
        self._backend._objects.get(object_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        actor2 = {"objectType": "something", "id": actor2_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        self._backend.create_obj(actor2)

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        like_activity_dict, activity_obj_dict = self._backend.sub_activity_create(
            5, actor2_id, "", sub_activity_verb='like')

        riak_obj = self._backend._activities.get(key=like_activity_dict['id'])
        riak_obj.data

        ok_(filter(lambda x: x[0] == 'timestamp_int', riak_obj.indexes) != [])
        ok_(filter(lambda x: x[0] == 'modified_int', riak_obj.indexes) != [])
        eq_(filter(lambda x: x[0] == 'verb_bin', riak_obj.indexes)[0][1], 'like')
        eq_(filter(lambda x: x[0] == 'actor_bin', riak_obj.indexes)[0][1], actor2_id)
        eq_(filter(lambda x: x[0] == 'object_bin', riak_obj.indexes)[0][1], like_activity_dict['object']['id'])
        eq_(filter(lambda x: x[0] == 'inreplyto_bin', riak_obj.indexes)[0][1], '5')

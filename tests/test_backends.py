from __future__ import absolute_import

from nose.tools import ok_, eq_, set_trace

from sunspear.aggregators.property import PropertyAggregator
from sunspear.backends import RiakBackend
from sunspear.exceptions import SunspearValidationException, SunspearNotFoundException

import datetime

riak_connection_options = {
    "host_list": [{'port': 8081}],
    "defaults": {'host': '127.0.0.1'},
}


class TestRiakBackend(object):
    def setUp(self):
        self._backend = RiakBackend(**riak_connection_options)
        self._riak_client = self._backend._get_riak_client()

    def test_create_object(self):
        self._backend._objects.get('1234').delete()
        obj = {"objectType": "Hello", "id": "1234", "published": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"}

        actstream_obj = self._backend.create_object(obj)

        eq_(actstream_obj, obj)

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

        act_obj = self._backend.create_activity({"id": 5, "title": "Stream Item",
            "verb": "post",
            "actor": actor,
            "object": obj,
            "other": other
        })
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

        self._backend._objects.new(key=actor["id"]).set_data(actor).store()

        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
        ok_(self._backend._objects.get(obj["id"]).exists())
        ok_('content' not in self._backend._objects.get(actor["id"]).get_data())

        actor['content'] = "Some new content that wasn't there before."
        self._backend.create_activity({"id": 6, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
        ok_(self._backend._objects.get(obj["id"]).exists())
        eq_(actor['content'], self._backend._objects.get(actor["id"]).get_data()['content'])

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

        self._backend._objects.new(key=actor["id"]).set_data(actor).store()

        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
        ok_(self._backend._objects.get(obj["id"]).exists())
        ok_('content' not in self._backend._objects.get(actor["id"]).get_data())

        try:
            actor['content'] = "Some new content that wasn't there before."
            self._backend.create_activity({"id": 6, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj, 'client': self._riak_client})
            ok_(False)
        except Exception:
            ok_(True)
            ok_('content' not in self._backend._objects.get(actor["id"]).get_data())

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

        self._backend._objects.new(key=obj["id"]).set_data(obj).store()

        self._backend.create_activity({"id": activity_id, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        ok_(self._backend._objects.get(actor["id"]).exists())

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

        eq_(activities[0]['id'], activity_3['id'])
        eq_(activities[1]['id'], activity_4['id'])
        eq_(activities[2]['id'], activity_5['id'])

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

        result = self._backend.dehydrate_activities(activities)
        eq_(result, expected)

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

        self._backend._objects.new(key=actor["id"]).set_data(actor).store()
        self._backend._objects.new(key=actor2["id"]).set_data(actor2).store()
        self._backend._objects.new(key=actor3["id"]).set_data(actor3).store()

        self._backend._objects.new(key=obj["id"]).set_data(obj).store()
        self._backend._objects.new(key=obj2["id"]).set_data(obj2).store()

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
        reply_activity_obj = self._backend.create_reply(5, actor2_id, "This is a reply.")
        reply_activity_dict = reply_activity_obj.get_data()
        activity_obj_dict = self._backend._activities.get('5').get_data()

        eq_(reply_activity_dict['activity_author'], actor_id)
        eq_(reply_activity_dict['target_activity'], '5')
        eq_(reply_activity_dict['actor'], actor2_id)
        eq_(reply_activity_dict['verb'], 'reply')

        eq_(activity_obj_dict['replies']['totalItems'], 1)
        eq_(activity_obj_dict['replies']['items'][0]['object']['id'], reply_activity_dict['id'])
        eq_(activity_obj_dict['replies']['items'][0]['verb'], 'reply')
        eq_(activity_obj_dict['replies']['items'][0]['actor'], actor2_id)

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
        self._backend._objects.get(object_id).delete()
        self._backend._objects.get(reply_id).delete()

        published_time = datetime.datetime.utcnow()

        actor = {"objectType": "something", "id": actor_id, "published": published_time}
        obj = {"objectType": "something", "id": object_id, "published": published_time}

        #create the activity
        self._backend.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})

        #now create a reply for the activity
        reply_activity_obj = self._backend.create_reply(5, actor2_id, reply_dict)
        reply_activity_dict = reply_activity_obj.get_data()
        activity_obj_dict = self._backend._activities.get('5').get_data()

        eq_(reply_activity_dict['activity_author'], actor_id)
        eq_(reply_activity_dict['target_activity'], '5')
        eq_(reply_activity_dict['actor'], actor2_id)
        eq_(reply_activity_dict['verb'], 'reply')

        eq_(activity_obj_dict['replies']['totalItems'], 1)
        eq_(activity_obj_dict['replies']['items'][0]['object']['id'], reply_activity_dict['id'])
        eq_(activity_obj_dict['replies']['items'][0]['verb'], 'reply')
        eq_(activity_obj_dict['replies']['items'][0]['actor'], actor2_id)

    def test_create_like(self):
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
        like_activity_obj = self._backend.create_like(5, actor2_id)
        like_activity_dict = like_activity_obj.get_data()
        activity_obj_dict = self._backend._activities.get('5').get_data()

        eq_(like_activity_dict['activity_author'], actor_id)
        eq_(like_activity_dict['target_activity'], '5')
        eq_(like_activity_dict['actor'], actor2_id)
        eq_(like_activity_dict['verb'], 'like')

        eq_(activity_obj_dict['likes']['totalItems'], 1)
        eq_(activity_obj_dict['likes']['items'][0]['object']['id'], like_activity_dict['id'])
        eq_(activity_obj_dict['likes']['items'][0]['verb'], 'like')
        eq_(activity_obj_dict['likes']['items'][0]['actor'], actor2_id)

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
        like_activity_obj = self._backend.create_like(5, actor2_id)
        like_activity_dict = like_activity_obj.get_data()
        activity_obj_dict = self._backend._activities.get('5').get_data()

        eq_(like_activity_dict['activity_author'], actor_id)
        eq_(like_activity_dict['target_activity'], '5')
        eq_(like_activity_dict['actor'], actor2_id)
        eq_(like_activity_dict['verb'], 'like')

        eq_(activity_obj_dict['likes']['totalItems'], 1)
        eq_(activity_obj_dict['likes']['items'][0]['object']['id'], like_activity_dict['id'])
        eq_(activity_obj_dict['likes']['items'][0]['verb'], 'like')
        eq_(activity_obj_dict['likes']['items'][0]['actor'], actor2_id)

        #now delete the like and make sure everything is ok:
        returned_updated_activity = self._backend.delete_like(like_activity_dict['id'])
        activity_obj_dict = self._backend._activities.get('5').get_data()

        ok_('likes' not in returned_updated_activity)
        ok_('likes' not in activity_obj_dict)

    def test_delete_reply(self):
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
        reply_activity_obj = self._backend.create_reply(5, actor2_id, "This is a reply.")
        reply_activity_dict = reply_activity_obj.get_data()
        activity_obj_dict = self._backend._activities.get('5').get_data()

        eq_(reply_activity_dict['activity_author'], actor_id)
        eq_(reply_activity_dict['target_activity'], '5')
        eq_(reply_activity_dict['actor'], actor2_id)
        eq_(reply_activity_dict['verb'], 'reply')

        eq_(activity_obj_dict['replies']['totalItems'], 1)
        eq_(activity_obj_dict['replies']['items'][0]['object']['id'], reply_activity_dict['id'])
        eq_(activity_obj_dict['replies']['items'][0]['verb'], 'reply')
        eq_(activity_obj_dict['replies']['items'][0]['actor'], actor2_id)

        #now delete the reply and make sure everything is ok:
        returned_updated_activity = self._backend.delete_reply(reply_activity_dict['id'])
        activity_obj_dict = self._backend._activities.get('5').get_data()

        ok_('replies' not in returned_updated_activity)
        ok_('replies' not in activity_obj_dict)

    def test_get_activities_doesnt_crash_for_missing_activities(self):
        self._backend._activities.get('1').delete()
        self._backend._activities.get('2').delete()

        result = self._backend.get_activities(activity_ids=['1', '2'])
        eq_(result, [])


class TestRiakBackendHydrate(object):
    def setUp(self):
        self._backend = RiakBackend(**riak_connection_options)
        self._riak_client = self._backend._get_riak_client()

        self.actor_id = '1234'
        self.actor_id2 = '4321'
        self.actor_id3 = '31415'

        self.object_id = '4353'
        self.object_id2 = '7654'

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

        self._backend._objects.get(self.actor_id).delete()
        self._backend._objects.get(self.actor_id2).delete()
        self._backend._objects.get(self.actor_id3).delete()
        self._backend._objects.get(self.object_id).delete()
        self._backend._objects.get(self.object_id2).delete()
        self._backend._objects.get(self.reply_obj_id).delete()
        self._backend._objects.get(self.reply_obj_id2).delete()
        self._backend._objects.get(self.like_obj_id).delete()
        self._backend._objects.get(self.like_obj_id2).delete()

        self._backend._activities.get(self.reply_activity_id).delete()
        self._backend._activities.get(self.reply_activity_id2).delete()
        self._backend._activities.get(self.like_activity_id).delete()
        self._backend._activities.get(self.activity_id).delete()
        self._backend._activities.get(self.activity_id2).delete()

        self._backend._objects.new(key=self.actor["id"]).set_data(self.actor).store()
        self._backend._objects.new(key=self.actor2["id"]).set_data(self.actor2).store()
        self._backend._objects.new(key=self.actor3["id"]).set_data(self.actor3).store()
        self._backend._objects.new(key=self.obj["id"]).set_data(self.obj).store()
        self._backend._objects.new(key=self.obj2["id"]).set_data(self.obj2).store()
        self._backend._objects.new(key=self.reply_1["id"]).set_data(self.reply_1).store()
        self._backend._objects.new(key=self.reply_2["id"]).set_data(self.reply_2).store()
        self._backend._objects.new(key=self.like_1["id"]).set_data(self.like_1).store()
        self._backend._objects.new(key=self.like_2["id"]).set_data(self.like_2).store()

        self._backend._activities.new(key=self.reply_activity_1["id"]).set_data(self.reply_activity_1).store()
        self._backend._activities.new(key=self.reply_activity_2["id"]).set_data(self.reply_activity_2).store()
        self._backend._activities.new(key=self.like_activity_1["id"]).set_data(self.like_activity_1).store()
        self._backend._activities.new(key=self.like_activity_2["id"]).set_data(self.like_activity_2).store()
        self._backend._activities.new(key=self.activity_1["id"]).set_data(self.activity_1).store()
        self._backend._activities.new(key=self.activity_2["id"]).set_data(self.activity_2).store()

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
            {"id": 1, "title": "Stream Item", "verb": "post", "actor": [self.actor, self.actor2], "object": self.obj,
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

        result = self._backend.get_activities(activity_ids=[self.activity_id])
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

        result = self._backend.get_activities(activity_ids=[self.activity_id2])
        eq_(result, expected)

    def test_get_activities_with_aggregation_pipline(self):
        activity_ids = [self.like_activity_id, self.reply_activity_id, self.activity_id, self.activity_id2, self.like_activity_id2, self.reply_activity_id2]

        activities = self._backend.get_activities(activity_ids, aggregation_pipeline=[PropertyAggregator(properties=['verb', 'actor'])])

        eq_([{u'id': u'7779', u'verb': u'like', u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'inReplyTo': [], u'objectType': u'like', u'id': u'6669', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}}, {u'id': u'8889', u'verb': u'reply', u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my first reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9999', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}}, {'grouped_by_attributes': ['verb', 'actor'], u'title': [u'Stream Item', u'Stream Item'], u'object': [{u'objectType': u'something', u'id': u'4353', u'published': u'2012-07-05T12:00:00Z'}, {u'published': u'2012-07-05T12:00:00Z', u'id': u'4353', u'objectType': u'something'}], u'actor': {u'published': u'2012-07-05T12:00:00Z', u'id': u'4321', u'objectType': u'something'}, u'verb': u'post', u'replies': [{u'totalItems': 2, u'items': [{u'verb': u'reply', u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my first reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9999', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'verb': u'reply', u'id': u'8889', u'objectType': u'activity'}}, {u'verb': u'reply', u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'target': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my second reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9998', u'published': u'2012-08-05T12:05:00Z'}, u'actor': {u'objectType': u'something', u'id': u'4321', u'published': u'2012-07-05T12:00:00Z'}, u'verb': u'reply', u'id': u'8888', u'objectType': u'activity'}}]}, {u'totalItems': 2, u'items': [{u'verb': u'reply', u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my first reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9999', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'verb': u'reply', u'id': u'8889', u'objectType': u'activity'}}, {u'verb': u'reply', u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'target': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my second reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9998', u'published': u'2012-08-05T12:05:00Z'}, u'actor': {u'objectType': u'something', u'id': u'4321', u'published': u'2012-07-05T12:00:00Z'}, u'verb': u'reply', u'id': u'8888', u'objectType': u'activity'}}]}], u'id': [u'5555', u'5556'], 'grouped_by_values': [u'post', {u'published': u'2012-07-05T12:00:00Z', u'id': u'4321', u'objectType': u'something'}]}, {u'id': u'7778', u'verb': u'like', u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'inReplyTo': [], u'objectType': u'like', u'id': u'6669', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}}, {u'id': u'8888', u'verb': u'reply', u'target': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my second reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9998', u'published': u'2012-08-05T12:05:00Z'}, u'actor': {u'objectType': u'something', u'id': u'4321', u'published': u'2012-07-05T12:00:00Z'}}], activities)

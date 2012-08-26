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
        eq_(reply_activity_dict['target'], '5')
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
        eq_(reply_activity_dict['target'], '5')
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
        eq_(like_activity_dict['target'], '5')
        eq_(like_activity_dict['actor'], actor2_id)
        eq_(like_activity_dict['verb'], 'like')

        eq_(activity_obj_dict['likes']['totalItems'], 1)
        eq_(activity_obj_dict['likes']['items'][0]['object']['id'], like_activity_dict['id'])
        eq_(activity_obj_dict['likes']['items'][0]['verb'], 'like')
        eq_(activity_obj_dict['likes']['items'][0]['actor'], actor2_id)


class TestRiakBackendHydrate(object):
    def setUp(self):
        self._backend = RiakBackend({
            'hosts': [{'port': 8094}, {'port': 8093}, {'port': 8092}, {'port': 8091}]
        })
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

    def test_hydrate_activities_with_replies(self):

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

        result = self._backend.hydrate_activities(activities)
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

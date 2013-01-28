from __future__ import absolute_import

from nose.tools import ok_, eq_, set_trace, raises
from mock import MagicMock, call, ANY

from sunspear.exceptions import SunspearValidationException
from sunspear.aggregators.property import PropertyAggregator
from sunspear.backends.riak import RiakBackend
from sunspear.clients import SunspearClient

import datetime

riak_connection_options = {
    "host_list": [{'port': 8087}],
    "defaults": {'host': '127.0.0.1'},
}


class TestSunspearClient(object):
    def setUp(self):
        backend = RiakBackend(**riak_connection_options)
        self._backend = backend
        self._client = SunspearClient(backend)

    def test_create_obj(self):
        self._backend._objects.get('1234').delete()
        obj = {"objectType": "Hello", "id": "1234", "published": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"}

        actstream_obj = self._client.create_object(obj)

        ok_(self._backend._objects.get(key=obj['id']).exists())
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

        act_obj = self._client.create_activity({"id": 5, "title": "Stream Item", "verb": "post", "actor": actor, "object": obj})
        act_obj_dict = act_obj

        ok_(self._backend._activities.get(key=act_obj['id']).exists())
        eq_(act_obj_dict['actor'], actor)
        eq_(act_obj_dict['object'], obj)

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
        reply_activity_dict, activity_obj_dict = self._client.create_reply(5, actor2_id, "This is a reply.", extra=extra)

        eq_(extra['published'].strftime('%Y-%m-%dT%H:%M:%S') + "Z", reply_activity_dict['published'])
        eq_(extra['foo'], reply_activity_dict['foo'])

        eq_(reply_activity_dict['actor']['id'], actor2_id)
        eq_(reply_activity_dict['verb'], 'reply')

        eq_(activity_obj_dict['replies']['totalItems'], 1)
        eq_(activity_obj_dict['replies']['items'][0]['object']['id'], reply_activity_dict['id'])
        eq_(activity_obj_dict['replies']['items'][0]['verb'], 'reply')
        eq_(activity_obj_dict['replies']['items'][0]['actor']['id'], actor2_id)

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
        like_activity_dict, activity_obj_dict = self._client.create_like(5, actor2_id)

        eq_(like_activity_dict['actor']['id'], actor2_id)
        eq_(like_activity_dict['verb'], 'like')

        eq_(activity_obj_dict['likes']['totalItems'], 1)
        eq_(activity_obj_dict['likes']['items'][0]['object']['id'], like_activity_dict['id'])
        eq_(activity_obj_dict['likes']['items'][0]['verb'], 'like')
        eq_(activity_obj_dict['likes']['items'][0]['actor']['id'], actor2_id)


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

        self._client.delete_activity(act_obj_dict)
        ok_(not self._backend._activities.get(key=act_obj_dict['id']).exists())


    def test_delete_activity_with_like(self):
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
        like_activity_dict, activity_obj_dict = self._backend.create_sub_activity(5, actor2_id, "",\
            sub_activity_verb='like')

        eq_(like_activity_dict['actor']['id'], actor2_id)
        eq_(like_activity_dict['verb'], 'like')

        eq_(activity_obj_dict['likes']['totalItems'], 1)
        eq_(activity_obj_dict['likes']['items'][0]['object']['id'], like_activity_dict['id'])
        eq_(activity_obj_dict['likes']['items'][0]['verb'], 'like')
        eq_(activity_obj_dict['likes']['items'][0]['actor']['id'], actor2_id)

        #now delete the activity and make sure the like gets deleted too:
        self._client.delete_like(like_activity_dict)

        ok_(not self._backend._activities.get(like_activity_dict['id']).exists())
        ok_(not self._backend._activities.get(activity_obj_dict['likes']['items'][0]['id']).exists())


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
        like_activity_dict, activity_obj_dict = self._backend.create_sub_activity(5, actor2_id, "",\
            sub_activity_verb='like')

        eq_(like_activity_dict['actor']['id'], actor2_id)
        eq_(like_activity_dict['verb'], 'like')

        eq_(activity_obj_dict['likes']['totalItems'], 1)
        eq_(activity_obj_dict['likes']['items'][0]['object']['id'], like_activity_dict['id'])
        eq_(activity_obj_dict['likes']['items'][0]['verb'], 'like')
        eq_(activity_obj_dict['likes']['items'][0]['actor']['id'], actor2_id)

        #now delete the like and make sure everything is ok:
        returned_updated_activity = self._client.delete_like(like_activity_dict['id'])
        activity_obj_dict = self._backend._activities.get('5').get_data()

        ok_('likes' not in returned_updated_activity)
        ok_('likes' not in activity_obj_dict)


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
        reply_activity_dict, activity_obj_dict = self._backend.sub_activity_create(5, actor2_id, "This is a reply.",\
            sub_activity_verb='reply')

        eq_(reply_activity_dict['actor']['id'], actor['id'])
        eq_(reply_activity_dict['verb'], 'reply')

        eq_(activity_obj_dict['replies']['totalItems'], 1)
        eq_(activity_obj_dict['replies']['items'][0]['object']['id'], reply_activity_dict['id'])
        eq_(activity_obj_dict['replies']['items'][0]['verb'], 'reply')
        eq_(activity_obj_dict['replies']['items'][0]['actor']['id'], actor2_id)

        #now delete the reply and make sure everything is ok:
        returned_updated_activity = self._client.delete_reply(reply_activity_dict['id'])
        activity_obj_dict = self._backend._activities.get('5').get_data()

        ok_('replies' not in returned_updated_activity)
        ok_('replies' not in activity_obj_dict)

    def test_get_objects(self):
        obj1_id = '1111'
        obj2_id = '1112'
        obj3_id = '1113'
        obj4_id = '1114'

        self._backend._objects.get(obj1_id).delete()
        self._backend._objects.get(obj2_id).delete()
        self._backend._objects.get(obj3_id).delete()
        self._backend._objects.get(obj4_id).delete()

        obj1 = {"objectType": "Hello", "id": obj1_id,\
            "published": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"}
        obj2 = {"objectType": "Hello", "id": obj2_id,\
            "published": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"}
        obj3 = {"objectType": "Hello", "id": obj3_id,\
            "published": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"}
        obj4 = {"objectType": "Hello", "id": obj4_id,\
            "published": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S') + "Z"}

        self._backend.create_obj(obj1)
        self._backend.create_obj(obj2)
        self._backend.create_obj(obj3)
        self._backend.create_obj(obj4)

        objects = self._client.get_objects(object_ids=[obj1_id, obj2_id, obj3_id, obj4_id, 'xxx'])

        eq_(len(objects), 4)
        for obj in objects:
            ok_(obj['id'] in [obj1_id, obj2_id, obj3_id, obj4_id])



class TestSunnspearClientActivities(object):
    def setUp(self):
        self._backend = RiakBackend(**riak_connection_options)
        self._client = SunspearClient(self._backend)

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

        self._backend._objects.new(key=self.actor["id"]).set_data(self.actor).store()
        self._backend._objects.new(key=self.actor2["id"]).set_data(self.actor2).store()
        self._backend._objects.new(key=self.actor3["id"]).set_data(self.actor3).store()
        self._backend._objects.new(key=self.obj["id"]).set_data(self.obj).store()
        self._backend._objects.new(key=self.obj2["id"]).set_data(self.obj2).store()
        self._backend._objects.new(key=self.obj3["id"]).set_data(self.obj3).store()
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

        result = self._client.get_activities(activity_ids=[self.activity_id])
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

        result = self._client.get_activities(activity_ids=[self.activity_id2])
        eq_(result, expected)

    def test_get_activities_with_aggregation_pipline(self):
        activity_ids = [self.like_activity_id, self.reply_activity_id, self.activity_id, self.activity_id2, self.like_activity_id2, self.reply_activity_id2]

        activities = self._client.get_activities(activity_ids=activity_ids, aggregation_pipeline=[PropertyAggregator(properties=['verb', 'actor'])])

        eq_([{u'id': u'7779', u'verb': u'like', u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'inReplyTo': [], u'objectType': u'like', u'id': u'6669', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}}, {u'id': u'8889', u'verb': u'reply', u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my first reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9999', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}}, {'grouped_by_attributes': ['verb', 'actor'], u'title': [u'Stream Item', u'Stream Item'], u'object': [{u'objectType': u'something', u'id': u'4353', u'published': u'2012-07-05T12:00:00Z'}, {u'published': u'2012-07-05T12:00:00Z', u'id': u'4353', u'objectType': u'something'}], u'actor': {u'published': u'2012-07-05T12:00:00Z', u'id': u'4321', u'objectType': u'something'}, u'verb': u'post', u'replies': [{u'totalItems': 2, u'items': [{u'verb': u'reply', u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my first reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9999', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'verb': u'reply', u'id': u'8889', u'objectType': u'activity'}}, {u'verb': u'reply', u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'target': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my second reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9998', u'published': u'2012-08-05T12:05:00Z'}, u'actor': {u'objectType': u'something', u'id': u'4321', u'published': u'2012-07-05T12:00:00Z'}, u'verb': u'reply', u'id': u'8888', u'objectType': u'activity'}}]}, {u'totalItems': 2, u'items': [{u'verb': u'reply', u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my first reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9999', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'verb': u'reply', u'id': u'8889', u'objectType': u'activity'}}, {u'verb': u'reply', u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'target': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my second reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9998', u'published': u'2012-08-05T12:05:00Z'}, u'actor': {u'objectType': u'something', u'id': u'4321', u'published': u'2012-07-05T12:00:00Z'}, u'verb': u'reply', u'id': u'8888', u'objectType': u'activity'}}]}], u'id': [u'5555', u'5556'], 'grouped_by_values': [u'post', {u'published': u'2012-07-05T12:00:00Z', u'id': u'4321', u'objectType': u'something'}]}, {u'id': u'7778', u'verb': u'like', u'target': {u'objectType': u'something', u'id': u'31415', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'inReplyTo': [], u'objectType': u'like', u'id': u'6669', u'published': u'2012-08-05T12:00:00Z'}, u'actor': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}}, {u'id': u'8888', u'verb': u'reply', u'target': {u'objectType': u'something', u'id': u'1234', u'published': u'2012-07-05T12:00:00Z'}, u'object': {u'content': u'This is my second reply', u'inReplyTo': [], u'objectType': u'reply', u'id': u'9998', u'published': u'2012-08-05T12:05:00Z'}, u'actor': {u'objectType': u'something', u'id': u'4321', u'published': u'2012-07-05T12:00:00Z'}}], activities)


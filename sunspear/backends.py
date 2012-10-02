"""
Copyright 2012 Numan Sachwani <numan856@gmail.com>

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from sunspear.activitystreams.models import Object, Activity, Model, ReplyActivity


from riak import RiakPbcTransport

import uuid
import riak
import copy

JS_MAP = """
    function(value, keyData, arg) {
      if (value["not_found"]) {
        return [value];
      }
      var newValues = Riak.mapValues(value, keyData, arg);
      newValues = newValues.map(function(nv) {
        try {
            var parsedNv = JSON.parse(nv); parsedNv["timestamp"] = value.values[0].metadata.index.timestamp_int; return parsedNv;
        } catch(e) {
            return;
        }
      });
      //filter out undefinded things
      return newValues.filter(function(value){ return value; });
    }
"""

JS_REDUCE_FILTER_PROP = """
    function(value, arg) {
        return value.filter(function(obj){
            for (var filter in arg['filters']){
                if (filter in obj) {
                    for(var i in arg['filters'][filter]) {
                        if (obj[filter] == arg['filters'][filter][i]) {
                            return true;
                        }
                    }
                }
            }
            return false;
        });
    }
"""

JS_REDUCE_FILTER_AUD_TARGETTING = """
    function(value, arg) {
        var audience_targeting = ['to', 'bto', 'cc', 'bcc'];
        return value.filter(function(obj){
            for (var i in audience_targeting){
                var targeting_field = audience_targeting[i];
                if (targeting_field in obj && targeting_field in arg['filters']) {
                    for(var j in arg['filters'][targeting_field]) {
                        return obj[targeting_field].indexOf(arg['filters'][targeting_field][j]) != -1;
                    }
                }
            }
            return false;
        });
    }
"""

JS_REDUCE = """
    function(value, arg) {
      var sortFunc = function(x,y) {
        if(x["timestamp"] == y["timestamp"]) return 0;
        return x["timestamp"] > y["timestamp"] ? 1 : -1;
      }
      var newValues = Riak.filterNotFound(value);
      return newValues.sort(sortFunc);
    }
"""

JS_REDUCE_OBJS = """
    function(value, arg) {
      return Riak.filterNotFound(value);
    }
"""


class RiakBackend(object):
    def __init__(self, settings, **kwargs):

        self._riak_backend = riak.RiakClient(host="127.0.0.1", port=8081,\
            transport_class=RiakPbcTransport, transport_options={"max_attempts": 2})

        self._streams = self._riak_backend.bucket("streams")
        self._followers = self._riak_backend.bucket("followers")
        self._objects = self._riak_backend.bucket("objects")
        self._activities = self._riak_backend.bucket("activities")

    def create_object(self, object_dict):
        """
        Creates an object that can be used as part of an activity. If you specific and object with an id
        that already exists, that object is overriden
        """
        obj = Object(object_dict, bucket=self._objects)
        obj.save()

        return obj.get_riak_object()

    def create_activity(self, actstream_dict):
        """
        Creates an activity. You can provide objects for activities as dictionaries or as ids for already
        existing objects. If you provide a dictionary for an object, it is saved as a new object. If you provide
        an object id and the object does not exist, it is saved anyway, and returned as an empty dictionary when
        retriving the activity.
        """
        activity_obj = Activity(actstream_dict, bucket=self._activities, objects_bucket=self._objects)
        activity_obj.save()

        return activity_obj.get_riak_object()

    def create_reply(self, activity_id, actor, reply):
        activity = Activity({}, bucket=self._activities, objects_bucket=self._objects)
        activity.get(key=activity_id)

        reply_activity, activity = activity.create_reply(actor, reply)
        return reply_activity

    def create_like(self, activity_id, actor):
        activity = Activity({}, bucket=self._activities, objects_bucket=self._objects)
        activity.get(key=activity_id)

        like_activity, activity = activity.create_like(actor)
        return like_activity

    def delete_reply(self, reply_id):
        reply = ReplyActivity(bucket=self._activities, objects_bucket=self._objects)
        reply.delete(key=reply_id)

    def get_activities(self, activity_ids=[], filters={}, audience_targeting={}, aggregation_pipeline=[]):
        """
        Gets a list of activities. You can also group activities by providing a list of attributes to group
        by.

        :param activity_ids: The list of activities you want to retrieve
        :param group_by_attributes: A list of attributes you want to group by. The attributes can be any attribute of
        the activity. Example: ['verb', 'actor'] will ```roll up``` activities by those 2 attributes. If you have defined
        custom nested object for an activity, you can roll up by a nested attribute by using the dot notation: ``group.name``
        """
        if not activity_ids:
            return []

        activities = self._get_many_activities(activity_ids, filters=filters, audience_targeting=audience_targeting)

        def _extract_activity_keys(activity):
            keys = []
            for activity_key in Model._object_fields:
                if activity_key not in activity:
                    continue
                obj = activity.get(activity_key)
                if isinstance(obj, dict) and obj.get('objectType', None) == 'activity':
                    keys.append(obj['id'])
            return keys

        def _dehydrate_sub_activity(sub_activity, obj_list):
            for activity_key in Model._object_fields:
                if activity_key not in sub_activity:
                    continue
                if isinstance(sub_activity[activity_key], dict) and sub_activity[activity_key].get('objectType', None) == 'activity':
                    sub_activity[activity_key].update(obj_list[sub_activity[activity_key]['id']])
            return sub_activity

        #We might also have to get sub activities for things like replies and likes
        sub_activity_ids = set()
        for activity in activities:
            for collection in Activity._response_fields:
                if collection in activity and activity[collection]['items']:
                    for item in activity[collection]['items']:
                        sub_activity_ids.update(_extract_activity_keys(item))

        if sub_activity_ids:
            sub_activities = self._get_many_activities(sub_activity_ids)
            sub_activities_dict = dict(((sub_activity["id"], sub_activity,) for sub_activity in sub_activities))

            #Hydrate out any subactivities we may have
            for activity in activities:
                for collection in Activity._response_fields:
                    if collection in activity and activity[collection]['items']:
                        for i, item in enumerate(activity[collection]['items']):
                            activity[collection]['items'][i] = _dehydrate_sub_activity(item, sub_activities_dict)

        activities = self.dehydrate_activities(activities)
        original_activities = copy.deepcopy(activities)

        for aggregator in aggregation_pipeline:
            activities = aggregator.process(activities, original_activities, aggregation_pipeline)
        return activities

    def dehydrate_activities(self, activities):

        def _extract_object_keys(activity):
            keys = []
            for object_key in Model._object_fields + Activity._direct_audience_targeting_fields + Activity._indirect_audience_targeting_fields:
                if object_key not in activity:
                    continue
                objects = activity.get(object_key)
                if isinstance(objects, dict) and objects.get('objectType', None) == 'activity':
                    keys = keys + _extract_object_keys(objects)
                if isinstance(objects, list):
                    keys = keys + objects
                if isinstance(objects, basestring):
                    keys.append(objects)
            return keys

        def _dehydrate_object_keys(activity, objects_dict):
            for object_key in Model._object_fields + Activity._direct_audience_targeting_fields + Activity._indirect_audience_targeting_fields:
                if object_key not in activity:
                    continue
                activity_objects = activity.get(object_key)
                if isinstance(activity_objects, dict) and activity_objects.get('objectType', None) == 'activity':
                    activity[object_key] = _dehydrate_object_keys(activity_objects, objects_dict)
                if isinstance(activity_objects, list):
                    activity[object_key] = [objects_dict.get(obj_id, {}) for obj_id in activity_objects]
                if isinstance(activity_objects, basestring):
                    activity[object_key] = objects_dict.get(activity_objects, {})
            return activity

        #collect a list of unique object ids. We only iterate through the fields that we know
        #for sure are objects. User is responsible for hydrating all other fields.
        object_ids = set()
        for activity in activities:
            object_ids.update(_extract_object_keys(activity))

            for collection in Activity._response_fields:
                if collection in activity and activity[collection]['items']:
                    for item in activity[collection]['items']:
                        object_ids.update(_extract_object_keys(item))

        #Get the ids of the objects we have collected
        objects = self._get_many_objects(object_ids)
        objects_dict = dict(((obj["id"], obj,) for obj in objects))

        #replace the object ids with the hydrated objects
        for activity in activities:
            activity = _dehydrate_object_keys(activity, objects_dict)

            for collection in Activity._response_fields:
                if collection in activity and activity[collection]['items']:
                    for i, item in enumerate(activity[collection]['items']):
                        activity[collection]['items'][i] = _dehydrate_object_keys(item, objects_dict)

        return activities

    def _get_many_objects(self, object_ids):
        if not object_ids:
            return object_ids
        object_bucket_name = self._objects.get_name()
        objects = self._riak_backend

        for object_id in object_ids:
            objects = objects.add(object_bucket_name, str(object_id))

        return objects.map("Riak.mapValuesJson").run()

    def _get_many_activities(self, activity_ids=[], filters={}, audience_targeting={}):
        activity_bucket_name = self._activities.get_name()
        activities = self._riak_backend

        for activity_id in activity_ids:
            activities = activities.add(activity_bucket_name, str(activity_id))

        results = activities.map(JS_MAP)

        if audience_targeting:
            results = results.reduce(JS_REDUCE_FILTER_AUD_TARGETTING, options={'arg': {'filters': audience_targeting}})

        if filters:
            results = results.reduce(JS_REDUCE_FILTER_PROP, options={'arg': {'filters': filters}})

        results = results.reduce(JS_REDUCE).run()
        results = results if results is not None else []

        #riak does not return the results in any particular order (unless we sort). So,
        #we have to put the objects returned by riak back in order
        results_map = dict(map(lambda result: (result['id'], result,), results))
        reordered_results = [results_map[id] for id in activity_ids if id in results_map]

        return reordered_results

    def _get_new_uuid(self):
        return uuid.uuid1().hex

    def _get_riak_client(self):
        return self._riak_backend

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
import uuid
import datetime
import riak
import copy

from itertools import groupby

from sunspear.activitystreams.models import Object, Activity, Model, ReplyActivity
from sunspear.lib.dotdict import dotdictify

from riak import RiakPbcTransport

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

JS_FILTER_REDUCE = """
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
        Creates an object that can be used as part of an activity. The id of the object **MUST** be unique.
        If the id of the object is not provided, it will raise a ``SunspearValidationException``.
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

    def get_activities(self, activity_ids=[], group_by_attributes=[], filters={}):
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

        activities = self._get_many_activities(activity_ids, filters=filters)

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
            for collection in ['replies', 'likes']:
                if collection in activity and activity[collection]['items']:
                    for item in activity[collection]['items']:
                        sub_activity_ids.update(_extract_activity_keys(item))

        if sub_activity_ids:
            sub_activities = self._get_many_activities(sub_activity_ids)
            sub_activities_dict = dict(((sub_activity["id"], sub_activity,) for sub_activity in sub_activities))

            #Hydrate out any subactivities we may have
            for activity in activities:
                for collection in ['replies', 'likes']:
                    if collection in activity and activity[collection]['items']:
                        for i, item in enumerate(activity[collection]['items']):
                            activity[collection]['items'][i] = _dehydrate_sub_activity(item, sub_activities_dict)

        if group_by_attributes:
            _raw_group_actvities = groupby(activities, self._group_by_aggregator(group_by_attributes))
            return self.dehydrate_activities(self._aggregate_activities(_raw_group_actvities))
        else:
            return self.dehydrate_activities(activities)

    def dehydrate_activities(self, activities):

        def _extract_object_keys(activity):
            keys = []
            for object_key in Model._object_fields:
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

        def _hydrate_object_keys(activity, objects_dict):
            for object_key in Model._object_fields:
                if object_key not in activity:
                    continue
                activity_objects = activity.get(object_key)
                if isinstance(activity_objects, dict) and activity_objects.get('objectType', None) == 'activity':
                    activity[object_key] = _hydrate_object_keys(activity_objects, objects_dict)
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
            for collection in ['replies', 'likes']:
                if collection in activity and activity[collection]['items']:
                    for item in activity[collection]['items']:
                        object_ids.update(_extract_object_keys(item))

        #Get the ids of the objects we have collected
        objects = self._get_many_objects(object_ids)
        objects_dict = dict(((obj["id"], obj,) for obj in objects))

        #replace the object ids with the hydrated objects
        for activity in activities:
            activity = _hydrate_object_keys(activity, objects_dict)
            for collection in ['replies', 'likes']:
                if collection in activity and activity[collection]['items']:
                    for i, item in enumerate(activity[collection]['items']):
                        activity[collection]['items'][i] = _hydrate_object_keys(item, objects_dict)

        return activities

    def _get_many_objects(self, object_ids):
        if not object_ids:
            return object_ids
        object_bucket_name = self._objects.get_name()
        objects = self._riak_backend

        for object_id in object_ids:
            objects = objects.add(object_bucket_name, str(object_id))

        return objects.map("Riak.mapValuesJson").run()

    def _get_many_activities(self, activity_ids=[], filters={}):
        activity_bucket_name = self._activities.get_name()
        activities = self._riak_backend

        for activity_id in activity_ids:
            activities = activities.add(activity_bucket_name, str(activity_id))

        result = activities.map(JS_MAP)

        if filters:
            result = result.reduce(JS_FILTER_REDUCE, options={'arg': {'filters': filters}})

        result = result.reduce(JS_REDUCE).run()

        return result if result else []

    def _aggregate_activities(self, group_by_attributes=[], grouped_activities=[]):
        """
        Rolls up activities by group_by_attributes, collapsing all grouped activities into one activity object
        """
        grouped_activities_list = []
        for keys, group in grouped_activities:
            group_list = list(group)
            #special case. If we just grouped one activity, we don't need to aggregate
            if len(group_list) == 1:
                grouped_activities_list.append(group_list[0])
            else:
                #we have sevral activities that can be grouped together
                aggregated_activity = dotdictify({})
                aggregated_activity.update(group_list[0])

                nested_root_attributes, aggregated_activity = self._listify_attributes(group_by_attributes=group_by_attributes,\
                    activity=aggregated_activity)

                #aggregate the rest of the activities into lists
                for activity in group_list[1:]:
                    activity = dotdictify(activity)
                    for key in aggregated_activity.keys():
                        if key not in group_by_attributes and key not in nested_root_attributes:
                            aggregated_activity[key].append(activity.get(key))

                    #for nested attributes append all other attributes in a list
                    for attr in group_by_attributes:
                        if '.' in attr:
                            nested_val = activity.get(attr)
                            if nested_val is not None:
                                nested_dict, deepest_attr = attr.rsplit('.', 1)

                                for nested_dict_key, nested_dict_value in activity.get(nested_dict).items():
                                    if nested_dict_key != deepest_attr:
                                        aggregated_activity['.'.join([nested_dict, nested_dict_key])].append(nested_dict_value)

                #this might not be useful but meh, we'll see
                aggregated_activity.update({'grouped_by_values': keys})
                grouped_activities_list.append(aggregated_activity)
        return grouped_activities_list

    def _listify_attributes(self, group_by_attributes=[], activity={}):
        if not isinstance(activity, dotdictify):
            activity = dotdictify(activity)

        listified_dict = copy.copy(activity)

        nested_root_attributes = []
        #special handeling if we are grouping by a nested attribute
        #In this case, we listify all the other keys
        for attr in group_by_attributes:
            if '.' in attr:
                nested_val = activity.get(attr)
                if nested_val is not None:
                    nested_dict, deepest_attr = attr.rsplit('.', 1)
                    nested_root, rest = attr.split('.', 1)
                    #store a list of nested roots. We'll have to be careful not to listify these
                    nested_root_attributes.append(nested_root)
                    for nested_dict_key, nested_dict_value in activity.get(nested_dict).items():
                        if nested_dict_key != deepest_attr:
                            listified_dict['.'.join([nested_dict, nested_dict_key])] = [nested_dict_value]

        #now we listify all other non nested attributes
        for key, val in activity.items():
            if key not in group_by_attributes and key not in nested_root_attributes:
                listified_dict[key] = [val]

        return nested_root_attributes, listified_dict

    def _group_by_aggregator(self, group_by_attributes=[]):
        def _callback(activity):
            activity_dict = dotdictify(activity)
            matching_attributes = []

            for attribute in group_by_attributes:
                value = activity_dict.get(attribute)
                if activity_dict.get(attribute) is not None:
                    matching_attributes.append(value)
            return matching_attributes
        return _callback

    def _get_new_uuid(self):
        return uuid.uuid1().hex

    def _get_riak_client(self):
        return self._riak_backend

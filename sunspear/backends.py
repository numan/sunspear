"""
Copyright 2013 Numan Sachwani <numan856@gmail.com>

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

from sunspear.activitystreams.models import Object, Activity, Model, ReplyActivity, LikeActivity
from sunspear.exceptions import (SunspearDuplicateEntryException, SunspearInvalidActivityException,
    SunspearInvalidObjectException)

from nydus.db import create_cluster

from riak import RiakPbcTransport

import uuid
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
        if (arg['raw_filter'] != "") {
            raw_filter = eval(arg['raw_filter']);
            return value.filter(raw_filter);
        }
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
            if (arg['public'] && audience_targeting.reduce(function(prev, next){ return prev && !(next in obj) }, true)) {
                return true;
            }
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


class BaseBackend(object):
    def clear_all_objects(self):
        raise NotImplementedError()

    def clear_all_activities(self):
        raise NotImplementedError()

    #TODO: Tests
    def create_activity(self, activity, **kwargs):
        """
        Stores a new ``activity`` in the backend. If an object with the same id already exists in
        the backend, a ``SunspearDuplicateEntryException`` is raised. If an ID is not provided, one
        is generated on the fly.

        :type activity: dict
        :param activity: activity we want to store in the backend

        :raises: ``SunspearDuplicateEntryException`` if the record already exists in the database.
        :return: dict representing the new activity.
        """
        activity_id = self._extract_id(activity)
        if activity_id:
            existing_activity = self.get_activity(activity_id)
            if existing_activity:
                raise SunspearDuplicateEntryException()
        else:
            activity['id'] = self.get_new_id()

        return self.save(activity, **kwargs)

    def activity_create(self, activity, **kwargs):
        """
        Stores a new activity to the backend.

        :type activity: dict
        :param activity: a dict representing the activity

        :return: a dict representing the newly stored activity
        """
        raise NotImplementedError()

    def update_activity(self, activity, **kwargs):
        """
        Updates an existing activity in the backend. If the object does not exist,
        it is created in the backend.

        :type activity: dict
        :param activity: a dict representing the activity

        :raises: ``SunspearInvalidActivityException`` if the activity doesn't have a valid id.
        :return: a dict representing the newly stored activity
        """
        activity_id = self._extract_id(activity)
        if not activity_id:
            raise SunspearInvalidActivityException()

        return self.update(activity, **kwargs)

    def activity_update(self, activity, **kwargs):
        raise NotImplementedError()

    def delete_activity(self, activity, **kwargs):
        """
        Deletes an existing activity from the backend.

        :type activity: dict
        :param activity: a dict representing the activity

        :raises: ``SunspearInvalidActivityException`` if the activity doesn't have a valid id.
        """
        activity_id = self._extract_id(activity)
        if not activity_id:
            raise SunspearInvalidActivityException()

        return self.delete(activity, **kwargs)

    def activity_delete(self, activity, **kwargs):
        raise NotImplementedError()

    def get_activity(self, obj, **kwargs):
        """
        Gets an obj or a list of activities from the backend.

        :type obj: list
        :param obj: a list of ids of activities that will be retrieved from
            the backend.

        :return: a list of activities. If an obj is not found, a partial list should
            be returned.
        """
        return self.activity_get(self._listify(obj), **kwargs)

    def activity_get(self, activity, **kwargs):
        raise NotImplementedError()

    def create_obj(self, obj, **kwargs):
        """
        Stores a new ``obj`` in the backend. If an object with the same id already exists in
        the backend, a ``SunspearDuplicateEntryException`` is raised. If an ID is not provided, one
        is generated on the fly.

        :type obj: dict
        :param obj: obj we want to store in the backend

        :raises: ``SunspearDuplicateEntryException`` if the record already exists in the database.
        :return: dict representing the new obj.
        """
        obj_id = self._extract_id(obj)
        if obj_id:
            existing_obj = self.get_obj(obj_id)
            if existing_obj:
                raise SunspearDuplicateEntryException()
        else:
            obj['id'] = self.get_new_id()

        return self.obj_create(obj, kwargs)(obj, **kwargs)

    def obj_create(self, obj, **kwargs):
        """
        Stores a new obj to the backend.

        :type obj: dict
        :param obj: a dict representing the obj

        :return: a dict representing the newly stored obj
        """
        raise NotImplementedError()

    def update_obj(self, obj, **kwargs):
        """
        Updates an existing obj in the backend. If the object does not exist,
        it is created in the backend.

        :type obj: dict
        :param obj: a dict representing the obj

        :raises: ``SunspearInvalidObjectException`` if the obj doesn't have a valid id.
        :return: a dict representing the newly stored obj
        """
        obj_id = self._extract_id(obj)
        if not obj_id:
            raise SunspearInvalidObjectException()

        return self.obj_update(obj, **kwargs)

    def obj_update(self, obj, **kwargs):
        raise NotImplementedError()

    def delete_obj(self, obj, **kwargs):
        """
        Deletes an existing obj from the backend.

        :type obj: dict
        :param obj: a dict representing the obj

        :raises: ``SunspearInvalidObjectException`` if the obj doesn't have a valid id.
        """
        obj_id = self._extract_id(obj)
        if not obj_id:
            raise SunspearInvalidObjectException()

        return self.delete(obj, **kwargs)

    def obj_delete(self, obj, **kwargs):
        raise NotImplementedError()

    def get_obj(self, obj, **kwargs):
        """
        Gets an obj or a list of activities from the backend.

        :type obj: list
        :param obj: a list of ids of activities that will be retrieved from
            the backend.

        :return: a list of activities. If an obj is not found, a partial list should
            be returned.
        """
        return self.get(self._listify(obj), **kwargs)

    def obj_get(self, obj, **kwargs):
        raise NotImplementedError()

    def create_sub_activity(self, activity, actor, content, extra={}, sub_activity_verb="",
        sub_activity_attribute="", **kwargs):
        """
        Creates a new sub-activity as a child of ``activity``.

        :type activity: a string or dict
        :param activity: the activity we want to create the sub-item for
        :type actor: a string or dict
        :param actor: the ``object`` creating the sub-activity
        :type content: a string or dict
        :param content: a string or an ``object`` representing the content of the sub-activity
        :type extra: dict
        :param extra: additional data the is to be included as part of the ``sub-activity`` activity
        :type sub_activity_verb: string
        :param sub_activity_verb: the verb of the sub activity
        :type sub_activity_attribute: string
        :param sub_activity_attribute: the attribute the sub activity will appear under as part of the
            original ``activity``
        """
        actor_id = self._extract_id(actor)
        if not actor_id:
            raise SunspearInvalidObjectException()

        activity_id = self._extract_id(activity)
        if not activity_id:
            raise SunspearInvalidActivityException()

        return self.sub_activity_create(activity, actor, content, extra={}, sub_activity_verb="",
            sub_activity_attribute="", **kwargs)

    def sub_activity_create(self, activity, actor, content, extra={}, sub_activity_verb="",
        sub_activity_attribute="", **kwargs):
        raise NotImplementedError()

    def delete_sub_activity(self, sub_activity, sub_activity_verb, **kwargs):
        """
        Deletes a ``sub_activity`` made on an activity. This will also update the corresponding activity.

        :type sub_activity: string
        :param sub_activity: the id of the reply activity to delete
        :type sub_activity_verb: string
        :param sub_activity_verb: the verb of the sub activity
        """
        activity_id = self._extract_id(sub_activity)
        if not activity_id:
            raise SunspearInvalidActivityException()

        return self.sub_activity_delete(sub_activity, sub_activity_verb, **kwargs)

    def sub_activity_delete(self, sub_activity, sub_activity_verb, **kwargs):
        raise NotImplementedError()

    def _listify(self, list_or_string):
        """
        A simple helper that converts a single ``stream_name`` into a list of 1

        :type list_or_string: string or list
        :param list_or_string: the name of things as a string or a list of strings
        """
        if isinstance(list_or_string, basestring):
            list_or_string = [list_or_string]
        else:
            list_or_string = list_or_string

        return list_or_string

    def _extract_id(self, activity_or_id):
        """
        Helper that returns an id if the activity has one.
        """
        this_id = activity_or_id
        if isinstance(activity_or_id, dict):
            this_id = activity_or_id.get('id', None)

        return this_id

    def get_new_id(self):
        """
        Generates a new unique ID. The default implementation uses uuid1 to
        generate a unique ID.

        :return: a new id
        """
        return uuid.uuid1().hex

SUB_ACTIVITY_MAP = {
    'reply': (ReplyActivity, 'replies',),
    'like': (LikeActivity, 'likes',),
}


class RiakBackend(BaseBackend):
    def __init__(self, host_list=[], defaults={}, **kwargs):

        sunspear_defaults = {
         'transport_options': {"max_attempts": 4},
         'transport_class': RiakPbcTransport,
        }

        sunspear_defaults.update(defaults)

        hosts = {}
        for i, host_settings in enumerate(host_list):
            hosts[i] = host_settings

        self._riak_backend = create_cluster({
            'engine': 'nydus.db.backends.riak.Riak',
            'defaults': sunspear_defaults,
            'router': 'nydus.db.routers.keyvalue.PartitionRouter',
            'hosts': hosts,
        })

        self._objects = self._riak_backend.bucket("objects")
        self._activities = self._riak_backend.bucket("activities")

    def clear_all(self):
        """
        Deletes all activity stream data from riak
        """
        self.clear_all_activities()
        self.clear_all_objects()

    def clear_all_objects(self):
        """
        Deletes all objects data from riak
        """
        for key in self._objects.get_keys():
            self._objects.get(key).delete(rw='all', r='all', w='all', dw='all')
            assert not self._objects.get(key).exists()

    def clear_all_activities(self):
        """
        Deletes all activities data from riak
        """
        for key in self._activities.get_keys():
            self._activities.get(key).delete(rw='all', r='all', w='all', dw='all')
            assert not self._activities.get(key).exists()

    def obj_create(self, obj, **kwargs):
        obj = Object(obj, backend=self)
        obj_dict = obj.save()

        return obj_dict

    def obj_update(self, obj, **kwargs):
        self.obj_create(obj, **kwargs)

    def obj_get(self, obj, **kwargs):
        """
        Given a list of object ids, returns a list of objects
        """
        if not obj:
            return obj
        object_bucket_name = self._objects.get_name()
        objects = self._riak_backend

        for object_id in obj:
            objects = objects.add(object_bucket_name, str(object_id))

        results = objects.map("Riak.mapValuesJson").reduce(JS_REDUCE_OBJS).run()
        return results or []

    def activity_create(self, activity, **kwargs):
        """
        Creates an activity. You can provide objects for activities as dictionaries or as ids for already
        existing objects.

        If you provide a dictionary for an object, it is saved as a new object.

        If you provide an object id and the object does not exist, it is saved anyway, and returned as an empty
        dictionary when retriving the activity later.
        """
        activity_obj = Activity(activity, backend=self)
        activity_dict = activity_obj.save()

        return self.dehydrate_activities([activity_dict])[0]

    def activity_delete(self, activity):
        """
        Deletes an activity item and all associated sub items
        """
        activity = Activity({}, backend=self)
        activity.get(key=self._extract_id(activity))
        activity.delete()

    def activity_update(self, activity, **kwargs):
        return self.activity_create(activity, **kwargs)

    def activity_get(self, activity_ids=[], raw_filter="", filters={}, include_public=False, \
        audience_targeting={}, aggregation_pipeline=[], **kwargs):
        """
        Gets a list of activities. You can also group activities by providing a list of attributes to group
        by.

        :type activity_ids: list
        :param activity_ids: The list of activities you want to retrieve
        :type filters: dict
        :param filters: filters list of activities by key, value pair. For example, ``{'verb': 'comment'}`` would only return activities where the ``verb`` was ``comment``.
        Filters do not work for nested dictionaries.
        :type raw_filter: string
        :param raw_filter: allows you to specify a javascript function as a string. The function should return ``true`` if the activity should be included in the result set
        or ``false`` it shouldn't. If you specify a raw filter, the filters specified in ``filters`` will not run. How ever, the results will still be filtered based on
        the ``audience_targeting`` parameter.
        :type include_public: boolean
        :param include_public: If ``True``, and the ``audience_targeting`` dictionary is defined, activities that are
        not targeted towards anyone are included in the results
        :type audience_targeting: dict
        :param audience_targeting: Filters the list of activities targeted towards a particular audience. The key for the dictionary is one of ``to``, ``cc``, ``bto``, or ``bcc``.
        The values are an array of object ids
        :type aggregation_pipeline: array of ``sunspear.aggregators.base.BaseAggregator``
        :param aggregation_pipeline: modify the final list of activities. Exact results depends on the implementation of the aggregation pipeline
        """
        if not activity_ids:
            return []

        activities = self._get_many_activities(activity_ids, raw_filter=raw_filter, filters=filters, include_public=include_public, \
            audience_targeting=audience_targeting)

        activities = self.dehydrate_activities(activities)
        original_activities = copy.deepcopy(activities)

        for aggregator in aggregation_pipeline:
            activities = aggregator.process(activities, original_activities, aggregation_pipeline)
        return activities

    def create_sub_activity(self, activity, actor, content, extra={}, sub_activity_verb="",
        sub_activity_attribute="", **kwargs):
        if sub_activity_verb.lower() not in SUB_ACTIVITY_MAP:
            raise Exception('Verb not supported')
        return super(RiakBackend, self).create_sub_activity(activity, actor, content, extra,\
            sub_activity_verb, sub_activity_attribute, **kwargs)

    def sub_activity_create(self, activity, actor, content, extra={}, sub_activity_verb="",
        sub_activity_attribute="", **kwargs):
        sub_activity_model = SUB_ACTIVITY_MAP[sub_activity_verb.lower()][0]
        object_type = kwargs.get('object_type', sub_activity_verb)

        activity_id = self._extract_id(activity)
        activity_model = Activity({}, backend=self)
        activity_model.get(key=activity_id)

        sub_activity_obj, original_activity_obj = activity_model\
            .create_sub_activity(actor=actor, content=content, verb=sub_activity_verb,\
                object_type=object_type, collection=sub_activity_attribute,\
                activity_class=sub_activity_model, extra=extra)

        dehydrated_activities = self.dehydrate_activities([sub_activity_obj.get_data(), \
            original_activity_obj.get_data()])
        return dehydrated_activities[0], dehydrated_activities[1]

    def sub_activity_delete(self, sub_activity, sub_activity_verb, **kwargs):
        """
        Deletes a sub_activity made on an activity. This will also update the corresponding
        parent activity.

        :type sub_activity: string
        :param sub_activity: the id of the reply activity to delete.
        :type sub_activity_verb: string
        :param sub_activity_verb: the verb of the sub activity

        :return: a dict representing the updated parent activity
        """
        sub_activity_model = SUB_ACTIVITY_MAP[sub_activity_verb.lower()][0]
        sub_activity_id = self._extract_id(sub_activity)

        sub_activity_obj = sub_activity_model({}, backend=self)
        riak_object = sub_activity_obj.delete(key=sub_activity_id)
        return self.dehydrate_activities([riak_object.get_data()])[0]

    def dehydrate_activities(self, activities):
        """
        Takes a raw list of activities returned from riak and replace keys with contain ids for riak objects with actual riak object
        """
        activities = self._extract_sub_activities(activities)

        #collect a list of unique object ids. We only iterate through the fields that we know
        #for sure are objects. User is responsible for hydrating all other fields.
        object_ids = set()
        for activity in activities:
            object_ids.update(self._extract_object_keys(activity))

        #Get the objects for the ids we have collected
        objects = self._get_many_objects(object_ids)
        objects_dict = dict(((obj["id"], obj,) for obj in objects))

        #We also need to extract any activities that were diguised as objects. IE activities with
        #objectType=activity
        activities_in_objects_ids = set()

        #replace the object ids with the hydrated objects
        for activity in activities:
            activity = self._dehydrate_object_keys(activity, objects_dict)
            #Extract keys of any activities that were objects
            activities_in_objects_ids.update(self._extract_activity_keys(activity, skip_sub_activities=True))

        #If we did have activities that were objects, we need to hydrate those activities and
        #the objects for those activities
        if activities_in_objects_ids:
            sub_activities = self._get_many_activities(activities_in_objects_ids)
            activities_in_objects_dict = dict(((sub_activity["id"], sub_activity,) for sub_activity in sub_activities))
            for activity in activities:
                activity = self._dehydrate_sub_activity(activity, activities_in_objects_dict, skip_sub_activities=True)

                #we have to do one more round of object dehydration for our new sub-activities
                object_ids.update(self._extract_object_keys(activity))

            #now get all the objects we don't already have and for sub-activities and and hydrate them into
            #our list of activities
            object_ids -= set(objects_dict.keys())
            objects = self._get_many_objects(object_ids)
            for obj in objects:
                objects_dict[obj["id"]] = obj

            for activity in activities:
                activity = self._dehydrate_object_keys(activity, objects_dict)

        return activities

    def _extract_sub_activities(self, activities):
        """
        Extract all objects that have an objectType of activity as an activity
        """
        #We might also have to get sub activities for things like replies and likes
        activity_ids = set()
        activities_dict = dict(((activity["id"], activity,) for activity in activities))

        for activity in activities:
            activity_ids.update(self._extract_activity_keys(activity))

        if activity_ids:
            #don't bother fetching the activities we already have
            activity_ids -= set(activities_dict.keys())
            if activity_ids:
                sub_activities = self._get_many_activities(activity_ids)
                for sub_activity in sub_activities:
                    activities_dict[sub_activity["id"]] = sub_activity

            #Dehydrate out any subactivities we may have
            for activity in activities:
                activity = self._dehydrate_sub_activity(activity, activities_dict)

        return activities

    def _extract_activity_keys(self, activity, skip_sub_activities=False):
        keys = []
        for activity_key in Model._object_fields + ['inReplyTo']:
            if activity_key not in activity:
                continue
            obj = activity.get(activity_key)
            if isinstance(obj, dict):
                if obj.get('objectType', None) == 'activity':
                    keys.append(obj['id'])
                if obj.get('inReplyTo', None):
                    [keys.append(in_reply_to_obj['id']) for in_reply_to_obj in obj['inReplyTo']]

        if not skip_sub_activities:
            for collection in Activity._response_fields:
                if collection in activity and activity[collection]['items']:
                    for item in activity[collection]['items']:
                        keys.extend(self._extract_activity_keys(item))
        return keys

    def _dehydrate_sub_activity(self, sub_activity, obj_list, skip_sub_activities=False):
        for activity_key in Model._object_fields:
            if activity_key not in sub_activity:
                continue
            if isinstance(sub_activity[activity_key], dict):
                if sub_activity[activity_key].get('objectType', None) == 'activity':
                    sub_activity[activity_key].update(obj_list[sub_activity[activity_key]['id']])
                if sub_activity[activity_key].get('inReplyTo', None):
                    for i, in_reply_to_obj in enumerate(sub_activity[activity_key]['inReplyTo']):
                        sub_activity[activity_key]['inReplyTo'][i]\
                            .update(obj_list[sub_activity[activity_key]['inReplyTo'][i]['id']])

        if not skip_sub_activities:
            for collection in Activity._response_fields:
                if collection in sub_activity and sub_activity[collection]['items']:
                    for i, item in enumerate(sub_activity[collection]['items']):
                        sub_activity[collection]['items'][i] = self._dehydrate_sub_activity(item, obj_list)

        return sub_activity

    def _extract_object_keys(self, activity, skip_sub_activities=False):
        keys = []
        for object_key in Model._object_fields + Activity._direct_audience_targeting_fields \
            + Activity._indirect_audience_targeting_fields:
            if object_key not in activity:
                continue
            objects = activity.get(object_key)
            if isinstance(objects, dict):
                if objects.get('objectType', None) == 'activity':
                    keys = keys + self._extract_object_keys(objects)
                if objects.get('inReplyTo', None):
                    [keys.extend(self._extract_object_keys(in_reply_to_obj, skip_sub_activities=skip_sub_activities)) \
                        for in_reply_to_obj in objects['inReplyTo']]
            if isinstance(objects, list):
                for item in objects:
                    if isinstance(item, basestring):
                        keys.append(item)
            if isinstance(objects, basestring):
                keys.append(objects)

        if not skip_sub_activities:
            for collection in Activity._response_fields:
                if collection in activity and activity[collection]['items']:
                    for item in activity[collection]['items']:
                        keys.extend(self._extract_object_keys(item))
        return keys

    def _dehydrate_object_keys(self, activity, objects_dict, skip_sub_activities=False):
        for object_key in Model._object_fields + Activity._direct_audience_targeting_fields \
            + Activity._indirect_audience_targeting_fields:
            if object_key not in activity:
                continue
            activity_objects = activity.get(object_key)
            if isinstance(activity_objects, dict):
                if activity_objects.get('objectType', None) == 'activity':
                    activity[object_key] = self._dehydrate_object_keys(activity_objects, objects_dict, skip_sub_activities=skip_sub_activities)
                if activity_objects.get('inReplyTo', None):
                    for i, in_reply_to_obj in enumerate(activity_objects['inReplyTo']):
                        activity_objects['inReplyTo'][i] = \
                            self._dehydrate_object_keys(activity_objects['inReplyTo'][i], \
                                objects_dict, skip_sub_activities=skip_sub_activities)
            if isinstance(activity_objects, list):
                for i, obj_id in enumerate(activity_objects):
                    if isinstance(activity[object_key][i], basestring):
                        activity[object_key][i] = objects_dict.get(obj_id, {})
            if isinstance(activity_objects, basestring):
                activity[object_key] = objects_dict.get(activity_objects, {})

        if not skip_sub_activities:
            for collection in Activity._response_fields:
                if collection in activity and activity[collection]['items']:
                    for i, item in enumerate(activity[collection]['items']):
                        activity[collection]['items'][i] = self._dehydrate_object_keys(item, objects_dict)
        return activity

    def _get_many_activities(self, activity_ids=[], raw_filter="", filters={}, include_public=False, audience_targeting={}):
        """
        Given a list of activity ids, returns a list of activities from riak.

        :type activity_ids: list
        :param activity_ids: The list of activities you want to retrieve
        :type raw_filter: string
        :param raw_filter: allows you to specify a javascript function as a string. The function should return ``true`` if the activity should be included in the result set
        or ``false`` it shouldn't. If you specify a raw filter, the filters specified in ``filters`` will not run. How ever, the results will still be filtered based on
        the ``audience_targeting`` parameter.
        :type filters: dict
        :param filters: filters list of activities by key, value pair. For example, ``{'verb': 'comment'}`` would only return activities where the ``verb`` was ``comment``.
        Filters do not work for nested dictionaries.
        :type include_public: boolean
        :param include_public: If ``True``, and the ``audience_targeting`` dictionary is defined, activities that are
        not targeted towards anyone are included in the results
        :type audience_targeting: dict
        :param audience_targeting: Filters the list of activities targeted towards a particular audience. The key for the dictionary is one of ``to``, ``cc``, ``bto``, or ``bcc``.
        """
        activity_bucket_name = self._activities.get_name()
        activities = self._riak_backend

        for activity_id in activity_ids:
            activities = activities.add(activity_bucket_name, str(activity_id))

        results = activities.map(JS_MAP)

        if audience_targeting:
            results = results.reduce(JS_REDUCE_FILTER_AUD_TARGETTING, options={'arg': {'public': include_public, 'filters': audience_targeting}})

        if filters or raw_filter:
            results = results.reduce(JS_REDUCE_FILTER_PROP, options={'arg': {'raw_filter': raw_filter, 'filters': filters}})

        results = results.reduce(JS_REDUCE).run()
        results = results or []

        #riak does not return the results in any particular order (unless we sort). So,
        #we have to put the objects returned by riak back in order
        results_map = dict(map(lambda result: (result['id'], result,), results))
        reordered_results = [results_map[id] for id in activity_ids if id in results_map]

        return reordered_results

    def _extract_id(self, activity_or_id):
        """
        Helper that returns an id if the activity has one.
        """
        this_id = None
        if isinstance(activity_or_id, basestring):
            this_id = activity_or_id
        elif isinstance(activity_or_id, dict):
            this_id = activity_or_id.get('id', None)
        else:
            try:
                this_id = str(activity_or_id)
            except:
                pass
        return this_id

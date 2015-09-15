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
from __future__ import absolute_import

from sunspear.activitystreams.models import Object, Activity, Model
from sunspear.exceptions import (SunspearValidationException)
from sunspear.backends.base import BaseBackend, SUB_ACTIVITY_MAP

from riak import RiakClient

import uuid
import copy
import datetime
import calendar

__all__ = ('RiakBackend', )


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
      // filter out undefined things
      return newValues.filter(function(value){ return value; });
    }
"""

JS_REDUCE_FILTER_PROP = """
    function(value, arg) {

        // If filters is `null`, that means ONLY filter by the raw filter (if provided). For all other
        // values, we return all activities that match ANY filter. In particular, this means that an empty filters
        // object will filter out all activities, i.e. never return any activities, because no filter matched.
        var has_filters = arg.filters !== null;
        var matchall = function() { return true; };

        // filter functions
        var raw_filter = matchall;
        var matches_filters = matchall;

        if (arg['raw_filter'] != '') {
            raw_filter = eval(arg['raw_filter']);
        }

        if (has_filters) {
            matches_filters = function(obj) {
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
            };
        }

        return value.filter(function(obj) {
            return raw_filter(obj) && matches_filters(obj);
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

JS_MAP_OBJS = """
    function(value, keyData, arg) {
      if (value["not_found"]) {
        return [value];
      }
      var newValues = Riak.mapValues(value, keyData, arg);
      newValues = newValues.map(function(nv) {
        try {
            var parsedNv = JSON.parse(nv);
            return parsedNv;
        } catch(e) {
            return;
        }
      });
      //filter out undefinded things
      return newValues.filter(function(value){ return value; });
    }
"""

JS_REDUCE_OBJS = """
    function(value, arg) {
        return Riak.filterNotFound(value);
    }
"""


class RiakBackend(BaseBackend):
    custom_epoch = datetime.datetime(month=1, day=1, year=2013)

    def __init__(
        self, protocol="pbc", nodes=[], objects_bucket_name="objects",
            activities_bucket_name="activities", **kwargs):

        self._riak_backend = RiakClient(protocol=protocol, nodes=nodes)

        r_value = kwargs.get("r")
        w_value = kwargs.get("w")
        dw_value = kwargs.get("dw")
        pr_value = kwargs.get("pr")
        pw_value = kwargs.get("pw")

        self._objects = self._riak_backend.bucket(objects_bucket_name)
        self._activities = self._riak_backend.bucket(activities_bucket_name)

        if r_value:
            self._objects.r = r_value
            self._activities.r = r_value

        if w_value:
            self._objects.w = w_value
            self._activities.w = w_value

        if dw_value:
            self._objects.dw = dw_value
            self._activities.dw = dw_value

        if pr_value:
            self._objects.pr = pr_value
            self._activities.pr = pr_value

        if pw_value:
            self._objects.pw = pw_value
            self._activities.pw = pw_value

    def clear_all(self, **kwargs):
        """
        Deletes all activity stream data from riak
        """
        self.clear_all_activities()
        self.clear_all_objects()

    def clear_all_objects(self, **kwargs):
        """
        Deletes all objects data from riak
        """
        for key in self._objects.get_keys():
            self._objects.get(key).delete(r='all', w='all', dw='all')
            assert not self._objects.get(key).exists

    def clear_all_activities(self, **kwargs):
        """
        Deletes all activities data from riak
        """
        for key in self._activities.get_keys():
            self._activities.get(key).delete(r='all', w='all', dw='all')
            assert not self._activities.get(key).exists

    def obj_exists(self, obj, **kwargs):
        obj_id = self._extract_id(obj)
        return self._objects.get(obj_id).exists

    def activity_exists(self, activity, **kwargs):
        activity_id = self._extract_id(activity)
        return self._activities.get(activity_id).exists

    def obj_create(self, obj, **kwargs):
        obj = Object(obj, backend=self)

        obj.validate()
        obj_dict = obj.get_parsed_dict()

        key = self._extract_id(obj_dict)

        riak_obj = self._objects.new(key=key)
        riak_obj.data = obj_dict
        riak_obj = self.set_general_indexes(riak_obj)

        riak_obj.store()

        #finally save the data
        return obj_dict

    def set_general_indexes(self, riak_object):
        """
        Sets the default riak 2Is.

        :type riak_object: RiakObject
        :param riak_object: a RiakObject representing the model of  the class
        """
        if not filter(lambda x: x[0] == "timestamp_int", riak_object.indexes):
            riak_object.add_index("timestamp_int", self._get_timestamp())

        riak_object.remove_index('modified_int')
        riak_object.add_index("modified_int", self._get_timestamp())
        return riak_object

    def obj_update(self, obj, **kwargs):
        self.obj_create(obj, **kwargs)

    def obj_get(self, obj, **kwargs):
        """
        Given a list of object ids, returns a list of objects
        """
        if not obj:
            return obj
        object_bucket_name = self._objects.name
        objects = self._riak_backend

        for o in obj:
            objects = objects.add(object_bucket_name, self._extract_id(o))

        results = objects.map(JS_MAP_OBJS).reduce(JS_REDUCE_OBJS).run()
        return results or []

    def obj_delete(self, obj, **kwargs):
        obj_id = self._extract_id(obj)
        self._objects.new(key=obj_id).delete()

    def activity_create(self, activity, **kwargs):
        """
        Creates an activity. You can provide objects for activities as dictionaries or as ids for already
        existing objects.

        If you provide a dictionary for an object, it is saved as a new object.

        If you provide an object id and the object does not exist, it is saved anyway, and returned as an empty
        dictionary when retriving the activity later.
        """
        activity = Activity(activity, backend=self)

        activity.validate()
        activity_dict = activity.get_parsed_dict()

        key = self._extract_id(activity_dict)

        riak_obj = self._activities.new(key=key)
        riak_obj.data = activity_dict
        riak_obj = self.set_activity_indexes(self.set_general_indexes(riak_obj))
        if activity_dict['verb'] in SUB_ACTIVITY_MAP:
            riak_obj = self.set_sub_item_indexes(riak_obj, **kwargs)

        riak_obj.store()

        return self.dehydrate_activities([riak_obj.data])[0]

    def set_activity_indexes(self, riak_object):
        """
        Store indexes specific to an ``Activity``. Stores the following indexes:
        1. ``verb`` of the ``Activity``
        2. ``actor`` of the ``Activity``
        3. ``object`` of the ``Activity``
        4. if target is defined, verb for the ``target`` of the Activity

        :type riak_object: RiakObject
        :param riak_object: a RiakObject representing the model of  the class
        """
        _dict = riak_object.data

        riak_object.remove_index('verb_bin')
        riak_object.remove_index('actor_bin')
        riak_object.remove_index('object_bin')
        riak_object.add_index("verb_bin", self._extract_id(_dict['verb']))
        riak_object.add_index("actor_bin", self._extract_id(_dict['actor']))
        riak_object.add_index("object_bin", self._extract_id(_dict['object']))
        if 'target' in _dict and _dict.get("target"):
            riak_object.remove_index('target_bin')
            riak_object.add_index("target_bin", self._extract_id(_dict['target']))

        return riak_object

    def activity_delete(self, activity, **kwargs):
        """
        Deletes an activity item and all associated sub items
        """
        activity_dict = self.get_activity(activity, **kwargs)[0]
        for response_field in Activity._response_fields:
            if response_field in activity_dict:
                for response_item in activity_dict[response_field]['items']:
                    self._activities.get(response_item['id']).delete()
        self._activities.get(self._extract_id(activity_dict)).delete()

    def activity_update(self, activity, **kwargs):
        return self.activity_create(activity, **kwargs)

    def activity_get(
        self, activity_ids=[], raw_filter="", filters={}, include_public=False,
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

        :return: list -- a list of activities matching ``activity_ids``. If the activities is not found, it is not included in the result set.
            Activities are returned in the order of ids provided.
        """
        activity_ids = map(self._extract_id, activity_ids)
        if not activity_ids:
            return []

        activities = self._get_many_activities(
            activity_ids, raw_filter=raw_filter, filters=filters, include_public=include_public,
            audience_targeting=audience_targeting)

        activities = self.dehydrate_activities(activities)
        original_activities = copy.deepcopy(activities)

        for aggregator in aggregation_pipeline:
            activities = aggregator.process(activities, original_activities, aggregation_pipeline)
        return activities

    def create_sub_activity(self, activity, actor, content, extra={}, sub_activity_verb="", **kwargs):
        if sub_activity_verb.lower() not in SUB_ACTIVITY_MAP:
            raise Exception('Verb not supported')
        return super(RiakBackend, self).create_sub_activity(
            activity, actor, content, extra=extra,
            sub_activity_verb=sub_activity_verb, **kwargs)

    def sub_activity_create(
        self, activity, actor, content, extra={}, sub_activity_verb="",
            **kwargs):
        sub_activity_model = SUB_ACTIVITY_MAP[sub_activity_verb.lower()][0]
        sub_activity_attribute = SUB_ACTIVITY_MAP[sub_activity_verb.lower()][1]
        object_type = kwargs.get('object_type', sub_activity_verb)

        activity_id = self._extract_id(activity)
        activity_model = Activity(self._activities.get(key=activity_id).data, backend=self)

        sub_activity_obj, original_activity_obj = activity_model\
            .get_parsed_sub_activity_dict(
                actor=actor, content=content, verb=sub_activity_verb,
                object_type=object_type, collection=sub_activity_attribute,
                activity_class=sub_activity_model, extra=extra)

        sub_activity_obj = self.create_activity(sub_activity_obj, activity_id=original_activity_obj['id'])

        original_activity_obj[sub_activity_attribute]['items'][0]['actor'] = sub_activity_obj['actor']['id']
        original_activity_obj[sub_activity_attribute]['items'][0]['id'] = sub_activity_obj['id']
        original_activity_obj[sub_activity_attribute]['items'][0]['published'] = sub_activity_obj['published']
        original_activity_obj[sub_activity_attribute]['items'][0]['object']['id'] = sub_activity_obj['id']
        original_activity_obj = self.activity_update(original_activity_obj)

        return sub_activity_obj, original_activity_obj

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

        sub_activity_riak_model = self._activities.get(sub_activity_id)
        if sub_activity_riak_model.data['verb'] != sub_activity_model.sub_item_verb:
            raise SunspearValidationException("Trying to delete something that is not a {}.".format(sub_activity_model.sub_item_verb))

        #clean up the reference from the original activity
        in_reply_to_key = filter(lambda x: x[0] == 'inreplyto_bin', sub_activity_riak_model.indexes)[0][1]
        activity = self._activities.get(key=in_reply_to_key)
        activity_data = activity.data
        activity_data[sub_activity_model.sub_item_key]['totalItems'] -= 1
        activity_data[sub_activity_model.sub_item_key]['items'] = filter(
            lambda x: x["id"] != sub_activity_id,
            activity_data[sub_activity_model.sub_item_key]['items'])

        updated_activity = self.update_activity(activity_data, **kwargs)
        self.delete_activity(sub_activity_id)

        return updated_activity

    def set_sub_item_indexes(self, riak_object, **kwargs):
        """
        Store indexes specific to a sub-activity. Stores the following indexes:
        1. id of the the parent ``Activity`` of this sub-activity

        :type riak_object: RiakObject
        :param riak_object: a RiakObject representing the model of  the class
        """
        original_activity_id = kwargs.get('activity_id')
        if not original_activity_id:
            raise SunspearValidationException()
        riak_object.remove_index('inreplyto_bin')
        riak_object.add_index("inreplyto_bin", str(original_activity_id))

        return riak_object

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
        objects = self.get_obj(object_ids)
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
            objects = self.get_obj(object_ids)
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
                    dehydrated_sub_items = []
                    for i, item in enumerate(sub_activity[collection]['items']):
                        try:
                            dehydrated_sub_items.append(self._dehydrate_sub_activity(item, obj_list))
                        except KeyError, e:
                            pass
                        sub_activity[collection]['items'] = dehydrated_sub_items
                        sub_activity[collection]['totalItems'] = len(dehydrated_sub_items)

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

    def _get_many_activities(self, activity_ids=[], raw_filter="", filters=None, include_public=False, audience_targeting={}):
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
        activity_bucket_name = self._activities.name
        activities = self._riak_backend

        for activity_id in activity_ids:
            activities = activities.add(activity_bucket_name, str(activity_id))

        results = activities.map(JS_MAP)

        if audience_targeting:
            results = results.reduce(JS_REDUCE_FILTER_AUD_TARGETTING, options={'arg': {'public': include_public, 'filters': audience_targeting}})

        if filters or raw_filter:
            # An empty `filters` dict would cause all activities to be filtered out. If you wanted that effect, you
            # wouldn't have to call this function, so let's assume that an empty dict is a typical default value and
            # should denote that there are no filters to apply.
            filters = filters or None
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
            try:
                this_id = str(this_id)
            except:
                pass
        else:
            try:
                this_id = str(activity_or_id)
            except:
                pass
        return this_id

    def _get_timestamp(self):
        """
        returns a unix timestamp representing the ``datetime`` object
        """
        dt_obj = datetime.datetime.utcnow()
        return long((calendar.timegm(dt_obj.utctimetuple()) * 1000)) + (dt_obj.microsecond / 1000)

    def get_new_id(self):
        """
        Generates a new unique ID. The default implementation uses uuid1 to
        generate a unique ID.

        :return: a new id
        """
        return uuid.uuid1().hex
        # now = datetime.datetime.utcnow()
        # return str(long(calendar.timegm(now.utctimetuple()) - calendar.timegm(self.custom_epoch.utctimetuple())) + now.microsecond)

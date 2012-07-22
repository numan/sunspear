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

from sunspear.activitystreams.models import Object, Activity
from sunspear.lib.dotdict import dotdictify

from riak import RiakPbcTransport


class RiakBackend(object):
    def __init__(self, settings, **kwargs):

        self._riak_backend = riak.RiakClient(host="127.0.0.1", port=8081,\
            transport_class=RiakPbcTransport, transport_options={"max_attempts": 2})

        self._streams = self._riak_backend.bucket("streams")
        self._followers = self._riak_backend.bucket("followers")
        self._objects = self._riak_backend.bucket("objects")
        self._activities = self._riak_backend.bucket("activities")

    def create_object(self, object_dict):
        obj = Object(object_dict, bucket=self._objects)
        obj.save()

        return obj.get_riak_object()

    def create_activity(self, actstream_dict):
        activity_obj = Activity(actstream_dict, bucket=self._activities, objects_bucket=self._objects)
        activity_obj.save()

        return activity_obj.get_riak_object()

    def create_stream(self, name):
        stream_id = self._get_new_uuid()
        stream_obj = Object({
            "id": stream_id,
            "displayName": name,
            "published": datetime.datetime.utcnow(),
        }, bucket=self._streams)
        stream_obj.save()
        return stream_obj.get_riak_object()

    def get_activities(self, activity_ids=[], groupby_list=[]):
        if not activity_ids:
            return []

        activities = self._get_many_activities(activity_ids)

        if groupby_list:
            _raw_group_actvities = groupby(activities, self._group_by_aggregator(groupby_list))
            return self._aggregate_activities(_raw_group_actvities)
        else:
            return activities

    def _get_many_activities(self, activity_ids=[]):
        activity_buckey_name = self._activities.get_name()
        activities = self._riak_backend

        for activity_id in activity_ids:
            activities = activities.add(activity_buckey_name, str(activity_id))

        return activities.map("Riak.mapValuesJson").run()

    def _aggregate_activities(self, group_by_attributes=[], grouped_activities=[]):
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

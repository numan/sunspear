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

from sunspear.activitystreams.models import Object, Activity


class RiakBackend(object):
    def __init__(self, settings, **kwargs):

        self._riak_backend = riak.RiakClient(host="127.0.0.1", port=8091)

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

    def _get_new_uuid(self):
        return uuid.uuid1().hex

    def _get_riak_client(self):
        return self._riak_backend

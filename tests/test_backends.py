from __future__ import absolute_import

from nose.tools import ok_, eq_, raises, set_trace

from sunspear.backends import RiakBackend

import datetime


class TestRiakBackend(object):
    def setUp(self):
        self._backend = RiakBackend({
            'hosts': [{'port': 8094}, {'port': 8093}, {'port': 8092}, {'port': 8091}]
        })
        self._riak_client = self._backend._get_riak_client()

    def test_create_stream(self):
        stream_name = "user1:timeline"
        stream = self._backend.create_stream(stream_name)
        stream_data = stream.get_data()

        saved_stream = self._backend._streams.get(stream.get_key())
        saved_stream_dict = saved_stream.get_data()

        eq_(stream_data, saved_stream_dict)
        saved_stream.delete()

    def test_create_object(self):
        for stream in self._riak_client.index("objects", 'clientid_bin', '1234').run():
            self._backend._objects.get(stream.get_key()).delete()

        actstream_obj = self._backend.create_object({"displayName": "Hello", "id": 1234, "published": datetime.datetime.utcnow()})

        ok_(actstream_obj.get_key())
        saved_obj = self._backend._objects.get(actstream_obj.get_key())
        saved_obj_dict = saved_obj.get_data()

        eq_(actstream_obj.get_data(), saved_obj_dict)
        actstream_obj.delete()

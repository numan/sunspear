from __future__ import absolute_import

from nose.tools import ok_, eq_, raises, set_trace

from sunspear.backends import RiakBackend


class TestRiakBackend(object):
    def setUp(self):
        self._backend = RiakBackend({
            'hosts': [{'port': 8094}, {'port': 8093}, {'port': 8092}, {'port': 8091}]
        })
        self._riak_client = self._backend._get_backend()

    def test_create_stream(self):
        stream_name = "user1:timeline"
        stream = self._backend.create_stream(stream_name)
        stream_data = stream.get_data()

        saved_stream = self._backend._streams.get(stream_name)
        saved_stream_dict = saved_stream.get_data()

        eq_(stream_data, saved_stream_dict)

from sunspear.exceptions import SunspearValidationException, SunspearInvalidConfigurationError
from sunspear.lib.rfc3339 import rfc3339

import uuid
import datetime
import calendar


class Model(object):
    _required_fields = []
    _media_fields = []
    _reserved_fields = []
    _object_fields = ['actor', 'generator', 'object', 'provider', 'target', 'author'
        'image']
    _datetime_fields = ['published', 'updated']

    def __init__(self, object_dict={}, riak_object=None):
        self._riak_object = riak_object
        self._dict = {}
        for key, value in object_dict.iteritems():
            if key in self._media_fields:
                self._dict[key] = MediaLink(value)
            elif key in self._object_fields:
                self._dict[key] = Object(value)
            else:
                self._dict[key] = value

    def validate(self):
        for field in self._required_fields:
            if not self._dict.get(field, None):
                raise SunspearValidationException("Required field missing: %s" % field)

        for field in self._reserved_fields:
            if self._dict.get(field, None):
                raise SunspearValidationException("Reserved field name used: %s" % field)

        for field in self._media_fields:
            if self._dict.get(field, None):
                self._dict.get(field).validate()

        for field in self._object_fields:
            if self._dict.get(field, None):
                self._dict.get(field).validate()

    def parse_data(self, data):
        _parsed_data = data.copy()

        for d in self._datetime_fields:
            if d in _parsed_data.keys() and _parsed_data[d]:
                _parsed_data[d] = rfc3339(_parsed_data[d], utc=True, use_system_timezone=False)
        for c in self._object_fields:
            if c in _parsed_data.keys() and _parsed_data[c]:
                _parsed_data[c] = _parsed_data[c].get_dict()
        for k, v in _parsed_data.items():
            if v == []:
                _parsed_data[k] = None

        return _parsed_data

    def _parse_date(self, date=None):
        dt = datetime.datetime.utcnow() if date is None or not isinstance(date, datetime.datetime) else date
        return rfc3339(dt)

    def _get_timestamp(self):
        now = datetime.datetime.utcnow()
        return long(str(calendar.timegm(now.timetuple())) + now.strftime("%f"))

    def _get_new_uuid(self):
        return uuid.uuid1().hex

    def get_dict(self):
        return self._dict

    def riak_validate(self):
        return True

    def set_indexes(self, riak_object):
        #store a secondary index so we can search by it to check for duplicates
        riak_object.add_index("clientid_bin", str(self._dict["id"]))
        riak_object.add_index("timestamp_int", self._get_timestamp())
        return riak_object

    def save(self, riak_object=None):
        _riak_object = None
        if riak_object is None and self._riak_object is None:
            raise SunspearInvalidConfigurationError("You must pass a riak object to save() or in the constructor.")
        if self._riak_object is not None:
            _riak_object = self._riak_object
        else:
            self._riak_object = _riak_object = riak_object

        self.validate()
        self.riak_validate()

        parsed_data = self.parse_data(self._dict)

        _riak_object.set_data(parsed_data)
        _riak_object = self.set_indexes(_riak_object)

        _riak_object.store()
        return _riak_object

    def __getitem__(self, key):
        return self._dict[key]


class Activity(Model):
    _required_fields = ['id', 'title', 'verb', 'actor', 'object']
    _media_fields = ['icon']
    _reserved_fields = ['published', 'updated']


class Object(Model):
    _required_fields = ['displayName', 'id', 'published']
    _media_fields = ['image']

    def riak_validate(self):
        #bad...
        client = self._riak_object._client
        result = client.index(self._riak_object.get_bucket().get_name(), 'clientid_bin', str(self._dict["id"])).run()

        if len(result) > 0:
            raise SunspearValidationException("Object with ID already exists")


class MediaLink(Model):
    _required_fields = ['url']

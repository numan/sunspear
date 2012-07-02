from sunspear.exceptions import SunspearValidationException


class Model(object):
    _required_fields = []
    _media_fields = []
    _object_fields = []
    _reserved_fields = []

    def __init__(self, object_dict={}):
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

    def __getitem__(self, key):
        return self._dict[key]

    def get_dict(self):
        return self._dict


class Activity(Model):
    _required_fields = ['id', 'title', 'verb', 'actor', 'object']
    _media_fields = ['icon']
    _object_fields = ['actor', 'object', 'target', 'provider', 'generator']
    _reserved_fields = ['published', 'updated']


class Object(Model):
    _required_fields = ['displayName', 'id', 'published']
    _media_fields = ['image']
    _object_fields = ['author', 'objectType']


class MediaLink(Model):
    _required_fields = ['url']

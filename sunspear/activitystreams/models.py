from sunspear.exceptions import SunspearValidationException

from sunspear.lib.rfc3339 import rfc3339

from dateutil.parser import parse

import datetime

__all__ = ('Model', 'Activity', 'ReplyActivity', 'LikeActivity',
    'Object', 'MediaLink', )


class Model(object):
    _required_fields = []
    _media_fields = []
    _reserved_fields = []
    _object_fields = ['actor', 'generator', 'object', 'provider', 'target', 'author']
    _datetime_fields = ['published', 'updated']
    _response_fields = []
    _direct_audience_targeting_fields = []
    _indirect_audience_targeting_fields = []

    def __init__(self, object_dict, backend, *args, **kwargs):
        self._backend = backend
        self._dict = self._set_defaults(object_dict)

    def _set_defaults(self, model_dict):
        """
        Stringifies the id even if it is a integer value

        :type model_dict: dict
        :param model_dict: The dictionary describing the model
        """
        if "id" not in model_dict or not model_dict["id"]:
            model_dict["id"] = self.get_new_id()

        if 'id' in model_dict:
            model_dict['id'] = str(model_dict['id'])
        return model_dict

    def validate(self):
        for field in self._required_fields:
            if not self._dict.get(field, None):
                raise SunspearValidationException("Required field missing: %s" % field)

        for field in self._reserved_fields:
            if self._dict.get(field, None) is not None\
                and field not in ['updated', 'published']:
                #updated and publised are special eceptions because if they are in reserved fields, the'll be overridden
                raise SunspearValidationException("Reserved field name used: %s" % field)

        for field in self._media_fields:
            if self._dict.get(field, None) and isinstance(self._dict.get(field, None), dict):
                MediaLink(self._dict.get(field), backend=self._backend).validate()

        for field in self._object_fields:
            if self._dict.get(field, None) and isinstance(self._dict.get(field, None), dict):
                Object(self._dict.get(field), backend=self._backend).validate()

        for field in self._direct_audience_targeting_fields + self._indirect_audience_targeting_fields:
            if self._dict.get(field, None):
                for sub_obj in self._dict.get(field):
                    if sub_obj and isinstance(sub_obj, dict):
                        Object(sub_obj, backend=self._backend).validate()

    def parse_data(self, data, *args, **kwargs):
        #TODO Rename to jsonify_dict
        _parsed_data = data.copy()

        #parse datetime fields
        for d in self._datetime_fields:
            if d in _parsed_data and _parsed_data[d]:
                _parsed_data[d] = self._parse_date(_parsed_data[d], utc=True, use_system_timezone=False)

        #parse object fields
        for c in self._object_fields:
            if c in _parsed_data and _parsed_data[c] and isinstance(_parsed_data[c], Model):
                _parsed_data[c] = _parsed_data[c].parse_data(_parsed_data[c].get_dict())

        #parse direct and indirect audience targeting
        for c in self._indirect_audience_targeting_fields + self._direct_audience_targeting_fields:
            if c in _parsed_data and _parsed_data[c]:
                _parsed_data[c] = [obj.parse_data(obj.get_dict()) if isinstance(obj, Model) else obj\
                    for obj in _parsed_data[c]]

        #parse media fields
        for c in self._media_fields:
            if c in _parsed_data and _parsed_data[c] and isinstance(_parsed_data[c], Model):
                _parsed_data[c] = _parsed_data[c].parse_data(_parsed_data[c].get_dict())

        #parse anything that is a dictionary for things like datetime fields that are datetime objects
        for k, v in _parsed_data.items():
            if isinstance(v, dict) and k not in self._response_fields:
                _parsed_data[k] = self.parse_data(v)

        return _parsed_data

    def get_parsed_dict(self, *args, **kwargs):

        #we are suppose to maintain our own published and updated fields
        if not self._dict.get('published', None):
            self._dict['published'] = datetime.datetime.utcnow()
        elif 'updated' in self._reserved_fields:
            self._dict['updated'] = datetime.datetime.utcnow()

        parsed_data = self.parse_data(self._dict, *args, **kwargs)

        return parsed_data

    def get_dict(self):
        return self._dict

    def _parse_date(self, date=None, utc=True, use_system_timezone=False):
        dt = None
        if date is None or not isinstance(date, datetime.datetime):
            if isinstance(date, basestring):
                try:
                    dt = parse(date)
                except ValueError:
                    dt = datetime.datetime.utcnow()
            else:
                dt = datetime.datetime.utcnow()
        else:
            dt = date
        return rfc3339(dt, utc=utc, use_system_timezone=use_system_timezone)

    def get_new_id(self):
        """
        Generates a new unique ID. The default implementation uses uuid1 to
        generate a unique ID.

        :return: a new id
        """
        return self._backend.get_new_id()

    def __getitem__(self, key):
        return self._dict[key]


class Activity(Model):
    _required_fields = ['verb', 'actor', 'object']
    _media_fields = ['icon']
    _reserved_fields = ['updated']
    _response_fields = ['replies', 'likes']
    _direct_audience_targeting_fields = ['to', 'bto']
    _indirect_audience_targeting_fields = ['cc', 'bcc']

    def _set_defaults(self, model_dict):
        model_dict = super(Activity, self)._set_defaults(model_dict)

        if 'replies' not in model_dict:
            model_dict['replies'] = {'totalItems': 0, 'items': []}

        if 'likes' not in model_dict:
            model_dict['likes'] = {'totalItems': 0, 'items': []}

        return model_dict

    def get_parsed_sub_activity_dict(self, actor, content="", verb="reply", object_type="reply", \
        collection="replies", activity_class=None, extra={}, **kwargs):
        #TODO: Doesn't feel like this should be here Feels like it belongs in the backend.

        in_reply_to_dict = {
            'objectType': 'activity',
            'displayName': self._dict['verb'],
            'id': self._dict['id'],
            'published': self._dict['published']
        }
        reply_obj = {
            'objectType': object_type,
            'id': self.get_new_id(),
            'published': datetime.datetime.utcnow(),
            'content': content,
            'inReplyTo': [in_reply_to_dict],
        }

        reply_dict = {
            'actor': actor,
            'object': reply_obj,
            'verb': verb
        }

        if isinstance(content, dict):
            reply_dict['object'].update(content)

        if extra:
            extra.update(reply_dict)
            reply_dict = extra

        _activity = reply_dict

        _sub_dict = {
            # 'actor': _activity_data['actor'],
            'verb': verb,
            # 'id': _activity_data['id'],
            # 'published': _activity_data['published'],
            'object': {
                'objectType': 'activity',
                # 'id': _activity_data['id'],
            }
        }

        self._dict[collection]['totalItems'] += 1
        #insert the newest comment at the top of the list
        self._dict[collection]['items'].insert(0, _sub_dict)

        parent_activity = self.parse_data(self._dict, **kwargs)

        return _activity, parent_activity

    def parse_data(self, data, *args, **kwargs):
        #TODO Rename to jsonify_dict
        _parsed_data = super(Activity, self).parse_data(data, *args, **kwargs)
        for response_field in self._response_fields:
            if response_field in _parsed_data:
                if not _parsed_data[response_field]['items']:
                    del _parsed_data[response_field]
                else:
                    for i, comment in enumerate(_parsed_data[response_field]['items']):
                        _parsed_data[response_field]['items'][i] = super(Activity, self).parse_data(comment, *args, **kwargs)

        return _parsed_data


class SubItemMixin(object):
    sub_item_verb = "reply"
    sub_item_key = "replies"

    def __init__(self, object_dict, *args, **kwargs):

        super(SubItemMixin, self).__init__(object_dict, *args, **kwargs)

        del self._dict['replies']
        del self._dict['likes']


class ReplyActivity(SubItemMixin, Activity):
    sub_item_verb = "reply"
    sub_item_key = "replies"


class LikeActivity(SubItemMixin, Activity):
    sub_item_verb = "like"
    sub_item_key = "likes"


class Object(Model):
    _required_fields = ['objectType', 'id', 'published']
    _media_fields = ['image']


class MediaLink(Model):
    _required_fields = ['url']

from sunspear.activitystreams.models import (
    Activity, ReplyActivity, LikeActivity, Model)
from sunspear.exceptions import (
    SunspearDuplicateEntryException, SunspearInvalidActivityException,
    SunspearInvalidObjectException)

import uuid
import copy

__all__ = ('BaseBackend', 'SUB_ACTIVITY_MAP')

SUB_ACTIVITY_MAP = {
    'reply': (ReplyActivity, 'replies',),
    'like': (LikeActivity, 'likes',),
}


class BaseBackend(object):
    def clear_all_objects(self):
        """
        Clears all objects from the backend.
        """
        raise NotImplementedError()

    def clear_all_activities(self):
        """
        Clears all activities from the backend.
        """
        raise NotImplementedError()

    def obj_exists(self, obj, **kwargs):
        """
        Determins if an ``object`` already exists in the backend.

        :type obj: dict
        :param obj: the activity we want to determin if it exists

        :return: ``True`` if the ``object`` exists, otherwise ``False``
        """
        raise NotImplementedError()

    def activity_exists(self, activity, **kwargs):
        """
        Determins if an ``activity`` already exists in the backend.

        :type activity: dict
        :param activity: the activity we want to determin if it exists

        :return: ``True`` if the ``activity`` exists, otherwise ``False``
        """
        raise NotImplementedError()

    #TODO: Tests
    def create_activity(self, activity, **kwargs):
        """
        Stores a new ``activity`` in the backend. If an object with the same id already exists in
        the backend, a ``SunspearDuplicateEntryException`` is raised. If an ID is not provided, one
        is generated on the fly.

        Activities that provide ``objects`` as dictionaries have their objects processed and stored using
        ``create_obj``, and the ``objects`` are replaced with their id's within the activity.

        :type activity: dict
        :param activity: activity we want to store in the backend

        :raises: ``SunspearDuplicateEntryException`` if the record already exists in the database.
        :return: dict representing the new activity.
        """
        activity_id = self._extract_id(activity)
        if activity_id:
            if self.activity_exists(activity, **kwargs):
                raise SunspearDuplicateEntryException()
        else:
            activity['id'] = self.get_new_id()

        activity_copy = copy.copy(activity)

        objs_created = []
        objs_modified = []
        for key, value in activity_copy.items():
            if key in Activity._object_fields and isinstance(value, dict):
                if self.obj_exists(value):
                    previous_value = self.get_obj([self._extract_id(value)])[0]
                else:
                    previous_value = None

                try:
                    if previous_value:
                        objs_modified.append(previous_value)
                        self.update_obj(value)
                    else:
                        new_obj = self.create_obj(value)
                        objs_created.append(new_obj)
                except Exception:
                    #there was an error, undo everything we just did
                    self._rollback(objs_created, objs_modified)
                    raise

                activity[key] = value["id"]

            if key in Activity._direct_audience_targeting_fields + Activity._indirect_audience_targeting_fields\
                    and value:
                for i, target_obj in enumerate(value):
                    if isinstance(target_obj, dict):
                        previous_value = self.get_obj(target_obj)
                        if self.obj_exists(target_obj):
                            previous_value = self.get_obj(target_obj)
                        else:
                            previous_value = None

                        try:
                            if previous_value:
                                objs_modified.append(previous_value)
                                self.update_obj(target_obj, **kwargs)
                            else:
                                new_obj = self.create_obj(target_obj)
                                objs_created.append(new_obj)
                        except Exception:
                            self._rollback(objs_created, objs_modified)
                            raise
                        activity[key][i] = target_obj["id"]

        try:
            return_val = self.activity_create(activity, **kwargs)
        except Exception:
            self._rollback(objs_created, objs_modified)
            raise

        return return_val

    def _rollback(self, new_objects, modified_objects, **kwargs):
        [self.delete_obj(obj, **kwargs) for obj in new_objects]
        [self.update_obj(obj, **kwargs) for obj in modified_objects]

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

        return self.activity_update(activity, **kwargs)

    def activity_update(self, activity, **kwargs):
        """
        Performs the actual task of updating the activity in the backend.

        :type activity: dict
        :param activity: a dict representing the activity

        :return: a dict representing the newly stored activity
        """
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

        return self.activity_delete(activity, **kwargs)

    def activity_delete(self, activity, **kwargs):
        """
        Performs the task of actually deleting the activity from the backend.

        :type activity: dict
        :param activity: a dict representing the activity
        """
        raise NotImplementedError()

    def get_activity(self, activity_ids=[], **kwargs):
        """
        Gets an activity or a list of activities from the backend.

        :type activity_ids: list
        :param activity_ids: a list of ids of activities that will be retrieved from
            the backend.

        :return: a list of activities. If an activity is not found, a partial list should
            be returned.
        """
        return self.activity_get(self._listify(activity_ids), **kwargs)

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
        if not obj_id:
            obj['id'] = self.get_new_id()

        return self.obj_create(obj, **kwargs)

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

        **raises**:

        * ``SunspearInvalidObjectException`` -- if the obj doesn't have a valid id.

        :type obj: dict
        :param obj: a dict representing the obj

        :raises: SunspearInvalidObjectException
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

        **raises**:

        * ``SunspearInvalidObjectException`` -- if the obj doesn't have a valid id.

        :type obj: dict
        :param obj: a dict representing the obj

        :raises: SunspearInvalidObjectException
        """
        obj_id = self._extract_id(obj)
        if not obj_id:
            raise SunspearInvalidObjectException()

        return self.obj_delete(obj, **kwargs)

    def obj_delete(self, obj, **kwargs):
        raise NotImplementedError()

    def get_obj(self, obj_ids=[], **kwargs):
        """
        Gets an obj or a list of activities from the backend.

        :type obj: list
        :param obj: a list of ids of activities that will be retrieved from
            the backend.

        :return: a list of activities. If an obj is not found, a partial list should
            be returned.
        """
        return self.obj_get(self._listify(obj_ids), **kwargs) if obj_ids else []

    def obj_get(self, obj, **kwargs):
        raise NotImplementedError()

    def create_sub_activity(self, activity, actor, content, extra={}, sub_activity_verb="", **kwargs):
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

        :return: a tuple containing the new sub activity and the original activity
            the sub activity was created for.
        """
        actor_id = self._extract_id(actor)
        if not actor_id:
            raise SunspearInvalidObjectException()

        activity_id = self._extract_id(activity)
        if not activity_id:
            raise SunspearInvalidActivityException()

        return self.sub_activity_create(activity, actor, content, extra=extra, sub_activity_verb=sub_activity_verb,
            **kwargs)

    def sub_activity_create(self, activity, actor, content, extra={}, sub_activity_verb="",
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
        :param sub_activity_attribute: the attribute in the activity the ``sub-activity`` will be a part of

        :return: a tuple containing the new sub activity and the original activity
            the sub activity was created for.
        """
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
        if not isinstance(list_or_string, (list, tuple, set)):
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

    def _get_many_activities(self, activity_ids=None, raw_filter="", filters=None,
                             include_public=False, audience_targeting=None):
        """
        Given a list of activity ids, returns a list of activities from riak.

        :param list activity_ids: The list of activities you want to retrieve
        :param raw_filter: allows you to specify a javascript function as a
          string. The function should return ``true`` if the activity should
          be included in the result set or ``false`` it shouldn't. If you
          specify a raw filter, the filters specified in ``filters`` will not
          run. However, the results will still be filtered based on the
          ``audience_targeting`` parameter.
        :param dict filters: filters list of activities by key, value pair.
          For example, ``{'verb': 'comment'}`` would only return activities
          where the ``verb`` was ``comment``.
          Filters do not work for nested dictionaries.
        :param bool include_public: If ``True``, and the ``audience_targeting``
          dictionary is defined, activities that are not targeted towards
          anyone are included in the results
        :param dict audience_targeting: Filters the list of activities targeted
          towards a particular audience. The key for the dictionary is one of
          ``to``, ``cc``, ``bto``, or ``bcc``.

        :return list: the activities retrieved from riak
        """
        raise NotImplementedError()

    def dehydrate_activities(self, activities):
        """
        Takes a raw list of activities returned from riak and replace keys with
        contain ids for riak objects with actual riak object
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

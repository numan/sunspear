from sunspear.activitystreams.models import Activity, ReplyActivity, LikeActivity
from sunspear.exceptions import (SunspearDuplicateEntryException, SunspearInvalidActivityException,
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

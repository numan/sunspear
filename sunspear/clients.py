class SunspearClient(object):
    """
    The class is used to create, delete, remove and update activity stream items.
    This is the main class you use to interact with ``sunspear``
    """
    def __init__(self, backend, **kwargs):
        self._backend = backend

    def clear_all(self):
        """
        Deletes all activity stream data
        """
        self._backend.clear_all()

    def clear_all_objects(self):
        """
        Deletes all objects data
        """
        self._backend.clear_all_objects()

    def clear_all_activities(self):
        """
        Deletes all activities data
        """
        self._backend.clear_all_activities()

    def create_object(self, object_dict):
        """
        Creates an object that can be used as part of an activity. If you specific and object with an id
        that already exists, that object is overidden.

        :type object_dict: dict
        :param object_dict: a dictionary representing the object we want to store in the backend.
        """

        return self._backend.create_obj(object_dict)

    def create_activity(self, actstream_dict):
        """
        Creates an activity. You can provide objects for activities as dictionaries or as ids for already
        existing objects.

        If you provide a dictionary for an object, it is saved as a new object. If you provide
        an object id and the object does not exist, it is saved anyway, and returned as an empty dictionary when
        retriving the activity.

        :type actstream_dict: dict
        :param actstream_dict: a dictionary representing the ``activity`` we want to store in the backend.
        """

        return self._backend.create_activity(actstream_dict)

    def create_reply(self, activity, actor, content, extra={}, **kwargs):
        """
        Creates a ``reply`` for an activity.


        :type activity: a string or dict
        :param activity: the activity we want to create the sub-item for
        :type actor: a string or dict
        :param actor: the ``object`` creating the sub-activity
        :type content: a string or dict
        :param content: a string or an ``object`` representing the content of the sub-activity
        :type extra: dict
        :param extra: additional data the is to be included as part of the ``sub-activity`` activity
        """
        return self._backend.create_sub_activity(activity, actor, content,\
            extra=extra, sub_activity_verb="reply", **kwargs)

    def create_like(self, activity, actor, content="", extra={}, **kwargs):
        """
        Creates a ``like`` for an activity.


        :type activity: a string or dict
        :param activity: the activity we want to create the sub-item for
        :type actor: a string or dict
        :param actor: the ``object`` creating the sub-activity
        :type content: a string or dict
        :param content: a string or an ``object`` representing the content of the sub-activity
        :type extra: dict
        :param extra: additional data the is to be included as part of the ``sub-activity`` activity
        """
        return self._backend.create_sub_activity(activity, actor, content,\
            extra=extra, sub_activity_verb="like", **kwargs)

    def delete_activity(self, activity_id, **kwargs):
        """
        Deletes an activity item and all associated sub items

        :type activity_id: string
        :param activity_id: The id of the activity we want to create a reply for
        """
        return self._backend.delete_activity(activity_id, **kwargs)

    def delete_reply(self, activity_id, **kwargs):
        """
        Deletes a ``reply`` made on an activity. This will also update the corresponding activity.

        :type activity_id: string
        :param activity_id: the id of the reply activity to delete.
        """
        return self._backend.delete_sub_activity(activity_id, "reply", **kwargs)

    def delete_like(self, activity_id, **kwargs):
        """
        Deletes a ``like`` made on an activity. This will also update the corresponding activity.

        :type activity_id: string
        :param activity_id: the id of the like activity to delete.
        """
        return self._backend.delete_sub_activity(activity_id, "like", **kwargs)

    def get_objects(self, object_ids=[]):
        """
        Gets a list of objects by object_ids.

        :type object_ids: list
        :param object_ids: a list of objects
        """
        return self._backend.get_obj(object_ids)

    def get_activities(self, activity_ids=[], **kwargs):
        """
        Gets a list of activities. Specific backends may support other arguments. Please
        see reference of the specific backends to see all ``kwargs`` supported.

        :type activity_ids: list
        :param activity_ids: The list of activities you want to retrieve
        """
        return self._backend.get_activity(activity_ids=activity_ids, **kwargs)

    def get_backend(self):
        """
        The backend the client was initialized with.

        :return: reference to the backend the client was initialized with.
        """
        return self._backend

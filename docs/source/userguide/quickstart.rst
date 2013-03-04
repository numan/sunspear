==================
Quick Start Guide
==================

Usage
=====

**Sunspear** provides a very simple api managing activity stream items 

Initialize
----------

You initialize sunspear by providing ``sunspear.clients.SunspearClient`` with an instance of a backend. All backends
are located in  ``sunspear.backends`` and extend the ``sunspear.backend.base.BaseBackend``:

.. code-block:: python

    from sunspear.backends.RiakBackend import RiakBackend
    from sunspear.clients import SunspearClient

    import datetime

    client = SunspearClient(RiakBackend(**{
        "host_list": [{'port': 8087}],
        "defaults": {'host': '127.0.0.1'},
    }))


Create Objects
---------------

Once you have a reference to the ``SunspearClient``, you can, create objects:

.. code-block:: python

    obj = client.create_object({
        "objectType": "user"
        "displayName": "John Doe",
        "email": "jdoe@gmail.com",
        "id": "user:1234",
    }) 


.. note::

    If you do not specify the ``id`` of an object, one will be automatically generated for you. This also applies for the ``published`` date.

Create Activity
----------------

You can also create an ``activity``. As per the `JSON Activity Stream 1.0 <http://activitystrea.ms/specs/json/1.0/>`_ specifications, an activity must have a ``verb``, ``actor`` and an ``object``.

Creating an activity for::
    
    John Doe created the team "Marketing"

may look something like this:

.. code-block:: python

    activity = client.create_activity({
        "verb": "create",
        "actor": "user:1234",
        "object": {
            "objectType": "team",
            "displayName": "Marketing",
        },
    })

Couple of intresting things to note here:

1. We used the ``id`` for the ``actor`` instead of the full blown ``object`` because we created the ``object`` earlier.
2. We didn't specify an ``id`` for our ``team`` object. One will be automatically generated.
3. The verb is arbratry. It can be anything except for `response and activity summary verbs <http://activitystrea.ms/specs/json/replies/1.0/#stream>`_. For list a list of common ``verbs`` and ``objects`` you may want to use, see the `Activity Base Schema <http://activitystrea.ms/specs/json/schema/activity-schema.html>`_.
4. ``create_activity`` returns the fully parsed activity.

.. note::

    The reason you can't use `response and activity summary verbs <http://activitystrea.ms/specs/json/replies/1.0/#stream>`_ is because **Sunspear** uses some of them internally.


Create Responses
-----------------
You can create responses to activities such as liking an activity or replying to an activity. Sunspear supports a few of the response types described `here <http://activitystrea.ms/specs/json/replies/1.0/#stream>`_.

All methods that create responses, return the newly created response activity and the original activity the response was created for with the response activity embedded.

.. note::

    **Sunspear** does responses a little bit differently than what is describe in the `specifications <http://activitystrea.ms/specs/json/replies/1.0/>`_.

    Responses themselves are fully fledged ``activities`` and not ``objects``. This was untimatly done to provide maximum flexibility.


Create a Reply
~~~~~~~~~~~~~~

You can create replies to activities.

.. code-block:: python

    reply_activity, original_activity = client.create_reply(activity['id'], "user:1234",
        "This is my Reply!")

Create a Like
~~~~~~~~~~~~~~

You can like activities.

.. code-block:: python

    like_activity, original_activity = client.create_like(activity['id'], "user:1234")


Delete Reply
~~~~~~~~~~~~

.. code-block:: python

    original_activity = client.delete_reply(reply_activity["id"])

Delete Like
~~~~~~~~~~~~

.. code-block:: python

    original_activity = client.delete_like(reply_activity["id"])


Get Activities
---------------

You can get activities by providing a list of ``ids``.

.. code-block:: python
    
    client.get_activities([activity["id"], "1234", "3456"])

.. note::

    If the activity with the id does not exist, it is simply ignored.

Get Objects
------------

You can get objects by providing a list of ``ids``.

.. code-block:: python
    
    client.get_objects([obj["id"], "1234", "3456"])

.. note::

    If the object with the id does not exist, it is simply ignored.
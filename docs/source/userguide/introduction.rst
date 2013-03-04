==============
Introduction
==============

**Sunspear** is a python library which provides an API to store and manage activity stream items.

What it is
-----------

**Sunspear** is a library which allows you to add feeds and activity streams to your applications. It serializes these activities and feed items as described in the `JSON Activity Stream 1.0 <http://activitystrea.ms/specs/json/1.0/>`_ specifications.

The specifications look scary, dull and boring (and they are), but essentially, an activity for::

    ``John Doe`` added ``Jane Doe`` to the group ``Party Planning``

boils down to:

.. code-block:: js

    {
        "actor": {
            "objectType": "user",
            "displayName": "John Doe",
            "id": "user:1234", # Totally optional, it will be generated for you if you don't provide one.
            "title": "Manager",
            "foo": "bar"
        },
        "object": {
            "objectType": "user",
            "displayName": "Jane Doe",
            "title": "Devloper",
            "foo": "baz"
        },
        "target": {
            "objectType": "group",
            "displayName": "Party Planning"
        },
        "bar": "bee",
        "verb": "added"
    }

The main takeaway points are:

* An ``activity`` is composed of ``objects``.
* An ``activity`` must define an ``actor``, ``verb`` and ``object``.
* An ``object`` must define an ``objectType`` and a ``displayName``
* ``objects`` and ``activities`` may contain arbitrary attributes.

.. note::
    For more info, see the specifications for `activity <http://activitystrea.ms/specs/json/1.0/#activity>`_ and `object <http://activitystrea.ms/specs/json/1.0/#object>`_.

**Sunspear** also implements parts of some extensions to the specificiations. More specifically, `Audience Targeting <http://activitystrea.ms/specs/json/targeting/1.0/>`_ and `Responses <http://activitystrea.ms/specs/json/replies/1.0/>`_.

What it isn't
--------------

**Sunspear** strictly deals with storage and retrival of JSON activity stream items. It does not include all adquate indexes that allow you to build a fully fledged feed system.

For indexing, you'll probably want to use something like `Sandsnake <https://github.com/numan/sandsnake>`_, a sorted index backed by `redis <http://redis.io>`_.
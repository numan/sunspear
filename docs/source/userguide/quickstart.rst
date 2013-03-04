==================
Quick Start Guide
==================

Usage
-----

You initialize sunspear by providing ``sunspear.clients.SunspearClient`` with an instance of a backend. All backends
are located in  ``sunspear.backends`` and extend the ``sunspear.backend.base.BaseBackend``:

..code python::
    from sunspear.backends.RiakBackend import RiakBackend
    from sunspear.clients import SunspearClient

    import datetime

    client = SunspearClient(RiakBackend(**{
        "host_list": [{'port': 8087}],
        "defaults": {'host': '127.0.0.1'},
    }))

Once you have a reference to the ``SunspearClient``, you can, create objects:

..code python::
    client.create_object({
        "objectType": "user"
        "displayName": "John Doe",
        "email": "jdoe@gmail.com",
        "id": "user:1234",
    }) 

..note::
    If you do not specify the ``id`` of an object, one will be automatically generated for you. This also applies for the ``published`` date.
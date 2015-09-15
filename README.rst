Sunspear
========
.. image:: https://secure.travis-ci.org/numan/sunspear.png?branch=master
        :target: https://travis-ci.org/numan/sunspear

Overview
--------
Sunspear is an implementation of the `JSON Activity Stream 1.0 <http://activitystrea.ms/specs/json/1.0/>`_ specification. It is written in python and backed by `riak <http://basho.com>`_.

It allows you to manage create and manage activity feeds for your applications.

Resources
---------
* `Documentation <https://sunspear.readthedocs.org/en/latest/index.html>`_ (Work in progress. Let me know if you want to help!)
* `Issues <https://github.com/numan/sunspear/issues>`_

Contact
-------
* `Follow me on twitter for updates <http://twitter.com/numan856>`_

Changelog
---------

- **0.2.4**

  - Getting an activity from the riak backend now respects both the raw
    filter as well as a dict of filters simultaneously.

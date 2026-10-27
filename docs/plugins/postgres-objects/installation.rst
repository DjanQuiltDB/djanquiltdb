============
Installation
============

Install through DjanQuiltDB's extra::

    pip install djanquiltdb[postgres-objects]

The plugin itself needs no setup of its own: DjanQuiltDB discovers it through its entry point as soon as it is
installed. The two libraries it bridges keep theirs. ``postgres_objects`` is a Django app, listed alongside
``djanquiltdb``::

    INSTALLED_APPS = (
        ...
        'djanquiltdb',
        'postgres_objects',
        ...
    )

and django-postgres-objects reads the module each kind of declaration lives in, relative to each app, from the
``POSTGRES_OBJECTS`` setting::

    POSTGRES_OBJECTS = {
        'FUNCTIONS_MODULE_PATH': 'functions',
        'VIEWS_MODULE_PATH': 'db_views',
    }

Every app may now declare functions in a ``functions.py`` and views in a ``db_views.py``, and ``makemigrations``
manages what it finds there. django-postgres-objects' own documentation has the details: its `settings reference
<https://django-postgres-objects.readthedocs.io/en/latest/reference/settings.html>`_ describes how to turn off one of
the module paths, and its page on `other migration libraries
<https://django-postgres-objects.readthedocs.io/en/latest/integrations/migration_libraries.html>`_ describes how to use
django-postgres-objects together with other libraries that extend the migration autodetector.

Annotating declarations
-----------------------

The decorators are importable from ``djanquiltdb.decorators`` beside the model ones::

    from djanquiltdb.decorators import public_function, sharded_view

For functions:

* ``@public_function()``
* ``@mirrored_function()``
* ``@sharded_function()``

For views:

* ``@public_view()``
* ``@mirrored_view()``
* ``@sharded_view()``

These name the same three modes as the ``@*_model()`` counterparts from the core DjanQuiltDB library for Django's core
Models.

A declaration without one of these decorators gets the default placement, the public schema of every node, and fails
the system check ``djanquiltdb_plugin_postgres_objects.E001``, so that every declaration says where it belongs.

``@single_node_unsharded_function(node)`` and ``@single_node_unsharded_view(node)`` put a declaration on the public
schema of one node only, like ``@single_node_unsharded_model(node)`` for a model (see :doc:`/guides/sharding_modes`).
They are strongly discouraged. ``node`` is the alias of a node, or ``PRIMARY_NODE`` for the node that
``PRIMARY_DB_ALIAS`` names, and a materialized view is refreshed on that node. Each such declaration gets the warning
``djanquiltdb_plugin_postgres_objects.W001``, which you can silence through ``SILENCED_SYSTEM_CHECKS``. A declaration
on a node that is not in ``DATABASES`` fails the system check ``djanquiltdb_plugin_postgres_objects.E002``, and
``router_hints`` with ``SINGLE_NODE_UNSHARDED`` that name no node fail ``djanquiltdb_plugin_postgres_objects.E003``.

Adopting on an existing project
-------------------------------

A migration records a declaration's placement as hints on its operations at the time ``makemigrations`` writes it.
Migrations generated before this plugin was installed therefore carry no placement hints. They still apply: at apply
time an operation without hints falls back to the default placement the plugin installs, the public schema of every
node (the same placement a declaration without a decorator gets). Review such pre-existing migrations against where each
object should live, and regenerate them, or annotate the declarations and re-run ``makemigrations``, for anything that
should not be public.

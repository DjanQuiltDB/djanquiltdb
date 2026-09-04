=============
API reference
=============

The rest of the documentation explains what each part is for. This page describes what each part takes and returns.


Sharding modes
==============

.. currentmodule:: djanquiltdb

.. autoclass:: ShardingMode
    :members:
    :undoc-members:

.. autoclass:: State
    :members:
    :undoc-members:


.. _api_decorators:

Decorators
==========

.. automodule:: djanquiltdb.decorators
    :members:


Models
======

.. currentmodule:: djanquiltdb.models

The bases a project subclasses. A project declares its own shard model, since the shard table is the one piece of the
schema that cannot live in the library.

.. autoclass:: BaseShard
    :members:

.. autoclass:: BaseQuiltSession
    :members:

.. autoclass:: MappingQuerySet
    :members:

.. currentmodule:: djanquiltdb.options

.. autoclass:: ShardOptions
    :members:


Routing
=======

.. currentmodule:: djanquiltdb.router

The router decides which node and schema a query reaches, from the sharding mode its model was decorated with and
whichever shard is active on the current thread.

.. autoclass:: DynamicDbRouter
    :members:

.. autofunction:: get_active_connection

.. autofunction:: set_active_connection


.. _api_utils:

Utilities
=========

.. automodule:: djanquiltdb.utils
   :members: use_shard, use_shard_for, create_schema_on_node, create_template_schema, migrate_schema


Transactions
============

.. currentmodule:: djanquiltdb.transaction

Django's ``atomic`` binds to one connection; these resolve the connection the active shard is on first.

.. autofunction:: atomic

.. autofunction:: get_connection


Forms
=====

.. currentmodule:: djanquiltdb.forms

.. autoclass:: ModelForm
    :members:
    :exclude-members: media


Middleware
==========

.. currentmodule:: djanquiltdb.middleware

Selecting a shard per request, and turning an unreachable shard into a response rather than a traceback.

.. autoclass:: ExceptionMiddlewareMixin
    :members:

.. autoclass:: BaseUseShardMiddleware
    :members:

    .. automethod:: get_shard_id

.. autoclass:: BaseUseShardForMiddleware
    :members:

    .. automethod:: get_mapping_value

.. autoclass:: UseShardMiddleware
    :members:

.. autoclass:: UseShardForMiddleware
    :members:


Test support
============

.. currentmodule:: djanquiltdb.testing

Ships with the package rather than with the suite, so a project's own tests and a plugin's suite can use it.

.. autoclass:: ShardingTestCase
    :members:

.. autoclass:: ShardingTransactionTestCase
    :members:

.. autoclass:: ResetConnectionTestCaseMixin
    :members:

.. autoclass:: CleanShardingArtifactsMixin
    :members:

.. autoclass:: OverrideMirroredRoutingMixin
    :members:

.. autofunction:: skip_without_virtual_generated_column_support

.. autofunction:: disable_db_reconnect

.. autofunction:: pending_migration_changes


Plugins
=======

.. currentmodule:: djanquiltdb.plugins

Optional features are separate distributions, which DjanQuiltDB finds through the ``djanquiltdb.plugins`` entry-point
group. See :doc:`/plugins/postgres-objects/index` and :doc:`/plugins/pghistory/index` for the plugins in this
repository.

A plugin's API is served from inside ``djanquiltdb``, under the name that the plugin registers, so that name is part of
the plugin's public API. ``djanquiltdb.decorators`` and ``djanquiltdb.models`` each serve the plugin module that has the
same name:

``djanquiltdb.decorators.<name>``
    The plugin's ``decorators`` module, so its decorators are imported from the same place as the model decorators.

``djanquiltdb.models.<name>``
    The plugin's ``models`` module: base classes, querysets, and the like.

A plugin that has only one of the two modules is only served under that one. Both namespaces support importing the
plugin's module and importing a single name from it, and both ways are supported::

    from djanquiltdb.decorators import pghistory
    from djanquiltdb.decorators.pghistory import track

The served module is the plugin's own module object and not a copy, so a class is the same class whichever way it is
imported. Neither namespace is a package on disk: the names are resolved when they are imported or read, so a plugin is
only imported when something uses it. A type checker cannot follow these names, so for a type checker, import the names
from the modules in the plugin's distribution package that define them.

By default, the decorators of a plugin are also served directly from ``djanquiltdb.decorators``, next to the model
decorators, which suits a placement decorator such as ``@public_function()``. A plugin that keeps its own namespace sets
``SERVE_UNQUALIFIED = False`` in its decorators module. ``djanquiltdb.models`` has no such setting, because it only
serves a plugin under the plugin's name.

When two installed distributions register the same name, DjanQuiltDB raises ``ImproperlyConfigured`` the first time it
looks up a plugin. A registered plugin name takes precedence over a decorator with the same name that another plugin
serves directly.

.. autofunction:: load_plugins

.. autofunction:: load_plugin_by_name

.. autofunction:: served_plugin_module

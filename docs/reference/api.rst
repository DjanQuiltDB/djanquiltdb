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

.. autodata:: PRIMARY_NODE
    :no-value:

.. autofunction:: single_node_unsharded


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
   :members: use_shard, use_shard_for, get_shard_for, for_each_shard, create_schema_on_node, create_template_schema,
      migrate_schema


Migration operations
====================

.. currentmodule:: djanquiltdb.operations

.. autoclass:: CreateMissingMirroredTable


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
group. See :doc:`/plugins/celery/index`, :doc:`/plugins/pghistory/index`, :doc:`/plugins/postgres-denormalize/index` and
:doc:`/plugins/postgres-objects/index` for the plugins in this repository.

A plugin's API is served from inside ``djanquiltdb``, under the name that the plugin registers, so that name is part of
the plugin's public API. ``djanquiltdb.decorators``, ``djanquiltdb.models`` and ``djanquiltdb.utils`` each serve the
plugin module that has the same name:

``djanquiltdb.decorators.<name>``
    The plugin's ``decorators`` module, so its decorators are imported from the same place as the model decorators.

``djanquiltdb.models.<name>``
    The plugin's ``models`` module: base classes, querysets, and the like.

``djanquiltdb.utils.<name>``
    The plugin's ``utils`` module: functions, such as helpers that run in every shard. A plugin cannot be served there
    under a name that ``djanquiltdb.utils`` defines itself, such as ``use_shard``, because that name is found first.

A plugin is only served under the modules that it has. Every namespace supports importing the plugin's module and
importing a single name from it, and both ways are supported::

    from djanquiltdb.decorators import pghistory
    from djanquiltdb.decorators.pghistory import track

The served module is the plugin's own module object and not a copy, so a class is the same class whichever way it is
imported. No namespace is a package on disk: the names are resolved when they are imported or read, so a plugin is
only imported when something uses it. A type checker cannot follow these names, so for a type checker, import the names
from the modules in the plugin's distribution package that define them.

By default, the decorators of a plugin are also served directly from ``djanquiltdb.decorators``, next to the model
decorators, which suits a placement decorator such as ``@public_function()``. A plugin that keeps its own namespace sets
``SERVE_UNQUALIFIED = False`` in its decorators module. ``djanquiltdb.models`` and ``djanquiltdb.utils`` have no such
setting, because they only serve a plugin under the plugin's name.

When two installed distributions register the same name, DjanQuiltDB raises ``ImproperlyConfigured`` when the app
registry is ready, so the project does not start, and every time that name is looked up. Other names are served as
usual, so a name that no plugin serves is still a missing attribute or module. A registered plugin name takes precedence
over a decorator with the same name that another plugin serves directly.

.. autofunction:: load_plugins

.. autofunction:: load_plugin_by_name

.. autofunction:: raise_for_duplicate_plugin_names

.. autofunction:: served_plugin_module

.. _command_hooks:

Command hooks
-------------

Besides ``install()``, the object that the entry point loads can have hooks, which the management commands call at
fixed points. Every hook is optional. The hooks of a command are grouped in a section that is named after the command,
and that the plugin object has as an attribute, so ``move_data_to_shard.pre_copy`` is
``plugin.move_data_to_shard.pre_copy``. A section can be any object with these attributes, such as a
``SimpleNamespace`` or a module.

Each command has one context hook, whose name ends in ``_context``. It returns a context manager, which the command
enters before the first of its other hooks and exits after the last of them. The ``post_collect`` hooks of
``move_data_to_shard`` and ``purge_shard_data`` are the exception: they run before the context is entered, see below.
The other hooks are plain functions. Every hook receives the running command as its first argument. The hooks of a
command that runs in a transaction are called inside it, so a hook can act in the same transaction, and an exception in
a hook rolls the command back. When several plugins have a hook, they are called in the order in which the plugins are
loaded, and their contexts are exited in reverse order.

``move_data_to_shard``
    ``post_collect(command, data)``, ``move_context(command)``, ``pre_copy(command, pk_set)``,
    ``post_copy(command, pk_set)``, ``pre_delete(command, pk_set)`` and ``post_delete(command, pk_set)``.

    ``post_collect`` runs once the rows to move are collected, before the transaction of the move. ``data`` maps each
    collected model to the set of its collected instances, and a hook can remove instances from those sets. A row that
    is removed is neither copied nor deleted. Keep every model in ``data``, also when all of its instances are removed,
    because the collector orders the delete by the dependencies between the models that it collected. When a removed
    row refers to a moved row through a foreign key with a database constraint, the delete of the moved row fails the
    move, so the move changes nothing.

    ``pk_set`` maps each moved model to the primary keys of its moved rows. ``post_copy`` runs once the copy is checked.
    ``pre_delete`` and ``post_delete`` run around the delete from the source shard, and not with ``--no-delete``.
    Django's collector, which finds the rows to delete, does not follow a relation with ``on_delete=DO_NOTHING``, so
    ``post_delete`` is where a plugin deletes the moved rows that such a relation leaves behind. The delete and both
    hooks run with the ``pre_delete`` and ``post_delete`` signals disconnected. The command has the ``source_shard`` and
    the ``target_shard``.

``move_shard_to_node``
    ``move_context(command)``, ``pre_copy(command)`` and ``post_copy(command)``. They run once the schema exists on the
    target node. ``post_copy`` runs after the rows are copied and retargeted, before the sequences are moved. The
    command has the ``source_shard``, the ``target_node`` and the ``target_shard_options``.

``purge_shard_data``
    ``post_collect(command, data)``, ``purge_context(command)``, ``pre_delete(command, pk_set)`` and
    ``post_delete(command, pk_set)``. ``post_collect`` runs once the rows to purge are collected, and before the
    confirmation. As for ``move_data_to_shard``, a hook can remove instances from the sets in ``data``, and keeps every
    model in it. A row that is removed is not deleted. The other hooks run around the delete of the collected rows.
    ``pk_set`` maps each model to the primary keys of the rows that are purged. The delete and the hooks run in one
    transaction on the node of the shard, with the ``pre_delete`` and ``post_delete`` signals disconnected. The command
    has the ``shard``.

``purge_schema``
    ``purge_context(command)``, ``pre_drop(command)`` and ``post_drop(command)``, around the drop of the schema. The
    command has the ``node`` and the ``schema_name``.

``align_shards_with_template``
    ``align_context(command, shard)``, ``pre_align(command, shard)`` and ``post_align(command, shard)``, for each shard,
    inside the transaction of that shard and around its statements. A dry run changes nothing, and calls none of them.

``move_sharded_models``
    ``move_context(command)``, ``pre_move(command)`` and ``post_move(command)``, around the move of the sharded tables
    from the public schema into the schema of the shard. The command has the ``shard``.

.. autofunction:: plugin_hooks

.. autofunction:: run_plugin_hooks

.. autofunction:: entered_plugin_hook_contexts

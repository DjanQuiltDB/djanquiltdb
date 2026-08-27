=============
API reference
=============

The rest of the guide explains what each piece is for; this page is the reference for what it takes and returns. The
decorators and the utility functions have pages of their own: see :doc:`decorators` and :doc:`utils`.


Sharding modes
==============

.. currentmodule:: djanquiltdb

.. autoclass:: ShardingMode
    :members:
    :undoc-members:

.. autoclass:: State
    :members:
    :undoc-members:


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


Plugins
=======

.. currentmodule:: djanquiltdb.plugins

Optional features ship as separate distributions and are discovered through the ``djanquiltdb.plugins`` entry-point
group. See :doc:`/plugins/postgres-objects/index` for the one in this repository.

.. autofunction:: load_plugins

=============
API reference
=============

Once the plugin is installed, its API is served from inside ``djanquiltdb`` itself, as
``djanquiltdb.utils.postgres_denormalize``, next to ``use_shard()`` and ``for_each_shard()``. Both ways of importing it
are supported::

    from djanquiltdb.utils import postgres_denormalize     # the namespace
    from djanquiltdb.utils.postgres_denormalize import repair_shard  # the function, imported by name

The same functions can also be imported from ``djanquiltdb_plugin_postgres_denormalize``, the distribution package. A
type checker or an IDE cannot follow the served namespace, so point it at
``djanquiltdb_plugin_postgres_denormalize.utils``, the module that defines them.


Repair helpers
==============

.. currentmodule:: djanquiltdb_plugin_postgres_denormalize.utils

.. autofunction:: stale_rows

.. autofunction:: repair_shard

.. autofunction:: repair_every_shard

.. autofunction:: sharded_leaves

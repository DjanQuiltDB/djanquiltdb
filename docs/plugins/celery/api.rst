=============
API reference
=============

Once the plugin is installed, its API is served from inside ``djanquiltdb`` itself: the task decorators from
``djanquiltdb.decorators.celery``, the task classes from ``djanquiltdb.base.celery``, and the fan-out helpers, the
exceptions and the name of the target header from ``djanquiltdb.utils.celery``. The decorators are served only as
``djanquiltdb.decorators.celery``, and not as ``djanquiltdb.decorators``, so that a call site names the plugin. Both
ways of importing are supported::

    from djanquiltdb.decorators import celery            # the namespace, for @celery.task_for_shard
    from djanquiltdb.decorators.celery import task_for_shard  # the decorator, imported by name
    from djanquiltdb.base.celery import QuiltTask
    from djanquiltdb.utils.celery import TaskQuiltError

The same names can also be imported from ``djanquiltdb_plugin_celery``, the distribution package. A type checker or an
IDE cannot follow the served namespaces, so point it at the modules that define them: ``djanquiltdb_plugin_celery``
``.decorators``, ``.tasks`` for the task classes, ``.targets`` for the exceptions and the header name, and ``.utils``.


Task decorators
===============

.. currentmodule:: djanquiltdb.decorators.celery

.. autofunction:: task_for_shard

.. autofunction:: task_for_node

.. autofunction:: task_for_active_shard

.. autofunction:: task_for_every_shard

.. autofunction:: task_for_every_node


Fan-out helpers
===============

.. currentmodule:: djanquiltdb.utils.celery

.. autofunction:: fan_out

.. autofunction:: fan_out_to_nodes

.. autofunction:: per_shard_signatures

.. autofunction:: per_node_signatures

.. autofunction:: fanout_shards

.. autofunction:: aggregate_per_shard


Targets
=======

.. currentmodule:: djanquiltdb.utils.celery

.. autodata:: TARGET_HEADER_NAME

.. autoexception:: TaskQuiltError

.. autoexception:: TaskShardError

.. autoexception:: TaskNodeError


The task class
==============

.. currentmodule:: djanquiltdb.base.celery

The decorators create their tasks with this class as the base. A task class of your own that is passed as ``base`` has
to inherit from this class.

.. autoclass:: QuiltTask
    :members: for_shard, for_node, delay_for_shard, apply_async_for_shard, delay_on_commit_for_shard,
        apply_async_on_commit_for_shard, delay_for_node, apply_async_for_node, delay_on_commit_for_node,
        apply_async_on_commit_for_node

.. autoclass:: TaskSenderForTarget

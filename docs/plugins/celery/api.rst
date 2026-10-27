=============
API reference
=============

Once the plugin is installed, its API is served from inside ``djanquiltdb`` itself: the task decorators from
``djanquiltdb.decorators.celery``, the task classes and the Celery app class from ``djanquiltdb.base.celery``, and the fan-out helpers, the
exceptions and the name of the target header from ``djanquiltdb.utils.celery``. The decorators are served only as
``djanquiltdb.decorators.celery``, and not as ``djanquiltdb.decorators``, so that a call site names the plugin. Both
ways of importing are supported::

    from djanquiltdb.decorators import celery            # the namespace, for @celery.shared_task_for_shard
    from djanquiltdb.decorators.celery import shared_task_for_shard  # the decorator, imported by name
    from djanquiltdb.base.celery import QuiltTask
    from djanquiltdb.utils.celery import TaskQuiltError

The same names can also be imported from ``djanquiltdb_plugin_celery``, the distribution package. A type checker or an
IDE cannot follow the served namespaces, so point it at the modules that define them: ``djanquiltdb_plugin_celery``
``.decorators``, ``.tasks`` for the task classes, ``.targets`` for the exceptions and the header name, and ``.utils``.


Task decorators
===============

.. currentmodule:: djanquiltdb.decorators.celery

The decorators wrap ``celery.shared_task()``, which their names say. The ``task_for_*`` methods of
:class:`~djanquiltdb.base.celery.QuiltCelery` create a task of one Celery app instead, see :ref:`celery-app-classes`.

.. autofunction:: shared_task_for_shard

.. autofunction:: shared_task_for_node

.. autofunction:: shared_task_for_active_shard

.. autofunction:: shared_task_for_every_shard

.. autofunction:: shared_task_for_every_node


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

The decorators and the methods of ``QuiltCelery`` create their tasks with this class as the base, combined with the task
class of the Celery app, see :doc:`installation`. A task class of your own that is passed as ``base``, or that
``QUILT_CELERY['TASK_BASE']`` names, has to inherit from this class.

.. autoclass:: QuiltTask
    :members: for_shard, for_node, delay_for_shard, apply_async_for_shard, delay_on_commit_for_shard,
        apply_async_on_commit_for_shard, delay_for_node, apply_async_for_node, delay_on_commit_for_node,
        apply_async_on_commit_for_node

.. autoclass:: TaskSenderForTarget


.. _celery-app-classes:

The Celery app class
====================

.. currentmodule:: djanquiltdb.base.celery

.. autoclass:: QuiltCelery
    :members: task_for_shard, task_for_node, task_for_active_shard, task_for_every_shard, task_for_every_node

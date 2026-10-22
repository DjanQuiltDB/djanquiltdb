celery
======
This plugin makes DjanQuiltDB and `Celery <https://docs.celeryq.dev/>`_ work together. Its decorators turn a function
into a Celery task that runs in one shard, with ``shared_task_for_shard``, or in the public schema of one node, with
``shared_task_for_node``. That shard or schema is the *target* of the task. ``for_shard()`` and ``for_node()`` send a
task to a target, and the keyword arguments of the task can identify the target too. A task can also run in the shard
that is active where the task is sent. A task whose shard is in maintenance is retried. A *fan-out* sends a task once to
every shard, or once to every node. Install the plugin through an extra of ``djanquiltdb`` itself::

    pip install djanquiltdb[celery]

Plugin release |celery-version|.

.. toctree::
    :maxdepth: 2

    installation
    tasks
    fan_out
    api
    changelog


Things to know
--------------

* The function of a task receives a keyword argument that identifies the target only when the function declares that
  keyword argument by name, and ``**kwargs`` does not receive it. For a task of ``shared_task_for_shard``, that keyword
  argument is ``shard_id`` or the mapping field of the ``MAPPING_MODEL``. For a task of ``shared_task_for_node``, it is
  ``node_name``. A function that declares ``shard_id`` or ``node_name`` receives the target that the task runs in,
  however the task is sent. See :doc:`tasks` and :doc:`fan_out`.
* ``task.for_shard(shard).delay(...)`` and ``task.delay_for_shard(shard, ...)`` send a task to a shard, and
  ``for_node()`` and ``delay_for_node()`` to a node. The keyword arguments of such a task cannot identify a target as
  well. See :doc:`tasks`.
* Sending a task without a target, or building its signature without one, raises ``TaskShardError`` or
  ``TaskNodeError`` before anything is published. Both inherit from ``TaskQuiltError``. See :doc:`tasks`.
* Only a task that is decorated with ``shared_task_for_active_shard``, or with ``inherit_active_shard=True``, runs in
  the shard that is active where the task is sent. The shard of ``for_shard()`` and a shard in the keyword arguments
  take precedence over that shard. The public schema of a node is not a shard, so it is not inherited. See :doc:`tasks`.
* The plugin resolves the target when the task is sent or when its signature is built, also for ``delay_on_commit()``
  and for the later tasks of a chain. The target is sent in the ``djanquiltdb_target`` message header. A message that is
  not sent through the task, such as a message from ``send_task()``, has no such header, so its keyword arguments need a
  target. See :doc:`tasks`.
* A task whose shard is in maintenance is retried with an exponential backoff, five times by default, and then fails
  with the ``StateException``. A ``StateException`` that the function raises itself is retried too. See :doc:`tasks`.
* A running task holds the shared advisory lock of its shard, as a request does. ``move_data_to_shard`` and
  ``move_shard_to_node`` wait for the task to finish before they put the shard in maintenance. ``lock=False`` turns the
  lock off. See :doc:`tasks`.
* Celery retries an eager task, with ``task_always_eager``, only when ``task_eager_propagates`` is off, and runs its
  retries at once, without their countdown. See :doc:`tasks`.
* A fan-out skips the shards that are in maintenance, unless ``include_maintenance`` is True. A task of
  ``shared_task_for_every_shard`` that is sent inside ``use_shard()`` also runs in every shard. See :doc:`fan_out`.
* ``aggregate_per_shard()`` builds a Celery chord, which needs a result backend. See :doc:`fan_out`.
* A type checker cannot follow the names that DjanQuiltDB serves, such as ``djanquiltdb.decorators.celery``. Point the
  type checker at the modules that define these names. See :doc:`api`.

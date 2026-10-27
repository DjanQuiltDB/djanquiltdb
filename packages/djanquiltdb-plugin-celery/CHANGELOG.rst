v 1.1.0 (2026-10-27)
--------------------
Added:
 * ``QuiltTask`` and ``TaskSenderForTarget`` are served from ``djanquiltdb.base.celery``.
 * ``TaskQuiltError``, ``TaskShardError``, ``TaskNodeError`` and ``TARGET_HEADER_NAME`` are served from
   ``djanquiltdb.utils.celery``.
 * A task whose decorator is not given a ``base`` also inherits from the task class of each Celery app that registers
   it (``task_cls`` in stock Celery) so a task class of the project applies to sharding-aware tasks as well. An
   override specifically for sharding-aware tasks is provided in ``QUILT_CELERY['TASK_BASE']``, which is read when the
   app registers the task, as Celery reads ``task_cls``.
 * A function that declares ``shard_id``, the keyword argument for a mapping value, or (for a task that runs on a node)
   ``node_name``, can receive it by position. Note that a task that runs once per shard/node still takes its target by
   keyword only.
 * ``QuiltCelery``, a Celery app class served from ``djanquiltdb.base.celery``. Its ``task_for_*`` methods create a task
   of that app, as ``app.task()`` does, next to the ``shared_task_for_*`` decorators, which create a shared task.

Altered:
 * Require djanquiltdb 4.5.
 * The decorators were wrongly named ``task_for_*``, as they wrap ``celery.shared_task()``. They are renamed to
   ``shared_task_for_*``, and the ``task_for_*`` names are removed. For app-bound tasks that mirror Celery's own
   ``app.task()`` decorator, see ``app.task_for_*`` on ``QuiltCelery``.
 * Fixed a ``QUILT_CELERY`` that is not a dict being silently replaced by the defaults.

v 1.0.0 (2026-10-22)
--------------------
Added:
 * Initial release.

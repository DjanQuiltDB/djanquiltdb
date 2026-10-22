==========================================
Running tasks in every shard or every node
==========================================

Every shard
-----------

``shared_task_for_every_shard`` runs a task once in every active shard when the task is sent without a shard. The
message without a shard starts the *fan-out* on the worker: the task sends itself once for each active shard, with the
``shard_id`` of the shard added to its keyword arguments, and returns the ids of the tasks that it sends. A task that is
sent with a shard, through ``for_shard()`` or in its keyword arguments, runs in that shard only, as a task of
``shared_task_for_shard`` does.

.. code-block:: python

    from djanquiltdb.decorators.celery import shared_task_for_every_shard


    @shared_task_for_every_shard(queue='fanout', spread=(0, 300))
    def clean_up_drafts():
        Draft.objects.filter(updated_at__lt=a_month_ago()).delete()


    clean_up_drafts.delay()  # runs once in every active shard
    clean_up_drafts.delay_for_shard(3)  # runs in shard 3 only

The function receives ``shard_id`` only when it declares it by name, as for ``shared_task_for_shard``. A function that
declares ``shard_id`` can declare it without a default, because the check of the arguments of a message counts
``shard_id`` as given. Each task that the fan-out sends has ``shard_id`` in its keyword arguments, so the parameters
after ``shard_id`` are passed by keyword too. For ``def purge_drafts(shard_id, days)``, ``purge_drafts.delay(days=30)``
starts a fan-out, and ``purge_drafts.delay(30)`` raises ``TypeError``. The task does not inherit the active shard, so
the task also runs in every shard when it is sent inside ``use_shard()``.

A periodic task in the beat schedule needs only the name of the task, because the fan-out happens in the worker:

.. code-block:: python

    CELERY_BEAT_SCHEDULE = {
        'clean-up-drafts': {
            'task': 'myapp.tasks.clean_up_drafts',
            'schedule': crontab(hour=3, minute=0),
        },
    }

The decorator takes these arguments besides those of ``shared_task_for_shard``:

``queue``
    The name of the queue of each task that the fan-out sends. Defaults to ``QUILT_CELERY['FANOUT_QUEUE']``. When
    ``queue`` and ``QUILT_CELERY['FANOUT_QUEUE']`` are both None, the routing of the Celery app picks the queue. A queue
    that is not a string raises ``ValueError`` when the function is decorated. The message that starts the fan-out goes
    to the queue that the Celery app routes the task to. See :ref:`celery-routing-per-shard` for a queue per shard or
    per node.

``spread``
    Delays each task that the fan-out sends by a random number of seconds: from 0 to an integer, or between a pair of
    integers, so that the shards do not all start at once. The integers are not negative and in ascending order, and any
    other value raises ``ValueError`` when the function is decorated. Defaults to ``QUILT_CELERY['FANOUT_SPREAD']``.

``include_maintenance``
    When True, the fan-out also sends the task to the shards that are in maintenance, and the task is retried there like
    any task whose shard is in maintenance. When the shard is in maintenance after the last retry, the task fails with
    the ``StateException``. So the retry settings decide how long a shard can be in maintenance before the task fails.
    By default, the fan-out skips the shards that are in maintenance.


Every node
----------

``shared_task_for_every_node`` runs a task once on every node, in the public schema of the node, when the task is sent
without a ``node_name``. The fan-out sends the task once for each node in ``DATABASES``, with the ``node_name`` of the
node added to its keyword arguments. The function receives ``node_name`` only when it declares it by name, and can
declare it without a default too. A task that is sent with a node, through ``for_node()`` or in its keyword arguments,
runs on that node only, as a task of ``shared_task_for_node`` does. ``queue`` and ``spread`` work as they do for
``shared_task_for_every_shard``.

.. code-block:: python

    from djanquiltdb.decorators.celery import shared_task_for_every_node


    @shared_task_for_every_node
    def refresh_exchange_rates():
        ExchangeRate.objects.refresh()


Fanning out by hand
-------------------

The helpers in ``djanquiltdb.utils.celery`` send a task of the plugin to the shards or nodes that they are given. The
helpers for shards take a task that runs in a shard, and the helpers for nodes take a task of ``shared_task_for_node``
or ``shared_task_for_every_node``. A task of the other kind raises ``TypeError``:

.. code-block:: python

    from djanquiltdb.utils.celery import fan_out, fan_out_to_nodes

    fan_out(send_reminders, kwargs={'days': 3}, queue='reminders', spread=(0, 60))
    fan_out(send_reminders, shards=Shard.objects.filter(node_name='eu'))
    fan_out_to_nodes(refresh_exchange_rates, nodes=['eu'])

``per_shard_signatures()`` and ``per_node_signatures()`` return the signatures without sending them, for a canvas of
your own. ``fanout_shards()`` returns the shards that a fan-out sends a task to by default. ``queue`` and ``spread``
work as they do for the decorators, and a value that they do not accept raises ``ValueError`` before anything is
published.


.. _celery-routing-per-shard:

A queue per shard or per node
-----------------------------

``queue`` sets one queue for every task of a fan-out. To send each task to a queue for its shard or node, route the task
with a router function in Celery's ``task_routes``. The router receives the keyword arguments of each task, which have
the ``shard_id`` or ``node_name`` that the fan-out adds:

.. code-block:: python

    def route_by_node(name, args, kwargs, options, task=None, **kw):
        if 'node_name' in kwargs:
            return {'queue': 'node-{}'.format(kwargs['node_name'])}

        return None


    CELERY_TASK_ROUTES = (route_by_node,)

A router that routes by ``shard_id`` can look up the shard, or keep a dict of its own that maps shard ids to queues. A
router that returns None passes the task on to the next router, or to the default queue.


Collecting the results of every shard
-------------------------------------

``aggregate_per_shard()`` runs a task once in every active shard, and then runs a callback with the list of the results
of these tasks, in the order of the shards. ``aggregate_per_shard()`` does this with a Celery chord:

.. code-block:: python

    from celery import shared_task
    from djanquiltdb.decorators.celery import shared_task_for_shard
    from djanquiltdb.utils.celery import aggregate_per_shard


    @shared_task_for_shard
    def count_invoices():
        return Invoice.objects.count()


    @shared_task
    def report_total(counts):
        Report.objects.create(total=sum(counts))


    aggregate_per_shard(count_invoices, report_total.s())

A chord needs a result backend. The tasks in the shards store their results, also when the task has ``ignore_result``
set, because the callback needs the results. When there are no shards, the callback receives an empty list.

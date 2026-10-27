"""
Helpers that send a task once for every shard or once for every node. Sending a task in this way is a fan-out.
DjanQuiltDB serves this module as ``djanquiltdb.utils.celery``, which also serves the exceptions of a task without a
target and the name of the message header that carries the target, which are defined in
:mod:`djanquiltdb_plugin_celery.targets`.

The task is one that the plugin's decorators create. The helpers for shards take a task that runs in a shard, and each
task that they send runs in the shard of its ``shard_id``. The helpers for nodes take a task that runs on a node, and
each task that they send runs in the public schema of the node of its ``node_name``. The helpers import the DjanQuiltDB
modules that they need inside their bodies, because DjanQuiltDB loads the plugin before the app registry is ready.
"""

import random

from celery import chord
from celery.canvas import Signature

from djanquiltdb_plugin_celery.settings import ensure_valid_queue, ensure_valid_spread, quilt_celery_setting
from djanquiltdb_plugin_celery.targets import TARGET_HEADER_NAME, TaskNodeError, TaskQuiltError, TaskShardError


def fanout_shards(include_maintenance=False):
    """
    Returns the shards that a fan-out sends a task to, in the order of their primary keys: the active shards, and also
    the shards in maintenance when ``include_maintenance`` is True. The Shard table is read in the public schema of the
    node that the router picks, also inside a shard whose search_path does not have the public schema.
    """
    from djanquiltdb import State
    from djanquiltdb.utils import get_shard_class, public_schema_alias_for_reading

    shard_class = get_shard_class()
    shards = shard_class.objects.using(public_schema_alias_for_reading(shard_class)).order_by('pk')

    return shards if include_maintenance else shards.filter(state=State.ACTIVE)


def _queue_options(queue):
    """
    Returns the ``queue`` option of a task that a fan-out sends: ``queue``, else ``QUILT_CELERY['FANOUT_QUEUE']``. When
    both are None, the result has no ``queue`` option, so that the routing of the Celery app decides. A queue that is
    not a string raises ValueError.
    """
    queue = quilt_celery_setting('FANOUT_QUEUE') if queue is None else queue
    ensure_valid_queue(queue, 'the fan-out')

    return {} if queue is None else {'queue': queue}


def _countdown_options(spread):
    """
    Returns the ``countdown`` option of a task that a fan-out sends: a random number of seconds inside ``spread``, else
    inside ``QUILT_CELERY['FANOUT_SPREAD']``. An integer spread starts at 0. When both are None, the result has no
    ``countdown`` option. A value that is not a spread raises ValueError.
    """
    spread = quilt_celery_setting('FANOUT_SPREAD') if spread is None else spread
    ensure_valid_spread(spread, 'the fan-out')

    if spread is None:
        return {}

    low, high = (0, spread) if isinstance(spread, int) else spread

    return {'countdown': random.randint(low, high)}  # nosec: B311, the countdown is not a secret.


def _ensure_task_runs_in_a_shard(task):
    if getattr(task, 'runs_in', 'shard') != 'shard':
        raise TypeError(
            '{} runs on a node, so it cannot be sent once for each shard. Send it to every node with '
            'fan_out_to_nodes().'.format(task.name)
        )


def _ensure_task_runs_on_a_node(task):
    if getattr(task, 'runs_in', 'node') != 'node':
        raise TypeError(
            '{} runs in a shard, so it cannot be sent once for each node. Send it to every shard with '
            'fan_out().'.format(task.name)
        )


def per_shard_signatures(task, args=(), kwargs=None, shards=None, queue=None, include_maintenance=False):
    """
    Returns a signature of ``task`` for each of ``shards``, with the ``shard_id`` of the shard added to ``kwargs``.
    A task that is decorated to run on a node raises TypeError.

    :param shards: The shards. Defaults to :func:`fanout_shards`.
    :param queue: The name of a queue. Defaults to ``QUILT_CELERY['FANOUT_QUEUE']``.
    :param include_maintenance: Passed on to :func:`fanout_shards` when ``shards`` is not given.
    """
    _ensure_task_runs_in_a_shard(task)
    shards = fanout_shards(include_maintenance) if shards is None else shards
    queue_options = _queue_options(queue)

    return [task.signature(args, {**(kwargs or {}), 'shard_id': shard.pk}, **queue_options) for shard in shards]


def per_node_signatures(task, args=(), kwargs=None, nodes=None, queue=None):
    """
    Returns a signature of ``task`` for each of ``nodes``, with the ``node_name`` of the node added to ``kwargs``. A
    task that is decorated to run in a shard raises TypeError.

    :param nodes: The names of the nodes. Defaults to every database in ``DATABASES``.
    :param queue: The name of a queue. Defaults to ``QUILT_CELERY['FANOUT_QUEUE']``.
    """
    from djanquiltdb.utils import get_all_databases

    _ensure_task_runs_on_a_node(task)
    nodes = get_all_databases() if nodes is None else nodes
    queue_options = _queue_options(queue)

    return [task.signature(args, {**(kwargs or {}), 'node_name': node_name}, **queue_options) for node_name in nodes]


def fan_out(task, args=(), kwargs=None, shards=None, queue=None, spread=None, include_maintenance=False):
    """
    Sends ``task`` once for each of ``shards``, as :func:`per_shard_signatures` describes, and returns the results.

    :param spread: Delays each task by a random number of seconds: from 0 to an integer, or between a pair of
        integers. Defaults to ``QUILT_CELERY['FANOUT_SPREAD']``.

    :Example:
        .. code-block:: python

            from djanquiltdb.utils.celery import fan_out

            fan_out(send_reminders, kwargs={'days': 3}, queue='reminders', spread=(0, 60))
    """
    signatures = per_shard_signatures(task, args, kwargs, shards, queue, include_maintenance)

    return [signature.apply_async(**_countdown_options(spread)) for signature in signatures]


def fan_out_to_nodes(task, args=(), kwargs=None, nodes=None, queue=None, spread=None):
    """
    Sends ``task`` once for each of ``nodes``, as :func:`per_node_signatures` describes, and returns the results.
    ``spread`` works as it does for :func:`fan_out`.
    """
    signatures = per_node_signatures(task, args, kwargs, nodes, queue)

    return [signature.apply_async(**_countdown_options(spread)) for signature in signatures]


def aggregate_per_shard(
    shard_task, callback, args=(), kwargs=None, shards=None, queue=None, include_maintenance=False, **options
):
    """
    Runs ``shard_task`` once for each of ``shards``, as :func:`per_shard_signatures` describes, and then runs
    ``callback`` with the list of the results of those tasks, in the order of the shards, as a Celery chord. Returns the
    result of the callback.

    A chord needs a result backend. The tasks of the shards store their results even when ``shard_task`` ignores its
    results, because the callback needs them. When there are no shards, the callback receives an empty list.

    :param callback: A signature or a task.
    :param options: The options of ``apply_async()`` for the chord.

    :Example:
        .. code-block:: python

            from djanquiltdb.utils.celery import aggregate_per_shard

            aggregate_per_shard(count_invoices, report_totals.s(month=3))
    """
    header = [
        signature.set(ignore_result=False)
        for signature in per_shard_signatures(shard_task, args, kwargs, shards, queue, include_maintenance)
    ]
    body = callback if isinstance(callback, Signature) else callback.s()

    return chord(header, body).apply_async(**options)


__all__ = [
    'TARGET_HEADER_NAME',
    'TaskNodeError',
    'TaskQuiltError',
    'TaskShardError',
    'aggregate_per_shard',
    'fan_out',
    'fan_out_to_nodes',
    'fanout_shards',
    'per_node_signatures',
    'per_shard_signatures',
]

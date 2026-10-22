"""
The target of a task is the place where the task runs. For a task that runs in a shard, the target is a shard that is
identified by its primary key or by its mapping value. For a task that runs on a node, the target is the public schema
of a node. A target is a dict in one of these forms::

    {'shard_id': 3}
    {'mapping_value': 7}
    {'node_name': 'other', 'schema_name': 'public'}

The values of a target are integers or strings, so that the target can be sent in a message header with any serializer.
"""

import functools

from django.core.signals import setting_changed
from django.db.models import Model
from django.dispatch import receiver
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME

from djanquiltdb_plugin_celery.settings import quilt_celery_setting

#: The name of the message header in which the target of a task is sent to the worker.
TARGET_HEADER_NAME = 'djanquiltdb_target'

SHARD_ID_KWARG = 'shard_id'
NODE_NAME_KWARG = 'node_name'


class TaskQuiltError(ValueError):
    """
    A task has no shard or node to run in, or more than one shard.
    """

    pass


class TaskShardError(TaskQuiltError):
    """
    A task that runs in a shard has no shard, or more than one.
    """

    pass


class TaskNodeError(TaskQuiltError):
    """
    A task that runs on a node has no node.
    """

    pass


@functools.cache
def mapping_kwarg():
    """
    Returns the keyword argument that identifies a shard by its mapping value: ``QUILT_CELERY['MAPPING_KWARG']``, else
    the mapping field of the ``MAPPING_MODEL``, else None. The result is cached until ``QUILT_CELERY`` or ``QUILT_DB``
    changes.
    """
    from djanquiltdb.utils import get_mapping_class

    name = quilt_celery_setting('MAPPING_KWARG')

    if name:
        return name

    mapping_model = get_mapping_class()

    return None if mapping_model is None else mapping_model.mapping_field


@receiver(setting_changed)
def reset_cached_mapping_kwarg(setting, **kwargs):
    """
    Clears the cached result of mapping_kwarg() when ``QUILT_CELERY`` or ``QUILT_DB`` changes, such as in
    override_settings().
    """
    if setting in ('QUILT_CELERY', 'QUILT_DB'):
        mapping_kwarg.cache_clear()


def accepted_shard_kwargs():
    """
    Returns the keyword arguments that can identify the shard of a task, in this order: ``shard_id``, and the keyword
    argument for a mapping value when the project has one.
    """
    names = [SHARD_ID_KWARG]
    mapping_name = mapping_kwarg()

    if mapping_name:
        names.append(mapping_name)

    return names


def _value_for_header(value):
    if isinstance(value, Model):
        value = value.pk

    return value if isinstance(value, (int, str)) else str(value)


def _mapping_value_for_header(mapping_value):
    """
    Returns the value that the mapping field stores for ``mapping_value``, as a value for a message header. A model
    instance is replaced by the value that the mapping field stores for it, which is the value that the worker looks up
    and the value that ``use_shard_for()`` and the move commands lock.
    """
    from djanquiltdb.utils import stored_mapping_value

    return _value_for_header(stored_mapping_value(mapping_value))


def _given_shard_kwargs(kwargs):
    return [name for name in accepted_shard_kwargs() if kwargs.get(name) is not None]


def shard_target_from_task_kwargs(kwargs):
    """
    Returns the shard that the keyword arguments of a task identify, or None when they do not identify a shard. A
    keyword argument whose value is None does not identify a shard. Raises TaskShardError when the keyword arguments
    identify more than one shard. So that the target can be sent in a message header, a shard instance is replaced by
    its primary key, a model instance that is passed as a mapping value is replaced by the value that the mapping field
    stores for it, and a value that is not an integer or a string is replaced by its string.
    """
    given = _given_shard_kwargs(kwargs)

    if not given:
        return None

    if len(given) > 1:
        raise TaskShardError(
            'The keyword arguments {} each identify a shard. Pass only one of them.'.format(', '.join(given))
        )

    name = given[0]

    if name == SHARD_ID_KWARG:
        return {'shard_id': _value_for_header(kwargs[name])}

    return {'mapping_value': _mapping_value_for_header(kwargs[name])}


def node_target_from_task_kwargs(kwargs):
    """
    Returns the public schema of the node that the ``node_name`` of a task identifies, or None when the task has no
    ``node_name``, or None as its ``node_name``.
    """
    node_name = kwargs.get(NODE_NAME_KWARG)

    return None if node_name is None else {'node_name': _value_for_header(node_name), 'schema_name': PUBLIC_SCHEMA_NAME}


def shard_target_from_active_connection():
    """
    Returns the shard that is active on this thread, inside ``use_shard()`` or ``use_shard_for()``, or None when no
    shard is active. The public schema of a node is not a shard, so the function returns None for it too. A shard that
    is entered for a mapping value is identified by that mapping value, so that the task checks the state of the mapping
    too.
    """
    from djanquiltdb.options import ShardOptions
    from djanquiltdb.router import get_active_connection

    active_connection = get_active_connection()

    if not isinstance(active_connection, ShardOptions):
        return None

    if active_connection.mapping_value is not None:
        return {'mapping_value': _mapping_value_for_header(active_connection.mapping_value)}

    if active_connection.shard_id is not None:
        return {'shard_id': _value_for_header(active_connection.shard_id)}

    return None


def use_target(target, lock=None):
    """
    Returns the context manager that enters ``target``, as ``ShardOptions.use()`` builds it: ``use_shard()`` for a shard
    or a schema, and ``use_shard_for()`` for a mapping value. When ``lock`` is not None and the target is a shard,
    ``lock`` is passed on to that context manager. The public schema of a node has no lock, so a target with a
    ``node_name`` ignores ``lock``. Entering a shard that is not active raises StateException.
    """
    from djanquiltdb.options import ShardOptions

    options = {} if lock is None or 'node_name' in target else {'lock': lock}

    return ShardOptions(
        node_name=target.get('node_name'),
        schema_name=target.get('schema_name'),
        shard_id=target.get('shard_id'),
        mapping_value=target.get('mapping_value'),
        **options,
    ).use()


def describe_missing_shard(task_name, kwargs):
    """
    Returns the message for a task that runs in a shard and has no shard. When the keyword arguments of the task have a
    ``node_name``, the message says to decorate a task that runs on a node with ``shared_task_for_node``.
    """
    names = accepted_shard_kwargs()

    if len(names) == 1:
        asked, repeated = 'the keyword argument {}'.format(names[0]), 'that keyword argument'
    else:
        asked = 'one of the keyword arguments {} or {}'.format(', '.join(names[:-1]), names[-1])
        repeated = 'one of those keyword arguments'

    message = (
        '{task} has no shard to run in. Pass {asked}, or, for a task that inherits the active shard, send it inside '
        'use_shard(). A message that is not sent through the task, such as one from send_task(), needs {repeated} or a '
        '{header} header.'
    ).format(task=task_name, asked=asked, repeated=repeated, header=TARGET_HEADER_NAME)

    if kwargs.get(NODE_NAME_KWARG) is not None:
        message += ' {} identifies a node. Decorate a task that runs on a node with shared_task_for_node.'.format(
            NODE_NAME_KWARG
        )

    return message


def describe_missing_node(task_name, kwargs):
    """
    Returns the message for a task that runs on a node and has no node. When the keyword arguments of the task identify
    a shard, the message says to decorate a task that runs in a shard with ``shared_task_for_shard``.
    """
    message = (
        '{task} has no node to run on. Pass the keyword argument {kwarg}. A message that is not sent through the task, '
        'such as one from send_task(), needs that keyword argument or a {header} header.'
    ).format(task=task_name, kwarg=NODE_NAME_KWARG, header=TARGET_HEADER_NAME)
    given = _given_shard_kwargs(kwargs)

    if given:
        message += ' {} identifies a shard. Decorate a task that runs in a shard with shared_task_for_shard.'.format(
            given[0]
        )

    return message

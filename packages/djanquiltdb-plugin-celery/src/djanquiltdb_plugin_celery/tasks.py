import functools
import inspect

from celery.contrib.django.task import DjangoTask
from django.db import connections, transaction
from django.db.models import Model
from djanquiltdb.utils import get_all_databases, get_shard_class

from djanquiltdb_plugin_celery.targets import (
    NODE_NAME_KWARG,
    SHARD_ID_KWARG,
    TARGET_HEADER_NAME,
    TaskNodeError,
    TaskShardError,
    accepted_shard_kwargs,
    describe_missing_node,
    describe_missing_shard,
    mapping_kwarg,
    node_target_from_task_kwargs,
    shard_target_from_active_connection,
    shard_target_from_task_kwargs,
)

#: The kinds of parameter that a function declares by name, and that can be passed by keyword.
KEYWORD_PARAMETER_KINDS = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)


class QuiltTask(DjangoTask):
    """
    The base class of the tasks that the plugin's decorators create. Each message of such a task has its target in the
    ``djanquiltdb_target`` header. ``apply_async()``, ``apply()`` and ``signature()`` add that header from the keyword
    arguments of the task, or from the shard that is active where the task is sent or its signature is built. Every way
    of sending a task goes through one of these three methods, also ``delay()``, a group, a chain, a chord, a link
    callback and a retry. :meth:`for_shard` and :meth:`for_node` send a task to a target that is passed separately from
    the keyword arguments of the task.

    The function of a task receives a keyword argument that identifies the target only when the function declares that
    keyword argument by name.
    """

    #: Where the task runs: ``'shard'`` in a shard, which ``shard_id`` or the keyword argument for a mapping value
    #: identifies, or ``'node'`` in the public schema of a node, which ``node_name`` identifies.
    runs_in = 'shard'

    #: Passed on to ``use_shard()`` as ``lock`` when it is not None.
    lock_target = None

    #: Whether a task without a shard in its keyword arguments or its header gets the shard that is active where the
    #: task is sent or its signature is built.
    inherit_active_shard = False

    #: Whether a task that runs without a target sends itself once for each shard of the fan-out, or once for every
    #: node, as ``runs_in`` says. Without a fan-out, such a task raises TaskShardError or TaskNodeError. The
    #: ``fanout_*`` attributes below are passed on to the fan-out.
    fans_out = False
    fanout_queue = None
    fanout_spread = None
    fanout_include_maintenance = False

    #: The signature of the function of the task, without the parameter for the task. The decorators set it.
    function_signature = None

    def target_of_task_kwargs(self, kwargs):
        """
        Returns the shard or node that ``kwargs`` identifies, as ``runs_in`` says, or None.
        """
        if self.runs_in == 'node':
            return node_target_from_task_kwargs(kwargs or {})

        return shard_target_from_task_kwargs(kwargs or {})

    def target_of_message(self, headers, kwargs):
        """
        Returns the target of a message of this task, or of a call of this task as a function: the shard or node that
        ``kwargs`` identifies, as ``runs_in`` says, else the target header in ``headers``, else, for a task that
        inherits the active shard, the shard that is active on this thread, else None. The keyword arguments come first,
        so that they take precedence over the header of a signature that is built inside a shard.
        """
        target = self.target_of_task_kwargs(kwargs)

        if target is None:
            target = (headers or {}).get(TARGET_HEADER_NAME)

        if target is None and self.inherit_active_shard:
            target = shard_target_from_active_connection()

        return target

    def missing_target_error(self, kwargs):
        """
        Returns the exception for a message of this task without a target: TaskNodeError for a task that runs on a
        node, and TaskShardError for a task that runs in a shard.
        """
        if self.runs_in == 'node':
            return TaskNodeError(describe_missing_node(self.name, kwargs or {}))

        return TaskShardError(describe_missing_shard(self.name, kwargs or {}))

    def target_kwargs(self):
        """
        Returns the keyword arguments that identify the target of this task: ``node_name`` for a task that runs on a
        node, and ``shard_id`` and the keyword argument for a mapping value for a task that runs in a shard.
        """
        return [NODE_NAME_KWARG] if self.runs_in == 'node' else accepted_shard_kwargs()

    def kwargs_for_function(self, kwargs, filled_value):
        """
        Returns ``kwargs`` as the function of this task receives them. Each keyword argument that identifies the target
        is skipped, unless the function declares it by name. When the function declares ``shard_id``, or ``node_name``
        for a task that runs on a node, and ``kwargs`` has no value for it, the function receives ``filled_value``.
        """
        declared_names = {
            name
            for name, parameter in self.function_signature.parameters.items()
            if parameter.kind in KEYWORD_PARAMETER_KINDS
        }
        target_kwargs = self.target_kwargs()
        function_kwargs = {
            name: value for name, value in kwargs.items() if name in declared_names or name not in target_kwargs
        }
        filled_name = NODE_NAME_KWARG if self.runs_in == 'node' else SHARD_ID_KWARG

        if filled_name in declared_names and function_kwargs.get(filled_name) is None:
            function_kwargs[filled_name] = filled_value

        return function_kwargs

    def entered_target_value(self, target):
        """
        Returns the value that the function receives for ``shard_id``, or for ``node_name`` for a task that runs on a
        node, while the task runs inside ``target``: the primary key of the shard that is entered, also for a target
        with a mapping value, or the name of the node.
        """
        from djanquiltdb.router import get_active_connection

        if self.runs_in == 'node':
            return target['node_name']

        return get_active_connection().shard_id

    def check_arguments(self, args, kwargs):
        """
        Raises TypeError when the function of this task cannot be called with ``args`` and ``kwargs``, as Celery checks
        the arguments of a task that is sent, unless the task is decorated with ``typing=False``. The arguments are
        checked as the function receives them, so a ``shard_id`` or ``node_name`` that the function declares counts as
        given.
        """
        if not self.typing or self.function_signature is None:
            return

        self.function_signature.bind(*(args or ()), **self.kwargs_for_function(kwargs or {}, filled_value=0))

    def options_with_target_header(self, kwargs, options):
        """
        Returns ``options`` with the target header of the message added. For a message of a fan-out task without a
        target, ``options`` is returned unchanged, because that message starts the fan-out. Any other message without a
        target raises TaskShardError or TaskNodeError, so that nothing is sent.
        """
        target = self.target_of_message(options.get('headers'), kwargs)

        if target is None and self.fans_out:
            return options

        if target is None:
            raise self.missing_target_error(kwargs)

        return {**options, 'headers': {**(options.get('headers') or {}), TARGET_HEADER_NAME: target}}

    def apply_async(self, args=None, kwargs=None, **options):
        self.check_arguments(args, kwargs)

        return super().apply_async(args, kwargs, **self.options_with_target_header(kwargs, options))

    def apply(self, args=None, kwargs=None, **options):
        return super().apply(args, kwargs, **self.options_with_target_header(kwargs, options))

    def signature(self, args=None, *starargs, **starkwargs):
        """
        Returns a signature of this task with the target header of its message, so that the target is resolved where the
        signature is built. The worker sends a later task of a chain, the body of a chord and a link callback, and the
        shard of the sender is not active in the worker. A signature without a target raises TaskShardError or
        TaskNodeError, as sending the task does, unless the task is decorated with ``task_for_every_shard`` or
        ``task_for_every_node``.
        """
        signature = super().signature(args, *starargs, **starkwargs)

        return signature.set(**self.options_with_target_header(signature.kwargs, signature.options))

    def apply_async_on_commit(self, args=None, kwargs=None, **options):
        """
        Sends the task once the transactions that are open at the call commit, on every node. The target is resolved at
        the call, so a task that inherits the active shard runs in the shard that is active at the call, and not in the
        shard that is active at the commit. The task is not sent when one of the transactions rolls back, and is sent at
        once when no transaction is open.
        """
        self.check_arguments(args, kwargs)
        options = self.options_with_target_header(kwargs, options)
        nodes_waiting_for_commit = {
            node_name for node_name in get_all_databases() if connections[node_name].in_atomic_block
        }

        if not nodes_waiting_for_commit:
            self.apply_async(args, kwargs, **options)
            return

        def send_once_every_node_commits(node_name):
            nodes_waiting_for_commit.discard(node_name)

            if not nodes_waiting_for_commit:
                self.apply_async(args, kwargs, **options)

        # Each node has a connection of its own. So each node that is in a transaction gets an on_commit() of its own
        # at the call, inside the savepoint that is open there, and the last of them to run sends the task. A rollback
        # on a node drops the on_commit() of that node, and then the task is not sent.
        for node_name in list(nodes_waiting_for_commit):
            transaction.on_commit(functools.partial(send_once_every_node_commits, node_name), using=node_name)

    def delay_on_commit(self, *args, **kwargs):
        self.apply_async_on_commit(args, kwargs)

    def for_shard(self, shard=None, /, **mapping_value):
        """
        Returns a :class:`TaskSenderForTarget` that sends this task to a shard: ``shard``, which is a shard or its
        primary key, or the shard of the mapping value that is passed by the keyword argument for a mapping value, such
        as ``tenant_id``. The shard takes precedence over the shard that is active where the task is sent. Raises
        TaskShardError when the method receives no shard or two shards, and TypeError for a task that runs on a node,
        for an instance of another model, and for another keyword argument.

        :Example:
            .. code-block:: python

                send_invoice.for_shard(shard).delay(invoice_id=12)
                send_invoice.for_shard(tenant_id=3).apply_async(kwargs={'invoice_id': 12}, countdown=60)
        """
        if self.runs_in == 'node':
            raise TypeError(
                '{} runs on a node, so it is sent to a node with for_node(), and not to a shard.'.format(self.name)
            )

        mapping_name = mapping_kwarg()
        unknown_names = sorted(name for name in mapping_value if name != mapping_name)

        if unknown_names:
            raise TypeError(
                'for_shard() of {task} received the keyword argument {names}, and it accepts a shard{mapping}.'.format(
                    task=self.name,
                    names=', '.join(unknown_names),
                    mapping=' or {}'.format(mapping_name) if mapping_name else '',
                )
            )

        if isinstance(shard, Model) and not isinstance(shard, get_shard_class()):
            raise TypeError(
                'for_shard() of {} received an instance of {}, which is not a shard.'.format(
                    self.name, type(shard).__name__
                )
            )

        if shard is not None and mapping_value.get(mapping_name) is not None:
            raise TaskShardError(
                'for_shard() of {} received a shard and {}. Pass only one of them.'.format(self.name, mapping_name)
            )

        target = shard_target_from_task_kwargs({SHARD_ID_KWARG: shard, **mapping_value})

        if target is None:
            raise TaskShardError(
                'for_shard() of {} received no shard. Pass a shard or its primary key{}.'.format(
                    self.name, ', or {}'.format(mapping_name) if mapping_name else ''
                )
            )

        return TaskSenderForTarget(self, target)

    def for_node(self, node_name):
        """
        Returns a :class:`TaskSenderForTarget` that sends this task to the public schema of the node ``node_name``.
        Raises TaskNodeError when ``node_name`` is None, and TypeError for a task that runs in a shard.

        :Example:
            .. code-block:: python

                refresh_exchange_rates.for_node('other').delay()
        """
        if self.runs_in != 'node':
            raise TypeError(
                '{} runs in a shard, so it is sent to a shard with for_shard(), and not to a node.'.format(self.name)
            )

        target = node_target_from_task_kwargs({NODE_NAME_KWARG: node_name})

        if target is None:
            raise TaskNodeError('for_node() of {} received None, and not the name of a node.'.format(self.name))

        return TaskSenderForTarget(self, target)

    def delay_for_shard(self, shard, /, *args, **kwargs):
        """
        ``for_shard(shard).delay(*args, **kwargs)``. ``shard`` is a shard or its primary key.
        """
        return self.for_shard(shard).delay(*args, **kwargs)

    def apply_async_for_shard(self, shard, /, args=None, kwargs=None, **options):
        """
        ``for_shard(shard).apply_async(args, kwargs, **options)``. ``shard`` is a shard or its primary key.
        """
        return self.for_shard(shard).apply_async(args, kwargs, **options)

    def delay_on_commit_for_shard(self, shard, /, *args, **kwargs):
        """
        ``for_shard(shard).delay_on_commit(*args, **kwargs)``. ``shard`` is a shard or its primary key.
        """
        return self.for_shard(shard).delay_on_commit(*args, **kwargs)

    def apply_async_on_commit_for_shard(self, shard, /, args=None, kwargs=None, **options):
        """
        ``for_shard(shard).apply_async_on_commit(args, kwargs, **options)``. ``shard`` is a shard or its primary key.
        """
        return self.for_shard(shard).apply_async_on_commit(args, kwargs, **options)

    def delay_for_node(self, node_name, /, *args, **kwargs):
        """
        ``for_node(node_name).delay(*args, **kwargs)``.
        """
        return self.for_node(node_name).delay(*args, **kwargs)

    def apply_async_for_node(self, node_name, /, args=None, kwargs=None, **options):
        """
        ``for_node(node_name).apply_async(args, kwargs, **options)``.
        """
        return self.for_node(node_name).apply_async(args, kwargs, **options)

    def delay_on_commit_for_node(self, node_name, /, *args, **kwargs):
        """
        ``for_node(node_name).delay_on_commit(*args, **kwargs)``.
        """
        return self.for_node(node_name).delay_on_commit(*args, **kwargs)

    def apply_async_on_commit_for_node(self, node_name, /, args=None, kwargs=None, **options):
        """
        ``for_node(node_name).apply_async_on_commit(args, kwargs, **options)``.
        """
        return self.for_node(node_name).apply_async_on_commit(args, kwargs, **options)

    def fan_out_to_every_target(self, args, kwargs):
        """
        Sends this task once for every shard or every node, as ``runs_in`` says, with the arguments of the running
        request, and returns the ids of the tasks that it sends.
        """
        from djanquiltdb_plugin_celery.utils import fan_out, fan_out_to_nodes

        if self.runs_in == 'node':
            results = fan_out_to_nodes(self, args, kwargs, queue=self.fanout_queue, spread=self.fanout_spread)
        else:
            results = fan_out(
                self,
                args,
                kwargs,
                queue=self.fanout_queue,
                spread=self.fanout_spread,
                include_maintenance=self.fanout_include_maintenance,
            )

        return [result.id for result in results]


class TaskSenderForTarget:
    """
    Sends a task to its target, with the methods of a task that send it. :meth:`QuiltTask.for_shard` and
    :meth:`QuiltTask.for_node` return it. Each method adds the target to the headers of the message, next to the headers
    that the caller passes, and then calls the method of the task with the same name. The keyword arguments of the task
    cannot identify a target as well: that raises TaskShardError or TaskNodeError before anything is sent.
    """

    def __init__(self, task, target):
        self.task = task
        self.target = target

    def options_with_target_header(self, kwargs, options):
        """
        Returns ``options`` with the target in their headers. Raises TaskShardError or TaskNodeError when ``kwargs``
        identifies a target too.
        """
        if self.task.target_of_task_kwargs(kwargs) is not None:
            if self.task.runs_in == 'node':
                error_class, method, kind = TaskNodeError, 'for_node', 'node'
            else:
                error_class, method, kind = TaskShardError, 'for_shard', 'shard'

            raise error_class(
                '{task} is sent through {method}(), and its keyword arguments identify a {kind} as well. Pass the '
                '{kind} only to {method}().'.format(task=self.task.name, method=method, kind=kind)
            )

        return {**options, 'headers': {**(options.get('headers') or {}), TARGET_HEADER_NAME: self.target}}

    def apply_async(self, args=None, kwargs=None, **options):
        return self.task.apply_async(args, kwargs, **self.options_with_target_header(kwargs, options))

    def delay(self, *args, **kwargs):
        return self.apply_async(args, kwargs)

    def apply_async_on_commit(self, args=None, kwargs=None, **options):
        return self.task.apply_async_on_commit(args, kwargs, **self.options_with_target_header(kwargs, options))

    def delay_on_commit(self, *args, **kwargs):
        return self.apply_async_on_commit(args, kwargs)

    def signature(self, args=None, kwargs=None, **options):
        return self.task.signature(args, kwargs, **self.options_with_target_header(kwargs, options))

    def s(self, *args, **kwargs):
        return self.signature(args, kwargs)

    def si(self, *args, **kwargs):
        return self.signature(args, kwargs, immutable=True)

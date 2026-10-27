"""
The task decorators of the plugin. DjanQuiltDB serves this module as ``djanquiltdb.decorators.celery``. Because
``SERVE_UNQUALIFIED`` is False, DjanQuiltDB does not also serve these decorators as attributes of
``djanquiltdb.decorators``, so that a call site names the plugin, as in ``@celery.shared_task_for_shard``.
"""

import annotationlib
import functools
import inspect

from celery import _state as celery_state
from celery import shared_task
from celery.local import PromiseProxy, Proxy
from djanquiltdb.utils import StateException

from djanquiltdb_plugin_celery.settings import ensure_valid_queue, ensure_valid_spread, quilt_celery_setting
from djanquiltdb_plugin_celery.targets import use_target
from djanquiltdb_plugin_celery.tasks import QuiltTask, task_base_of_app, task_base_of_the_setting

SERVE_UNQUALIFIED = False

#: The signature of the wrapper of each function: the task, and any arguments.
WRAPPER_SIGNATURE = inspect.Signature(
    [
        inspect.Parameter('self', inspect.Parameter.POSITIONAL_ONLY),
        inspect.Parameter('args', inspect.Parameter.VAR_POSITIONAL),
        inspect.Parameter('kwargs', inspect.Parameter.VAR_KEYWORD),
    ]
)


def _retry_options(options):
    """
    Returns the Celery options that retry a task while its shard is in maintenance, with the values of ``options``
    taking precedence over the QUILT_CELERY settings.
    """
    return {
        'max_retries': quilt_celery_setting('MAX_RETRIES'),
        'retry_backoff': quilt_celery_setting('RETRY_BACKOFF'),
        'retry_backoff_max': quilt_celery_setting('RETRY_BACKOFF_MAX'),
        'retry_jitter': quilt_celery_setting('RETRY_JITTER'),
        **options,
        'autoretry_for': (StateException, *options.get('autoretry_for', ())),
    }


def _signature_of_function(func, bind):
    """
    Returns the signature of ``func`` without the parameter for the task, which ``func`` takes when ``bind`` is True.
    The arguments of each message of the task are checked against this signature.
    """
    signature = inspect.signature(func, annotation_format=annotationlib.Format.STRING)
    parameters = list(signature.parameters.values())

    return signature.replace(parameters=parameters[1:] if bind else parameters)


def _create_quilt_task(func, task_attributes, options, app):
    """
    Returns the task of ``func``, with ``task_attributes`` as attributes of its QuiltTask class. When ``app`` is None,
    the task is shared, and ``options`` are the options of ``celery.shared_task()``. Otherwise the task is a task of
    ``app``, and ``options`` are the options of ``app.task()``.
    """
    ensure_valid_spread(task_attributes.get('fanout_spread'), func.__qualname__)
    ensure_valid_queue(task_attributes.get('fanout_queue'), func.__qualname__)

    options = dict(options)
    bind = options.pop('bind', False)
    base = options.pop('base', None) or task_base_of_the_setting()

    if base is not None and not (isinstance(base, type) and issubclass(base, QuiltTask)):
        raise TypeError(
            'The base of {} is {}, which does not inherit from QuiltTask. The tasks of the celery plugin need '
            'the methods of QuiltTask to send and to enter their targets.'.format(
                func.__qualname__, getattr(base, '__qualname__', repr(base))
            )
        )

    # The wrapper is always bound, because it reads the request of the task. The function receives the task only when it
    # is decorated with bind=True.
    @functools.wraps(func)
    def run_in_target(self, *args, **kwargs):
        target = self.target_of_message(self.request.headers, kwargs, args)

        if target is None and self.fans_out:
            return self.fan_out_to_every_target(args, kwargs)

        if target is None:
            raise self.missing_target_error(kwargs)

        with use_target(target, lock=self.lock_target):
            function_kwargs = self.kwargs_for_function(kwargs, self.entered_target_value(target), args)

            return func(self, *args, **function_kwargs) if bind else func(*args, **function_kwargs)

    # functools.wraps makes the wrapper report the signature of the function, and Celery checks the arguments of a
    # message against the signature of the wrapper. The function does not receive a keyword argument of the target that
    # it does not declare, so the wrapper takes any arguments, and QuiltTask checks the arguments in the form in which
    # the function receives them.
    run_in_target.__signature__ = WRAPPER_SIGNATURE
    task_attributes = {**task_attributes, 'function_signature': _signature_of_function(func, bind)}
    task_options = {'bind': True, **task_attributes, **_retry_options(options)}

    if app is None and base is not None:
        return shared_task(base=base, **task_options)(run_in_target)

    if app is None:
        return _shared_task_based_on_each_app(run_in_target, task_options)

    if base is not None:
        return app.task(base=base, **task_options)(run_in_target)

    return _app_task_based_on_each_app(app, run_in_target, task_options)


def _registration_with_the_base_of_each_app(fun, options):
    """
    Returns the function that registers the task of ``fun`` in an app, with ``options`` and with the base class that
    task_base_of_app() returns for that app.
    """

    def register_in(app):
        return app._task_from_fun(fun, base=task_base_of_app(app), **options)

    return register_in


def _shared_task_based_on_each_app(fun, options):
    """
    Returns the task of ``fun`` as ``celery.shared_task(**options)`` returns it, except that each app that registers the
    task gives it the base class that task_base_of_app() returns for that app. The body is the body of
    ``celery.shared_task()`` with that one change, and uses the same hooks of Celery. The base is resolved for each app,
    and not from the current app, because a tasks module can be imported before the Celery app of the project exists.
    """
    register_in = _registration_with_the_base_of_each_app(fun, options)

    # An app that is finalized later registers the task when it is finalized, and an app that is finalized already
    # registers it now.
    celery_state.connect_on_app_finalize(register_in)

    for app in celery_state._get_active_apps():
        if app.finalized:
            with app._finalize_mutex:
                register_in(app)

    def task_of_the_current_app():
        app = celery_state.get_current_app()
        return app.tasks[options.get('name') or app.gen_task_name(fun.__name__, fun.__module__)]

    return Proxy(task_of_the_current_app)


def _app_task_based_on_each_app(app, fun, options):
    """
    Returns the task of ``fun`` as ``app.task(**options)`` returns it, except that each app that registers the task
    gives it the base class that task_base_of_app() returns for that app. The body is the body of ``app.task()`` with
    that one change, and uses the same hooks of Celery. Celery's ``app.task()`` returns a shared task instead in a
    worker process that is started with execv, as on Windows, which Celery 5 does not support, so this body has no such
    branch.
    """
    options = dict(options)
    shared = options.pop('shared', True)
    lazy = options.pop('lazy', True)
    task_filter = options.pop('filter', None)
    register_in = _registration_with_the_base_of_each_app(fun, options)

    # As for app.task(), an app that is finalized later also registers a shared task when it is finalized.
    if shared:
        celery_state.connect_on_app_finalize(register_in)

    if not lazy or app.finalized:
        task = register_in(app)
    else:
        # The app registers the task when the proxy is first used, or when the app is finalized.
        task = PromiseProxy(register_in, (app,), __doc__=fun.__doc__)
        app._pending.append(task)

    return task_filter(task) if task_filter else task


def _refuse_app_option(options):
    """
    Raises TypeError when ``options`` has an ``app``. Celery would make it an attribute of the task class, in place of
    the app that registers the task.
    """
    if 'app' in options:
        raise TypeError(
            'The decorators of the celery plugin take no app. To create a task of one Celery app, use the task_for_* '
            'methods of a QuiltCelery app, as with app.task(). For a task that every Celery app shares, use the '
            'shared_task_for_* decorators.'
        )


def task_decorator_or_task(task_attributes, func, options, app=None):
    """
    Returns the task of ``func``, or, when ``func`` is None, the decorator that returns the task, so that each decorator
    works with and without arguments. The task is a task of ``app``, or a shared task when ``app`` is None.
    """
    _refuse_app_option(options)

    def decorate(func):
        return _create_quilt_task(func, task_attributes, options, app)

    return decorate if func is None else decorate(func)


def attributes_of_shard_task(inherit_active_shard, lock):
    """
    Returns the attributes of the QuiltTask class of a task that runs in a shard.
    """
    return {'runs_in': 'shard', 'lock_target': lock, 'inherit_active_shard': inherit_active_shard}


def attributes_of_every_shard_task(queue, spread, include_maintenance, lock):
    """
    Returns the attributes of the QuiltTask class of a task that runs once in every active shard when it is sent without
    a shard.
    """
    return {
        'runs_in': 'shard',
        'lock_target': lock,
        'fans_out': True,
        'fanout_queue': queue,
        'fanout_spread': spread,
        'fanout_include_maintenance': include_maintenance,
    }


def attributes_of_every_node_task(queue, spread):
    """
    Returns the attributes of the QuiltTask class of a task that runs once in the public schema of every node when it is
    sent without a node.
    """
    return {'runs_in': 'node', 'fans_out': True, 'fanout_queue': queue, 'fanout_spread': spread}


def shared_task_for_shard(func=None, *, inherit_active_shard=False, lock=None, **options):
    """
    Turns a function into a Celery task that runs in a shard: the shard that ``for_shard()`` is given, else the shard
    that its keyword arguments identify. That shard is the target of the task. ``shard_id`` identifies a shard by its
    primary key. The mapping field of the ``MAPPING_MODEL``, such as ``tenant_slug``, identifies a shard by its
    mapping value, and ``QUILT_CELERY['MAPPING_KWARG']`` can set another keyword argument for the mapping value.

    The function receives ``shard_id`` and the keyword argument for a mapping value only when it declares them by name,
    and ``**kwargs`` does not receive them. A function that declares ``shard_id`` receives the primary key of the shard
    that the task runs in, also when the task is sent without ``shard_id``. ``node_name`` is an ordinary keyword
    argument for the function. For a task that runs on a node, use :func:`shared_task_for_node`. The
    arguments of the task are checked against the function when the task is sent, unless the task is decorated with
    ``typing=False``.

    Sending the task without a shard raises TaskShardError, unless the task inherits the active shard and a shard is
    active where the task is sent. Sending the task with two shards raises TaskShardError too. While the shard is in
    maintenance, the task is retried with an exponential backoff, and after the last retry the task fails with the
    StateException.

    :param inherit_active_shard: When True, a task that is sent without a shard in its keyword arguments runs in the
        shard that is active where it is sent, as :func:`shared_task_for_active_shard` does.
    :param lock: Passed on to ``use_shard()`` as ``lock``. By default, ``use_shard()`` locks the shard while the task
        runs.
    :param options: The options of ``celery.shared_task()``. ``max_retries``, ``retry_backoff``, ``retry_backoff_max``
        and ``retry_jitter`` default to the QUILT_CELERY settings, and StateException is added to the exceptions of
        ``autoretry_for``. ``base`` defaults to the class that ``QUILT_CELERY['TASK_BASE']`` names, else to
        :class:`~djanquiltdb.base.celery.QuiltTask` combined with the task class of the Celery app, and a ``base``
        that does not inherit from QuiltTask raises TypeError. ``app`` raises TypeError, because the task is shared. To
        create a task of one Celery app, use :meth:`~djanquiltdb.base.celery.QuiltCelery.task_for_shard`.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators.celery import shared_task_for_shard


            @shared_task_for_shard
            def send_invoice(invoice_id):
                Invoice.objects.get(pk=invoice_id).send()


            send_invoice.for_shard(shard).delay(invoice_id=12)
            send_invoice.delay_for_shard(shard, invoice_id=12)  # the same
            send_invoice.delay(invoice_id=12, tenant_slug='acme')
    """
    return task_decorator_or_task(attributes_of_shard_task(inherit_active_shard, lock), func, options)


def shared_task_for_node(func=None, **options):
    """
    Turns a function into a Celery task that runs in the public schema of a node: the node that ``for_node()`` is given,
    else the node that its ``node_name`` keyword argument identifies. That schema is the target of the task. The
    function receives ``node_name`` only when it declares it by name, and then also when the task is sent without
    ``node_name``. ``shard_id`` and the keyword argument for a mapping value are ordinary keyword arguments for the
    function. Sending the task without a node raises TaskNodeError. The public schema of a node has no lock, so the task
    takes no ``lock``. ``options`` work as they do for :func:`shared_task_for_shard`.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators.celery import shared_task_for_node


            @shared_task_for_node
            def refresh_exchange_rates():
                ExchangeRate.objects.refresh()


            refresh_exchange_rates.for_node('other').delay()
            refresh_exchange_rates.delay(node_name='other')  # the same
    """
    return task_decorator_or_task({'runs_in': 'node'}, func, options)


def shared_task_for_active_shard(func=None, **options):
    """
    :func:`shared_task_for_shard` for a task that runs in the shard that is active where the task is sent: inside
    ``use_shard()``, ``use_shard_for()`` or a request that the DjanQuiltDB middleware wraps. The shard is resolved when
    the task is sent, and is sent in the message header. The shard of ``for_shard()`` and a shard in the keyword
    arguments take precedence over the active shard. Sending the task without a shard while no shard is active, or while
    only the public schema of a node is active, raises TaskShardError.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators.celery import shared_task_for_active_shard


            @shared_task_for_active_shard
            def recount_invoices():
                Invoice.objects.recount()


            with use_shard(shard):
                recount_invoices.delay()  # runs in shard
                recount_invoices.delay_for_shard(other_shard)  # runs in other_shard
    """
    return shared_task_for_shard(func, inherit_active_shard=True, **options)


def shared_task_for_every_shard(func=None, *, queue=None, spread=None, include_maintenance=False, lock=None, **options):
    """
    :func:`shared_task_for_shard` for a task that runs once in every active shard when it is sent without a shard. The
    message that is sent without a shard starts a fan-out where it runs: the task sends itself once for each active
    shard, with the ``shard_id`` of the shard added to the keyword arguments. So a periodic task in the beat schedule
    needs only the name of the task. A task that is sent with a shard, in ``for_shard()`` or in its keyword arguments,
    runs in that shard only. The task does not inherit the active shard, so that it also runs in every active shard when
    it is sent inside ``use_shard()``.

    The function receives ``shard_id`` only when it declares it by name, as for :func:`shared_task_for_shard`. A
    function that declares ``shard_id`` can declare it without a default, because the check of the arguments of a
    message counts ``shard_id`` as given. The parameters after ``shard_id`` are passed by keyword, as ``shard_id`` is.

    :param queue: The name of the queue of each task that the fan-out sends. Defaults to
        ``QUILT_CELERY['FANOUT_QUEUE']``, else the routing of the Celery app, whose ``task_routes`` can route each task
        by its ``shard_id``. A queue that is not a string raises ValueError.
    :param spread: Delays each task that the fan-out sends by a random number of seconds: from 0 to an integer, or
        between a pair of integers, which are not negative and in ascending order. Defaults to
        ``QUILT_CELERY['FANOUT_SPREAD']``. Any other value raises ValueError.
    :param include_maintenance: When True, the fan-out also sends the task to the shards in maintenance, where it is
        retried as in any shard in maintenance, and fails with the StateException after the last retry.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators.celery import shared_task_for_every_shard


            @shared_task_for_every_shard(queue='fanout', spread=(0, 300))
            def clean_up_drafts():
                Draft.objects.filter(updated_at__lt=a_month_ago()).delete()


            clean_up_drafts.delay()  # runs once in every active shard
    """
    task_attributes = attributes_of_every_shard_task(queue, spread, include_maintenance, lock)

    return task_decorator_or_task(task_attributes, func, options)


def shared_task_for_every_node(func=None, *, queue=None, spread=None, **options):
    """
    :func:`shared_task_for_node` for a task that runs once in the public schema of every node when it is sent without a
    ``node_name``. The message that is sent without a ``node_name`` starts a fan-out where it runs: the task sends
    itself once for each node in ``DATABASES``, with the ``node_name`` of the node added to the keyword arguments. A
    task that is sent with a node, in ``for_node()`` or in its keyword arguments, runs on that node only. The function
    receives ``node_name`` only when it declares it by name. A function that declares ``node_name`` can declare it
    without a default, and the parameters after ``node_name`` are passed by keyword. ``queue`` and ``spread`` work as
    they do for :func:`shared_task_for_every_shard`.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators.celery import shared_task_for_every_node


            @shared_task_for_every_node
            def refresh_exchange_rates():
                ExchangeRate.objects.refresh()
    """
    return task_decorator_or_task(attributes_of_every_node_task(queue, spread), func, options)


__all__ = [
    'shared_task_for_active_shard',
    'shared_task_for_every_node',
    'shared_task_for_every_shard',
    'shared_task_for_node',
    'shared_task_for_shard',
]

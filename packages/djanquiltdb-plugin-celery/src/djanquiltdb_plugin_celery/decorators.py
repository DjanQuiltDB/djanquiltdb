"""
The task decorators of the plugin. DjanQuiltDB serves this module as ``djanquiltdb.decorators.celery``. Because
``SERVE_UNQUALIFIED`` is False, DjanQuiltDB does not also serve these decorators as attributes of
``djanquiltdb.decorators``, so that a call site names the plugin, as in ``@celery.task_for_shard``.
"""

import annotationlib
import functools
import inspect

from celery import shared_task
from djanquiltdb.utils import StateException

from djanquiltdb_plugin_celery.settings import quilt_celery_setting
from djanquiltdb_plugin_celery.targets import use_target
from djanquiltdb_plugin_celery.tasks import QuiltTask

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


def _create_quilt_task(func, runs_in, lock, options):
    options = dict(options)
    bind = options.pop('bind', False)

    # The wrapper is always bound, because it reads the request of the task. The function receives the task only when it
    # is decorated with bind=True.
    @functools.wraps(func)
    def run_in_target(self, *args, **kwargs):
        target = self.target_of_request(kwargs)

        if target is None:
            raise self.missing_target_error(kwargs)

        with use_target(target, lock=self.lock_target):
            function_kwargs = self.kwargs_for_function(kwargs, self.entered_target_value(target))

            return func(self, *args, **function_kwargs) if bind else func(*args, **function_kwargs)

    # functools.wraps makes the wrapper report the signature of the function, and Celery checks the arguments of a
    # message against the signature of the wrapper. The function does not receive a keyword argument of the target that
    # it does not declare, so the wrapper takes any arguments, and QuiltTask checks the arguments in the form in which
    # the function receives them.
    run_in_target.__signature__ = WRAPPER_SIGNATURE

    return shared_task(
        bind=True,
        base=QuiltTask,
        runs_in=runs_in,
        lock_target=lock,
        function_signature=_signature_of_function(func, bind),
        **_retry_options(options),
    )(run_in_target)


def task_for_shard(func=None, *, lock=None, **options):
    """
    Turns a function into a Celery task that runs in a shard: the shard that ``for_shard()`` is given, else the shard
    that its keyword arguments name: ``shard_id`` names a shard by its primary key, and the mapping field of the
    ``MAPPING_MODEL``, such as ``tenant_id``, names a shard by its mapping value.

    The function receives ``shard_id`` and the keyword argument for a mapping value only when it declares them by name,
    and ``**kwargs`` does not receive them. A function that declares ``shard_id`` receives the primary key of the shard
    that the task runs in, also when the task is sent without ``shard_id``. ``node_name`` is an ordinary keyword
    argument for the function: a task that runs on a node is decorated with :func:`task_for_node`. The arguments of the
    task are checked against the function when the task is sent, unless the task is decorated with ``typing=False``.

    Sending the task without a shard, or with two, raises TaskShardError. While the shard is in maintenance, the task
    is retried with an exponential backoff, and after the last retry the StateException is raised.

    :param lock: Passed on to ``use_shard()`` as ``lock``. By default, ``use_shard()`` locks the shard while the task
        runs.
    :param options: The options of ``celery.shared_task()``. ``max_retries``, ``retry_backoff``, ``retry_backoff_max``
        and ``retry_jitter`` default to the QUILT_CELERY settings, and the exceptions of ``autoretry_for`` are retried
        besides StateException.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators.celery import task_for_shard


            @task_for_shard
            def send_invoice(invoice_id):
                Invoice.objects.get(pk=invoice_id).send()


            send_invoice.for_shard(shard).delay(invoice_id=12)
            send_invoice.delay_for_shard(shard, invoice_id=12)  # the same
            send_invoice.delay(invoice_id=12, tenant_id=3)
    """

    def decorate(func):
        return _create_quilt_task(func, 'shard', lock, options)

    return decorate if func is None else decorate(func)


def task_for_node(func=None, **options):
    """
    Turns a function into a Celery task that runs in the public schema of a node: the node that ``for_node()`` is given,
    else the node that its ``node_name`` keyword argument identifies. That schema is the target of the task. The
    function receives ``node_name`` only when it declares it by name, and then also when the task is sent without
    ``node_name``. ``shard_id`` and the keyword argument for a mapping value are ordinary keyword arguments for the
    function. Sending the task without a node raises TaskNodeError. The public schema of a node has no lock, so the task
    takes no ``lock``. ``options`` work as they do for :func:`task_for_shard`.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators.celery import task_for_node


            @task_for_node
            def refresh_exchange_rates():
                ExchangeRate.objects.refresh()


            refresh_exchange_rates.for_node('other').delay()
            refresh_exchange_rates.delay(node_name='other')  # the same
    """

    def decorate(func):
        return _create_quilt_task(func, 'node', None, options)

    return decorate if func is None else decorate(func)


__all__ = ['task_for_node', 'task_for_shard']

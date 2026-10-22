"""
The ``QUILT_CELERY`` setting, an optional dict whose keys override the defaults below.
"""

from django.conf import settings

QUILT_CELERY_DEFAULTS = {
    # The keyword argument that identifies a shard by its mapping value, which is not shard_id or node_name. None means
    # the mapping field of the MAPPING_MODEL.
    'MAPPING_KWARG': None,
    # Celery's max_retries, retry_backoff, retry_backoff_max and retry_jitter options for a task whose shard is in
    # maintenance. A decorator argument overrides each of them. The defaults retry after 5, 10, 20, 40 and 60 seconds.
    'MAX_RETRIES': 5,
    'RETRY_BACKOFF': 5,
    'RETRY_BACKOFF_MAX': 60,
    'RETRY_JITTER': False,
    # The queue of each task that a fan-out sends. None means the routing of the Celery app.
    'FANOUT_QUEUE': None,
    # The countdown of each task that a fan-out sends: a random number of seconds from 0 to an integer, or between a
    # pair of integers, which are not negative and in ascending order. None means no countdown.
    'FANOUT_SPREAD': None,
    # The dotted path of the base class of a task whose decorator is not given a base, which inherits from QuiltTask.
    # None means QuiltTask combined with the task class of the Celery app.
    'TASK_BASE': None,
}

#: How a message describes the values that a spread accepts.
SPREAD_DESCRIPTION = 'an integer that is not negative, a pair of such integers in ascending order, or None'

#: How a message describes the values that a queue accepts.
QUEUE_DESCRIPTION = 'a string or None'


def quilt_celery_setting(name):
    """
    Returns the value of ``name`` in QUILT_CELERY, else the default of ``name``. When QUILT_CELERY is not a dict, the
    function returns the default, so that a tasks module can be imported and the system check can report the setting.
    """
    quilt_celery = getattr(settings, 'QUILT_CELERY', {})

    if not isinstance(quilt_celery, dict):
        quilt_celery = {}

    return quilt_celery.get(name, QUILT_CELERY_DEFAULTS[name])


def is_spread(value):
    """
    Returns whether ``value`` is a spread of the countdown of a fan-out: None, an integer that is not negative, or a
    pair of such integers in ascending order. A boolean is an instance of int, and is not a spread.
    """

    def is_bound(bound):
        return isinstance(bound, int) and not isinstance(bound, bool) and bound >= 0

    if value is None:
        return True

    if isinstance(value, int):
        return is_bound(value)

    return isinstance(value, (list, tuple)) and len(value) == 2 and all(map(is_bound, value)) and value[0] <= value[1]


def ensure_valid_spread(spread, owner):
    """
    Raises ValueError, whose message includes ``owner``, when ``spread`` is not a spread of the countdown of a fan-out.
    """
    if not is_spread(spread):
        raise ValueError('The spread of {} is {!r}, and it accepts {}.'.format(owner, spread, SPREAD_DESCRIPTION))


def ensure_valid_queue(queue, owner):
    """
    Raises ValueError, whose message includes ``owner``, when ``queue`` is neither a string nor None.
    """
    if not isinstance(queue, (str, type(None))):
        raise ValueError('The queue of {} is {!r}, and it accepts {}.'.format(owner, queue, QUEUE_DESCRIPTION))

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
}


def quilt_celery_setting(name):
    """
    Returns the value of ``name`` in QUILT_CELERY, else the default of ``name``. When QUILT_CELERY is not a dict, the
    function returns the default, so that a tasks module can be imported and the system check can report the setting.
    """
    quilt_celery = getattr(settings, 'QUILT_CELERY', {})

    if not isinstance(quilt_celery, dict):
        quilt_celery = {}

    return quilt_celery.get(name, QUILT_CELERY_DEFAULTS[name])

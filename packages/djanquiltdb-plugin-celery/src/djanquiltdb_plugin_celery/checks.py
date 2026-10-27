from django.conf import settings
from django.core import checks
from djanquiltdb.utils import get_mapping_class

from djanquiltdb_plugin_celery.settings import (
    QUEUE_DESCRIPTION,
    QUILT_CELERY_DEFAULTS,
    SPREAD_DESCRIPTION,
    is_spread,
)
from djanquiltdb_plugin_celery.targets import NODE_NAME_KWARG, SHARD_ID_KWARG
from djanquiltdb_plugin_celery.tasks import import_task_base

#: For each key of QUILT_CELERY: a function that returns whether a value of the key is valid, and how a message
#: describes the valid values. A boolean is an instance of int, so the keys that accept an integer and not a boolean
#: exclude it.
QUILT_CELERY_VALIDATORS = {
    'MAPPING_KWARG': (lambda value: isinstance(value, (str, type(None))), 'a string or None'),
    'MAX_RETRIES': (
        lambda value: value is None or (isinstance(value, int) and not isinstance(value, bool)),
        'an integer or None',
    ),
    'RETRY_BACKOFF': (lambda value: isinstance(value, (bool, int)), 'a boolean or an integer'),
    'RETRY_BACKOFF_MAX': (lambda value: isinstance(value, int) and not isinstance(value, bool), 'an integer'),
    'RETRY_JITTER': (lambda value: isinstance(value, bool), 'a boolean'),
    'FANOUT_QUEUE': (lambda value: isinstance(value, (str, type(None))), QUEUE_DESCRIPTION),
    'FANOUT_SPREAD': (is_spread, SPREAD_DESCRIPTION),
    'TASK_BASE': (lambda value: isinstance(value, (str, type(None))), 'a dotted path or None'),
}


def check_task_base(task_base):
    """
    Returns the error for a ``QUILT_CELERY['TASK_BASE']`` that is a string, but does not import or does not name a
    subclass of QuiltTask, or None.
    """
    try:
        import_task_base(task_base)
    except ImportError as error:
        return checks.Error(
            "QUILT_CELERY['TASK_BASE'] is {!r}, which does not import: {}".format(task_base, error),
            id='djanquiltdb_plugin_celery.E002',
        )
    except TypeError as error:
        return checks.Error(
            str(error),
            hint='Name a subclass of djanquiltdb.base.celery.QuiltTask, or remove the key.',
            id='djanquiltdb_plugin_celery.E002',
        )

    return None


@checks.register(checks.Tags.compatibility)
def check_quilt_celery_setting(app_configs, **kwargs):
    """
    Reports a QUILT_CELERY setting, key or value that the celery plugin cannot use.
    """
    quilt_celery = getattr(settings, 'QUILT_CELERY', {})

    if not isinstance(quilt_celery, dict):
        return [checks.Error('QUILT_CELERY is not a dict.', id='djanquiltdb_plugin_celery.E002')]

    messages = [
        checks.Error(
            'QUILT_CELERY has the key {!r}, which the celery plugin does not read.'.format(name),
            hint='The plugin reads the keys {}.'.format(', '.join(QUILT_CELERY_DEFAULTS)),
            id='djanquiltdb_plugin_celery.E001',
        )
        for name in quilt_celery
        if name not in QUILT_CELERY_DEFAULTS
    ]

    for name, (is_valid, description) in QUILT_CELERY_VALIDATORS.items():
        if name in quilt_celery and not is_valid(quilt_celery[name]):
            messages.append(
                checks.Error(
                    "QUILT_CELERY['{}'] is {!r}, and it accepts {}.".format(name, quilt_celery[name], description),
                    id='djanquiltdb_plugin_celery.E002',
                )
            )

    if quilt_celery.get('MAPPING_KWARG') in (SHARD_ID_KWARG, NODE_NAME_KWARG):
        messages.append(
            checks.Error(
                "QUILT_CELERY['MAPPING_KWARG'] is {!r}, which already identifies a target in another way.".format(
                    quilt_celery['MAPPING_KWARG']
                ),
                hint='Set it to the keyword argument that has the mapping value, such as tenant_id.',
                id='djanquiltdb_plugin_celery.E002',
            )
        )
    elif quilt_celery.get('MAPPING_KWARG') and get_mapping_class() is None:
        messages.append(
            checks.Error(
                "QUILT_CELERY['MAPPING_KWARG'] is {!r}, but QUILT_DB has no MAPPING_MODEL in which a task can look up "
                'the shard of a mapping value.'.format(quilt_celery['MAPPING_KWARG']),
                hint="Set QUILT_DB['MAPPING_MODEL'], or remove QUILT_CELERY['MAPPING_KWARG'].",
                id='djanquiltdb_plugin_celery.E002',
            )
        )

    if isinstance(quilt_celery.get('TASK_BASE'), str):
        task_base_error = check_task_base(quilt_celery['TASK_BASE'])

        if task_base_error is not None:
            messages.append(task_base_error)

    return messages

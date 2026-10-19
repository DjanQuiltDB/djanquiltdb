from django.conf import settings
from django.core import checks
from djanquiltdb.utils import get_mapping_class

from djanquiltdb_plugin_celery.settings import QUILT_CELERY_DEFAULTS
from djanquiltdb_plugin_celery.targets import NODE_NAME_KWARG, SHARD_ID_KWARG

#: The types that each key of QUILT_CELERY takes, and how a message describes them.
QUILT_CELERY_TYPES = {
    'MAPPING_KWARG': ((str, type(None)), 'a string or None'),
    'MAX_RETRIES': ((int, type(None)), 'an integer or None'),
    'RETRY_BACKOFF': ((bool, int), 'a boolean or an integer'),
    'RETRY_BACKOFF_MAX': ((int,), 'an integer'),
    'RETRY_JITTER': ((bool,), 'a boolean'),
}


@checks.register(checks.Tags.compatibility)
def check_quilt_celery_setting(app_configs, **kwargs):
    """
    Reports a QUILT_CELERY setting that is not a dict (djanquiltdb_plugin_celery.E002), a key that the plugin does not
    read (djanquiltdb_plugin_celery.E001), and a value of the wrong type (djanquiltdb_plugin_celery.E002).
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

    for name, (types, description) in QUILT_CELERY_TYPES.items():
        if name in quilt_celery and not isinstance(quilt_celery[name], types):
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

    return messages

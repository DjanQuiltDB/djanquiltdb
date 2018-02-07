from django.apps import AppConfig, apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from sharding.utils import ShardingMode


class ShardingConfig(AppConfig):
    name = 'sharding'
    verbose_name = "Sharding"

    def ready(self):
        from .models import BaseShard

        if 'SHARDING' not in dir(settings) or not isinstance(settings.SHARDING, dict):
            raise ImproperlyConfigured('Missing or incorrect type of a setting SHARDING.')

        # Validate shard and node class settings
        if 'SHARD_CLASS' not in settings.SHARDING:
            raise ImproperlyConfigured('Missing or incorrect type of a setting SHARDING["{}"].'.format('SHARD_CLASS'))
        class_ = import_string(settings.SHARDING['SHARD_CLASS'])
        if not issubclass(class_, BaseShard):
            raise ImproperlyConfigured(
                'The type {} should inherit from {}.'.format(settings.SHARDING['SHARD_CLASS'], BaseShard.__name__))
        if hasattr(class_, 'sharding_mode') and getattr(class_, 'sharding_mode') == ShardingMode.SHARDED:
            raise ImproperlyConfigured(
                'The Shard model cannot itself be sharded. It can only be non-sharded or mirrored.'
            )

        override_sharding_mode = settings.SHARDING.setdefault('OVERRIDE_SHARDING_MODE', {})
        if not isinstance(override_sharding_mode, dict):
            raise ImproperlyConfigured('Incorrect setting value of SHARDING["OVERRIDE_SHARDING_MODE"].')

        for key, value in override_sharding_mode.items():
            _validate_override_shardin_mode_entry(key, value)

        if 'DATABASE_ROUTERS' not in dir(settings) or 'sharding.utils.DynamicDbRouter' not in settings.DATABASE_ROUTERS:
            raise ImproperlyConfigured(
                'sharding.utils.DynamicDbRouter must be present in the DATABASE_ROUTERS setting.')

        if 'SESSION_ENGINE' in dir(settings) and \
            settings.SESSION_ENGINE == 'django.contrib.sessions.backends.cached_db' and \
                getattr(get_user_model(), 'sharding_mode', False) in [ShardingMode.MIRRORED, ShardingMode.SHARDED]:

            raise ImproperlyConfigured(
                "When the user model is sharded, you cannot use django.contrib.sessions.backends.cached_db "
                "to store sessions. It references the user table and won't know where to find it."
            )


def _validate_override_shardin_mode_entry(key, value):
    if not (isinstance(key, (tuple, list)) and len(key) in (1, 2) and isinstance(value, ShardingMode)):
        raise ImproperlyConfigured('The override sharding mode entry is improperly configured: '
                                   '{{ {}: {} }}'.format(repr(key), repr(value)))

    app_label = key[0]
    if not app_label.islower():
        raise ImproperlyConfigured('The override sharding mode entry app_label is not in lower case: '
                                   '{{ {}: {} }}'.format(repr(key), repr(value)))
    if len(key) == 2:
        model_name = key[1]
        if not model_name.islower():
            raise ImproperlyConfigured('The override sharding mode entry model_name is not in lower case: '
                                       '{{ {}: {} }}'.format(repr(key), repr(value)))
        try:
            apps.get_model(app_label, model_name)
        except LookupError:
            raise ImproperlyConfigured('Cannot find model to override sharding mode: ({}, {})'.format(app_label,
                                                                                                      model_name))
    else:  # len(key) == 1
        try:
            apps.get_app_config(app_label)
        except LookupError:
            raise ImproperlyConfigured('Cannot find app_label to override sharding mode: {}'.format(app_label))

from django.apps import AppConfig
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

        if 'DATABASE_ROUTERS' not in dir(settings) or 'sharding.utils.DynamicDbRouter' not in settings.DATABASE_ROUTERS:
            raise ImproperlyConfigured(
                'sharding.utils.DynamicDbRouter must be present in the DATABASE_ROUTERS setting.')

        if 'SESSION_ENGINE' in dir(settings) and \
                settings.SESSION_ENGINE == 'django.contrib.sessions.backends.cached_db':
            if getattr(get_user_model(), 'sharding_mode', False) in [ShardingMode.MIRRORED, ShardingMode.SHARDED]:
                raise ImproperlyConfigured(
                    "When the user model is sharded, you cannot use django.contrib.sessions.backends.cached_db "
                    "to store sessions. It references the user table and won't know where to find it."
                )

from django.apps import AppConfig
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string


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

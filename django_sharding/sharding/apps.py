from django.apps import AppConfig
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string


class ShardingConfig(AppConfig):
    name = 'sharding'
    verbose_name = "Sharding"

    def ready(self):
        from .models import BaseShard, BaseNode

        if 'SHARDING' not in dir(settings) or not isinstance(settings.SHARDING, dict):
            raise ImproperlyConfigured('Missing or incorrect type of a setting SHARDING.')

        # Validate shard and node class settings
        setting_model_class_base = (('SHARD_CLASS', BaseShard),
                                    ('NODE_CLASS', BaseNode),)
        for setting_name, base_class in setting_model_class_base:
            if setting_name not in settings.SHARDING:
                raise ImproperlyConfigured(
                    'Missing or incorrect type of a setting SHARDING["{}"].'.format(setting_name))
            class_name = settings.SHARDING[setting_name]
            class_ = import_string(class_name)
            if not issubclass(class_, base_class):
                raise ImproperlyConfigured(
                    'The type {} should inherit from {}.'.format(class_name, base_class.__name__))
        shard_class = import_string(settings.SHARDING['SHARD_CLASS'])
        node_class_name = settings.SHARDING['NODE_CLASS']
        node_class = import_string(node_class_name)

        if not any(f.related_model == node_class for f in shard_class._meta.get_fields() if f.many_to_one):
            raise ImproperlyConfigured('Missing many-to-one relation from Shard to Node class.')

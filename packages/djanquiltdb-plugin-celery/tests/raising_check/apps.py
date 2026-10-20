from django.apps import AppConfig, apps
from django.core import checks
from django.db import router


class ShardedModelOnTheNode(RuntimeError):
    pass


def raise_for_a_sharded_model_on_the_node(app_configs=None, **kwargs):
    if router.allow_migrate_model('default', apps.get_model('example', 'Widget')):
        raise ShardedModelOnTheNode("The router places the sharded Widget on the node 'default'.")

    return []


class RaisingCheckConfig(AppConfig):
    name = 'raising_check'

    def ready(self):
        checks.register(raise_for_a_sharded_model_on_the_node)

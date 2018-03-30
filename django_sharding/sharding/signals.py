from django.db import connection

from sharding.utils import get_shard_class


def store_initial_shard(instance, **kwargs):
    instance._schema_name = connection.get_schema()
    instance._node_name = connection.alias
    instance._shard = get_shard_class().objects.get(schema_name=instance._schema_name, node_name=instance._node_name)

from django.db import connection


def store_initial_shard(instance, **kwargs):
    instance._schema_name = connection.get_schema()
    instance._node_name = connection.alias

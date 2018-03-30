def store_initial_shard(instance, **kwargs):
    from django.db import connection

    instance._schema_name = connection.get_schema()
    instance._node_name = connection.alias

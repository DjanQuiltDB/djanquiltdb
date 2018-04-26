from sharding.exceptions import ShardingError
from sharding.utils import get_shard_class, use_shard_for, use_shard


class InstanceShardOptions:
    def __init__(self, schema_name, node_name, id, mapping_value, active_only_schemas):
        self.schema_name = schema_name
        self.node_name = node_name
        self.id = id
        self.mapping_value = mapping_value
        self.active_only_schemas = active_only_schemas

    def get(self):
        """
        Lazy method that returns the shard the instance was retrieved from
        """
        if not self.id:
            raise ShardingError('Shard ID is not known for this instance')

        return get_shard_class().objects.get(id=self.id)

    def use(self):
        """
        Returns the context manager to enter a shard with the same parameters the instance was fetched with
        """
        if self.mapping_value:
            # It turns out we fetched the model instance with use_shard_for, so we know the mapping value.
            # So let's use that value to target the shard in the model method. use_shard_for will do a DB query,
            # which allows us to see if the mapping object still has state active.
            use_shard_func = use_shard_for
            use_shard_kwargs = {'target_value': self.mapping_value}
        else:
            # If we didn't use the mapping value to fetch the model instance, then we did it with use_shard.
            # We could've done it with selecting the shard or by providing the node name and schema name.
            # If we did it with providing the shard, then use that now to target the shard in the model methods.
            # We do a new DB request to the shard to see if it still has state active.
            use_shard_func = use_shard
            use_shard_kwargs = {'shard': self.get()} if self.id else {'node_name': self.node_name,
                                                                      'schema_name': self.schema_name}

            # If the model instance has been fetched with active_only_schemas set to True, then we pass that to
            # the model methods as well. This is needed for commands like move_data_to_shard, that do model
            # methods like delete() when the shard is in maintenance.
            use_shard_kwargs['active_only_schemas'] = self.active_only_schemas

        return use_shard_func(**use_shard_kwargs)


def store_initial_shard(instance, **kwargs):
    from django.db import connection

    instance._shard = InstanceShardOptions(
        schema_name=connection.get_schema(),
        node_name=connection.alias,
        id=connection._shard_id,
        mapping_value=connection._mapping_value,
        active_only_schemas=connection._active_only_schemas
    )

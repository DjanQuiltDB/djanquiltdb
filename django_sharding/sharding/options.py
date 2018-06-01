from sharding.db import connection
from sharding.exceptions import ShardingError
from sharding.utils import use_shard_for, use_shard, get_shard_class


class InstanceShardOptions:
    def __init__(self, schema_name, node_name, id_, mapping_value, active_only_schemas, lock,
                 check_active_mapping_values):
        self.schema_name = schema_name
        self.node_name = node_name
        self.id = id_
        self.mapping_value = mapping_value
        self.active_only_schemas = active_only_schemas
        self.lock = lock
        self.check_active_mapping_values = check_active_mapping_values

    @classmethod
    def from_connection(cls, connection):
        return cls(
            schema_name=connection.get_schema(),
            node_name=connection.alias,
            id_=connection._shard_id,
            mapping_value=connection._mapping_value,
            active_only_schemas=connection._active_only_schemas,
            lock=connection._lock
        )

    DEFINING_ATTRIBUTES = ('schema_name', 'node_name', 'id', 'mapping_value', 'active_only_schemas', 'lock',
                           'check_active_mapping_values')

    def __eq__(self, other):
        if not isinstance(other, InstanceShardOptions):
            return False

        return hash(self) == hash(other)

    def __hash__(self):
        return hash(tuple(getattr(self, attr) for attr in self.DEFINING_ATTRIBUTES))


def get_shard_from_instance_options(options):
    """
    Lazy method that returns the shard the instance was retrieved from
    """
    if not options.id:
        raise ShardingError('Shard ID is not known for this instance')

    return get_shard_class().objects.get(id=options.id)


def connection_has_same_shard_options(options):
    return InstanceShardOptions.from_connection(connection) == options


def use_shard_from_instance_options(options):
    """
    Returns the context manager to enter a shard with the same parameters the instance was fetched with
    """
    if options.mapping_value:
        # It turns out we fetched the model instance with use_shard_for, so we know the mapping value.
        # So let's use that value to target the shard in the model method. use_shard_for will do a DB query,
        # which allows us to see if the mapping object still has state active.
        use_shard_func = use_shard_for
        use_shard_kwargs = {'target_value': options.mapping_value}
    else:
        # If we didn't use the mapping value to fetch the model instance, then we did it with use_shard.
        # We could've done it with selecting the shard or by providing the node name and schema name.
        # If we did it with providing the shard, then use that now to target the shard in the model methods.
        # We do a new DB request to the shard to see if it still has state active.
        use_shard_func = use_shard
        use_shard_kwargs = {'shard': get_shard_from_instance_options(options)} if options.id else \
            {'node_name': options.node_name, 'schema_name': options.schema_name}

        # If the model instance has been fetched with active_only_schemas set to True, then we pass that to
        # the model methods as well. This is needed for commands like move_data_to_shard, that do model
        # methods like delete() when the shard is in maintenance.
        use_shard_kwargs['active_only_schemas'] = options.active_only_schemas

        # Pass the lock argument as well. If an object was fetched during a use_shard with lock=False
        # we don't want a lock to be set because we used a function on that object.
        use_shard_kwargs['lock'] = options.lock

    return use_shard_func(**use_shard_kwargs)

from sharding import State
from sharding.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from sharding.utils import use_shard, get_shard_class, StateException, get_mapping_class, use_shard_for


class ShardOptions:
    def __init__(self, **options):
        # Save the options, so we can compare it with other ShardOptions instances in `__eq__`
        self.options = frozenset(options.items())

        self.node_name = options.pop('node_name')
        self.schema_name = options.pop('schema_name')

        self.shard_id = options.pop('shard_id', None)
        self.mapping_value = options.pop('mapping_value', None)

        # Keeps track whether we activated the connection in a use_shard context
        self.use_shard = options.pop('use_shard', False)

        # Saving the optional kwargs passed to ShardOptions by use_shard
        self.kwargs = options

        # Get whether we should lock the shard or not. It's not allowed to lock the public schema, so we make sure we
        # don't accidentally do that.
        if self.kwargs.get('lock') and self.schema_name == PUBLIC_SCHEMA_NAME:
            raise ValueError('You cannot lock the public schema')

        self.lock = self.kwargs.get('lock', not self.schema_name == PUBLIC_SCHEMA_NAME)

    def __hash__(self):
        return hash(self.options)

    def __eq__(self, other):
        if not isinstance(other, ShardOptions):
            return False

        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return 'ShardOptions for {}|{}'.format(self.node_name, self.schema_name)

    @classmethod
    def from_shard(cls, shard, **kwargs):
        active_only_schemas = kwargs.get('active_only_schemas', True)
        check_active_mapping_values = kwargs.get('check_active_mapping_values', False)

        if active_only_schemas and shard.state != State.ACTIVE:
            raise StateException('Shard {} state is {}'.format(shard, shard.state), shard.state)

        if check_active_mapping_values:
            mapping_model = get_mapping_class()
            if not mapping_model:
                raise ValueError("You set 'check_active_mapping_values' to True while you didn't define the "
                                 "mapping model.")

            if mapping_model.objects.for_shard(shard).in_maintenance().exists():
                raise StateException('Shard {} contains mapping objects that are in maintenance'.format(shard),
                                     State.MAINTENANCE)

        return cls(
            schema_name=shard.schema_name,
            node_name=shard.node_name,
            shard_id=shard.id,
            **kwargs
        )

    @classmethod
    def from_alias(cls, alias):
        if isinstance(alias, cls):
            return alias
        elif isinstance(alias, get_shard_class()):
            return cls.from_shard(alias)
        elif isinstance(alias, str):
            node_name, schema_name = alias.split('|') if '|' in alias else (alias, PUBLIC_SCHEMA_NAME)
            return cls(node_name=node_name, schema_name=schema_name)
        elif isinstance(alias, tuple) and len(alias) == 2:
            node_name, schema_name = alias
            return cls(node_name=node_name, schema_name=schema_name)

        raise ValueError('{} is an invalid connection alias.'.format(alias))

    @property
    def lock_keys(self):
        lock_keys = []

        if self.shard_id:
            lock_keys.append('shard_{}'.format(self.shard_id))

        if self.mapping_value:
            lock_keys.append('mapping_{}'.format(self.mapping_value))

        return lock_keys

    def is_public_schema(self):
        return self.schema_name == PUBLIC_SCHEMA_NAME

    def use(self):
        if self.mapping_value:
            return use_shard_for(self.mapping_value, **self.kwargs)
        elif self.shard_id:
            return use_shard(get_shard_class().objects.get(id=self.shard_id), **self.kwargs)

        return use_shard(node_name=self.node_name, schema_name=self.schema_name, **self.kwargs)

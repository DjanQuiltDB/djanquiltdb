from djanquiltdb import State
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import StateException, get_mapping_class, get_shard_class, use_shard, use_shard_for


class ShardOptions:
    """
    A resolved routing target: the node and schema a query is to reach, plus how it was arrived at.

    This is what the router hands back in place of a plain connection alias, and what ``connections`` accepts as one.
    Build it from whatever you have with :meth:`from_alias`, which also takes a shard instance, a ``'node|schema'``
    string or a ``(node, schema)`` tuple.
    """

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
        """
        Options targeting ``shard``.

        Raises ``StateException`` when the shard is not ACTIVE, unless ``active_only_schemas`` is False; pass
        ``check_active_mapping_values`` to refuse a shard any of whose mapping rows are in maintenance too.
        """
        active_only_schemas = kwargs.get('active_only_schemas', True)
        check_active_mapping_values = kwargs.get('check_active_mapping_values', False)

        if active_only_schemas and shard.state != State.ACTIVE:
            raise StateException('Shard {} state is {}'.format(shard, shard.state), shard.state)

        if check_active_mapping_values:
            mapping_model = get_mapping_class()
            if not mapping_model:
                raise ValueError(
                    "You set 'check_active_mapping_values' to True while you didn't define the mapping model."
                )

            if mapping_model.objects.for_shard(shard).in_maintenance().exists():
                raise StateException(
                    'Shard {} contains mapping objects that are in maintenance'.format(shard), State.MAINTENANCE
                )

        return cls(schema_name=shard.schema_name, node_name=shard.node_name, shard_id=shard.id, **kwargs)

    @classmethod
    def from_alias(cls, alias):
        """
        Options for any of the forms ``connections`` accepts as an alias: an existing ``ShardOptions``, a shard
        instance, a ``'node'`` or ``'node|schema'`` string, or a ``(node, schema)`` tuple. A string naming only the
        node targets its public schema.
        """
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
        """The advisory lock names this target implies, one per shard or mapping value it was resolved from."""
        lock_keys = []

        if self.shard_id:
            lock_keys.append('shard_{}'.format(self.shard_id))

        if self.mapping_value:
            lock_keys.append('mapping_{}'.format(self.mapping_value))

        return lock_keys

    def is_public_schema(self):
        """Whether this target is a node's public schema rather than a shard's."""
        return self.schema_name == PUBLIC_SCHEMA_NAME

    def use(self):
        """
        The context manager that makes this target the active connection, equivalent to whichever of ``use_shard``
        or ``use_shard_for`` matches how these options were resolved.
        """
        if self.mapping_value:
            return use_shard_for(self.mapping_value, **self.kwargs)
        elif self.shard_id:
            return use_shard(get_shard_class().objects.get(id=self.shard_id), **self.kwargs)

        return use_shard(node_name=self.node_name, schema_name=self.schema_name, **self.kwargs)

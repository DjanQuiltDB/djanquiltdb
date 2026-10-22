import logging
from contextlib import contextmanager
from contextvars import ContextVar
from threading import local

from django.db import ProgrammingError

from djanquiltdb import ShardingMode, public_modes
from djanquiltdb.options import ShardOptions
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import (
    get_model_definition,
    get_model_placement,
    get_model_sharding_mode,
    get_model_single_node_alias,
    get_placement,
    get_primary_db_alias,
    placement_of_sharding_mode_value,
    relation_with_a_model_on_a_single_node_is_allowed,
    resolve_node_alias,
)

logger = logging.getLogger(__name__)

_active_connection = local()


def get_active_connection():
    """
    Returns the active connection of this thread, which set_active_connection() sets: ShardOptions or a connection
    alias. When the thread has no active connection, returns QUILT_DB['PRIMARY_DB_ALIAS'], or DEFAULT_DB_ALIAS when
    QUILT_DB does not have that key.
    """
    try:
        return _active_connection.connection
    except AttributeError:
        return get_primary_db_alias()


def set_active_connection(connection):
    """
    Make ``connection`` the target for this thread until it is set again.

    ``use_shard`` and the middleware do this for you; call it directly only where neither fits, and reset it
    afterwards, since the value outlives the block that set it.
    """
    setattr(_active_connection, 'connection', connection)


_placement_is_node_scoped = ContextVar('placement_is_node_scoped', default=False)


@contextmanager
def node_scoped_placement():
    """
    Make ``allow_migrate`` answer a bare node name for the node as a whole, rather than for its public schema, until the
    block ends. DjanQuiltDB runs the system checks in this block, because the database properties that they check
    belong to the node.
    """
    token = _placement_is_node_scoped.set(True)
    try:
        yield
    finally:
        _placement_is_node_scoped.reset(token)


class DynamicDbRouter:
    """
    A router that decides what db to read from based on a variable local to the current thread.
    """

    def db_for_read(self, model, **hints):
        """
        Returns the public schema of its node for a model on a single node, and the primary connection for a model that
        has ``route_to_primary_db`` set. For any other model, returns the first of: the database of the ``instance``
        hint, the ``_shard_options`` hint, and the active connection, which ``use_shard`` usually sets.
        """
        model_definition = get_model_definition(model)
        # The placement is resolved on every call. That costs about 1% of a query round trip, and in return nothing has
        # to be cleared when the settings or the sharding mode of a model change, as they do in tests.
        single_node_alias = get_model_single_node_alias(model_definition)

        if single_node_alias is not None:
            return single_node_alias

        if getattr(model_definition, 'route_to_primary_db', None):
            return get_primary_db_alias()
        shard_options = hints.get('_shard_options')
        instance_options = hints.get('instance') is not None and hints['instance']._state.db
        return instance_options or shard_options or get_active_connection()

    def db_for_write(self, model, **hints):
        """
        For a MIRRORED model, returns the primary connection when db_for_read returns an alias, or options for a node
        other than the primary node. In all other cases, returns what db_for_read returns.
        """
        if get_model_sharding_mode(model) is ShardingMode.MIRRORED:
            default_node_name = get_primary_db_alias()
            db = self.db_for_read(model, **hints)
            if not isinstance(db, ShardOptions) or db.node_name != default_node_name:
                return default_node_name

        return self.db_for_read(model, **hints)

    def allow_relation(self, obj1, obj2, *args, **kwargs):
        """
        Allow a relation between two models placed the same way, between two that both live in a public schema, and
        from a sharded model to a public or mirrored one. Not the other way around: a public row cannot point into
        one shard's schema. ``obj1`` is the object that is referred to, and ``obj2`` the object that refers to it.

        A model on a single node may refer to a public or mirrored model, which every node has, and to a model on the
        same node. Only a model on the same node may refer to it.
        """
        obj1_mode, obj1_node = get_model_placement(obj1)
        obj2_mode, obj2_node = get_model_placement(obj2)

        # We have no opinion if neither of the models have sharding mode set.
        if not obj1_mode and not obj2_mode:
            return None

        allowed_with_a_model_on_a_single_node = relation_with_a_model_on_a_single_node_is_allowed(
            referred_mode=obj1_mode, referred_node=obj1_node, referring_mode=obj2_mode, referring_node=obj2_node
        )

        if allowed_with_a_model_on_a_single_node is not None:
            return allowed_with_a_model_on_a_single_node

        # All is good if they both have a sharding mode set.
        if obj1_mode == obj2_mode:
            return True

        # Also good if both belong to the pubic schema
        if (not obj1_mode or obj1_mode in public_modes) and (not obj2_mode or obj2_mode in public_modes):
            return True

        # Allow relations between SHARDED -> MIRRORED/PUBLIC. (But not the other way around).
        # This does not go for SHARDED -> non-sharded. We cannot confirm if those models exist on all nodes.
        if (obj1_mode in public_modes) and obj2_mode is ShardingMode.SHARDED:
            return True

        # Do not allow any other combination
        return False

    def allow_migrate(self, connection_name, app_label, model_name=None, **hints):
        """
        Decide whether an operation runs against the schema ``connection_name`` resolves to.

        Sharded models are applied everywhere but the public schema, public and mirrored ones only in a public
        schema, and undecorated ones only in the default node's. An operation carrying no model, such as ``RunSQL``,
        needs a ``sharding_mode`` hint or an ``OVERRIDE_SHARDING_MODE`` entry to be placed at all.

        Inside ``node_scoped_placement()``, which is where the system checks run, a bare node name stands for the whole
        node, so it allows sharded models too. So do the ``ShardOptions`` that ``ShardOptions.from_alias()`` returns for
        a bare node name.
        """
        options = ShardOptions.from_alias(connection_name)
        node_name, schema_name = (options.node_name, options.schema_name)
        model = hints.pop('model', False)

        # This is for our test cases.
        if model and getattr(model, 'test_model', False):
            return False

        # sharding_mode can be set as hints (on run_pyton/run_sql for example), also as single_node_unsharded(node).
        try:
            if hints.get('sharding_mode'):
                sharding_mode, node = placement_of_sharding_mode_value(hints['sharding_mode'])
            else:
                sharding_mode, node = get_placement(app_label, model_name)

            if sharding_mode is ShardingMode.SINGLE_NODE_UNSHARDED and node is None:
                # An operation without a model names its node in the 'node' hint, as it names its mode.
                node = hints['node'] if 'node' in hints else get_placement(app_label, model_name)[1]
        except LookupError:
            # Model does not exist anymore, probably because it's removed in another migration. Ignore it now.
            # Note that there is a separation between state_operations and database_operations.
            # state_operations do not take heed of allow_migrate. They will therefore always be performed.
            # Where database_operations ask allow_migrate if they should proceed.
            # Creating and removing a model will therefore happen in state,
            # but if the model is unknown to apps no mutations will be performed on the database.
            logger.warning('Migration operation for unknown models are ignored. Are you sure this model still exists?')
            return False  # Don't execute operations on missing models.

        if sharding_mode is None:
            # This happens when no model_name is given.
            # We only know the sharding_mode when it is overridden in the settings.
            # If we get None from get_sharding_mode, there is nothing we can do with it.
            raise ProgrammingError(
                'Cannot determine sharding mode for this operation '
                f'(app {app_label}{f", model {model_name}" if model_name else ""}). '
                'Are you sure it is bound to an existing model or has hints? '
                f'app_label: {app_label}, model_name: {model_name}'
            )

        elif sharding_mode == ShardingMode.SHARDED:
            if _placement_is_node_scoped.get() and options.names_only_the_node:
                return True
            # Sharded models should never reside in the public schema.
            # Only on templates and the shared schemas.
            return schema_name != PUBLIC_SCHEMA_NAME
        elif sharding_mode in public_modes:
            # Mirrored models belong to public schemas and no where else.
            return schema_name == PUBLIC_SCHEMA_NAME
        elif sharding_mode is ShardingMode.SINGLE_NODE_UNSHARDED:
            if node is None:
                raise ProgrammingError(
                    'Cannot determine the node of this operation on a single node '
                    f'(app {app_label}{f", model {model_name}" if model_name else ""}). Give it a node hint.'
                )
            return node_name == resolve_node_alias(node) and schema_name == PUBLIC_SCHEMA_NAME
        else:
            # Non-sharded models only belong to the default database.
            return node_name == 'default' and schema_name == PUBLIC_SCHEMA_NAME

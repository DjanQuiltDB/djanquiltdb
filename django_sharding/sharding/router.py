import logging
from threading import local

from django.db import DEFAULT_DB_ALIAS, ProgrammingError

from sharding import ShardingMode
from sharding.options import ShardOptions
from sharding.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from sharding.utils import get_model_sharding_mode, get_sharding_mode

logger = logging.getLogger(__name__)

_active_connection = local()


def get_active_connection():
    return getattr(_active_connection, 'connection', DEFAULT_DB_ALIAS)


def set_active_connection(connection):
    setattr(_active_connection, 'connection', connection)


class DynamicDbRouter:
    """
    A router that decides what db to read from based on a variable local to the current thread.
    """
    def db_for_read(self, model, **hints):
        shard_options = hints.get('_shard_options')
        instance_options = hints.get('instance') is not None and hints['instance']._state.db

        return instance_options or shard_options or get_active_connection()

    db_for_write = db_for_read

    def allow_relation(self, obj1, obj2, *args, **kwargs):
        obj1_mode = get_model_sharding_mode(obj1)
        obj2_mode = get_model_sharding_mode(obj2)

        if obj1_mode or obj2_mode:
            return obj1_mode and obj2_mode  # all is good if they both have a sharding mode set.

        return None  # We have no opinion if neither of the models have sharding mode set.

    def allow_syncdb(self, *args, **kwargs):
        model = kwargs.pop('model', False)
        if model and getattr(model, 'test_model', False):
            return False

        return None

    def allow_migrate(self, connection_name, app_label, model_name=None, **hints):
        options = ShardOptions.from_alias(connection_name)
        node_name, schema_name = (options.node_name, options.schema_name)
        model = hints.pop('model', False)

        # This is for our test cases.
        if model and getattr(model, 'test_model', False):
            return False

        # sharding_mode can be set as hints (on run_pyton/run_sql for example).
        try:
            sharding_mode = hints.get('sharding_mode') or get_sharding_mode(app_label, model_name)
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
            raise ProgrammingError('Cannot determine sharding mode for this operation. '
                                   'Are you sure it is bound to an existing model or has hints?')

        elif sharding_mode == ShardingMode.SHARDED:
            # Sharded models should never reside in the public schema.
            # Only on templates and the shared schemas.
            return schema_name != PUBLIC_SCHEMA_NAME
        elif sharding_mode == ShardingMode.MIRRORED:
            # Mirrored models belong to public schemas and no where else.
            return schema_name == PUBLIC_SCHEMA_NAME
        else:
            # Non-sharded models only belong to the default database.
            return node_name == 'default' and schema_name == PUBLIC_SCHEMA_NAME

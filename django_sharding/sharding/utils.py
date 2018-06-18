# DynamicDbRouter:
#
# source: https://github.com/ambitioninc/django-dynamic-db-router
# credits: ambitioninc
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import logging
import threading

from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.management.commands.migrate import Command as MigrateCommand
from django.db import connections, ProgrammingError
from django.db.migrations.executor import MigrationExecutor
from django.db.transaction import Atomic
from django.utils.module_loading import import_string
from functools import wraps

from sharding import ShardingMode, State
from sharding.db import connection

THREAD_LOCAL = threading.local()

logger = logging.getLogger(__name__)


class StateException(Exception):
    def __init__(self, message, state, *args, **kwargs):
        super().__init__(message, *args, **kwargs)
        self.state = state


def get_shard_class():
    """ Helper function to get implemented Shard class """
    return import_string(settings.SHARDING['SHARD_CLASS'])


def get_template_name():
    return settings.SHARDING.get('TEMPLATE_NAME', 'template')


def get_mapping_class():
    """
    Helper function to get implemented Mapping model, if the project has one.
    """
    if 'MAPPING_MODEL' not in settings.SHARDING:
        return None
    return import_string(settings.SHARDING['MAPPING_MODEL'])


class DynamicDbRouter(object):
    """ A router that decides what db to read from based on a variable local to the current thread. """

    def db_for_read(self, model, **hints):
        override_list = getattr(THREAD_LOCAL, 'DB_OVERRIDE', None)
        return override_list and override_list[-1]

    def db_for_write(self, model, **hints):
        override_list = getattr(THREAD_LOCAL, 'DB_OVERRIDE', None)
        return override_list and override_list[-1]

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
        schema_name = connections[connection_name].get_schema()
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
            return schema_name != 'public'
        elif sharding_mode == ShardingMode.MIRRORED:
            # Mirrored models belong to public schemas and no where else.
            return schema_name == 'public'
        else:
            # Non-sharded models only belong to the default database.
            return connection_name == 'default' and schema_name == 'public'


def _node_exists(node_name):
    if node_name not in connections:
        raise ValueError("Connection '{}' does not exist. Is it listed in settings.DATABASES?".format(node_name))


def _use_connection(node):
    if not hasattr(THREAD_LOCAL, 'DB_OVERRIDE') or not THREAD_LOCAL.DB_OVERRIDE:
        THREAD_LOCAL.DB_OVERRIDE = [node]
    else:
        THREAD_LOCAL.DB_OVERRIDE.append(node)
    return connections[node]


def _set_schema(schema_name, _connection=None, include_public=True, override_class_method_use_shard=False,
                shard_id=None, mapping_value=None, active_only_schemas=True, lock=True,
                check_active_mapping_values=False):
    if not _connection:
        _connection = connection
    _connection.set_schema(schema_name, include_public=include_public,
                           override_class_method_use_shard=override_class_method_use_shard, shard_id=shard_id,
                           mapping_value=mapping_value, active_only_schemas=active_only_schemas, lock=lock,
                           check_active_mapping_values=check_active_mapping_values)


class use_shard(object):
    """
    use_shard can be used as a decorator and as context manager to send all queries in the scope to the correct shard.

    If the shard's state is not State.ACTIVE, use_shard will raise a StateException error.

    :param object shard: Provide a Shard model object so the context manager knows all about where to send the queries.
    :param str node_name: Alternatively, you can provide the name of the database the schema can be found and the name
        of the schema.
    :param str schema_name: Alternatively, you can provide the name of the database the schema can be found and the
        name of the schema.
    :param bool active_only_schemas: True by default. Raises a StateException when the shard has a non active state.
    :param bool include_public: True by default. Includes the public schema in the cursor when set to True.
    :param bool override_class_method_use_shard: False by default. Skips using use_shard in model methods by default.

    :returns: The context manager as an object with the following members:

    * **connection:** Reference to the current database connection.
    * **shard:** Reference to the current shard model object.

    :Example:
        .. code-block:: python

            from sharding.utils import use_shard

            from config.models import shard
            from example.models import User


            shard = Shard.objects.get(alias="North")
            with use_shard(shard):
                # create user on the North shard
                User.objects.create(name="John Snow")

    """
    shard = None
    mapping_value = None

    # Both node_name and schema_name are not documented.
    # They bypass the shard.state check, and are only there to for internal use.
    # (Situations where the schema is made, but the shard object is not saved yet.)
    def __init__(self, shard=None, node_name=None, schema_name=None, active_only_schemas=True, include_public=True,
                 override_class_method_use_shard=False, lock=True, check_active_mapping_values=False):
        self.include_public = include_public
        self.override_class_method_use_shard = override_class_method_use_shard
        self.active_only_schemas = active_only_schemas
        self.check_active_mapping_values = check_active_mapping_values
        self.lock = lock

        shard_class = get_shard_class()
        if shard:
            if not isinstance(shard, shard_class):
                raise ValueError("Shard value {} ({}) must of type {}".format(shard,
                                                                              type(shard).__name__,
                                                                              shard_class.__name__))
            if self.active_only_schemas and shard.state != State.ACTIVE:
                raise StateException("Shard {} state is {}".format(shard, shard.state), shard.state)

            if check_active_mapping_values:
                mapping_model = get_mapping_class()
                if not mapping_model:
                    raise ValueError("You set 'check_active_mapping_values' to True while you didn't define the "
                                     "mapping model.")

                if mapping_model.objects.for_shard(shard).in_maintenance().exists():
                    raise StateException('Shard {} contains mapping objects that are in maintenance'.format(shard),
                                         State.MAINTENANCE)

            self.shard = shard
            self.schema_name = shard.schema_name
            self.node_name = shard.node_name
        else:
            self.shard = None
            self.schema_name = schema_name
            self.node_name = node_name

        if self.node_name not in connections:
            raise ValueError("Connection '{}' does not exist. Is it listed in settings.DATABASES?"
                             .format(self.node_name))

        self._enabled = False

    def __enter__(self):
        return self.enable()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.disable()

    def __call__(self, querying_func):
        @wraps(querying_func)
        def inner(*args, **kwargs):
            # Call the function in our context manager
            with self:
                return querying_func(*args, **kwargs)

        return inner

    def acquire_lock(self):
        if self.shard:
            self.connection.acquire_advisory_lock(key='shard_{}'.format(self.shard.id), shared=True)

    def release_lock(self):
        if self.shard:
            self.connection.release_advisory_lock(key='shard_{}'.format(self.shard.id), shared=True)

    def enable(self):
        # First: Set the connection
        self.connection = _use_connection(self.node_name)

        # Second: Note current connection settings
        self.old_schema_name = self.connection.get_schema()
        self.old_override_class_method_use_shard = self.connection._override_class_method_use_shard
        self.old_shard_id = self.connection._shard_id
        self.old_mapping_value = self.connection._mapping_value
        self.old_active_only_schemas = self.connection._active_only_schemas
        self.old_lock = self.connection._lock
        self.old_check_active_mapping_values = self.connection._check_active_mapping_values

        # Third: Set an advisory lock on the shard and mapping object (if available)
        if self.lock:
            self.acquire_lock()

        # Fourth: Tell the connection to switch schema, and give the data to the connection as well.
        _set_schema(
            self.schema_name,
            self.connection,
            include_public=self.include_public,
            override_class_method_use_shard=self.override_class_method_use_shard,
            shard_id=self.shard.id if self.shard else None,
            mapping_value=self.mapping_value,
            active_only_schemas=self.active_only_schemas,
            lock=self.lock,
            check_active_mapping_values=self.check_active_mapping_values,
        )

        self._enabled = True

        return self

    def disable(self):
        if not self._enabled:
            return

        if self.lock:
            self.release_lock()

        # Reset both the connection and the schema back to the old state
        _set_schema(
            self.old_schema_name,
            self.connection,
            override_class_method_use_shard=self.old_override_class_method_use_shard,
            shard_id=self.old_shard_id,
            mapping_value=self.old_mapping_value,
            active_only_schemas=self.old_active_only_schemas,
            lock=self.old_lock,
            check_active_mapping_values=self.old_check_active_mapping_values,
        )

        if not THREAD_LOCAL.DB_OVERRIDE or THREAD_LOCAL.DB_OVERRIDE == [self.node_name]:
            THREAD_LOCAL.DB_OVERRIDE = None
        else:
            THREAD_LOCAL.DB_OVERRIDE.pop()  # Remove last entry, which is self.node

        self._enabled = False  # Make sure we cannot call disable multiple times


def get_shard_for(target_value, active_only=True, field=None):
    """
    Helper function to easily retrieve a shard object from the mapping value listed in your shard_mapping_model.

    :note: This only works if you use a model decorated with @shard_mapping_model.
        See the Installation chapter for more info.

    :param target_value: Value for mapping_field in your mapping table.
    :param active_only: Set to True if you want an error raised if either the mapping object's state or
        the shard's state are not active.

    :returns: Object from the Shard model.

    :Example:
        .. code-block:: python

            # settings
            SHARDING = {
                'SHARD_CLASS': 'myapp.models.Shard',
                'MAPPING_MODEL': 'myapp.models.MyMappingModel',
            }

            # myapp.models
            from django_sharding.decorators import shard_mapping_model
            from django_sharding.models import BaseShard
            from django_sharding.utils import State

            class Shard(BaseShard):
                class Meta:
                    app_label = 'myapp'

            @shard_mapping_model(mapping_field='organization_id')
            class OrganizationShards(models.Model):
                shard = models.ForeignKey('example.Shard')
                organization_id = models.PositiveSmallIntegerField(db_index=True)
                state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)
                slug = models.SlugField(unique=True)

                objects = MappingQuerySet.as_manager()

                class Meta:
                    app_label = 'myapp'

            # myapp.views
            from django_sharding.utils import get_shard_for

            print (get_shard_for(user.organization.id))  # <class 'myapp.models.Shard'>
            print (get_shard_for(user.organization.id, active_only=True))  # StateException if state is not active
            print (get_shard_for(user.organization.id, field='slug'))  # <class 'myapp.models.Shard'>

       """
    mapping_model = get_mapping_class()
    if not mapping_model:
        raise ImproperlyConfigured('Missing or incorrect type of a setting SHARDING["{}"].'.format('MAPPING_MODEL'))

    mapping_object = mapping_model.objects.select_related('shard').for_target(target_value, field)
    if active_only:
        if mapping_object.state != State.ACTIVE:
            raise StateException('Mapping object {} state is {}'.format(mapping_object,
                                                                        mapping_object.get_state_display()),
                                 mapping_object.state)
        if mapping_object.shard.state != State.ACTIVE:
            raise StateException('Shard {} state is {}'.format(mapping_object.state,
                                                               mapping_object.shard.get_state_display()),
                                 mapping_object.shard.state)
    return mapping_object.shard


class use_shard_for(use_shard):
    """
    Extends use_shard. You provide the value occurring in the mapping table to easily find the correct shard.

    :note: This only works if you use a model decorated with @shard_mapping_model.
        See the Installation chapter for more info.

    :note: If the mapping model has a state, it will raise a StateException if the state is not State.ACTIVE.

    :param target_value: Value for mapping_field in your mapping table
    :param bool active_only_schemas: True by default. Raises a StateException when the mapping table or the shard
        has a non active state.
    :param bool override_class_method_use_shard: False by default. Skips using use_shard in model methods by default.

    :returns: The context manager as an object with the following members:

    * **connection:** Reference to the current database connection.
    * **shard:** Reference to the current shard model object.

    :Example:
        .. code-block:: python

            # Settings
            SHARDING = {
                'SHARD_CLASS': 'myapp.models.Shard',
                'MAPPING_MODEL': 'myapp.models.MyMappingModel',
            }

            # myapp.models
            from django_sharding.decorators import shard_mapping_model
            from django_sharding.models import BaseShard
            from django_sharding.utils import State


            class Shard(BaseShard):
                class Meta:
                    app_label = 'myapp'

            @shard_mapping_model(mapping_field='organization_id')
            class OrganizationShards(models.Model):
                shard = models.ForeignKey('example.Shard')
                organization_id = models.PositiveSmallIntegerField(db_index=True)
                state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)


                objects = MappingQuerySet.as_manager()

                class Meta:
                    app_label = 'myapp'

            # myapp.views
            from django_sharding.utils import use_shard_for

            with use_shard_for(user.organization.id):
                # Do things on my shard

    """
    def __init__(self, target_value, active_only_schemas=True, **kwargs):
        self.mapping_value = target_value
        shard = get_shard_for(target_value, active_only=active_only_schemas)
        super().__init__(shard=shard, active_only_schemas=active_only_schemas, **kwargs)

    def acquire_lock(self):
        self.connection.acquire_advisory_lock(key='mapping_{}'.format(self.mapping_value), shared=True)
        self.connection.acquire_advisory_lock(key='shard_{}'.format(self.shard.id), shared=True)

    def release_lock(self):
        self.connection.release_advisory_lock(key='mapping_{}'.format(self.mapping_value), shared=True)
        self.connection.release_advisory_lock(key='shard_{}'.format(self.shard.id), shared=True)


def get_new_shard_node():
    return settings.SHARDING.get('NEW_SHARD_NODE', None)


def create_schema_on_node(schema_name, node_name=None, migrate=True):
    """
    Create a schema on a given node. If no node is given, it will take the node set in SHARDING.NEW_SHARD_NODE settings.
    By default it will also call a migration to the newly made schema.

    :note: This will be called automatically when you make a Shard model object and save it.

    :param str schema_name: Provide the name of the schema to be made.
    :param str node_name: Provide the name of the database connection to be used. If empty it will use the current.
    :param bool migrate: Use `False` to disable automatic migration of all sharded models.

    :returns: None

    :Example:
        .. code-block:: python

            from sharding.utils import create_schema_on_node. use_shard

            from example.models import User


            create_schema_on_node(shard_name="North", node_name="default", migrate=True)

            with use_shard(node_name="default", schema_name="North"):
                # create user on the North shard
                User.objects.create(name="John Snow")

    """
    node_name = node_name or get_new_shard_node()
    if not node_name:
        raise ValueError('Neither a node_name given, nor a NEW_SHARD_NODE set in the SHARING settings.')
    _node_exists(node_name)
    connections[node_name].create_schema(schema_name)

    if migrate:
        connections[node_name].clone_schema(get_template_name(), schema_name)


def delete_schema(schema_name, node_name):
    _node_exists(node_name)
    connections[node_name].delete_schema(schema_name)


def create_template_schema(node_name='default'):
    """
    Each node needs to have a template schema. This is cloned for each new shard on the node.
    This function creates a new schema on a given node that is named 'template', or what you have set under
    settings.SHARDING.TEMPLATE_NAME.
    It then migrates only the sharded tables to this schema, by calling sharding.utils.migrate_schema.

    Since this only needs to happen once on each node, you can just run it in the shell.

    If the template schema already exists, this does nothing.

    :param str node_name: Provide the name of the database connection to be used. If empty it will use the current.

    :returns: None

    :Example:
        .. code-block:: python

            from sharding.utils import create_template_schema
            create_template_schema(node_name='default')

    """
    schema_name = get_template_name()
    _node_exists(node_name)

    if connections[node_name].get_ps_schema(schema_name):
        return

    connections[node_name].create_schema(schema_name, is_template=True)  # if it already exists, it's no problem
    migrate_schema(node_name, schema_name)


def migrate_schema(node_name, schema_name):
    """
    It then migrates only the sharded tables to the given schema.
    This actually performs a migration, it does not clone the template schema. (For we use it to create the template.)

    :note: Normally, you would not need to ever call this. It is used by create_template_schema.

    :param str node_name: Provide the name of the database connection to be used. If empty it will use the current.
    :param str schema_name: Provide the name of the schema to be made.

    :returns: None

    :Example:
        .. code-block:: python

            from django.db import connection

            from sharding.utils import migrate_schema

            connection.create_schema('North')
            migrate_schema(node_name='default', schema_name='North')

    """
    _node_exists(node_name)
    if not connections[node_name].get_ps_schema(schema_name):
        raise ValueError("Schema '{}' does not exist on node '{}'.".format(schema_name, node_name))

    with use_shard(node_name=node_name, schema_name=schema_name) as env:
        c = MigrateCommand()
        c.verbosity = 0
        c.interactive = False
        c.load_initial_data = False

        app_labels = [app_config.label for app_config in apps.get_app_configs()]
        c.sync_apps(env.connection, app_labels)  # we use the django native migration call for this.
        record_migrated(env.connection)


def record_migrated(connection):
    """
    Helper function to let the migration system know which migrations
    have been executed. Since the sync_apps does not do this.
    """
    executor = MigrationExecutor(connection)
    for key, migration in executor.loader.disk_migrations.items():
        executor.recorder.record_applied(*key)
    executor.loader.applied_migrations = executor.recorder.applied_migrations()
    executor.loader.build_graph()  # altered history, rebuild state


def for_each_shard(func, args=(), kwargs=None, as_id=False):
    """
    Function to call another function for each shard and pass the shard
    as a parameter.

    :param func: Function to call for each shard
    :param kwargs: Keyword arguments to pass to the function to call

    :returns: None

    :Example:
        .. code-block:: python

            from sharding.utils import for_each_shard, use_shard

            function sharded_function(shard=None, shard_id=None, prefix=None):
                shard_id = shard.id if shard else shard_id
                print('update user on {prefix}{shard_id}'.format(prefix=prefix, shard_id=shard_id))
                with use_shard(shard):
                    User.objects.update(last_updated=timezone.now())
                return '{} users updated'.format(User.objects.count())

            for_each_shard(sharded_function)

            # other ways of calling for_each_shard:
            for_each_shard(sharded_function, kwargs={'prefix': 'shard-'})
            for_each_shard(sharded_function, kwargs={'prefix': 'shard-'}, as_id=True)

    """
    for shard in get_shard_class().objects.all():
        if as_id:
            func(*args, shard_id=shard.id, **(kwargs or {}))
        else:
            func(*args, shard=shard, **(kwargs or {}))


def get_model_sharding_mode(model):
    app_label, model_name = model._meta.app_label, model._meta.model_name
    return get_sharding_mode(app_label, model_name)


def get_sharding_mode(app_label, model_name):
    override_sharding_mode = settings.SHARDING.get('OVERRIDE_SHARDING_MODE', {})

    if override_sharding_mode:
        if (app_label, model_name) in override_sharding_mode:
            # The configuration overrides the sharding_mode for a model in an app
            return override_sharding_mode[(app_label, model_name)]
        elif (app_label,) in override_sharding_mode:
            # The configuration overrides the sharding_mode for all models in an app
            return override_sharding_mode[(app_label,)]

    # Explicitly skip the migration model, since that one is both sharded and mirrored, so we can't determine the
    # sharding mode
    if not model_name or (app_label, model_name) == ('migrations', 'migration'):
        return None

    model = apps.get_model(app_label, model_name)

    # If this model is created automatically (for many-to-many relations for example),
    # return the sharding mode of the model responsible for its creation.
    if model._meta.auto_created:
        return get_sharding_mode(model._meta.auto_created._meta.app_label, model._meta.auto_created._meta.model_name)

    return getattr(model, 'sharding_mode', False)


def get_all_sharded_models(include_auto_created=False):
    models = apps.get_models(include_auto_created=include_auto_created)
    return [model for model in models
            if not model._meta.proxy and get_model_sharding_mode(model) == ShardingMode.SHARDED]


def get_all_mirrored_models():
    models = apps.get_models()
    return [model for model in models
            if not model._meta.proxy and get_model_sharding_mode(model) == ShardingMode.MIRRORED]


def get_all_databases():
    return [name for name, db in settings.DATABASES.items()]


def for_each_node(func, args=(), kwargs=None):
    """
    Function to call another function for each node and pass the node_name
    as a parameter.

    :param func: Function to call for each node
    :param kwargs: Keyword arguments to pass to the function to call

    :returns: Dict with the functions return values per node.

    :Example:
        .. code-block:: python

            from sharding.utils import for_each_node

            function sharded_function(node_name=None, prefix=None):
                return '{prefix}{node_name}'.format(prefix=prefix, node_name=node_name)

            for_each_node(sharded_function)  # {'default': 'default'}
            for_each_node(sharded_function, kwargs={'prefix': 'node-'})  # {'default': 'node-default'}

    """
    return_values = {}
    for node_name in get_all_databases():
        return_values[node_name] = func(*args, node_name=node_name, **(kwargs or {}))
    return return_values


class transaction_for_nodes(Atomic):
    """
    Context manager to start a transaction for each given node and close them afterwards.
    """
    def __init__(self, nodes, savepoint=True, lock_models=()):
        # we don't support the 'using' argument of transaction.Atomic
        self.databases = nodes
        self.savepoint = savepoint
        self.lock_models = lock_models

    def __enter__(self):
        for database in self.databases:
            self.using = database
            super().__enter__()  # will grab the connection corresponding to the database set in self.using

            for model, mode in self.lock_models:
                connection = connections[self.using]
                cursor = connection.cursor()
                cursor.execute('LOCK TABLE "{}" IN {} MODE'.format(model._meta.db_table, mode))

    def __exit__(self, exc_type, exc_value, traceback):
        for database in reversed(self.databases):
            self.using = database
            # will grab the connection corresponding to the database set in self.using
            super().__exit__(exc_type, exc_value, traceback)


class transaction_for_every_node(transaction_for_nodes):
    """
    Context manager to start a transaction for all existing nodes and close them afterwards.
    Used by atomic_write_to_every_node.
    """
    def __init__(self, **kwargs):
        super().__init__(nodes=get_all_databases(), **kwargs)


def move_model_to_schema(model, node_name, to_schema_name, from_schema_name='public'):
    """
    This alters a table in such a way it is lifted from it's original schema and placed into the target one.
    It assumes that the table moves from a sharded schema to the public or the other way around.
    """
    with use_shard(node_name=node_name, schema_name=from_schema_name) as env:
        cursor = env.connection.cursor()
        if cursor.execute(
                'SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s);',
                [to_schema_name, model._meta.db_table]):
            raise ProgrammingError("Table '{}' already exists on schema '{}'.".format(model._meta.db_table,
                                                                                      to_schema_name))
        cursor.execute('ALTER TABLE "{}" SET SCHEMA "{}";'.format(model._meta.db_table, to_schema_name))

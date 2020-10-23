import logging
from collections import defaultdict
from functools import wraps

from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command
from django.db import connections, ProgrammingError
from django.db.models.signals import pre_init, post_init, pre_save, post_save, pre_delete, post_delete, pre_migrate, \
    post_migrate
from django.db.transaction import Atomic
from django.utils.module_loading import import_string

from sharding import ShardingMode, State, public_modes
from sharding.postgresql_backend.base import PUBLIC_SCHEMA_NAME

logger = logging.getLogger(__name__)


class StateException(Exception):
    def __init__(self, message, state, *args, **kwargs):
        super().__init__(message, state, *args, **kwargs)
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


def _node_exists(node_name):
    if node_name not in connections:
        raise ValueError("Connection '{}' does not exist. Is it listed in settings.DATABASES?".format(node_name))


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
    * **options:** Reference to the current shard options.

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
    def __init__(self, shard=None, node_name=None, schema_name=None, **kwargs):
        from sharding.options import ShardOptions  # Prevent cyclic imports

        # Add an indication that we activated this connection from an use_shard context manager
        kwargs['use_shard'] = True

        if shard:
            shard_class = get_shard_class()

            if not isinstance(shard, shard_class):
                raise ValueError(
                    'Shard value {} ({}) must of type {}'.format(shard, type(shard).__name__, shard_class.__name__)
                )

            self.options = ShardOptions.from_shard(shard, **kwargs)
        elif node_name:
            self.options = ShardOptions(node_name=node_name, schema_name=schema_name or PUBLIC_SCHEMA_NAME, **kwargs)
        else:
            raise ValueError('You need to provide at least a shard or a node name.')

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
        self.connection.acquire_locks()

    def release_lock(self):
        self.connection.release_locks()

    def enable(self):
        from sharding.router import get_active_connection, set_active_connection  # Prevent cyclic imports

        # First: Set the connection
        self.connection = connections[self.options]

        # Second: Save the old connection
        self._old_connection = get_active_connection()

        # Third: Set an advisory lock on the shard and mapping object (if available)
        if self.options.lock:
            self.acquire_lock()

        # Fourth: Set the new active schema
        set_active_connection(self.options)

        self._enabled = True

        return self

    def disable(self):
        if not self._enabled:
            return

        from sharding.router import set_active_connection  # Prevent cyclic imports

        if self.options.lock:
            self.release_lock()

        # Set the active connection to the old connection
        set_active_connection(self._old_connection)

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
                shard = models.ForeignKey('example.Shard', on_delete=models.CASCADE)
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
                shard = models.ForeignKey('example.Shard', on_delete=models.CASCADE)
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
    def __init__(self, target_value, **kwargs):
        shard = get_shard_for(target_value, active_only=kwargs.get('active_only_schemas', True))
        super().__init__(shard=shard, mapping_value=target_value, **kwargs)


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


def delete_schema(schema_name, node_name, is_template=False):
    _node_exists(node_name)
    connections[node_name].delete_schema(schema_name, is_template=is_template)


def create_template_schema(node_name='default', interactive=False, verbosity=0, migrate=True):
    """
    Each node needs to have a template schema. This is cloned for each new shard on the node.
    This function creates a new schema on a given node that is named 'template', or what you have set under
    settings.SHARDING.TEMPLATE_NAME.
    It then migrates only the sharded tables to this schema, by calling sharding.utils.migrate_schema.

    Since this only needs to happen once on each node, you can just run it in the shell.

    If the template schema already exists, this does nothing.

    :param str node_name: Provide the name of the database connection to be used. If empty it will use the current.
    :param bool interactive: Tells Django to NOT prompt the user for input of any kind.
    :param int verbosity: Verbosity level; 0=minimal output, 1=normal output, 2=verbose output, 3=very verbose output
    :param bool migrate: Performs the migration after the template schema has been created

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

    connections[node_name].create_schema(schema_name, is_template=True)  # If it already exists, it's no problem

    if migrate:
        migrate_schema(node_name, schema_name, interactive=interactive, verbosity=verbosity)


def migrate_schema(node_name, schema_name, interactive=False, verbosity=0, check_shard=False):
    """
    It then migrates only the sharded tables to the given schema.
    This actually performs a migration, it does not clone the template schema. (For we use it to create the template.)

    :note: Normally, you would not need to ever call this. It is used by create_template_schema. It will therefore by
           default not check if the shard entry exists in the shard table.

    :param str node_name: Provide the name of the database connection to be used. If empty it will use the current.
    :param str schema_name: Provide the name of the schema to be made.
    :param bool interactive: Tells Django to NOT prompt the user for input of any kind.
    :param int verbosity: Verbosity level; 0=minimal output, 1=normal output, 2=verbose output, 3=very verbose output
    :param bool check_shard: If set, checks whether the shard exists in the shard table.

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

    call_command('migrate_shards', database=node_name, schema_name=schema_name, interactive=interactive,
                 verbosity=verbosity, check_shard=check_shard)


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
    if not hasattr(model, '_meta'):
        # Some models (ManyToOne for example) do not have a _meta them selves, but refer to another model for that.
        app_label, model_name = model.model._meta.app_label, model.model._meta.model_name
    else:
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

    return getattr(model, '__sharding_mode', False)


def get_all_sharded_models(include_auto_created=False, include_proxy=False):
    """
    Return all models that are decorated with @sharded_model.
    """
    models = apps.get_models(include_auto_created=include_auto_created)
    return [model for model in models
            if (not model._meta.proxy or include_proxy) and get_model_sharding_mode(model) == ShardingMode.SHARDED]


def get_all_mirrored_models():
    """
    Return all models that are decorated with @mirrored_model.
    """
    models = apps.get_models()
    return [model for model in models
            if not model._meta.proxy and get_model_sharding_mode(model) == ShardingMode.MIRRORED]


def get_all_public_models():
    """
    Return all models that are decorated with @public_model.
    """
    models = apps.get_models()
    return [model for model in models
            if not model._meta.proxy and get_model_sharding_mode(model) == ShardingMode.PUBLIC]


def get_all_public_schema_models():
    """
    Return all models that live on the public schema. So models that are decorated with @sharded_model or @public_model.
    """
    models = apps.get_models()
    return [model for model in models
            if not model._meta.proxy and get_model_sharding_mode(model) in public_modes]


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


def move_model_to_schema(model, node_name, to_schema_name, from_schema_name=PUBLIC_SCHEMA_NAME):
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


def schema_exists(node_name, schema_name):
    _node_exists(node_name)
    return bool(connections[node_name].get_ps_schema(schema_name))


class disable_signals(object):
    """
    Context Manager that temporarily disconnects given or all common django signals.

    Example usage:
        from django.db.models.signals import pre_save, post_save

        with disable_signals([pre_save, post_save]):
            user.save()  # Will not call save related signals
    """
    def __init__(self, disabled_signals=None):
        self.stashed_signals = defaultdict(list)
        self.disabled_signals = disabled_signals or [
            pre_init, post_init,
            pre_save, post_save,
            pre_delete, post_delete,
            pre_migrate, post_migrate,
        ]

    def __enter__(self):
        for signal in self.disabled_signals:
            self.disconnect(signal)

    def __exit__(self, exc_type, exc_val, exc_tb):
        for signal in list(self.stashed_signals):
            self.reconnect(signal)

    def disconnect(self, signal):
        with signal.lock:
            signal._clear_dead_receivers()
            self.stashed_signals[signal] = signal.receivers
            signal.receivers = []
            signal.sender_receivers_cache.clear()

    def reconnect(self, signal):
        with signal.lock:
            signal._clear_dead_receivers()
            signal.receivers = self.stashed_signals.get(signal, [])
            signal.sender_receivers_cache.clear()
        del self.stashed_signals[signal]

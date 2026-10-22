import contextlib
import copy
import logging
from collections import defaultdict
from functools import cache, wraps

from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, ObjectDoesNotExist
from django.core.management import call_command
from django.core.signals import setting_changed
from django.db import DatabaseError, ProgrammingError, connections, router
from django.db.models.signals import (
    post_delete,
    post_init,
    post_migrate,
    post_save,
    pre_delete,
    pre_init,
    pre_migrate,
    pre_save,
)
from django.db.transaction import Atomic
from django.dispatch import receiver
from django.utils.module_loading import import_string

from djanquiltdb import ShardingMode, State, public_modes
from djanquiltdb.db import DefaultConnectionProxy
from djanquiltdb.plugins import install_plugin_importer, served_plugin_module
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME, DatabaseWrapper

# A plugin's utils module is served as the submodule djanquiltdb.utils.<plugin>, and the import system only asks
# PluginImporter for a submodule when the parent module has a __path__. The list is empty, because no submodule is found
# by searching it.
__path__ = []

install_plugin_importer()

logger = logging.getLogger(__name__)


class StateException(Exception):
    def __init__(self, message, state, *args, **kwargs):
        super().__init__(message, state, *args, **kwargs)
        self.state = state


@cache
def get_shard_class():
    """Returns the Shard class that QUILT_DB['SHARD_CLASS'] points to. The class is cached until QUILT_DB changes."""
    return import_string(settings.QUILT_DB['SHARD_CLASS'])


def get_template_name():
    return settings.QUILT_DB.get('TEMPLATE_NAME', 'template')


def get_primary_db_alias():
    from django.db import DEFAULT_DB_ALIAS

    return settings.QUILT_DB.get('PRIMARY_DB_ALIAS', DEFAULT_DB_ALIAS)


def shard_lock_key(shard_id):
    """
    Returns the name of the advisory lock of the shard with ``shard_id``. ``use_shard()`` takes the shared lock of this
    name, and the move commands take the exclusive lock.
    """
    return 'shard_{}'.format(shard_id)


def stored_mapping_value(mapping_value):
    """
    Returns the value that the mapping field stores for ``mapping_value``. When the mapping field is a foreign key, the
    mapping value can be a model instance. The stored value is then the primary key of the instance, or the field that
    ``to_field`` names. Any other mapping value is returned as it is.
    """
    from django.db.models import Model

    if not isinstance(mapping_value, Model):
        return mapping_value

    mapping_model = get_mapping_class()
    mapping_field = mapping_model and mapping_model._meta.get_field(mapping_model.mapping_field)
    stored_field = mapping_field.target_field if mapping_field and mapping_field.is_relation else None

    return getattr(mapping_value, stored_field.attname) if stored_field else mapping_value.pk


def mapping_lock_key(mapping_value):
    """
    Returns the name of the advisory lock of ``mapping_value``. ``use_shard_for()`` takes the shared lock of this name,
    and the move commands take the exclusive lock. The name has the value that the mapping field stores, as
    ``stored_mapping_value()`` returns it. ``use_shard_for()`` then locks the same key for a model instance and for
    the value that the mapping field stores for that instance. The move commands read that stored value from the
    mapping table, so they lock the same key too.
    """
    return 'mapping_{}'.format(stored_mapping_value(mapping_value))


@cache
def get_mapping_class():
    """
    Returns the mapping model that QUILT_DB['MAPPING_MODEL'] points to, or None when QUILT_DB does not have that key.
    The result is cached until QUILT_DB changes.
    """
    if 'MAPPING_MODEL' not in settings.QUILT_DB:
        return None
    return import_string(settings.QUILT_DB['MAPPING_MODEL'])


@receiver(setting_changed)
def reset_cached_shard_and_mapping_classes(setting, **kwargs):
    """
    Clears the cached shard class and mapping model when override_settings() changes QUILT_DB.
    """
    if setting == 'QUILT_DB':
        get_shard_class.cache_clear()
        get_mapping_class.cache_clear()


def _node_exists(node_name):
    if node_name not in connections:
        raise ValueError("Connection '{}' does not exist. Is it listed in settings.DATABASES?".format(node_name))


class use_shard(object):
    """
    use_shard can be used as a decorator and as context manager to send all queries in the scope to the correct shard.

    If the shard's state is not State.ACTIVE, use_shard will raise a StateException error.

    Once it holds the lock of the shard, a use_shard that is not inside an entered use_shard of the same shard reads the
    shard again, in one query, and raises StateException when ``move_shard_to_node`` or ``move_data_to_shard`` changed
    the shard after the shard that use_shard is given was read. ``use_shard_for()``, and a use_shard that is given a
    mapping value, read the mapping value and its shard again instead. A use_shard inside an entered use_shard of the
    same shard runs no statements when the entered use_shard holds the locks of the shard for the session, which it
    does outside a transaction. Inside a transaction the locks last until the transaction ends, so a use_shard inside
    an entered use_shard of the same shard takes its own locks and reads the shard again.

    use_shard does not check that the schema of the shard exists when it is entered. The first query in a shard whose
    schema does not exist raises IntegrityError.

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

            from djanquiltdb.utils import use_shard

            from example.models import Shard, User


            shard = Shard.objects.get(alias='North')
            with use_shard(shard):
                # create user on the North shard
                User.objects.create(name='John Snow')

    """

    def __init__(self, shard=None, node_name=None, schema_name=None, **kwargs):
        from djanquiltdb.options import ShardOptions  # Prevent cyclic imports

        # Mark that a use_shard context manager activates these options.
        kwargs['use_shard'] = True
        self._kwargs = kwargs

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

        self._clear_entry_state()

    def _clear_entry_state(self):
        """
        Sets the state that this context keeps while it is entered to the state of a context that is not entered.
        """
        self._enabled = False
        self._locks_are_xact = False
        # The lock keys that this context adds to the lock counts of the node connection, and removes from those counts
        # when it is left.
        self._counted_session_lock_keys = []
        # Whether this context took the locks of the shard itself, so that it releases them when it is left. A context
        # whose locks are held by other entered use_shard contexts takes no locks of its own, and neither does a
        # context that has no lock keys.
        self._holds_its_own_locks = False
        self._node_connection = None
        self._connection = None

    @property
    def connection(self):
        """
        The connection to the target, which this property builds the first time that code reads it. A use_shard that
        enters a shard whose locks are held by other entered use_shard contexts does not need this connection, so
        enable() does not build it.
        """
        if self._connection is None:
            self._connection = connections[self.options]
        return self._connection

    @connection.setter
    def connection(self, value):
        self._connection = value

    def __enter__(self):
        return self.enable()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.disable()

    def __call__(self, querying_func):
        @wraps(querying_func)
        def inner(*args, **kwargs):
            # Each call enters a copy of this context, so that a call inside another call of the function, and a call
            # from another thread, have state of their own.
            with self.copy_for_one_call():
                return querying_func(*args, **kwargs)

        return inner

    def copy_for_one_call(self):
        """
        Returns a copy of this context that has the same options and is not entered.
        """
        context = copy.copy(self)
        context._clear_entry_state()
        return context

    def acquire_lock(self):
        # Record how the locks are scoped: inside a transaction acquire_locks takes transaction-scoped locks,
        # which the transaction's own commit or rollback releases.
        self._locks_are_xact = self.connection.in_atomic_block
        self.connection.acquire_locks()

    def release_lock(self):
        if not self._locks_are_xact:
            self.connection.release_locks()

    def read_shard_after_locking(self):
        """
        Reads the shard of ``self.options`` again, and returns it. When the options have a mapping value, as they have
        for use_shard_for and for a use_shard that is given a mapping value, this method reads the shard of the mapping
        value from the mapping table, and returns None when the mapping table does not have the mapping value. Unless
        ``active_only_schemas`` is False, it then raises StateException when the mapping object or its shard is not
        active. Without a mapping value, this method reads the shard from the shard registry, and returns None when the
        registry does not have the shard.
        """
        if self.options.mapping_value is not None:
            try:
                return get_shard_for(
                    self.options.mapping_value, active_only=self._kwargs.get('active_only_schemas', True)
                )
            except ObjectDoesNotExist:
                return None

        shard_class = get_shard_class()
        return (
            shard_class.objects.using(public_schema_alias_for_reading(shard_class))
            .filter(pk=self.options.shard_id)
            .first()
        )

    def check_shard_after_locking(self):
        """
        Raises StateException when the registry has the shard on another node or in another schema than
        ``self.options``, or, when the options have a mapping value, when the mapping value points to another shard.
        Unless ``active_only_schemas`` is False, it also raises StateException when the shard is not active.
        ``move_shard_to_node`` and ``move_data_to_shard`` change the shard while they hold its exclusive lock, so the
        shard can change between the check in ``__init__`` and the moment that this context gets the shared lock.

        This method reads the shard registry in the public schema of the active node, where ``ShardOptions.use()`` looks
        up the shard too, so the shard is compared with the copy of the registry that it comes from. The mapping table
        is read where the router sends the mapping model, which is the primary node unless the mapping model sets
        ``route_to_primary_db`` to False. When the copy of a mirrored registry on the active node is not in sync with
        the primary node, this method compares the shard with the row that this copy has. A registry that is not
        mirrored exists on the default node only, so code that enters a shard while another node is active needs a
        mirrored registry, as the documentation recommends. When the copy does not have the shard, for example after
        ``flush`` deletes the shard from it, this method has nothing to compare, and the context enters the shard as
        passed.
        """
        from djanquiltdb.options import ShardOptions  # Prevent cyclic imports

        shard = self.read_shard_after_locking()

        if shard is None:
            return

        # A use_shard of a node and a schema can have the shard id in its keyword arguments, and the use_shard that
        # ShardOptions.use() returns has it there. from_shard() takes the shard id from the shard.
        kwargs = {name: value for name, value in self._kwargs.items() if name != 'shard_id'}

        if ShardOptions.from_shard(shard, **kwargs) != self.options:
            raise StateException(
                'Once use_shard holds its lock, the registry has shard {} on {}|{}, which differs from the shard that '
                'use_shard looked up before it waited for the lock'.format(shard, shard.node_name, shard.schema_name),
                shard.state,
            )

    def enable(self):
        from djanquiltdb.router import get_active_connection, set_active_connection  # Prevent cyclic imports

        # A context keeps the state of one entry, so it cannot be entered again before it is left. A function that is
        # decorated with this context enters a copy of it for each call instead.
        if self._enabled:
            raise RuntimeError(
                'This use_shard for {}|{} is entered already, and cannot be entered again.'.format(
                    self.options.node_name, self.options.schema_name
                )
            )

        # First: Look up the connection of the node, so that an unknown node raises an error here. The connection
        # property builds the connection to the target the first time that code reads it.
        self._node_connection = connections[self.options.node_name]
        self._connection = None

        # Second: Save the old connection
        self._old_connection = get_active_connection()

        # Third: Take the advisory locks of the shard and of the mapping value (if there is one), and check the shard
        # again once this context holds the locks. Locks that are held for the session are added to the lock counts of
        # the node connection. When other entered use_shard contexts hold all of these locks for the session, this
        # context takes no locks and does not check the shard: those contexts hold the locks until after this context is
        # left, so the move commands cannot change the shard in the meantime. A context for a node and a schema without
        # a shard id or a mapping value has no locks to take, so it takes none and builds no connection.
        lock_keys = self.options.lock_keys if self.options.lock else []

        if lock_keys and not self._node_connection.entered_use_shards_hold_session_locks(lock_keys):
            self.acquire_lock()

            if self.options.shard_id is not None:
                try:
                    self.check_shard_after_locking()
                except BaseException:
                    self.release_lock()
                    raise

            self._holds_its_own_locks = True

            if not self._locks_are_xact:
                self._counted_session_lock_keys = lock_keys
                self._node_connection.session_locks_of_entered_use_shards.update(self._counted_session_lock_keys)

        # Fourth: Set the new active schema
        set_active_connection(self.options)

        self._enabled = True

        return self

    def disable(self):
        if not self._enabled:
            return

        from djanquiltdb.router import set_active_connection  # Prevent cyclic imports

        if self._holds_its_own_locks:
            if self._counted_session_lock_keys:
                # Closing the connection resets the counts, so the count of a key can be zero before the context that
                # added the key is left. The unary plus removes the counts that drop below one.
                held_lock_keys = self._node_connection.session_locks_of_entered_use_shards
                held_lock_keys.subtract(self._counted_session_lock_keys)
                self._node_connection.session_locks_of_entered_use_shards = +held_lock_keys
                self._counted_session_lock_keys = []

            self.release_lock()

        self._holds_its_own_locks = False

        # Set the active connection to the old connection
        set_active_connection(self._old_connection)

        self._enabled = False  # Make sure we cannot call disable multiple times


def public_schema_alias_for_reading(model):
    """
    Returns the alias of the public schema of the node that the router picks to read ``model``. A lookup in the shard
    registry or in the mapping table passes this alias to ``using()``, so that the lookup reads the copy that this node
    has, also when the active connection is a shard whose search_path does not have the public schema. A mapping model
    with ``route_to_primary_db`` is read on the primary node, as the router picks it.
    """
    from djanquiltdb.options import ShardOptions  # Prevent cyclic imports

    return ShardOptions.from_alias(router.db_for_read(model)).node_name


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
            QUILT_DB = {
                'SHARD_CLASS': 'myapp.models.Shard',
                'MAPPING_MODEL': 'myapp.models.MyMappingModel',
            }

            # myapp.models
            from djanquiltdb.decorators import mirrored_model, shard_mapping_model
            from djanquiltdb.models import BaseShard
            from djanquiltdb.utils import State


            @mirrored_model()
            class Shard(BaseShard):
                class Meta:
                    app_label = 'myapp'


            @mirrored_model()
            @shard_mapping_model(mapping_field='organization_id')
            class OrganizationShard(models.Model):
                shard = models.ForeignKey('example.Shard', on_delete=models.CASCADE)
                organization_id = models.PositiveSmallIntegerField(db_index=True)
                state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)
                slug = models.SlugField(unique=True)

                objects = MappingQuerySet.as_manager()

                class Meta:
                    app_label = 'myapp'


            # myapp.views
            from djanquiltdb.utils import get_shard_for

            print(get_shard_for(user.organization.id))  # <class 'myapp.models.Shard'>
            print(get_shard_for(user.organization.id, active_only=True))  # StateException if state is not active
            print(get_shard_for(user.organization.id, field='slug'))  # <class 'myapp.models.Shard'>

    """
    mapping_model = get_mapping_class()
    if not mapping_model:
        raise ImproperlyConfigured('Missing or incorrect type of a setting QUILT_DB["{}"].'.format('MAPPING_MODEL'))

    mapping_object = (
        mapping_model.objects.using(public_schema_alias_for_reading(mapping_model))
        .select_related('shard')
        .for_target(target_value, field)
    )
    if active_only:
        if mapping_object.state != State.ACTIVE:
            raise StateException(
                'Mapping object {} state is {}'.format(mapping_object, mapping_object.get_state_display()),
                mapping_object.state,
            )
        if mapping_object.shard.state != State.ACTIVE:
            raise StateException(
                'Shard {} state is {}'.format(mapping_object.shard, mapping_object.shard.get_state_display()),
                mapping_object.shard.state,
            )
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
            QUILT_DB = {
                'SHARD_CLASS': 'myapp.models.Shard',
                'MAPPING_MODEL': 'myapp.models.MyMappingModel',
            }

            # myapp.models
            from djanquiltdb.decorators import mirrored_model, shard_mapping_model
            from djanquiltdb.models import BaseShard
            from djanquiltdb.utils import State


            @mirrored_model()
            class Shard(BaseShard):
                class Meta:
                    app_label = 'myapp'


            @mirrored_model()
            @shard_mapping_model(mapping_field='organization_id')
            class OrganizationShard(models.Model):
                shard = models.ForeignKey('example.Shard', on_delete=models.CASCADE)
                organization_id = models.PositiveSmallIntegerField(db_index=True)
                state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

                objects = MappingQuerySet.as_manager()

                class Meta:
                    app_label = 'myapp'


            # myapp.views
            from djanquiltdb.utils import use_shard_for

            with use_shard_for(user.organization.id):
                ...  # Do things on my shard

    """

    def __init__(self, target_value, **kwargs):
        shard = get_shard_for(target_value, active_only=kwargs.get('active_only_schemas', True))
        super().__init__(shard=shard, mapping_value=target_value, **kwargs)


def get_new_shard_node():
    return settings.QUILT_DB.get('NEW_SHARD_NODE', None)


def create_schema_on_node(schema_name, node_name=None, migrate=True):
    """
    Create a schema on a given node. If no node is given, it will take the node set in QUILT_DB.NEW_SHARD_NODE settings.
    By default it will also call a migration to the newly made schema.

    :note: This will be called automatically when you make a Shard model object and save it.

    :param str schema_name: Provide the name of the schema to be made.
    :param str node_name: Provide the name of the database connection to be used. If empty it will use the current.
    :param bool migrate: Use ``False`` to disable automatic migration of all sharded models.

    :returns: None

    :Example:
        .. code-block:: python

            from djanquiltdb.utils import create_schema_on_node, use_shard

            from example.models import User


            create_schema_on_node(schema_name='North', node_name='default', migrate=True)

            with use_shard(node_name='default', schema_name='North'):
                # create user on the North shard
                User.objects.create(name='John Snow')

    """
    node_name = node_name or get_new_shard_node()
    if not node_name:
        raise ValueError('Neither a node_name given, nor a NEW_SHARD_NODE set in the QUILT_DB settings.')
    _node_exists(node_name)
    connections[node_name].create_schema(schema_name)

    if migrate:
        connections[node_name].clone_schema(get_template_name(), schema_name)


def table_exists(connection, db_table):
    """
    Whether the schema of ``connection`` has a table named ``db_table``.
    """
    with connection.cursor() as cursor:
        return db_table in connection.introspection.table_names(cursor)


def delete_schema(schema_name, node_name, is_template=False):
    _node_exists(node_name)
    connections[node_name].delete_schema(schema_name, is_template=is_template)


def create_template_schema(node_name='default', interactive=False, verbosity=0, migrate=True):
    """
    Each node needs to have a template schema. This is cloned for each new shard on the node.
    This function creates a new schema on a given node that is named 'template', or what you have set under
    settings.QUILT_DB.TEMPLATE_NAME.
    It then migrates only the sharded tables to this schema, by calling ``djanquiltdb.utils.migrate_schema``.

    Since this only needs to happen once on each node, you can just run it in the shell.

    If the template schema already exists, this does nothing.

    :param str node_name: Provide the name of the database connection to be used. If empty it will use the current.
    :param bool interactive: Tells Django to NOT prompt the user for input of any kind.
    :param int verbosity: Verbosity level; 0=minimal output, 1=normal output, 2=verbose output, 3=very verbose output
    :param bool migrate: Performs the migration after the template schema has been created

    :returns: None

    :Example:
        .. code-block:: python

            from djanquiltdb.utils import create_template_schema

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

            from djanquiltdb.utils import migrate_schema

            connection.create_schema('North')
            migrate_schema(node_name='default', schema_name='North')

    """
    _node_exists(node_name)
    if not connections[node_name].get_ps_schema(schema_name):
        raise ValueError("Schema '{}' does not exist on node '{}'.".format(schema_name, node_name))

    call_command(
        'migrate',
        database=node_name,
        schema_name=schema_name,
        interactive=interactive,
        verbosity=verbosity,
        check_shard=check_shard,
    )


def for_each_shard(func, args=(), kwargs=None, as_id=False):
    """
    Function to call another function for each shard and pass the shard
    as a parameter.

    :param func: Function to call for each shard
    :param kwargs: Keyword arguments to pass to the function to call

    :returns: None

    :Example:
        .. code-block:: python

            from djanquiltdb.utils import for_each_shard, use_shard


            def sharded_function(shard=None, shard_id=None, prefix=None):
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
    shard_class = get_shard_class()

    for shard in shard_class.objects.using(public_schema_alias_for_reading(shard_class)):
        if as_id:
            func(*args, shard_id=shard.id, **(kwargs or {}))
        else:
            func(*args, shard=shard, **(kwargs or {}))


@contextlib.contextmanager
def pgtrigger_triggers_ignored(shard):
    """
    Ignores every django-pgtrigger trigger on the node of ``shard`` while the block runs, through
    ``pgtrigger.ignore()``. When django-pgtrigger cannot be imported, the block runs as it is. Run this inside a
    transaction on that node.

    ``move_data_to_shard`` and ``purge_shard_data`` copy and delete rows inside it. The rows only change shards, or
    leave a shard that they were copied from, so a trigger must not act on them a second time, as ``move_shard_to_node``
    also ensures by disabling the user triggers of the new schema. That command can alter the tables of its new schema,
    but the target shard of a data move is in use, and altering its tables would lock out the writes to them until the
    move ends.

    pgtrigger.ignore() sends its setting with each statement that passes Django's execute wrappers, and COPY does not.
    The setting belongs to the connection of the node, and is local to the transaction, so one statement on the node of
    the shard sets it for every COPY that follows.
    """
    # The import decides, and not apps.is_installed(): the triggers are registered when their models are defined, and a
    # test case that limits available_apps would otherwise leave them firing.
    try:
        import pgtrigger
    except ImportError:
        yield
        return

    with contextlib.ExitStack() as ignored_triggers:
        ignored_triggers.enter_context(pgtrigger.ignore())

        with use_shard(shard, active_only_schemas=False, lock=False) as env:
            env.connection.cursor().execute('SELECT 1')

        try:
            yield
        except BaseException:
            # pgtrigger.ignore() resets its setting with a statement when it exits inside a transaction. After a failed
            # statement, the transaction only accepts a rollback, so the reset would fail and hide the error of the
            # block. The rollback discards the setting anyway.
            with contextlib.suppress(DatabaseError):
                ignored_triggers.close()

            raise


def get_model_definition(model):
    # Some models (ManyToOne for example) do not have a _meta them selves, but refer to another model for that.
    if not hasattr(model, '_meta'):
        return model.model
    return model


def get_model_sharding_mode(model):
    model_class = get_model_definition(model)
    return get_sharding_mode(model_class._meta.app_label, model_class._meta.model_name)


class OverrideShardingModeWithLowercaseKeys(dict):
    """
    The ``OVERRIDE_SHARDING_MODE`` setting once ``AppConfig.ready()`` has lowercased its keys. The type tells
    :func:`get_declared_sharding_mode` that the keys need no lowercasing, so it uses the setting as it is.
    """


def get_declared_sharding_mode(model):
    """
    Returns the sharding mode that is declared for a model class, or None when nothing declares one.

    The mode is resolved in the same way as the router resolves it, but only from the class, so this also works while
    models are still being defined. An ``OVERRIDE_SHARDING_MODE`` entry takes precedence over the mode decorator, and
    its keys are compared in lowercase, because before ``AppConfig.ready()`` they have the casing of the settings file.
    A proxy model gets the mode of its concrete model, and an auto-created model gets the mode of the model that caused
    its creation.
    """
    override_sharding_mode = settings.QUILT_DB.get('OVERRIDE_SHARDING_MODE', {})

    if not isinstance(override_sharding_mode, OverrideShardingModeWithLowercaseKeys):
        override_sharding_mode = {
            tuple(part.lower() for part in key): mode for key, mode in override_sharding_mode.items()
        }

    opts = model._meta

    for key in ((opts.app_label, opts.model_name), (opts.app_label,)):
        if key in override_sharding_mode:
            return override_sharding_mode[key]

    mode = getattr(model, '__sharding_mode', None)

    if mode is None and opts.proxy:
        # A proxy of a third-party model has no marker of its own, and its table is the table of the concrete model.
        return get_declared_sharding_mode(opts.concrete_model)

    if mode is None and opts.auto_created:
        return get_declared_sharding_mode(opts.auto_created)

    return mode


def get_sharding_mode(app_label, model_name):
    override_sharding_mode = settings.QUILT_DB.get('OVERRIDE_SHARDING_MODE', {})

    # The override is read before the model is looked up, because the router also asks about migration operations that
    # name no model, and about models that the app registry does not have, such as a model that a later migration
    # deletes. get_declared_sharding_mode() reads it again for the models that the registry has.
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

    mode = get_declared_sharding_mode(apps.get_model(app_label, model_name))

    # Callers of this function expect False, and not None, when no mode is declared.
    return False if mode is None else mode


def get_all_sharded_models(include_auto_created=False, include_proxy=False):
    """
    Return all models that are decorated with @sharded_model.
    """
    models = apps.get_models(include_auto_created=include_auto_created)
    return [
        model
        for model in models
        if (not model._meta.proxy or include_proxy) and get_model_sharding_mode(model) == ShardingMode.SHARDED
    ]


def get_all_mirrored_models(include_auto_created=False, include_proxy=False):
    """
    Return all models that are decorated with @mirrored_model.
    """
    models = apps.get_models(include_auto_created=include_auto_created)
    return [
        model
        for model in models
        if (not model._meta.proxy or include_proxy) and get_model_sharding_mode(model) == ShardingMode.MIRRORED
    ]


def get_all_public_models(include_auto_created=False, include_proxy=False):
    """
    Return all models that are decorated with @public_model.
    """
    models = apps.get_models(include_auto_created=include_auto_created)
    return [
        model
        for model in models
        if (not model._meta.proxy or include_proxy) and get_model_sharding_mode(model) == ShardingMode.PUBLIC
    ]


def get_all_public_schema_models(include_auto_created=False, include_proxy=False):
    """
    Return all models that live on the public schema. So models that are decorated with @sharded_model or @public_model.
    """
    models = apps.get_models(include_auto_created=include_auto_created)
    return [
        model
        for model in models
        if (not model._meta.proxy or include_proxy) and get_model_sharding_mode(model) in public_modes
    ]


def get_all_databases():
    return [name for name, db in settings.DATABASES.items()]


def get_connection_alias(conn):
    """
    Helper function to retrieve the node name from a given connection.
    This is because a connection can be a simple string, a ConnectionProxy or a DatabaseWrapper.
    """
    from djanquiltdb.options import ShardOptions  # Prevent cyclic imports

    if isinstance(conn, str):
        return conn

    if isinstance(conn, DefaultConnectionProxy):
        if isinstance(conn.db_alias, str):
            return conn.db_alias
        if isinstance(conn.db_alias, ShardOptions):
            return conn.db_alias.node_name

    if isinstance(conn, DatabaseWrapper):
        return conn.alias

    raise ValueError('Could not determine node name for the given connection {}'.format(conn))


def for_each_node(func, args=(), kwargs=None):
    """
    Function to call another function for each node and pass the node_name
    as a parameter.

    :param func: Function to call for each node
    :param kwargs: Keyword arguments to pass to the function to call

    :returns: Dict with the functions return values per node.

    :Example:
        .. code-block:: python

            from djanquiltdb.utils import for_each_node

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

    def __init__(self, nodes, savepoint=True, lock_models=(), durable=True):
        # we don't support the 'using' argument of transaction.Atomic
        self.databases = nodes
        self.savepoint = savepoint
        self.lock_models = lock_models
        self.durable = durable

    def __enter__(self):
        for database in self.databases:
            self.using = database
            super().__enter__()  # will grab the connection corresponding to the database set in self.using

            for model, mode in self.lock_models:
                connection = connections[self.using]
                cursor = connection.cursor()
                cursor.execute('LOCK TABLE "{}" IN {} MODE'.format(model._meta.db_table, mode))

    def __exit__(self, exc_type, exc_value, traceback):
        # Exit every node even when one of them fails: otherwise a commit that raises on the first node
        # leaves the remaining connections inside an open atomic block for the rest of the process. The
        # failure also flows into the later exits, so their transactions roll back instead of committing
        # around a half-applied change.
        error = None
        for database in reversed(self.databases):
            self.using = database
            try:
                # will grab the connection corresponding to the database set in self.using
                super().__exit__(exc_type, exc_value, traceback)
            except BaseException as caught:
                if error is None:
                    error = caught
                exc_type, exc_value, traceback = type(caught), caught, caught.__traceback__

        if error is not None:
            raise error


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
        cursor.execute(
            'SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s);',
            [to_schema_name, model._meta.db_table],
        )
        if cursor.fetchone()[0]:
            raise ProgrammingError(
                "Table '{}' already exists on schema '{}'.".format(model._meta.db_table, to_schema_name)
            )
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
            pre_init,
            post_init,
            pre_save,
            post_save,
            pre_delete,
            post_delete,
            pre_migrate,
            post_migrate,
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


def __getattr__(name):
    """
    Serves the utils module of a plugin under the plugin's registered name, for plain attribute access.
    :class:`~djanquiltdb.plugins.PluginImporter` handles import statements. A name that this module defines itself is
    found before this function runs, so a plugin cannot replace it.
    """
    module = served_plugin_module(__name__, name)

    if module is None:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

    return module

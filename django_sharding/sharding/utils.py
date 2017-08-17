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

import threading
from enum import Enum

from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.management.commands.migrate import Command as MigrateCommand
from django.db import connections, connection
from django.utils.module_loading import import_string
from functools import wraps

THREAD_LOCAL = threading.local()


class ShardingMode(Enum):
    MIRRORED = 'M'
    DEFINING = 'D'
    SHARDED = 'S'


class State(object):
    ACTIVE = 'A'
    MAINTENANCE = 'M'


STATES = (
    (State.ACTIVE, 'Active'),
    (State.MAINTENANCE, 'Maintenance'),
)


class StateException(Exception):
    def __init__(self, message, state, *args, **kwargs):
        super().__init__(message, *args, **kwargs)
        self.state = state


def get_shard_class():
    """ Helper function to get implemented Shard class """
    return import_string(settings.SHARDING['SHARD_CLASS'])


def get_template_name():
    return settings.SHARDING.get('TEMPLATE_NAME', 'template')


class DynamicDbRouter(object):
    # A router that decides what db to read from based on a variable local to the current thread.

    def db_for_read(self, model, **hints):
        override_list = getattr(THREAD_LOCAL, 'DB_OVERRIDE', None)
        return override_list and override_list[-1]

    def db_for_write(self, model, **hints):
        override_list = getattr(THREAD_LOCAL, 'DB_OVERRIDE', None)
        return override_list and override_list[-1]

    def allow_relation(self, obj1, obj2, *args, **kwargs):
        obj1_mode = getattr(obj1, 'sharding_mode', False)
        obj2_mode = getattr(obj2, 'sharding_mode', False)

        if obj1_mode or obj2_mode:
            return obj1_mode and obj2_mode  # all is good if they are both sharded

        return None  # We have no opinion about non-sharded models

    def allow_syncdb(self, *args, **kwargs):
        model = kwargs.pop('model', False)
        if getattr(model, 'test_model', False):
            return False
        return None

    def allow_migrate(self, *args, **kwargs):
        model = kwargs.pop('model', False)

        if getattr(model, 'test_model', False):
            return False

        # Only migrate if we do a migration for the sharded models. They do not belong in the non-sharded database.
        sharded_migrate = getattr(THREAD_LOCAL, 'SHARDED_MIGRATE', None)
        if sharded_migrate:
            return getattr(model, 'sharding_mode', False) in [ShardingMode.MIRRORED, ShardingMode.SHARDED]

        return None


def _node_exists(node_name):
    if node_name not in connections:
        raise ValueError("Connection '{}' does not exist. Is it listed in settings.DATABASES?".format(node_name))


def _use_connection(node):
    if not hasattr(THREAD_LOCAL, 'DB_OVERRIDE') or not THREAD_LOCAL.DB_OVERRIDE:
        THREAD_LOCAL.DB_OVERRIDE = [node]
    else:
        THREAD_LOCAL.DB_OVERRIDE.append(node)
    return connections[node]


def _set_schema(schema_name, _connection=None):
    if not _connection:
        _connection = connection
    _connection.set_schema(schema_name)


class use_shard(object):
    """
    use_shard can be used as a decorator and as context manager to send all queries in the scope to the correct shard.

    If the shard's state is not State.ACTIVE, use_shard will raise a StateException error.

    :param object shard: Provide a Shard model object so the context manager knows all about where to send the queries.
    :param str node_name: Alternatively, you can provide the name of the database the schema can be found and the name
        of the schema.
    :param str schema_name: Alternatively, you can provide the name of the database the schema can be found and the
        name of the schema.
    :param bool active_only_schemas: True by default. Raises a StateException when any objects in the mapping table
        has a non active state and links to this shard.

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

    # Both node_name and schema_name are not documented.
    # They bypass the shard.state check, and are only there to for internal use.
    # (Situations where the schema is made, but the shard object is not saved yet.)
    def __init__(self, shard=None, node_name=None, schema_name=None, active_only_schemas=True):
        shard_class = get_shard_class()

        if shard:
            if not isinstance(shard, shard_class):
                raise ValueError("Shard value {} ({}) must of type {}".format(shard,
                                                                              type(shard).__name__,
                                                                              shard_class.__name__))
            if shard.state != State.ACTIVE:
                raise StateException("Shard {} state is {}".format(shard, shard.state), shard.state)

            if active_only_schemas and 'MAPPING_MODEL' in settings.SHARDING:
                mapping_model = import_string(settings.SHARDING['MAPPING_MODEL'])
                if mapping_model.objects.for_shard(shard).in_maintenance().exists():
                    raise StateException("Shard {} contains mapping objects that are in maintenance".format(shard),
                                         State.MAINTENANCE)

            self.shard = shard
            self.schema_name = shard.schema_name
            self.node_name = shard.node_name
        else:
            self.schema_name = schema_name
            self.node_name = node_name

        if self.node_name not in connections:
            raise ValueError("Connection '{}' does not exist. Is it listed in settings.DATABASES?"
                             .format(self.node_name))

    def __enter__(self):
        # first: set the connection
        self.old_connection_name = connection.settings_dict['NAME']
        self.connection = _use_connection(self.node_name)

        # second: set the correct search_path for the requested schema
        self.old_schema_name = self.connection.get_schema()

        _set_schema(self.schema_name, self.connection)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # reset both the connection and the schema back to the old state
        _set_schema(self.old_schema_name, self.connection)
        if not THREAD_LOCAL.DB_OVERRIDE or THREAD_LOCAL.DB_OVERRIDE == [self.node_name]:
            THREAD_LOCAL.DB_OVERRIDE = None
        else:
            THREAD_LOCAL.DB_OVERRIDE.pop()  # remove last entry, which is self.node

    def __call__(self, querying_func):
        @wraps(querying_func)
        def inner(*args, **kwargs):
            # Call the function in our context manager
            with self:
                return querying_func(*args, **kwargs)

        return inner


def get_shard_for(target_value, active_only=False):
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

                objects = MappingQuerySet.as_manager()

                class Meta:
                    app_label = 'myapp'

            # myapp.views
            from django_sharding.utils import get_shard_for

            print (get_shard_for(user.organization.id))  # <class 'myapp.models.Shard'>
            print (get_shard_for(user.organization.id, active_only=True))  # StateException if state is not active

       """
    if 'MAPPING_MODEL' not in settings.SHARDING:
        raise ImproperlyConfigured('Missing or incorrect type of a setting SHARDING["{}"].'.format('MAPPING_MODEL'))

    mapping_model = import_string(settings.SHARDING['MAPPING_MODEL'])
    mapping_object = mapping_model.objects.select_related('shard').for_target(target_value)
    if active_only:
        if mapping_object.state != State.ACTIVE:
            raise StateException("Mapping object {} state is {}".format(mapping_object, mapping_object.state),
                                 mapping_object.state)
        if mapping_object.shard.state != State.ACTIVE:
            raise StateException("Shard {} state is {}".format(mapping_object.state, mapping_object.shard.state),
                                 mapping_object.shard.state)
    return mapping_object.shard


class use_shard_for(use_shard):
    """
    Extends use_shard. You provide the value occurring in the mapping table to easily find the correct shard.

    :note: This only works if you use a model decorated with @shard_mapping_model.
        See the Installation chapter for more info.

    :note: If the mapping model has a state, it will raise a StateException if the state is not State.ACTIVE.

    :param target_value: Value for mapping_field in your mapping table

    :returns: The context manager as an object with the following members:

    * **connection:** Reference to the current database connection.
    * **shard:** Reference to the current shard model object.

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


                objects = MappingQuerySet.as_manager()

                class Meta:
                    app_label = 'myapp'

            # myapp.views
            from django_sharding.utils import use_shard_for

            with use_shard_for(user.organization.id):
                # do things on my shard

    """
    def __init__(self, target_value):
        super().__init__(shard=get_shard_for(target_value, active_only=True), active_only_schemas=False)


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
        raise ValueError("No node_name given, or no NEW_SHARD_NODE set in the SHARING settings.")
    _node_exists(node_name)
    connections[node_name].create_schema(schema_name)

    if migrate:
        connections[node_name].clone_schema(get_template_name(), schema_name)


def create_template_schema(node_name='default'):
    """
    Each node needs to have a template schema. This is cloned for each new shard on the node.
    This function creates a new schema on a given node that is named 'template', or what you have set under
    settings.SHARDING.TEMPLATE_NAME.
    It then migrates only the sharded tables to this schema, by calling sharding.utils.migrate_schema.

    Since this only needs to happen once on each node, you can just run it in the shell.

    :param str node_name: Provide the name of the database connection to be used. If empty it will use the current.

    :returns: None

    :Example:
        .. code-block:: python

            from sharding.utils import create_template_schema
            create_template_schema(node_name='default')

    """
    schema_name = get_template_name()
    _node_exists(node_name)

    connections[node_name].create_schema(schema_name, is_template=True)  # if it already exists, it's no problem
    migrate_schema(node_name, schema_name)


def migrate_schema(node_name, schema_name):
    """
    It then migrates only the sharded tables to the given schema.
    This actually performs a migration, it does not clone the template schema.

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

    con = connections[node_name]
    with use_shard(node_name=node_name, schema_name=schema_name):
        THREAD_LOCAL.SHARDED_MIGRATE = True  # tell the router that we are doing a sharded models migration
        c = MigrateCommand()
        c.verbosity = 0
        c.interactive = False
        c.load_initial_data = False

        app_labels = [app_config.label for app_config in apps.get_app_configs()]
        c.sync_apps(con, app_labels)  # we use the django native migration call for this.
        THREAD_LOCAL.SHARDED_MIGRATE = False

import copy
import functools
import inspect

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, FieldDoesNotExist
from django.db import models
from django.test import override_settings

from sharding import ShardingMode, STATES
from sharding.db import connection
from sharding.options import ShardOptions
from sharding.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from sharding.router import get_active_connection
from sharding.utils import transaction_for_every_node, get_all_databases, use_shard

shard_mapping_models = False


def _add_decorator_reference(wrapped, decorator, args=(), kwargs=None):
    """
    Function that allows us to identify the decorator that was used and
    the args and kwargs used to call the decorator.
    """
    wrapped.__decorator__ = decorator, inspect.signature(decorator).bind(*args, **(kwargs or {}))
    return wrapped


def _reset_shard_mapping_models():
    # for internal testing use only.
    global shard_mapping_models
    shard_mapping_models = False


def class_method_use_shard(func):
    """
    Decorator that is used to decorate all class methods of a certain class. It will make sure that the class methods
    are run in the same shard as the instance lives in.
    """
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        if not hasattr(self, '_state') or not getattr(self._state, 'db', None):
            return func(self, *args, **kwargs)

        shard_options = ShardOptions.from_alias(self._state.db)
        override_class_method_use_shard = hasattr(connection, 'shard_options') \
            and connection.shard_options.kwargs.get('override_class_method_use_shard', False)

        if shard_options == get_active_connection() or override_class_method_use_shard:
            return func(self, *args, **kwargs)

        with shard_options.use():
            return func(self, *args, **kwargs)
    return _add_decorator_reference(inner, decorator=class_method_use_shard, args=(func,))


# Decorator to cope with a Django signaling bug, which is resolved in version 2.1.
def class_method_use_shard_from_db(func):
    def inner(db, field_names, values):
        with ShardOptions.from_alias(db).use():
            return func(db, field_names, values)
    return inner


def mirrored_model():
    """
    A decorator for marking a model for being mirror across the various nodes.

    This will tell the migration system that this model is to be available on the public schema on all nodes.
    Keeping the data in sync is not done automatically. Helper functions are provided.

    :Example:
        .. code-block:: python

            from django.db import models
            from sharding.decorators import mirrored_model

            @mirrored_model()
            class Type(models.Model):
                name = models.CharField('name', max_length=100)

                class Meta:
                    app_label = 'example'


            @atomic_write_to_every_node
            def update_type(new_type_name):
                # This runs within a use_node
                Type.objects.create(name=new_type_name)

            update_type('new_type')  # add a new Type object to all nodes.
    """
    def configure(cls):
        cls.sharding_mode = ShardingMode.MIRRORED
        return cls

    return configure


def shard_mapping_model(mapping_field):  # noqa: C901
    """
    A decorator for marking a model that maps shards and their content.
    This model will hold the foreignkey to the appropriate Shard model.

    Optionally, you can add ``MappingQuerySet`` as the object manager.
    This adds a few convenient shortcuts as well as the ability to use ``utils.use_shard_for()``,
    to forgo consulting this table manually each time you want to use a shard.

    :param mapping_field: Name of the primary field used to query the table to find a shard.
        This is used by the MappingQuerySet object manager.

    :note: Some fields are required to use this model:

        * shard: ForeignKey to the Sharding model.
        * state: Single length char field with utils.States as options.
        * a field that is listed as the 'mapping_field' as argument to the decorator

    :note: This model will NOT be sharded. It would defeat its purpose if it did.

    :Example:
        .. code-block:: python

            from django.db import models
            from sharding.decorators import shard_mapping_model, sharded_model
            from sharding.models import BaseShard
            from sharding.utils import State

            class Shard(BaseShard):
                class Meta:
                    app_label = 'example'

            @shard_mapping_model(mapping_field='organization_id')
            class ShardMapping(models.Model):
                # List every organization in this model, so you can easily request in which shard they live.
                shard = models.ForeignKey('Shard', verbose_name='shard')
                organization_name = models.CharField('organization name', max_length=100)
                organization_id = models.PositiveIntegerField(_('organization id'))
                state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

                objects = MappingQuerySet.as_manager()

                class Meta:
                    app_label = 'example'

            @sharded_model()
            class Organization(models.Model):
                name = models.CharField('name', max_length=100)
                created_at = models.DateTimeField(_('created at'), null=True, blank=True, default=timezone.now)

                class Meta:
                    app_label = 'example'

    """
    def configure(cls):
        try:
            shard_field = cls._meta.get_field('shard')
        except FieldDoesNotExist:
            raise ImproperlyConfigured(
                "{} model is missing a foreignkey field named 'shard'. "
                "The @shard_mapping_model decorator requires this."
                .format(cls.__name__))
        else:
            if not isinstance(shard_field, models.ForeignKey):
                raise ImproperlyConfigured(
                    "The shard field of model '{}' is not a Foreignkey to the shard model. "
                    "The @shard_mapping_model decorator requires this."
                    .format(cls.__name__))

            related_to = shard_field.rel.to if type(shard_field.rel.to) is str else \
                shard_field.rel.to.__module__.replace('.models', '') + '.' + shard_field.rel.to.__name__
            if related_to != settings.SHARDING['SHARD_CLASS'].replace('.models', ''):
                raise ImproperlyConfigured("The shard field of model {} is points to '{}' instead of '{}'. "
                                           "The @shard_mapping_model decorator requires this."
                                           .format(cls.__name__, related_to,
                                                   settings.SHARDING['SHARD_CLASS'].replace('.models', '')))

        try:
            cls._meta.get_field(mapping_field)
        except FieldDoesNotExist:
            raise ImproperlyConfigured("{} model is missing a field named '{}'. Yet it is given as the mapping field."
                                       .format(cls.__name__, mapping_field))

        try:
            state_field = cls._meta.get_field('state')
        except FieldDoesNotExist:
            raise ImproperlyConfigured(
                "{} model is missing a CharField field named 'state'. "
                "The @shard_mapping_model decorator requires this.".format(cls.__name__))
        else:
            if not isinstance(state_field, models.CharField) or state_field.choices != STATES:
                raise ImproperlyConfigured("The state field of model '{}' is not a CharField with "
                                           "sharding.utils.STATES as choices".format(cls.__name__))

        # set global counter to detect multiple usages of this decorator, which is not allowed.
        global shard_mapping_models
        if shard_mapping_models:
            raise ImproperlyConfigured(
                'More than one model uses the @shard_mapping_model decorator. This is not allowed.')
        else:
            shard_mapping_models = True

        cls.mapping_field = mapping_field
        return cls

    return configure


def sharded_model():
    """
    A decorator for marking a model as being sharded.
    This means this model will be migrated to new shards, and not appear in the public schema.

    :Example:
        .. code-block:: python

            from django.db import models
            from sharding.decorators import sharded_model
            from sharding.models import BaseShard

            class Shard(BaseShard):
                class Meta:
                    app_label = 'example'

            @sharded_model()
            class Organization(models.Model):
                name = models.CharField('name', max_length=100)
                created_at = models.DateTimeField(_('created at'), null=True, blank=True, default=timezone.now)

                class Meta:
                    app_label = 'example'

    """
    def configure(cls):
        cls.sharding_mode = ShardingMode.SHARDED
        return cls

    return configure


def atomic_write_to_every_node(schema_name=PUBLIC_SCHEMA_NAME, lock_models=(), pass_node_name=True):
    """
    Decorator to execute wrapped function for every node.
    Runs inside a transaction_for_every_node to keep all nodes in sync.

    transaction_for_every_node builds a cascading transaction tree:
        a transaction for each node, each running inside the previous.

    If an error occurs, all the transactions are rolled back, and no node is changed.

    Optionally, you can give a set of models and locking modes to prevent other threads from accessing the table you
    are manipulating. This is useful to ensure uniqueness, which might be threatened in edge-cases.

    :param str schema_name: The name of the schema used. 'public' by default.
    :param tuple lock_models: Sets containing a model and a PostgreSQL lock mode. e.g.: "((User, 'ACCESS EXCLUSIVE'),)"

    :returns: The name of the node in use.

    :Example:
        .. code-block:: python

            from sharding.utils import atomic_write_to_every_node

            @atomic_write_to_every_node('public')
            def my_function(node_name):
                # Create an object on each node's public schema
                Type.objects.create(name='test_type')

            @atomic_write_to_every_node('public', ((Type, 'ACCESS EXCLUSIVE'),)
            def my_function(node_name):
                # Create an object on each node's public schema
                # We are in blocking mode, so nothing else can alter this table while this transaction is active.
                # This ensures, if the name field has enforced distinction, no duplicate can exist.
                Type.objects.create(name='unique type')

    """
    def decorate(func):
        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return_values = {}

            with transaction_for_every_node(lock_models=lock_models):
                for node_name in get_all_databases():
                    with use_shard(node_name=node_name, schema_name=schema_name):
                        if pass_node_name:
                            kwargs['node_name'] = node_name
                        return_values[node_name] = func(*args, **kwargs)

            return return_values
        return _add_decorator_reference(decorator, decorator=atomic_write_to_every_node,
                                        kwargs={'schema_name': schema_name, 'lock_models': lock_models})
    return decorate


class override_sharding_setting(override_settings):
    SENTINEL = object()  # Indicator for the absence of a value

    def __init__(self, name, value=SENTINEL):
        sharding = copy.deepcopy(settings.SHARDING)  # Important: use a deepcopy, don't modify the reference

        if value is self.SENTINEL:
            # If we didn't provide a value, then we delete the value from the settings
            sharding.pop(name, None)
        else:
            sharding[name] = value

        super().__init__(SHARDING=sharding)

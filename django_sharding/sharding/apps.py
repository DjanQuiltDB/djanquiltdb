import functools
import inspect
import types

from django.apps import AppConfig, apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from sharding import ShardingMode
from sharding.db import connection
from sharding.decorators import class_method_use_shard, class_method_use_shard_from_db_arg
from sharding.options import ShardOptions
from sharding.postgresql_backend.base import ShardDatabaseWrapper
from sharding.utils import get_all_sharded_models


class ShardingConfig(AppConfig):
    name = 'sharding'
    verbose_name = 'Sharding'

    def ready(self):
        from .models import BaseShard

        if 'SHARDING' not in dir(settings) or not isinstance(settings.SHARDING, dict):
            raise ImproperlyConfigured('Missing or incorrect type of a setting SHARDING.')

        # Validate shard and node class settings
        if 'SHARD_CLASS' not in settings.SHARDING:
            raise ImproperlyConfigured('Missing or incorrect type of a setting SHARDING["{}"].'.format('SHARD_CLASS'))
        class_ = import_string(settings.SHARDING['SHARD_CLASS'])
        if not issubclass(class_, BaseShard):
            raise ImproperlyConfigured(
                'The type {} should inherit from {}.'.format(settings.SHARDING['SHARD_CLASS'], BaseShard.__name__))
        if hasattr(class_, 'sharding_mode') and getattr(class_, 'sharding_mode') == ShardingMode.SHARDED:
            raise ImproperlyConfigured(
                'The Shard model cannot itself be sharded. It can only be non-sharded or mirrored.'
            )

        override_sharding_mode = settings.SHARDING.setdefault('OVERRIDE_SHARDING_MODE', {})
        if not isinstance(override_sharding_mode, dict):
            raise ImproperlyConfigured('Incorrect setting value of SHARDING["OVERRIDE_SHARDING_MODE"].')

        for key, value in override_sharding_mode.items():
            _validate_override_sharding_mode_entry(key, value)

        # Convert app and model names to lowercase
        settings.SHARDING['OVERRIDE_SHARDING_MODE'] = dict((tuple(x.lower() for x in k), v)
                                                           for k, v in override_sharding_mode.items())

        if 'DATABASE_ROUTERS' not in dir(settings) or 'sharding.router.DynamicDbRouter' \
                not in settings.DATABASE_ROUTERS:
            raise ImproperlyConfigured(
                'sharding.router.DynamicDbRouter must be present in the DATABASE_ROUTERS setting.')

        if 'SESSION_ENGINE' in dir(settings) and \
            settings.SESSION_ENGINE == 'django.contrib.sessions.backends.cached_db' and \
                getattr(get_user_model(), 'sharding_mode', False) in [ShardingMode.MIRRORED, ShardingMode.SHARDED]:

            raise ImproperlyConfigured(
                "When the user model is sharded, you cannot use django.contrib.sessions.backends.cached_db "
                "to store sessions. It references the user table and won't know where to find it."
            )

        _patch_connections()
        _initialize_sharded_models()


def _validate_override_sharding_mode_entry(key, value):
    if not (isinstance(key, tuple) and len(key) in (1, 2) and isinstance(value, ShardingMode)):
        raise ImproperlyConfigured('The override sharding mode entry is improperly configured: '
                                   '{{ {}: {} }}'.format(repr(key), repr(value)))

    app_label = key[0].lower()
    if len(key) == 2:
        model_name = key[1].lower()
        try:
            apps.get_model(app_label, model_name)
        except LookupError:
            raise ImproperlyConfigured('Cannot find model to override sharding mode: ({}, {})'.format(app_label,
                                                                                                      model_name))
    else:  # len(key) == 1
        try:
            apps.get_app_config(app_label)
        except LookupError:
            raise ImproperlyConfigured('Cannot find app_label to override sharding mode: {}'.format(app_label))


def _initialize_sharded_models():
    """
    Initialize sharded models by overriding all methods to add a use_shard context manager that makes sure all queries
    are done in that same shard as the object is living in.
    """
    from_db_functions = {model: model.from_db for model in get_all_sharded_models(include_proxy=True)}

    for model in get_all_sharded_models(include_proxy=True):
        for attr, func in inspect.getmembers(model, inspect.isfunction):
            # getattr(model, attr) will trigger dynamic lookup via the descriptor protocol,  __getattr__ or
            # __getattribute__. Therefore, we use inspect.getattr_static to strip out staticmethods (which we don't want
            # to decorate).
            if isinstance(inspect.getattr_static(model, attr), types.FunctionType):
                # And decorate all model methods so that the methods will all run in the same shard context as the
                # instance is living in
                setattr(model, attr, class_method_use_shard(func))

        # Setting the from_db function for a Model that is the parent of a ProxyModel will corrupt the ProxyModels
        # version of the same function. So we have saved the original from_db function at the start, and use that.
        model.add_to_class('from_db', class_method_use_shard_from_db_arg(from_db_functions[model]))

        _initialize_sharded_model_querysets(model)


def post_init(func):
    @functools.wraps(func)
    def inner(self, *args, hints=None, **kwargs):
        hints = hints or {}

        if '_shard_options' not in hints and not connection.is_public_schema():
            hints['_shard_options'] = connection.shard_options

        func(self, *args, hints=hints, **kwargs)
    inner.__decorator__ = post_init
    return inner


def _initialize_sharded_model_querysets(model):
    """
    Override all the querysets of the managers, so they can remember the shard where they are initialized on
    """
    for instance in model._meta.managers:
        if hasattr(instance._queryset_class.__init__, '__decorator__') and \
                instance._queryset_class.__init__.__decorator__ == post_init:
            continue
        setattr(instance._queryset_class, '__init__', post_init(instance._queryset_class.__init__))


def patch_getitem(func):
    @functools.wraps(func)
    def inner(self, alias):
        # If we're planning to just go into the public schema, then we're going to use the normal connection for that
        if isinstance(alias, str) and '|' not in alias or isinstance(alias, ShardOptions) and alias.is_public_schema():
            return func(self, alias if isinstance(alias, str) else alias.node_name)

        options = ShardOptions.from_alias(alias)

        # Retrieves the main connection to the database, so we can still do connection pooling
        connection_ = func(self, options.node_name)

        # Sets up the sharded connection
        return ShardDatabaseWrapper(connection_, options)
    return inner


def _patch_connections():
    """
    Monkeypatch django.db.connection and django.db.connections to accept:

      * node_name
      * node_name|schema_name
      * tuple (node_name, schema_name)
      * ShardOptions instance
      * Shard model instance
    """
    import django.db
    from django.db.utils import ConnectionHandler
    setattr(ConnectionHandler, '__getitem__', patch_getitem(ConnectionHandler.__getitem__))
    setattr(django.db, 'connection', connection)

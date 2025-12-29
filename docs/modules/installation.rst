============
Installation
============

.. _`install`:

Installing djanquiltdb
-----------------------------------

If you want to install stable version, you can do so doing::

    pip install git+ssh://git@github.com/sectigo/djanquiltdb.git@stable#egg=djanquiltdb

If you want to install development version (unstable), you can do so doing::

    pip install git+ssh://git@github.com/sectigo/djanquiltdb.git@master#egg=djanquiltdb

Or, if you'd like to install the development version as a git repository (so
you can ``git pull`` updates, use the ``-e`` flag with ``pip install``, like
so::

    pip install -e git+ssh://git@github.com/sectigo/djanquiltdb.git@master#egg=djanquiltdb

Add ``djanquiltdb`` to your ``INSTALLED_APPS`` in settings.py::

    INSTALLED_APPS = (
        ...
        'djanquiltdb',
        ...
    )

Creating models
---------------

The sharding application requires you to create custom ``Shard`` model, which inherit form the base model.


``myapp/models.py`` ::

    from django.db import models

    from djanquiltdb.models import BaseShard


    class Shard(BaseShard):
        class Meta:
            app_label = 'myapp'

Make migrations
---------------

``./manage makemigrations``::

    Migrations for 'myapp':
      0001_initial.py:
        - Create model Shard


Configuration settings
----------------------

There are several settings to make for sharding. All of them live in the ``QUILT_DB`` variable.

SHARD_CLASS
~~~~~~~~~~~
You must set ``SHARD_CLASS`` with the dot path to the ``shard`` classes in your
project settings e.g.::

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
    }

MAPPING_MODEL
~~~~~~~~~~~~~
When you use a mapping model with the ``@shard_mapping_model`` decorator,
it is beneficial to add that models name to the settings.
If you also add the ``MappingQuerySet`` as object manager to that model you can use ``utils.use_shard_for()`` to fetch
the wanted shard from mapping table automatically.

.. code-block:: python

    # settings
    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.MyMappingModel',
    }

    # myapp.models
    @shard_mapping_model(mapping_field='organization_id')
    class OrganizationShard(models.Model):
        shard = models.ForeignKey('example.Shard', on_delete=models.CASCADE)
        organization_id = models.PositiveSmallIntegerField(db_index=True)
        state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

        objects = MappingQuerySet.as_manager()

    # myapp.views
    from djanquiltdb.utils import use_shard_for

    with use_shard_for(user.organization_id):
        # do things on my shard

Additionally, you can mirror the mapping model by adding ``@mirrored_model`` to it.


NEW_SHARD_NODE
~~~~~~~~~~~~~~
Optionally you can tell DjanQuiltDB on which node new shards (schemas) will be created. e.g.::

    DATABASES = {'default': name='primary', engine='djanquiltdb.postgresql_backend'),
                 'node_2': name='db_2, engine='djanquiltdb.postgresql_backend')}

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'NEW_SHARD_NODE': 'node_2',
    }

ROUTER
~~~~~~
DjanQuiltDB uses a router to send each database transaction to the correct node.
It also uses the router to migrate the models to the correct shard when using ``./manage.py migrate_shards``
So set ``djanquiltdb.router.DynamicDbRouter`` as the database_router in the settings. e.g.::

    DATABASE_ROUTERS = ['djanquiltdb.router.DynamicDbRouter']

STATE_EXCEPTION_MIDDLEWARE
~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``djanquiltdb.middleware.StateExceptionMiddleware`` class allows you to deal with exceptions raised by accessing
unavailable shards. It is not required, but recommended to add it to the middleware settings.

The middleware raises a 503 error when a shard availability error pops up during view processing.
You can also tell it to render a specific view instead.
To do that set ``STATE_EXCEPTION_VIEW`` in the ``QUILT_DB`` setting to a view of your choice e.g.::

    MIDDLEWARE_CLASSES = (
        (...)
        'djanquiltdb.middleware.StateExceptionMiddleware'
        (...)
    )

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'STATE_EXCEPTION_VIEW': 'myapp.views.ShardExceptionView'
    }

.. _use_shard_middleware:

BASE_USE_SHARD_MIDDLEWARE
~~~~~~~~~~~~~~~~~~~~~~~~~
The ``djanquiltdb.middleware.BaseUseShardMiddleware`` class extends ``StateExceptionMiddleware`` and adds the option to
wrap views in a ``use_shard`` context manager. This prevents the need to take note of sharding in each of your views.

How the middleware determines which shard to use is up to you however. To use the ``UseShardMiddleware`` you have to
extend it and fill in the ``get_shard_id()`` function yourself.

Don't forget you can assign your own view as error page like in `STATE_EXCEPTION_MIDDLEWARE`_.

.. code-block:: python

    # settings.py
    MIDDLEWARE_CLASSES = (
        (...)
        'django.contrib.sessions.middleware.SessionMiddleware',
        'middleware.UseShardMiddleware',
        (...)
    )

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'STATE_EXCEPTION_VIEW': 'myapp.views.ShardExceptionView'
    }

    # middleware.py
    class UseShardMiddleware(BaseUseShardMiddleware):
        def get_shard_id(self, request):
            # A common way is to alter the login flow to set the shard_id in the session.
            return request.session.get('shard_id')


    # views.py
    from django.contrib.auth import views as django_auth_views

    # Example way of obtaining a shard_id is to add an additional input field in the login form.
    # Use the additional slug field to get the shard_id and save that to the session.
    def login_view(request):
        response = django_auth_views.login(request=request)

        if request.user.is_authenticated():
            slug = request.POST.get('slug')
            request.session['shard_id'] = get_shard_for_slug(slug).id

        return response

If you are using a mapping model in your project, you can also use the ``BaseUseShardForMiddleware`` in the same
fashion as the ``BaseUseShardMiddleware``. The only difference is that you now have to define the
``get_mapping_value()`` method instead of the ``get_shard_id()``.

.. code-block:: python

    # settings.py
    MIDDLEWARE_CLASSES = (
        (...)
        'django.contrib.sessions.middleware.SessionMiddleware',
        'middleware.UseShardForMiddleware',
        (...)
    )

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.ShardMappingModel',
        'STATE_EXCEPTION_VIEW': 'myapp.views.ShardExceptionView',
    }

    # middleware.py
    class UseShardForMiddleware(BaseUseShardForMiddleware):
        def get_mapping_value(self, request):
            # A common way is to alter the login flow to set the mapping value in the session.
            return request.session.get('mapping_value')


If you want to store the value on the session, you can also use `django_sharding.middleware.UseShardMiddleware` or
`django_sharding.middleware.UseShardForMiddleware` directly; these assume the shard selector value is stored on the
session with the key defined in the `SESSION_SHARD_SELECTOR_KEY` setting. The default is `shard_selector`, which is
compatible with the standard `django_sharding.sessions` backend. To configure this session backend, you need to make
the following changes (assuming here that the session storage is under an app called `users`):

.. code-block:: python

    # settings.py
    SESSION_ENGINE = 'djanquiltdb.sessions'
    
    QUILT_SESSIONS = {
        # Required: Configure the session model
        'SESSION_MODEL': 'users.models.QuiltSession',
        # Optional: Regex pattern for validating shard selector values
        'SHARD_SELECTOR_REGEX': '[0-9]+',
        # Optional: Delimiter used in session keys to separate shard selector from session key (default: 'K')
        'SESSION_KEY_DELIMITER': 'K',
    }
    
    # Optional: Customize the session key used by middleware to store shard selector (default: 'shard_selector')
    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'SESSION_SHARD_SELECTOR_KEY': 'shard_selector',  # Optional, defaults to 'shard_selector'
    }

    MIDDLEWARE_CLASSES = (
        (...)
        'django.contrib.sessions.middleware.SessionMiddleware',
        'djanquiltdb.middleware.UseShardForMiddleware',
        (...)
    )

    # users/models.py
    from django_sharding.models import BaseQuiltSession

    @sharded_model()
    class QuiltSession(BaseQuiltSession):
        class Meta:
            app_label = 'users'

You would only need to change the SHARD_SELECTOR_REGEX if the primary key of your shard or mapping value is not a number. For example, if your shard selectors are UUIDs, you might use: ``'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'``. If your regular expression supports string values that may include K, you also have to adjust the SESSION_KEY_DELIMITER to make sure it is a value that can't be matched by the regular expression.

OVERRIDE_SHARDING_MODE
~~~~~~~~~~~~~~~~~~~~~~
If you want to override the sharding_mode for a specific model or application you can use
``QUILT_DB['OVERRIDE_SHARDING_MODE']`` configuration setting. The setting is a dictionary with tuple or list as a key
and ShardingMode enum as value. The key is composed from one or two lowercase strings and it is used as a lookup for
an app or a model in an app.

.. code-block:: python

    # settings
    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.MyMappingModel',
        'OVERRIDE_SHARDING_MODE': {
            # The entry overrides app1.ExampleModel sharding_mode to ShardingMode.MIRRORED
            ('app1', 'examplemodel'): ShardingMode.MIRRORED,

            # The entry overrides all app2 models sharding_mode to ShardingMode.SHARDED
            ('app2',): ShardingMode.SHARDED,
        }
    }


This section can also be used to set sharding modes to models that no longer exist. Normally the router will look at
the model definition to see if a migration for it has to be performed on the publci schema or a shard. But if the
model is no longer in your code base, it cannot do this anymore. It will mention the definition is missing and skip
the migration.
If the migration is required, and the model is missing, simply mention it here so the router knows what to do:

.. code-block:: python

    # settings
    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.MyMappingModel',
        'OVERRIDE_SHARDING_MODE': {
            # The entry overrides app1.NonExistentModel sharding_mode to ShardingMode.SHARDED, even though this model is no longer defined, it might still be mentioned in migrations.
            ('app1', 'nonexistentmodel'): ShardingMode.SHARDED,
        }
    }

PRIMARY_DB_ALIAS
~~~~~~~~~~~~~~~~

Django has the notion of a 'default' database connection. When no routing is given, that connection is used.
In a sharded environment, the idea of a 'default' connection gains new meaning dependant on the type of model requested:

* Non-decorated:
  Will only live on the default node's public schema
* PUBLIC:
  Table will exist on all nodes' public schema. Their data can differ, and all nodes are writable.
* MIRRORED
  Table will exist on all nodes' public schema. Their data is marked as replicated, so the same across all nodes.
  Only the 'default' node is considered writable, the others read-only.
* SHARDED
  Table will exist on all nodes' sharded schemas. All nodes are writable since their data is unique.

MIRRORED is the interesting situation. In a replicated environment only one node is writable, the others will read
from it and block any mutations of their own.
It is wrong to assume the node that is call 'default' by Django is also always the writable replication node.
These roles can failover and change over time. We cannot change the connection names in the setting to match however.
A node called 'Rose' must always be called 'Rose', for that name is used to tell which data is on what node.
In order to tell the router which node is writable, we introduce 'PRIMARY_DB_ALIAS'.
It's value is a connection name of the node which is writable for MIRRORED data.

.. code-block:: python

  DATABASES = {'default': dj_database_url.parse(get_secret('DATABASE_URL'), engine='djanquiltdb.postgresql_backend'),
               'other': dj_database_url.parse(get_secret('DATABASE_URL2'), engine='djanquiltdb.postgresql_backend')}

  QUILT_DB = {
      'PRIMARY_DB_ALIAS': 'default',
      'NEW_SHARD_NODE': 'other',
  }

And for replication purposes, 'default' is the primary, and 'other' is the follower.
Once this failsover, 'other' become primary, and 'default' becomes follower. Their actual names are not to change.
So we alter 'PRIMARY_DB_ALIAS' to tell that 'other' is now writable.

.. code-block:: python

  DATABASES = {'default': dj_database_url.parse(get_secret('DATABASE_URL'), engine='djanquiltdb.postgresql_backend'),
               'other': dj_database_url.parse(get_secret('DATABASE_URL2'), engine='djanquiltdb.postgresql_backend')}

  QUILT_DB = {
      'PRIMARY_DB_ALIAS': 'other',
      'NEW_SHARD_NODE': 'other',
  }

It would be less confusing if we have non-suggestive names for all connections. But Django enforces the existence of
a 'default' node. Even if the router will always route to the primary assigned connection by default.

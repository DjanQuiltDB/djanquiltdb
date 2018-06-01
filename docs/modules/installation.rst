============
Installation
============

.. _`install`:

Installing django-sharding
--------------------------

If you want to install stable version, you can do so doing::

    pip install git+ssh://git@bitbucket.org/patchmanbv/django-sharding.git@stable#egg=django-sharding

If you want to install development version (unstable), you can do so doing::

    pip install git+ssh://git@bitbucket.org/patchmanbv/django-sharding.git@master#egg=django-sharding

Or, if you'd like to install the development version as a git repository (so
you can ``git pull`` updates, use the ``-e`` flag with ``pip install``, like
so::

    pip install -e git+ssh://git@bitbucket.org/patchmanbv/django-sharding.git@master#egg=django-sharding

Add ``sharding`` to your ``INSTALLED_APPS`` in settings.py::

    INSTALLED_APPS = (
        ...
        'sharding',
        ...
    )

Creating models
---------------

The sharding application requires you to create custom ``Shard`` model, which inherit form the base model.


``myapp/models.py`` ::

    from django.db import models

    from sharding.models import BaseShard


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

There are several settings to make for sharding. All of them live in the ``SHARDING`` variable.

SHARD_CLASS
~~~~~~~~~~~
You must set ``SHARD_CLASS`` with the dot path to the ``shard`` classes in your
project settings e.g.::

    SHARDING = {
        'SHARD_CLASS': 'myapp.models.Shard',
    }

MAPPING_MODEL
~~~~~~~~~~~~~
When you use a mapping model with the ``@shard_mapping_model`` decorator,
it is beneficial to add that models name to the settings.
If you also add the ``MappingQuerySet`` as object manager to that model you can use ``utils.use_shard_for()`` to fetch the wanted shard from mapping table automatically.

.. code-block:: python

    # settings
    SHARDING = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.MyMappingModel',
    }

    # myapp.models
    @shard_mapping_model(mapping_field='organization_id')
    class OrganizationShards(models.Model):
        shard = models.ForeignKey('example.Shard')
        organization_id = models.PositiveSmallIntegerField(db_index=True)
        state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

        objects = MappingQuerySet.as_manager()

    # myapp.views
    from django_sharding.utils import use_shard_for

    with use_shard_for(user.organization_id):
        # do things on my shard

Additionally, you can mirror the mapping model by adding ``@mirrored_model`` to it.


NEW_SHARD_NODE
~~~~~~~~~~~~~~
Optionally you can tell Django-sharding on which node new shards (schemas) will be created. e.g.::

    DATABASES = {'default': name='primary', engine='sharding.postgresql_backend'),
                 'node_2': name='db_2, engine='sharding.postgresql_backend')}

    SHARDING = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'NEW_SHARD_NODE': 'node_2',
    }

ROUTER
~~~~~~
Django-sharding uses a router to send each database transaction to the correct node.
It also uses the router to migrate the models to the correct shard when using ``./manage.py migrate_shards``
So set ``sharding.utils.DynamicDbRouter`` as the database_router in the settings. e.g.::

    DATABASE_ROUTERS = ['sharding.utils.DynamicDbRouter']

STATE_EXCEPTION_MIDDLEWARE
~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``sharding.middleware.StateExceptionMiddleware`` class allows you to deal with exceptions raised by accessing
unavailable shards. It is not required, but recommended to add it to the middleware settings.

The middleware raises a 503 error when a shard availability error pops up during view processing.
You can also tell it to render a specific view instead.
To do that set ``STATE_EXCEPTION_VIEW`` in the ``SHARDING`` setting to a view of your choice e.g.::

    MIDDLEWARE_CLASSES = (
        (...)
        'sharding.middleware.StateExceptionMiddleware'
        (...)
    )

    SHARDING = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'STATE_EXCEPTION_VIEW': 'myapp.views.ShardExceptionView'
    }

.. _use_shard_middleware:

BASE_USE_SHARD_MIDDLEWARE
~~~~~~~~~~~~~~~~~~~~~~~~~
The ``sharding.middleware.BaseUseShardMiddleware`` class extends ``StateExceptionMiddleware`` and adds the option to
wrap views in a ``use_shard`` context manager. This prevents the need to take note of sharding in each of your views.

How the middleware determines which shard to use is up to you however. To use the ``UseShardMiddleware`` you have to extend it and fill in the ``get_shard_id()`` function yourself.

Don't forget you can assign your own view as error page like in `STATE_EXCEPTION_MIDDLEWARE`_.

.. code-block:: python

    # settings.py
    MIDDLEWARE_CLASSES = (
        (...)
        'django.contrib.sessions.middleware.SessionMiddleware',
        'middleware.UseShardMiddleware',
        (...)
    )

    SHARDING = {
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

If you are using a mapping model in your project, you can also use the ``BaseUseShardForMiddleware`` in the same fashion as the ``BaseUseShardMiddleware``. The only difference is that you now have to define the ``get_mapping_value()`` method instead of the ``get_shard_id()``.

.. code-block:: python

    # settings.py
    MIDDLEWARE_CLASSES = (
        (...)
        'django.contrib.sessions.middleware.SessionMiddleware',
        'middleware.UseShardForMiddleware',
        (...)
    )

    SHARDING = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.ShardMappingModel',
        'STATE_EXCEPTION_VIEW': 'myapp.views.ShardExceptionView',
    }

    # middleware.py
    class UseShardForMiddleware(BaseUseShardForMiddleware):
        def get_mapping_value(self, request):
            # A common way is to alter the login flow to set the mapping value in the session.
            return request.session.get('mapping_value')

OVERRIDE_SHARDING_MODE
~~~~~~~~~~~~~~~~~~~~~~
If you want to override the sharding_mode for a specific model or application you can use ``SHARDING['OVERRIDE_SHARDING_MODE']`` configuration setting. The setting is a dictionary with tuple or list as a key and ShardingMode enum as value. The key is composed from one or two lowercase strings and it is used as a lookup for an app or a model in an app.

.. code-block:: python

    # settings
    SHARDING = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.MyMappingModel',
        'OVERRIDE_SHARDING_MODE': {
            # The entry overrides app1.ExampleModel sharding_mode to ShardingMode.MIRRORED
            ('app1', 'examplemodel'): ShardingMode.MIRRORED,

            # The entry overrides all app2 models sharding_mode to ShardingMode.SHARDED
            ('app2',): ShardingMode.SHARDED,
        }
    }


============
Installation
============

.. _`install`:

Installing django-sharding
~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~

The sharding application requires you to create custom ``Shard`` model, which inherit form the base model.


``myapp/models.py`` ::

    from django.db import models

    from sharding.models import BaseShard


    class Shard(BaseShard):
        class Meta:
            app_label = 'myapp'

Make migrations
~~~~~~~~~~~~~~~

``./manage makemigrations``::

    Migrations for 'myapp':
      0001_initial.py:
        - Create model Shard


Configuration settings
~~~~~~~~~~~~~~~~~~~~~~

You must set ``SHARDING`` Django settings variable with the dot path to the ``shard`` classes in your
project settings e.g.::

    SHARDING = {
        'SHARD_CLASS': 'myapp.models.Shard',
    }

Additionally Django-sharding uses a router to send each database transaction to the correct node.
So set ``sharding.utils.DynamicDbRouter`` as the database_router in the settings. e.g.::

    DATABASE_ROUTERS = ['sharding.utils.DynamicDbRouter']


The ``sharding.middleware.StateExceptionMiddleware`` class allows you to deal with exceptions raised by accessing
unavailable shards. It is not required, but recommended to add it to the middleware settings.

The middleware raises a 503 error when a shard availability error pops up during view processing.
You can also tell it to render a specific view instead.
To do that set ``STATE_EXCEPTION_VIEW`` in the ``SHARDING`` setting to a view of your choice e.g.::

    MIDDLEWARE_CLASSES = (
    (...)
    'sharding.middleware.StateExceptionMiddleware'
)

    SHARDING = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'STATE_EXCEPTION_VIEW': 'myapp.views.unavailableView'
    }

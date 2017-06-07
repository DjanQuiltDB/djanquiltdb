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

The sharding application requires you to create custom ``Shard`` and ``Node``, which inherit form the base models.


``myapp/models.py`` ::

    from django.db import models

    from sharding.models import BaseShard, BaseNode


    class Node(BaseNode):
        class Meta:
            app_label = 'myapp'


    class Shard(BaseShard):
        node = models.ForeignKey(Node, on_delete=models.PROTECT)

        class Meta:
            app_label = 'myapp'

Make migrations
~~~~~~~~~~~~~~~

``./manage makemigrations``::

    Migrations for 'myapp':
      0001_initial.py:
        - Create model Node
        - Create model Shard


Configuration settings
~~~~~~~~~~~~~~~~~~~~~~

You must set ``SHARDING`` Django settings variable with the dot path to the ``shard`` and ``node`` classes in your
project settings e.g.::

    SHARDING = {
        'NODE_CLASS': 'myapp.models.Node',
        'SHARD_CLASS': 'myapp.models.Shard',
    }


============
Installation
============

.. _`install`:

Installing djanquiltdb
----------------------

The stable version is available from PyPI::

    pip install djanquiltdb

If you want to install development version (unstable), you can do so doing::

    pip install git+ssh://git@github.com/djanquiltdb/djanquiltdb.git@master#egg=djanquiltdb

Or, if you'd like to install the development version as a git repository (so you can ``git pull`` updates), use the
``-e`` flag with ``pip install``, like so::

    pip install -e git+ssh://git@github.com/djanquiltdb/djanquiltdb.git@master#egg=djanquiltdb

Add ``djanquiltdb`` to your ``INSTALLED_APPS`` in ``settings.py``::

    INSTALLED_APPS = (
        ...
        'djanquiltdb',
        ...
    )

You will need to have a template schema on every node, which only has to be created once. For production purposes you
really only need to run it once at initialization, and once whenever a new node is added, but for local development and
testing purposes there can be a reason to run it more frequently. How you choose to set this up is considered up to
personal preference, but an example implementation you can use to ensure that it is always present, including in test
scenarios given default runners, is the following::

   from django.db.models.signals import pre_migrate

   from djanquiltdb.utils import create_template_schema, for_each_node

   def create_template_schema_before_migrations(sender, **kwargs):
       if sender.name != '<app in which you register this signal>':
           # emit_pre_migrate_signal runs once per app; we only need it once total.
           # Simply only run it for this app.
           return

       def _create_template_schema(node_name):
           create_template_schema(node_name=node_name, migrate=False)

       for_each_node(_create_template_schema)

   # In the ready() method of the AppConfig of that app:
   pre_migrate.connect(create_template_schema_before_migrations)


.. _`postgres_objects_extra`:

Optional extras
---------------

The base library covers models fully with decorators, and leaves database *objects* (views, functions, triggers) to be
managed with hand-written ``RunSQL`` in a migration.

The ``postgres-objects`` extra adds the declarative alternative for views and functions::

    pip install djanquiltdb[postgres-objects]

It installs a plugin carrying the glue between this library and `django-postgres-objects
<https://github.com/djanquiltdb/django-postgres-objects>`_, which declares Postgres views and functions as classes and
lets ``makemigrations`` manage them. That library works on its own against a single database and needs nothing from
here; what a sharded project adds is the question of which schemas each object belongs in, and the plugin provides
decorators for that purpose analogous to the model decorators in the base library. They are importable from
``djanquiltdb.decorators`` beside the model decorators.

See :doc:`/database_objects/views`, :doc:`/database_objects/functions` and :doc:`/database_objects/generated_columns`
for what the extra changes in each case, and :doc:`the plugin's documentation </plugins/postgres-objects/index>` for the
rest.

.. _`pghistory_extra`:

The ``pghistory`` extra adds model history that works with sharding::

    pip install djanquiltdb[pghistory]

It installs a plugin that makes this library work with `django-pghistory <https://django-pghistory.readthedocs.io/>`_.
The plugin's ``track()`` works like ``pghistory.track()``, and places each event model in the same place as the tracked
model. See :doc:`the plugin's documentation </plugins/pghistory/index>`.

If you wish to manage database triggers, we recommend
`django-pgtrigger <https://github.com/AmbitionEng/django-pgtrigger>`_. See :doc:`/database_objects/triggers` for more
information.


Creating models
---------------

The sharding application requires you to create a custom ``Shard`` model, which inherits from the base model.


``myapp/models.py`` ::

    from django.db import models

    from djanquiltdb.models import BaseShard


    class Shard(BaseShard):
        class Meta:
            app_label = 'myapp'

Make migrations
---------------

``python manage.py makemigrations``::

    Migrations for 'myapp':
      0001_initial.py:
        - Create model Shard


Configuration
-------------

Configure every node as a database that uses the DjanQuiltDB backend, add the DjanQuiltDB router so that queries go to
the right node, and set ``SHARD_CLASS`` in ``QUILT_DB`` to the shard model that was created above::

    DATABASES = {
        'default': {
            'ENGINE': 'djanquiltdb.postgresql_backend',
            ...
        },
    }

    DATABASE_ROUTERS = ['djanquiltdb.router.DynamicDbRouter']

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
    }

These are the settings that every project needs. A project that has mirrored models also has to set
``PRIMARY_DB_ALIAS``. :doc:`/reference/settings` describes all of the settings, and :doc:`/guides/requests` describes
the middleware that selects a shard for each request.

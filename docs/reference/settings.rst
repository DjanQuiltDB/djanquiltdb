========
Settings
========

There are several settings to make for sharding. All of them live in the ``QUILT_DB`` variable, except for the router,
which is Django's own ``DATABASE_ROUTERS`` setting, and the settings of the session backend, which are in
``QUILT_SESSIONS``.


QUILT_DB
--------

SHARD_CLASS
~~~~~~~~~~~
Required. You must set ``SHARD_CLASS`` with the dot path to the ``shard`` classes in your project settings e.g.::

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
    }

MAPPING_MODEL
~~~~~~~~~~~~~
When you use a mapping model with the ``@shard_mapping_model`` decorator,
it is beneficial to add that model's name to the settings.
If you also add the ``MappingQuerySet`` as object manager to that model you can use ``utils.use_shard_for()`` to fetch
the wanted shard from mapping table automatically.

.. code-block:: python

    # settings
    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.TenantShard',
    }

    # myapp.models
    @mirrored_model()
    @shard_mapping_model(mapping_field='tenant_slug')
    class TenantShard(models.Model):
        shard = models.ForeignKey('myapp.Shard', on_delete=models.CASCADE)
        tenant_slug = models.SlugField(unique=True)
        state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

        objects = MappingQuerySet.as_manager()

    # myapp.views
    from djanquiltdb.utils import use_shard_for

    with use_shard_for('acme'):
        ...  # Queries of sharded models run in the shard of the tenant acme.

Decorate the mapping model with ``@mirrored_model()``, like the Shard model, so that every node has the mapping table.
:doc:`/guides/mapping_model` explains what the mapping model is for, and the fields that it needs.


NEW_SHARD_NODE
~~~~~~~~~~~~~~
Optionally you can tell DjanQuiltDB on which node new shards (schemas) will be created. Without it, every new shard
has to be given its ``node_name``. For example::

    DATABASES = {
        'default': {'NAME': 'primary', 'ENGINE': 'djanquiltdb.postgresql_backend', ...},
        'node_2': {'NAME': 'db_2', 'ENGINE': 'djanquiltdb.postgresql_backend', ...},
    }

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'NEW_SHARD_NODE': 'node_2',
    }

STATE_EXCEPTION_VIEW and CONNECTION_EXCEPTION_VIEW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The dotted paths of the views that the exception middleware renders for a shard state error and for a database
connection error. Without them, the middleware returns a plain 503 response. See :ref:`exception_middleware`.

SESSION_SHARD_SELECTOR_KEY
~~~~~~~~~~~~~~~~~~~~~~~~~~
The attribute of the session that ``UseShardMiddleware`` and ``UseShardForMiddleware`` read the shard selector from.
Defaults to ``shard_selector``, which is the attribute that the ``djanquiltdb.sessions`` backend provides. See
:doc:`/guides/sessions`.

OVERRIDE_SHARDING_MODE
~~~~~~~~~~~~~~~~~~~~~~
If you want to override the sharding mode for a specific model or application you can use
``QUILT_DB['OVERRIDE_SHARDING_MODE']`` configuration setting. The setting is a dictionary with tuple or list as a key
and the ``ShardingMode`` enum as value. The key is composed from one or two lowercase strings and it is used as a lookup
for an app or a model in an app. To put the models of an entry on one node, set the value to
``single_node_unsharded(node)`` instead. This is strongly discouraged (see :doc:`/guides/sharding_modes`).

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
the model definition to see if a migration for it has to be performed on the public schema or a shard. But if the
model is no longer in your code base, it cannot do this anymore. It will mention the definition is missing and skip
the migration.
If the migration is required, and the model is missing, simply mention it here so the router knows what to do:

.. code-block:: python

    # settings
    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.MyMappingModel',
        'OVERRIDE_SHARDING_MODE': {
            # The entry overrides app1.NonExistentModel sharding_mode to ShardingMode.SHARDED. The model is no
            # longer defined, but it might still be mentioned in migrations.
            ('app1', 'nonexistentmodel'): ShardingMode.SHARDED,
        }
    }

PRIMARY_DB_ALIAS
~~~~~~~~~~~~~~~~

Django has the notion of a ``default`` database connection. When no routing is given, that connection is used.
In a sharded environment, the idea of a ``default`` connection gains new meaning dependent on the type of model
requested:

Non-decorated
  Will only live on the default node's public schema.
``PUBLIC``
  Table will exist on all nodes' public schema. Their data can differ, and all nodes are writable.
``MIRRORED``
  Table will exist on all nodes' public schema. Their data is marked as replicated, so the same across all nodes.
  Only the ``default`` node is considered writable, the others read-only.
``SHARDED``
  Table will exist on all nodes' sharded schemas. All nodes are writable since their data is unique.

``MIRRORED`` is the interesting situation. In a replicated environment only one node is writable, the others will read
from it and block any mutations of their own.
It is wrong to assume the node that is called ``default`` by Django is also always the writable replication node.
These roles can failover and change over time. We cannot change the connection names in the setting to match however.
A node called ``Rose`` must always be called ``Rose``, for that name is used to tell which data is on what node.
In order to tell the router which node is writable, we introduce ``PRIMARY_DB_ALIAS``.
Its value is a connection name of the node which is writable for ``MIRRORED`` data.
A project that has mirrored models must set it.

.. code-block:: python

  DATABASES = {'default': dj_database_url.parse(get_secret('DATABASE_URL'), engine='djanquiltdb.postgresql_backend'),
               'other': dj_database_url.parse(get_secret('DATABASE_URL2'), engine='djanquiltdb.postgresql_backend')}

  QUILT_DB = {
      'PRIMARY_DB_ALIAS': 'default',
      'NEW_SHARD_NODE': 'other',
  }

And for replication purposes, ``default`` is the primary, and ``other`` is the follower.
Once this fails over, ``other`` becomes primary, and ``default`` becomes follower. Their actual names are not to change.
So we alter ``PRIMARY_DB_ALIAS`` to tell that ``other`` is now writable.

.. code-block:: python

  DATABASES = {'default': dj_database_url.parse(get_secret('DATABASE_URL'), engine='djanquiltdb.postgresql_backend'),
               'other': dj_database_url.parse(get_secret('DATABASE_URL2'), engine='djanquiltdb.postgresql_backend')}

  QUILT_DB = {
      'PRIMARY_DB_ALIAS': 'other',
      'NEW_SHARD_NODE': 'other',
  }

It would be less confusing if we have non-suggestive names for all connections. But Django enforces the existence of
a ``default`` node. Even if the router will always route to the primary assigned connection by default.

TEMPLATE_NAME
~~~~~~~~~~~~~

The name of the template schema each new shard is cloned from (see :doc:`/guides/template_schema`). It defaults to
``template`` and rarely needs changing; set it when that name collides with an existing schema in your database.

.. code-block:: python

  QUILT_DB = {
      'SHARD_CLASS': 'myapp.models.Shard',
      'TEMPLATE_NAME': 'shard_template',
  }

AUTO_CREATE_TEMPLATE_SCHEMA
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Whether ``migrate`` creates the template schema of every node that it migrates and that has none, before it applies any
migration. It defaults to ``True``. Set it to ``False`` for a project that creates its template schemas itself. See
:doc:`/guides/template_schema`.

.. code-block:: python

  QUILT_DB = {
      'SHARD_CLASS': 'myapp.models.Shard',
      'AUTO_CREATE_TEMPLATE_SCHEMA': False,
  }

DATABASE_CREATION_CLASS
~~~~~~~~~~~~~~~~~~~~~~~

The dotted path of the class the backend uses to create and serialize test databases. It defaults to
``djanquiltdb.postgresql_backend.creation.DatabaseCreation``; point it at a subclass to customize how test
databases are built, for example ``djanquiltdb.postgresql_backend.creation.TemplateDatabaseCreation``.

.. code-block:: python

  QUILT_DB = {
      'SHARD_CLASS': 'myapp.models.Shard',
      'DATABASE_CREATION_CLASS': 'myapp.db.MyDatabaseCreation',
  }

SHARED_TEST_MIGRATION_STATES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Makes building a new test database faster. When ``True``, ``migrate`` renders the historical models once per
migration and hands the public and template schemas the same model classes, instead of rendering them for each schema.
It defaults to ``False``. Leave it off when a data migration keeps state on the historical models it is handed, and
consider leaving it off in CI. See :doc:`/guides/migrations` for the details.

.. code-block:: python

  QUILT_DB = {
      'SHARD_CLASS': 'myapp.models.Shard',
      'SHARED_TEST_MIGRATION_STATES': True,
  }

MUTE_PGTRIGGER_COMPATIBILITY_WARNING
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When ``django-pgtrigger`` is installed, DjanQuiltDB checks that it is left in migration mode and reports
``djanquiltdb.W001`` or ``djanquiltdb.W002`` (see :doc:`/reference/checks`) when it is not. See
:doc:`/database_objects/triggers` for why the other modes cannot reach every shard. Set this to ``True`` to silence
that check; it defaults to ``False``.

.. code-block:: python

  QUILT_DB = {
      'SHARD_CLASS': 'myapp.models.Shard',
      'MUTE_PGTRIGGER_COMPATIBILITY_WARNING': True,
  }


DATABASE_ROUTERS
----------------
DjanQuiltDB uses a router to send each database transaction to the correct node.
It also uses the router to migrate the models to the correct shard when using ``./manage.py migrate``.
So set ``djanquiltdb.router.DynamicDbRouter`` in ``DATABASE_ROUTERS`` in the settings, e.g.::

    DATABASE_ROUTERS = ['djanquiltdb.router.DynamicDbRouter']


QUILT_SESSIONS
--------------

The settings of the ``djanquiltdb.sessions`` session backend. See :doc:`/guides/sessions` for how to set it up.

``SESSION_MODEL``
    Required. The dotted path of the project's session model, a sharded subclass of ``BaseQuiltSession``.
``SHARD_SELECTOR_REGEX``
    The regular expression that a shard selector, a mapping value, has to match. Defaults to ``'[0-9]+'``.
``SESSION_KEY_DELIMITER``
    The text that separates the shard selector from the rest of a session key. Defaults to ``'K'``, and must be text
    that ``SHARD_SELECTOR_REGEX`` does not match.

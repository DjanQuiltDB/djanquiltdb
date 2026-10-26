=============
System checks
=============

``manage.py check`` runs these checks, and so do ``runserver`` and ``migrate``. They only run while ``djanquiltdb`` is
in ``INSTALLED_APPS``.

==================== ==================================================================================================
``djanquiltdb.W001`` ``django-pgtrigger`` is installed and ``PGTRIGGER_MIGRATIONS`` is off. pgtrigger then never writes
                     a trigger into a migration, so the shard-aware ``migrate`` cannot apply it to the public schema,
                     the template schema and every existing shard.
``djanquiltdb.W002`` ``django-pgtrigger`` is installed and ``PGTRIGGER_INSTALL_ON_MIGRATE`` is on. pgtrigger then
                     installs triggers directly into the database at the end of ``migrate``, so they only reach the
                     schema that the connection points at.
``djanquiltdb.E001`` A ``django-pgtrigger`` trigger on a public or mirrored model writes the table of a sharded model.
                     It writes the table of one shard only: the current shard, or the shard whose schema the name is
                     qualified with. A name without a schema fails outside a shard.
``djanquiltdb.E002`` A ``django-pgtrigger`` trigger on a public or sharded model writes the table of a mirrored model.
                     It writes the mirrored table of the node that it runs on, so the nodes no longer have the same
                     rows.
``djanquiltdb.W003`` A ``django-pgtrigger`` trigger on a mirrored model writes the table of a public model. It writes
                     the public table of the primary node only.
``djanquiltdb.E003`` The Shard model, or the mapping model, is not mirrored. DjanQuiltDB reads the shard registry and
                     the mapping table in the public schema of the node that the router picks, so every node needs a
                     copy of them. Decorate the model with ``@mirrored_model()``, and follow
                     :ref:`making_an_existing_model_mirrored` for a project with more than one node.
``djanquiltdb.E004`` A model has no sharding mode: no decorator and no ``OVERRIDE_SHARDING_MODE`` entry. Its table is
                     on the node that is named ``default`` only, and its queries go to the node that is active, so
                     they fail while another node is active. Give it a mode (see :doc:`/guides/sharding_modes`).
``djanquiltdb.W004`` A model is on a single node, through ``@single_node_unsharded_model()`` or
                     ``single_node_unsharded()``, which is strongly discouraged.
``djanquiltdb.E005`` A foreign key, one-to-one or many-to-many field goes to a model on a single node from a model that
                     is not on that node, or from a model on a single node to a sharded model or to a model on another
                     node.
``djanquiltdb.E006`` A model is on a single node that ``DATABASES`` does not have.
``djanquiltdb.E007`` A view of the URL configuration is async: an ``async def``, or a class-based view whose handlers
                     are async. DjanQuiltDB is not async-safe (see :doc:`/guides/async`).
``djanquiltdb.E008`` A middleware in ``MIDDLEWARE`` is async-only. A middleware that is also sync capable is not
                     reported: Django runs its hooks in the worker thread of the request. A middleware that does not
                     import is not checked: Django reports it when it loads the request handler.
``djanquiltdb.E009`` ``ASGI_APPLICATION`` is set, as Channels and Daphne have it. The consumers of the application run
                     on the event loop.
==================== ==================================================================================================

See :doc:`/database_objects/triggers` for why the settings of W001 and W002 matter, and for the triggers that E001,
E002 and W003 report. ``MUTE_PGTRIGGER_COMPATIBILITY_WARNING`` (see :doc:`/reference/settings`) silences W001 and W002.
The other checks have no setting of their own: ``SILENCED_SYSTEM_CHECKS`` is the only way to silence them. If you
keep models on a single node on purpose, silence W004 that way. ``manage.py check <app_label>`` reports E001, E002
and W003 for the triggers on the models of the apps that it names only, E003 for the Shard model and the mapping model
when they are models of those apps, and E004, W004, E005 and E006 for the models of those apps. It reports E007, E008 and
E009 for the whole project.

E007, E008 and E009 report async code, which DjanQuiltDB does not support. They do not mean that the code is wrong: async
code that enters shards only inside ``sync_to_async`` or ``database_sync_to_async`` is safe. Once your async code does
so, acknowledge each of them through ``SILENCED_SYSTEM_CHECKS``; :doc:`/guides/async` describes what is safe.

Plugins register their own checks, with their own prefix. The celery plugin documents ``djanquiltdb_plugin_celery.E001``
and ``E002`` on :doc:`its installation page </plugins/celery/installation>`. The pghistory plugin documents
``djanquiltdb_plugin_pghistory.W001`` to ``W005``, ``E002`` and ``E004`` on :doc:`its installation page
</plugins/pghistory/installation>`, and ``E001``, ``E003`` and ``E004`` on :doc:`its tracking page
</plugins/pghistory/tracking>`. The postgres-objects plugin documents ``djanquiltdb_plugin_postgres_objects.E001`` and
``W001`` on :doc:`its installation page </plugins/postgres-objects/installation>`.


Checks that depend on the database
----------------------------------

Some checks look at each database where a model's table lives. Django's own checks do this for the constraints and
indexes of a model, for the length of its column names and for the column types that it uses, and other packages do it
for settings such as the isolation level. They ask the routers whether the model's table lives in a database, through
``allow_migrate``.

When ``migrate`` runs its operations, a bare node name such as ``'default'`` stands for the node's ``public`` schema,
where a sharded model has no table. While the system checks run, the router answers a bare node name for the node as a
whole instead, so a sharded model is checked on every node, like a public or mirrored model. A name with a schema, such
as ``'default|template'``, keeps its usual answer.

Django runs these checks in ``migrate``, in the test runner, and in ``manage.py check --database``. ``migrate`` stops at
an error, so it also stops at an error for a sharded model, such as a constraint on a field that does not exist.

The router also answers this way when the caller took ``run_checks`` before DjanQuiltDB was ready. A Celery worker does
this as it starts. It runs the system checks without a database, so Django's checks that depend on the database do not
run there, and it ignores the messages of the checks. Only a check that raises an exception stops the worker.

A check that queries a table through the bare node name reaches the ``public`` schema, where a sharded model has no
table, so it fails for a sharded model.

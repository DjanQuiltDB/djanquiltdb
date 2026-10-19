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
==================== ==================================================================================================

See :doc:`/database_objects/triggers` for why the settings of W001 and W002 matter, and for the triggers that E001,
E002 and W003 report. ``MUTE_PGTRIGGER_COMPATIBILITY_WARNING`` (see :doc:`/reference/settings`) silences W001 and W002.
E001, E002 and W003 have no setting of their own: ``SILENCED_SYSTEM_CHECKS`` is the only way to silence them.
``manage.py check <app_label>`` reports them for the triggers on the models of the apps that it names only.

Plugins register their own checks, with their own prefix. The pghistory plugin documents
``djanquiltdb_plugin_pghistory.W001`` to ``W005``, ``E002`` and ``E004`` on :doc:`its installation page
</plugins/pghistory/installation>`, and ``E001``, ``E003`` and ``E004`` on :doc:`its tracking page
</plugins/pghistory/tracking>`.


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

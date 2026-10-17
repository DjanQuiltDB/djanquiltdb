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

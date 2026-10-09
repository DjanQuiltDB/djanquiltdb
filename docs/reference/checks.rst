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
==================== ==================================================================================================

See :doc:`/database_objects/triggers` for why both settings matter. ``MUTE_PGTRIGGER_COMPATIBILITY_WARNING`` (see
:doc:`/reference/settings`) silences both checks.

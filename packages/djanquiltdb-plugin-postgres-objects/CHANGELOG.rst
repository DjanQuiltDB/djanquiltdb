v 1.2.0 (2026-11-12)
--------------------
Added:
 * Support for Python 3.12 and 3.13.
 * Support for Python 3.15 (Django 6.1.3 or later).
 * Support for Django 5.2 (5.2.19 or later).

Altered:
 * Require django-postgres-objects 1.1, the first release that supports Python 3.12, 3.13 and 3.15.

v 1.1.0 (2026-10-27)
--------------------
Added:
 * System check ``djanquiltdb_plugin_postgres_objects.E001`` for a declared function or view without a placement
   decorator.
 * ``@single_node_unsharded_function(node)`` and ``@single_node_unsharded_view(node)``, which keep a declaration on one
   node, with the warning ``djanquiltdb_plugin_postgres_objects.W001``. Strongly discouraged, only intended for very
   specific and narrow use cases. Refer to the documentation.
 * System checks ``djanquiltdb_plugin_postgres_objects.E002`` for a declaration on a node that is not in ``DATABASES``,
   and ``djanquiltdb_plugin_postgres_objects.E003`` for single-node ``router_hints`` that name no node.

Altered:
 * Require djanquiltdb 4.5.

v 1.0.1 (2026-10-16)
--------------------
Altered:
 * Require djanquiltdb 4.2, for ``djanquiltdb.testing.pending_migration_changes`` in the test suite.

v 1.0.0 (2026-08-27)
--------------------
Added:
 * Initial release

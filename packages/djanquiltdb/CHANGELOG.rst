v 4.5.0 (2026-10-27)
--------------------
Added:
 * Migration operation ``CreateMissingMirroredTable``, which creates the table of a (newly) mirrored model on nodes
   which don't (yet) have it.
 * ``@single_node_unsharded_model(node)``, and ``single_node_unsharded(node)`` for ``OVERRIDE_SHARDING_MODE``, which
   keep a model on one node. Strongly discouraged, only intended for very specific and narrow use cases. Refer to the
   documentation.
 * System checks for unmirrored Shard/mapping models, models without sharding modes, and single-node models.
 * ``migrate`` now creates the template schema of every node that it migrates and that has none before it applies any
   migration, unless ``QUILT_DB['AUTO_CREATE_TEMPLATE_SCHEMA']`` is False.
 * Plugin API serving extended to ``djanquiltdb.base.<name>``.
 * Documentation notes on risks of using the library with async code, plus warnings and system checks about its
   compatibility gaps.

Removed:
 * The rebuild of a database clone with new migrations under ``--keepdb`` with ``--parallel``, added in 4.0.1. It
   works around a bug of Django (Django ticket 26822) and not, as previously thought, a bug in this library.

Altered:
 * **Warning:** ``migrate`` now creates the template schemas itself. A project that connects a ``pre_migrate``
   receiver of its own to create them, as the installation documentation suggested in 4.4.0 and earlier, should either
   remove that receiver or explicitly disable the library default by setting ``QUILT_DB['AUTO_CREATE_TEMPLATE_SCHEMA']``
   to False.
 * **Warning:** DjanQuiltDB now refuses async code where it can be easily detected. Pre-existing code might now trip
   these checks even though no behavioral change has occurred.
 * Fixed the plugin API leaking plugin-internal imports to ``djanquiltdb.decorators``.
 * Fixed the request middleware, quilt_admin, ``for_each_shard()``, ``purge_shard_data`` and saving an existing shard
   raising ``ProgrammingError`` inside ``use_shard(include_public=False)``.
 * Fixed the session backend raising ``DoesNotExist`` of the mapping model for a session key that holds no selector
   matching ``SHARD_SELECTOR_REGEX``, or a selector that the mapping table no longer has.
 * Fixed quilt_admin inconsistently dividing shard registry and mapping model reads between primary and active nodes.
 * Fixed ``move_shard_to_node`` and ``move_data_to_shard`` reading shard registry on a different node than where they
   write to it.
 * Fixed saving a shard with ``using`` looking up the shard in the registry in a different node than it would save to.
 * Fixed a query in a shard running in the public schema after ``transaction.savepoint_rollback()`` rolled back a
   failed query in that shard.
 * Speed improvement in reading and setting attributes of shard connections. No change in behavior.
 * ``use_shard()`` and ``use_shard_for()`` now acquire and release advisory locks of a shard in single statements.
 * Shard connections locking each statement now acquire and release advisory locks of a shard in single statements.
 * Inside a transaction, ``use_shard()`` and ``use_shard_for()`` no longer acquire locks or re-read a shard when an
   earlier ``use_shard()`` of the same shard already did so for that shard, nested or not. Safe because PostgreSQL holds
   those locks until the transaction ends. A rollback to a savepoint that was set before the locks were acquired
   releases them, and the next ``use_shard()`` re-acquires them.
 * Fixed ``use_shard()`` and ``use_shard_for()`` not checking that a shard is active when an earlier
   ``use_shard(active_only_schemas=False)`` of that shard held its locks.
 * Fixed creating a queryset in a shard building two shard connections.
 * Fixed ``loaddata`` creating and migrating the template schema for every fixture file, and migrating a template schema
   that it created twice.
 * Fixed ``loaddata`` ignoring ``--exclude`` and the router for fixtures with ``_schema`` entries.
 * Fixed ``loaddata`` looking up natural keys on the default node, and failing on a natural key that names a later
   entry.
 * Fixed ``loaddata`` failing on JSON Lines fixtures and on fixtures read from stdin.
 * ``loaddata`` now saves the entries of a fixture in the order of the fixture, and its output is the line of Django's
   ``loaddata``, with the counts of all fixtures.
 * Fixed a statement that fails inside a savepoint closing the database connection, which made the rest of the
   enclosing transaction fail with "the connection is closed". Outside a transaction, a connection that is usable again
   after an error is checked once instead of on every cursor.
 * Fixed a statement that fails on the node inside a savepoint of a shard, such as a write to a mirrored model in an
   inner ``transaction.atomic()``, ending the whole transaction instead of rolling back to the savepoint.
 * Fixed ``use_shard()`` and ``use_shard_for()`` entering the old node or schema of a shard that a move command moved on
   the same connection, inside an entered ``use_shard()`` of that shard or later in its transaction.
 * Fixed a quilt_admin session override of a shard or a mapping value that no longer exists breaking every admin page.
 * Fixed a subclass of ``BaseUseShardMiddleware`` or ``BaseUseShardForMiddleware`` raising ``DoesNotExist`` for a shard
   or a mapping value that no longer exists.

v 4.4.0 (2026-10-22)
--------------------
Added:
 * Celery tasks that run in a shard, installed through the ``djanquiltdb[celery]`` extra.

Altered:
 * Fixed the session store still using cached ``QUILT_SESSIONS`` values even after settings changed.
 * Fixed repeated unnecessary re-imports of the shard class, the mapping model and the database creation class.
 * Fixed a mapping value of 0 being ignored by ``ShardOptions.use()`` and by the advisory lock of ``use_shard_for()``.
 * Fixed ``use_shard()`` and ``use_shard_for()`` entering a shard that ``move_shard_to_node`` or ``move_data_to_shard``
   put in maintenance or moved during the wait for the lock of the shard.
 * Fixed ``use_shard_for()`` with a model instance locking the mapping value under the display string of the instance.
 * Fixed ``move_shard_to_node`` locking each mapping under the primary key of the row in the mapping table.
 * Fixed ``ShardOptions.use()`` raising ``ProgrammingError`` inside ``use_shard(include_public=False)``.
 * Fixed ``get_shard_for()`` and ``ShardOptions.from_shard(check_active_mapping_values=True)`` raising
   ``ProgrammingError`` for a mapping model with ``route_to_primary_db`` set to False.
 * Fixed a function decorated with ``use_shard()`` leaving the lock of the shard held and the shard active when the
   function calls itself, or when two threads call it at once. Entering a ``use_shard()`` that is entered already
   now raises ``RuntimeError``.
 * Fixed ``request.user`` raising ``ValueError`` on every read with ``ShardIdAdminOverrideMiddleware``, while the quilt
   admin has a different shard selected than the user's.
 * Fixed the ``StateException`` message of ``get_shard_for()`` and ``use_shard_for()`` for a shard that is not active
   showing the state of the mapping instead of the shard.
 * Entering and leaving a shard no longer sets the ``search_path`` to take or release its advisory locks, to save
   queries. Trade-off: entering a shard whose schema does not exist no longer raises when the shard is entered; the
   first query in the shard raises ``IntegrityError`` instead.
 * ``use_shard()`` takes no new locks when an entered ``use_shard()`` already holds the locks of the same shard for the
   session.
 * ``ShardOptions.use()`` no longer does a fresh shard lookup when an entered ``use_shard()`` already holds the locks of
   the same shard for the session.
 * Sharded model methods and ``use_shard()`` no longer build connections that they do not use.
 * Test coverage for profiling how often ``migrate`` renders the model states when
   ``QUILT_DB['SHARED_TEST_MIGRATION_STATES']`` is True. No change in behavior.

v 4.3.0 (2026-10-19)
--------------------
Added:
 * System checks for django-pgtrigger triggers that write a table in another sharding mode.
 * Plugin API serving extended to ``djanquiltdb.utils.<name>``.

Altered:
 * ``move_data_to_shard`` and ``purge_shard_data`` no longer fire django-pgtrigger triggers for the rows that they copy
   and delete.
 * Fixed ``djanquiltdb.testing.pending_migration_changes`` missing django-pgtrigger triggers that have no migration.
 * Fixed system checks that depend on the database (including Django and other libraries) skipping sharded models.

v 4.2.0 (2026-10-16)
--------------------
Added:
 * Compatibility decorators for ``django-pghistory``, installed through the ``djanquiltdb[pghistory]`` extra.
 * Plugin API serving extended from ``djanquiltdb.decorators.<name>`` to ``djanquiltdb.models.<name>``.
 * Plugin hooks to inject behavior into management commands.
 * ``djanquiltdb.utils.get_declared_sharding_mode`` helper method.

Altered:
 * Fixed ``dumpdata`` failing in every format when PyYAML is not installed.
 * Fixed entering a shard raising an error inside an execute wrapper.
 * Fixed the project failing to start when a sharded model has a class attribute that raises on access through the
   class.
 * Fixed the sharded queryset wrapper rejecting ``hints`` that are passed positionally.
 * Fixed a proxy model having no sharding mode when a mode is declared only for its concrete model.
 * Fixed ``move_shard_to_node`` firing user triggers of the target schema for the rows that it copies and retargets.
 * Fixed ``move_shard_to_node`` giving a public row that it copies to the target node a new primary key when the key
   is not an auto-increment column, such as a UUID, and failing for a public model whose primary key is not named
   ``id``.
 * Fixed ``move_shard_to_node`` setting the ``auto_now`` and ``auto_now_add`` fields of a public row that it copies to
   the target node to the time of the move.
 * Fixed ``clone_schema`` enabling every trigger of the clone, so a trigger that is disabled in the template was enabled
   in every new shard.
 * Fixed ``move_shard_to_node`` stopping on a foreign key without a database constraint that refers to a public row that
   the source node no longer has. The foreign key keeps its value.

v 4.1.0 (2026-10-09)
--------------------
Added:
 * The ``align_shards_with_template`` command, to repair various issues in shards cloned in previous versions related to
   sequences, primary key names and serial columns, which were all fixed in this version. Run it once after upgrading.

Altered:
 * Fixed ``move_shard_to_node``, ``move_data_to_shard`` and ``loaddata`` failing on, or resetting the wrong sequence of,
   a table whose sequence is not named after it (e.g. a table that was renamed after it was created).
 * Fixed ``move_shard_to_node`` leaving a moved shard's sequences that no model's primary key uses at the template's
   position.
 * Fixed cloned primary keys and identity sequences being named after their table instead of having the names they have
   on the template.
 * Fixed cloned serial sequences not being owned by their column (which would leave them orphaned upon dropping the
   table/column).
 * Fixed cloned sequences losing their data type, start, increment, bounds, cache size, cycling and persistence, and
   cloning failing on a sequence with a mixed-case name.
 * Fixed cloning failing when the schema cloned into already has a sequence with the same name as a template sequence
   whose position is outside that sequence's bounds.
 * Fixed cloning failing on a table or schema with a mixed-case name.
 * Fixed cloning failing on a template with a table that the cloning role cannot see, if that table has a column
   default, such as a serial column, or a constraint, index or trigger.
 * Fixed cloning overwriting the settings and position of an identity sequence or a serial column's sequence that the
   schema cloned into already has under the name of a template sequence.

v 4.0.1 (2026-10-05)
--------------------
Deprecated:
 * The ``class_method_use_shard_from_db_arg`` decorator, to be removed in 5.0. Use ``shard_aware_from_db`` instead.

Added:
 * Faster test database creation, opt-in through ``QUILT_DB['SHARED_TEST_MIGRATION_STATES'] = True``. See the migrations
   documentation for more information and potential risks.

Altered:
 * Fixed querying sharded models on Django 6.1.1.
 * Fixed a subclass of a sharded model getting instances of the sharded model instead of its own class when loading
   rows.
 * ``migrate`` now loads the migrations from disk and builds state once per run, rather than once per schema.
 * Fixed the verbose output of ``migrate`` naming every public and template schema ``default|public``.
 * Fixed ``--keepdb`` with ``--parallel`` running tests against outdated database clones from an earlier run. A clone is
   now rebuilt when the test database has new migrations.

v 4.0.0 (2026-08-27)
--------------------
Added:
 * Support for generated columns in sharded tables.
 * Support for sharded views.
 * Compatibility decorators for ``django-postgres-objects`` with ``djanquiltdb[postgres-objects]`` extra.
 * A system check warning when ``django-pgtrigger`` is installed with incompatible configuration.
 * Test coverage for ``django-pgtrigger`` triggers behaving correctly on sharded models. No change in behavior; this was
   already functional and manually confirmed, but not yet validated in the test suite.
 * Documentation for pre-existing but previously undocumented settings, the schema-cloning limitations, the data-moving
   commands' locking semantics, and ``move_shard_to_node`` general reference.
 * API reference in the documentation.
 * GitHub Actions for CI.
 * Documentation on readthedocs.io.
 * Support for Django 6.1 (no changes, just test coverage).

Removed:
 * The separate ``migrate_shards`` management command. Users should use standard ``migrate``.
 * The dead ``ShardingError`` exception module, two broken introspection hooks Django no longer calls, and the
   leftover empty bandit configuration.
 * Documentation naming classes and modules that no longer exist.

Altered:
 * Restructured the repository as a ``packages/`` monorepo with the PyPA src layout.
 * Cloned indexes, unique constraints and exclusion constraints now keep the names they have on the template
   instead of being renamed by PostgreSQL.
 * Fixed formatting and structural issues flagged by ruff
 * Fixed test coverage measurement on parallel test runs
 * Fixed test coverage broken in 3.1.1 but previously falsely ignored as flaky
 * Fixed two tests being flaky in non-parallel runs
 * Fixed a documentation error on passing ``sharding_mode`` to ``RunSQL``
 * Expanded documentation on trigger behavior and compatibility with ``django-pgtrigger`` in particular
 * Fixed generating new session keys with a custom ``SESSION_KEY_DELIMITER``.
 * Fixed renumbering of cloned identity sequences not named ``id``.
 * Fixed cloning of composite foreign keys aborting shard creation; ``ON DELETE``/``ON UPDATE`` actions and
   deferrability now carry over faithfully instead of being dropped and forced.
 * Fixed the template's search path leaking into the enclosing transaction when creating a shard under
   ``transaction.atomic()``, silently redirecting later reads and writes to the template schema.
 * Fixed advisory lock release masking errors and stranding locks inside atomic blocks.
 * Fixed ``loaddata`` and ``move_sharded_models`` ignoring the ``--database`` flag.
 * Fixed through-table sequences not being renumbered after moving a shard, silently losing the first many-to-many
   ``add()`` on the moved shard.
 * Fixed a crash retargeting relations after a shard move when a sharded table is empty.
 * Fixed ``move_data_to_shard`` firing delete signals while removing the moved rows.
 * Fixed ``flush`` and ``sqlflush`` handling each shard once per registry-holding node instead of once total.
 * Fixed a raw traceback when ``move_data_to_shard``'s external ``sort`` fails.
 * Fixed ``purge_shard_data --simple-collector`` refusing shards in maintenance.
 * Fixed mapped-value lookups for forbidden-copy models with relational natural keys hiding the intended error behind a
   ``DoesNotExist``.
 * Fixed connections being left inside open atomic blocks when one node's commit fails in a multi-node transaction; the
   remaining nodes now roll back.
 * Fixed Django's ``clearsessions`` failing on the sharded session backend; expired sessions are cleared on every active
   shard.
 * Fixed the admin shard selector class binding when ``quilt_admin`` modules are imported before the app registry is
   ready.
 * Fixed ``quilt_admin`` ignoring ``PRIMARY_DB_ALIAS`` after a failover.
 * Fixed ``quilt_admin``'s maintenance checks swallowing errors, silently disabling write protection; a stale shard
   override no longer breaks the admin.
 * Fixed function and trigger definitions being rewritten while cloning.
 * Fixed ``reset_sequence`` rewinding sequences advanced by concurrent writers.
 * Fixed maintenance states not being restored when entering maintenance fails midway.
 * Fixed the shard-table pre-flight check ignoring ``PRIMARY_DB_ALIAS`` after a failover.
 * ``move_shard_to_node`` now names the source schema it leaves behind and points at ``purge_schema`` for follow-up.
 * Schema-aware ``loaddata`` now accepts compressed fixtures.
 * Fixed the collector ordering multi-table inheritance the wrong way around, deleting parent rows before the child
   rows that point at them.
 * Fixed ``move_data_to_shard`` assuming every table has an ``id`` column, which failed the move outright on a
   multi-table inheritance child.
 * Fixed the test projects' models and migrations disagreeing on their state.

v 3.1.2 (2026-05-28)
--------------------
Added:
 * PEP 621 metadata

Altered:
 * Sped up attribute lookups in ``quilt_admin`` while switched to another shard
 * Fixed a bug where modifications in a shard-switched ``quilt_admin`` would raise ``IntegrityError`` on admin audit log

v 3.1.1 (2026-04-23)
--------------------
Altered:
 * Fixed a bug in the search path reset per transaction for PgBouncer transaction mode.

v 3.1.0 (2026-04-23)
--------------------
Altered:
 * Set search paths per transaction instead of per session for compatibility with PgBouncer transaction mode.
 * ``dumpdata`` management command now supports schema-aware fixture JSON/YAML files.
 * Django admin extension (``djanquiltdb.contrib.quilt_admin``) is now compatible with CSP-enforced applications.
 * Database serialization now includes sharded data.
 * Database-backed session backend (``djanquiltdb.sessions``) now cleanly clears a session if its signature is expired.

v 3.0.0 (2026-01-14)
--------------------
Altered:
 * Renamed from 'patchman-django-sharding' to 'djanquiltdb' due to shift in project stewardship.
 * Updated settings and models to match rename, breaking backwards compatibility.
 * Switched to ruff for linting and static checks, reformatted to match.
 * ``flushdb`` management command now allows flushing shards in maintenance state.
 * ``loaddata`` management command now supports schema-aware fixture JSON/YAML files (also used during tests).
 * Fixed a typo in the ``OrganizationShard`` model name in the example app.

Added:
 * Standardized implementation of database-backed session backend (``djanquiltdb.sessions``).
 * Django admin extension for switching shards (``djanquiltdb.contrib.quilt_admin``).
 * Explicit tests for different PostgreSQL versions, supporting PostgreSQL 17 and 18.
 * Cloning of functions and triggers from the template schema when creating a new shard, for compatibility with e.g.
   django-pgtrigger
 * Support for Django 6.0.
 * Support for Python 3.14.

Dropped:
 * Support for Django 3.2 and 4.0.
 * Support for Python 3.6 and 3.11.

v 2.0.0 (2023-11-20)
--------------------
Added:
 * support for Django 3.2 and 4.0
 * support for Python 3.11
Dropped:
 * support for Django 2.2

v 1.0.0 (2023-02-03)
--------------------
Altered:
 * Name change from 'django-sharding' to 'patchman-django-sharding' to make this library have a unique name.
 * ``move_shard_to_node`` management command is a lot faster in retargeting data.

v 0.6.3 (2022-12-15)
--------------------
Altered:
 * ``get_all_mirrored_models``, ``get_all_public_models``, and ``get_all_public_schema_models`` util functions now also
   accept ``include_auto_created`` and ``include_proxy`` arguments. Like ``get_all_sharded_models`` already had. They
   are ``False`` by default, and can be used to fetch a more complete set of models.

v 0.6.2 (2022-06-01)
--------------------
Altered:
 * ``move_shard_to_node`` management command to copy PUBLIC data if missing and retarget the copied data recursively.

Dropped:
 * Dropped support for Python 3.4 and 3.5. Lowest supported version of python now is 3.6.

v 0.6.1 (2021-06-10)
--------------------
Altered:
 * Routing for write queries to mirrored tables (if any) will be directed to the primary node if the current context
   does not do so already. This prevents your context to be destroyed if was correct already. (Example: a shard on the
   primary node is selected and you write to a mirrored table. In 0.6.0 this would scrap your shard context and only
   leave the ``public_schema`` in the ``search_path``.)

v 0.6.0 (2021-02-24)
--------------------
Added:
 * Dedicated view for the situation a node is down.
 * Primary connection as a setting. This is also the default connection the router uses. This means the 'default' name
   of a connection (Django stipulates) has no effect. It is just a name.
 * django.db.transaction gets monkey-patched to always start the transaction on the node that is active.
 * ``purge_schema`` management command to empty and remove a shard.

Altered:
 * Routing for write queries to mirrored tables (if any) will always lead to the primary node.
 * Routing for read/write queries to the mapping table (if any) will always lead to the primary node.

v 0.5.4 (2020-08-26)
--------------------
Added:
 * support for Django 2.2.
 * Ability to move a shard to a different node (``move_shard_to_node`` management command).
 * ``OVERRIDE_SHARDING_MODE`` to support removed models.

Altered:
 * Sharding mode decorators for models have been altered:

   * ``public_model``: Data lives on the public schema on the primary node only.
   * ``mirrored_model``: Data lives on the public schemas of all nodes.
   * ``sharded_model``: Data lives in a sharded schema on one of the nodes.

v 0.5.3 (2020-02-13)
--------------------
Added:
 * Allows SHARDED -> MIRRORED relations in migrations.

v 0.5.2 (2019-12-18)
--------------------
Altered:
 * Apply the ``shard_mode`` for model functions to proxy models as well.

v 0.5.1 (2019-12-17)
--------------------
Dropped:
 * Support for Django versions below 1.11.

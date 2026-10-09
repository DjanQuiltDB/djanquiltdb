v 4.1.0
-------
Added:
 * The `align_shards_with_template` command, to repair various issues in shards cloned in previous versions related to
   sequences, primary key names and serial columns, which were all fixed in this version. Run it once after upgrading.

Altered:
 * Fixed `move_shard_to_node`, `move_data_to_shard` and `loaddata` failing on, or resetting the wrong sequence of, a
   table whose sequence is not named after it (e.g. a table that was renamed after it was created).
 * Fixed `move_shard_to_node` leaving a moved shard's sequences that no model's primary key uses at the template's
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

v 4.0.1
-------
Deprecated:
 * The `class_method_use_shard_from_db_arg` decorator, to be removed in 5.0. Use `shard_aware_from_db` instead.

Added:
 * Faster test database creation, opt-in through `QUILT_DB['SHARED_TEST_MIGRATION_STATES'] = True`. See the migrations
   documentation for more information and potential risks.

Altered:
 * Fixed querying sharded models on Django 6.1.1.
 * Fixed a subclass of a sharded model getting instances of the sharded model instead of its own class when loading
   rows.
 * `migrate` now loads the migrations from disk and builds state once per run, rather than once per schema.
 * Fixed the verbose output of `migrate` naming every public and template schema `default|public`.
 * Fixed `--keepdb` with `--parallel` running tests against outdated database clones from an earlier run. A clone is
   now rebuilt when the test database has new migrations.

v 4.0.0
-------
Added:
 * Support for generated columns in sharded tables.
 * Support for sharded views.
 * Compatibility decorators for `django-postgres-objects` with `djanquiltdb[postgres-objects]` extra.
 * A system check warning when `django-pgtrigger` is installed with incompatible configuration.
 * Test coverage for `django-pgtrigger` triggers behaving correctly on sharded models. No change in behavior; this was
   already functional and manually confirmed, but not yet validated in the test suite.
 * Documentation for pre-existing but previously undocumented settings, the schema-cloning limitations, the data-moving
   commands' locking semantics, and `move_shard_to_node` general reference.
 * API reference in the documentation.
 * GitHub Actions for CI.
 * Documentation on readthedocs.io.
 * Support for Django 6.1 (no changes, just test coverage).

Removed:
 * The separate `migrate_shards` management command. Users should use standard `migrate`.
 * The dead `ShardingError` exception module, two broken introspection hooks Django no longer calls, and the
   leftover empty bandit configuration.
 * Documentation naming classes and modules that no longer exist.

Altered:
 * Restructured the repository as a `packages/` monorepo with the PyPA src layout.
 * Cloned indexes, unique constraints and exclusion constraints now keep the names they have on the template
   instead of being renamed by PostgreSQL.
 * Fixed formatting and structural issues flagged by ruff
 * Fixed test coverage measurement on parallel test runs
 * Fixed test coverage broken in 3.1.1 but previously falsely ignored as flaky
 * Fixed two tests being flaky in non-parallel runs
 * Fixed a documentation error on passing `sharding_mode` to `RunSQL`
 * Expanded documentation on trigger behavior and compatibility with `django-pgtrigger` in particular
 * Fixed generating new session keys with a custom `SESSION_KEY_DELIMITER`.
 * Fixed renumbering of cloned identity sequences not named `id`.
 * Fixed cloning of composite foreign keys aborting shard creation; `ON DELETE`/`ON UPDATE` actions and deferrability
   now carry over faithfully instead of being dropped and forced.
 * Fixed the template's search path leaking into the enclosing transaction when creating a shard under
   `transaction.atomic()`, silently redirecting later reads and writes to the template schema.
 * Fixed advisory lock release masking errors and stranding locks inside atomic blocks.
 * Fixed `loaddata` and `move_sharded_models` ignoring the `--database` flag.
 * Fixed through-table sequences not being renumbered after moving a shard, silently losing the first many-to-many
   `add()` on the moved shard.
 * Fixed a crash retargeting relations after a shard move when a sharded table is empty.
 * Fixed `move_data_to_shard` firing delete signals while removing the moved rows.
 * Fixed `flush` and `sqlflush` handling each shard once per registry-holding node instead of once total.
 * Fixed a raw traceback when `move_data_to_shard`'s external `sort` fails.
 * Fixed `purge_shard_data --simple-collector` refusing shards in maintenance.
 * Fixed mapped-value lookups for forbidden-copy models with relational natural keys hiding the intended error behind a
   `DoesNotExist`.
 * Fixed connections being left inside open atomic blocks when one node's commit fails in a multi-node transaction; the
   remaining nodes now roll back.
 * Fixed Django's `clearsessions` failing on the sharded session backend; expired sessions are cleared on every active
   shard.
 * Fixed the admin shard selector class binding when quilt_admin modules are imported before the app registry is ready.
 * Fixed quilt_admin ignoring `PRIMARY_DB_ALIAS` after a failover.
 * Fixed quilt_admin's maintenance checks swallowing errors, silently disabling write protection; a stale shard override
   no longer breaks the admin.
 * Fixed function and trigger definitions being rewritten while cloning.
 * Fixed `reset_sequence` rewinding sequences advanced by concurrent writers.
 * Fixed maintenance states not being restored when entering maintenance fails midway.
 * Fixed the shard-table pre-flight check ignoring `PRIMARY_DB_ALIAS` after a failover.
 * `move_shard_to_node` now names the source schema it leaves behind and points at `purge_schema` for follow-up.
 * Schema-aware `loaddata` now accepts compressed fixtures.
 * Fixed the collector ordering multi-table inheritance the wrong way around, deleting parent rows before the child
   rows that point at them.
 * Fixed `move_data_to_shard` assuming every table has an `id` column, which failed the move outright on a
   multi-table inheritance child.
 * Fixed the test projects' models and migrations disagreeing on their state.

v 3.1.2
-------
Added:
 * PEP 621 metadata

Altered:
 * Sped up attribute lookups in quilt_admin while switched to another shard
 * Fixed a bug where modifications in a shard-switched quilt_admin would raise IntegrityError on admin audit log

v 3.1.1
-------
Altered:
 * Fixed a bug in the search path reset per transaction for PgBouncer transaction mode.

v 3.1.0
-------
Altered:
 * Set search paths per transaction instead of per session for compatibility with PgBouncer transaction mode.
 * `dumpdata` management command now supports schema-aware fixture JSON/YAML files.
 * Django admin extension (djanquiltdb.contrib.quilt_admin) is now compatible with CSP-enforced applications.
 * Database serialization now includes sharded data.
 * Database-backed session backend (djanquiltdb.sessions) now cleanly clears a session if its signature is expired.

v 3.0.0
-------
Altered:
 * Renamed from 'patchman-django-sharding' to 'djanquiltdb' due to shift in project stewardship.
 * Updated settings and models to match rename, breaking backwards compatibility.
 * Switched to ruff for linting and static checks, reformatted to match.
 * `flushdb` management command now allows flushing shards in maintenance state.
 * `loaddata` management command now supports schema-aware fixture JSON/YAML files (also used during tests).
 * Fixed a typo in the `OrganizationShard` model name in the example app.

Added:
 * Standardized implementation of database-backed session backend (djanquiltdb.sessions).
 * Django admin extension for switching shards (djanquiltdb.contrib.quilt_admin).
 * Explicit tests for different PostgreSQL versions, supporting PostgreSQL 17 and 18.
 * Cloning of functions and triggers from the template schema when creating a new shard, for compatibility with e.g.
   django-pgtrigger
 * Support for Django 6.0.
 * Support for Python 3.14.

Dropped:
 * Support for Django 3.2 and 4.0.
 * Support for Python 3.6 and 3.11.

v 2.0.0
-------
Added:
 * support for Django 3.2 and 4.0
 * support for Python 3.11
Dropped:
 * support for Django 2.2

v 1.0.0
-------
Altered:
 * Name change from 'django-sharding' to 'patchman-django-sharding' to make this library have a unique name.
 * `move_shard_to_node` management command is a lot faster in retargeting data.

v 0.6.3
-------
Altered:
 * `get_all_mirrored_models`, `get_all_public_models`, and `get_all_public_schema_models` util functions now also accept `include_auto_created` and `include_proxy` arguments. Like `get_all_sharded_models` already had. They are `False` by default, and can be used to fetch a more complete set of models.

v 0.6.2
-------
Altered:
 * `move_shard_to_node` management command to copy PUBLIC data if missing and retarget the copied data recursively.

Dropped:
 * Dropped support for Python 3.4 and 3.5. Lowest supported version of python now is 3.6.

v 0.6.1
-------
Altered:
 * Routing for write queries to mirrored tables (if any) will be directed to the primary node if the current context does not do so already. This prevents your context to be destroyed if was correct already. (Example: a shard on the primary node is selected and you write to a mirrored table. In 0.6.0 this would scrap you shard context and only leave the public_schema in the search_path.)

v 0.6.0
-------
Added:
 * Dedicated view for the situation a node is down.
 * Primary connection as a setting. This is also the default connection the router use. This means the 'default' name of a connection (Django stipulates) has no effect. It is just a name.
 * django.db.transaction gets monkey-patched to always start the transaction on the node that is active.
 * `purge_schema` management command to empty and remove a shard.

Altered:
 * Routing for write queries to mirrored tables (if any) will always lead to the primary node.
 * Routing for read/write queries to the mapping table (if any) will always lead to the primary node.

v 0.5.4
-------
Added:
 * support for Django 2.2.
 * Ability to move a shard to a different node (`move_shard_to_node` management command).
 * OVERRIDE_SHARDING_MODE to support removed models.

Altered:
 * Sharding mode decorators for models have been altered:
    * `public_model`: Data lives on the public schema on the primary node only.
    * `mirrored_model`: Data lives on the public schemas of all nodes.
    * `sharded_model`: Data lives in a sharded schema on one of the nodes.

v 0.5.3
-------
Added:
 * Allows SHARDED -> MIRRORED relations in migrations.

v 0.5.2
-------
Altered:
 * Apply the shard_mode for model functions to proxy models as well.

v 0.5.1
-------
Dropped:
 * Support vor Django versions below 1.11.

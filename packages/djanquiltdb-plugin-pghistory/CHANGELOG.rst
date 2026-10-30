v 1.1.1 (unreleased)
--------------------
Altered:
 * Require djanquiltdb 4.6, whose ``djanquiltdb.natural_keys`` gives the public models of the plugin their natural
   key.

v 1.1.0 (2026-10-27)
--------------------
Added:
 * ``ShardRoutedRevert`` is served from ``djanquiltdb.base.pghistory``. Migrations generated in older versions which
   name ``djanquiltdb_plugin_pghistory.base`` keep working.
 * Tracking of models on a single node, whose events are stored on that node, with the event base ``SingleNodeEvent``.
 * System check ``djanquiltdb_plugin_pghistory.E005`` for an event model that cannot reach a ``Context`` table on a
   single node.

Altered:
 * Require djanquiltdb 4.5.

v 1.0.1 (2026-10-19)
--------------------
Altered:
 * ``move_data_to_shard`` and ``purge_shard_data`` no longer need plugin hooks to disable triggers as of DjanQuiltDB
   4.3.0.

v 1.0.0 (2026-10-16)
--------------------
Added:
 * Initial release

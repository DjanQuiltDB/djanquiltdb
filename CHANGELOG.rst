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


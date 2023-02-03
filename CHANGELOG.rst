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


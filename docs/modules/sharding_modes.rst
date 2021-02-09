==============
Sharding modes
==============

To define which table ends up in which schema we offer three decorators designed to be placed up on a model definition.
The decorator also influences how its queries get routed

* **sharded_model** This model will have its table migrates to the sharded schemas of all nodes. Queries will be
  routed according to the prevailing sharding context.

* **public_model**  This model will have its table migrates to the public schemas of only the default node. Queries
  will be routed according to the prevailing sharding context.

* **mirrored_model** This model will have its table migrates to the public schemas of all nodes. The write queries
  for this model will be routed to the primary node. The read queries will be routed according to the prevailing
  sharding context.

Special decorator:

* **shard_mapping_model(mapping_field='user_id')** The mapping table is optional. When defined it allows usages
  of helper functions to select shards based on the mapping values. Specifically the field designated as mapping field
  in the decorator.

`More info <decorators.html>`__  on decorators and how to use them.



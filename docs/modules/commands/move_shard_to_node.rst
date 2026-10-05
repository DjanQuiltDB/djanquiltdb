==================
Move Shard To Node
==================

Introduction
============

Shards live on nodes, and sometimes a shard has to move: a node fills up, a tenant outgrows its neighbours, or load
wants rebalancing. ``move_shard_to_node`` relocates one whole shard - schema, data, views and all - to another node,
and repoints the shard's registry row when everything has arrived.

Example: ``move_shard_to_node --source-shard-alias earth --target-node-alias node_2``

Steps
-----
1. Acquire an exclusive advisory lock on the shard (and on each of its mapping objects, if a mapping model is
   configured), and put them into MAINTENANCE. The exclusive lock waits for in-flight requests, which hold shared
   locks for the length of their transactions, and blocks new ones.

2. Inside one transaction spanning both nodes: create the schema on the target node by cloning its template, copy
   every table's rows over, retarget relations that point at PUBLIC models (whose ids may differ per node), reset the
   sequences, and repopulate the materialized views the source keeps populated.

3. On success, point the shard's registry row at the target node and restore the shard and mapping objects to their
   previous state. On failure the transaction rolls back, the states are restored, and the command can simply be run
   again - including when the failure happened while entering maintenance.

What is left behind
-------------------
A successful move does not delete anything: the source schema with the full data set remains on the old node, and the
closing output names it. Verify the move, then remove that schema with the ``purge_schema`` command. Until it is
gone, the shard cannot move back to that node - the command refuses a target node that already holds a schema by that
name - and the leftover is a stale copy of tenant data, which retention policies may care about.

Options
-------

``--source-shard-alias``
~~~~~~~~~~~~~~~~~~~~~~~~
The alias of the shard to move.

``--target-node-alias``
~~~~~~~~~~~~~~~~~~~~~~~
The connection name of the node that will receive the shard.

``--batch-size``
~~~~~~~~~~~~~~~~
How many rows the retargeting step fetches per batch. The default is 10000.

``--quiet``
~~~~~~~~~~~
Silence the progress output.

``--no-input``
~~~~~~~~~~~~~~
Skip the confirmation prompt.

Operational notes
-----------------
The copy buffers each table's export in memory, so peak memory use is proportional to the shard's largest table.
Retargeting relations to PUBLIC models requires those models to declare natural keys (``unique_together`` plus a
``get_by_natural_key`` manager method); a PUBLIC model that forbids copying (``@public_model(allow_copy=False)``)
stops the move when the target node misses one of its rows.

The sequence reset looks up each model's sequence through its auto-incrementing primary key column, so it also works for
a table that was renamed after it was created. If a model's auto-incrementing column has no sequence, the move stops
with an error that names the column. It stops before any sequence is reset.

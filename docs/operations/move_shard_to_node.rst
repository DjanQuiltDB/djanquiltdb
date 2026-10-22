==================
Move Shard To Node
==================

Introduction
============

Shards live on nodes, and sometimes a shard has to move: a node fills up, a tenant outgrows its neighbours, or load
wants rebalancing. ``move_shard_to_node`` relocates one whole shard - schema, data, views and all - to another node,
and repoints the shard's registry row when everything has arrived.

Command usage
=============

Steps
-----
1. Acquire an exclusive advisory lock on the shard (and on each of its mapping objects, if a mapping model is
   configured), and put them into MAINTENANCE. The exclusive lock waits for in-flight requests, which hold shared
   locks for the length of their transactions, and blocks new ones.

2. Inside one transaction spanning both nodes: create the schema on the target node by cloning its template, copy
   every table's rows over, retarget relations that point at PUBLIC models (whose ids may differ per node), move every
   sequence to its position on the source, reset the sequences of the models, and repopulate the materialized views the
   source keeps populated. The copy and the retargeting run with the user triggers of the target schema disabled.

3. On success, point the shard's registry row at the target node and restore the shard and mapping objects to their
   previous state. On failure the transaction rolls back, the states are restored, and the command can simply be run
   again - including when the failure happened while entering maintenance.

What is left behind
-------------------
A successful move does not delete anything: the source schema with the full data set remains on the old node, and the
closing output names it. Verify the move, then remove that schema with the ``purge_schema`` command. Until it is
gone, the shard cannot move back to that node - the command refuses a target node that already holds a schema by that
name - and the leftover is a stale copy of tenant data, which retention policies may care about.

Replication lag
---------------
The command saves the shard as in maintenance when it starts, and with the target node and its previous state when it
ends. The Shard model is mirrored, so both saves go to the primary node, and the replication of the project copies them
to the other nodes later. A process on another node reads the shard from the copy of the registry on that node:

* While that copy has the shard in maintenance, entering the shard raises ``StateException``, and a task of the celery
  plugin tries again later.
* While that copy is behind by longer than the whole move, it still has the shard on the source node and active. The
  process then enters the source schema, which keeps its rows until ``purge_schema`` removes it, and what it writes there
  is not in the moved shard.

Keep the lag of the replication well below the time that a move takes, and check that the registry of every node has
the target node before you remove the source schema.

Options
-------

.. code-block:: console

  manage.py move_shard_to_node --source-shard-alias earth --target-node-alias node_2

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
stops the move when the target node misses one of its rows. A row that the move copies gets a new primary key on the
target node when the database generates the key, such as an auto-incrementing id. Any other primary key, such as a UUID
that the model generates, is copied with the row.

A foreign key without a database constraint (``db_constraint=False``) can refer to a public row that the source node
no longer has. There is no natural key to retarget such a foreign key by, so it keeps its value, which is the id that
the row had on the source node. The target node may use that id for another row. A foreign key with a database
constraint whose row the source node does not have stops the move.

Every sequence of the target, including sequences that no model uses, is first moved forward to the position of the
matching sequence on the source. The matching sequence is the one owned by the same column. If there is none, it is the
sequence with the same name that no column owns. For a source sequence that no column owns, it can also be the sequence
with the same name that a serial column owns: on a shard cloned before 4.1.0, serial columns own no sequence, so only
the name links its sequences to those of a newer clone. A target sequence that the target's template has already
advanced further keeps its own position, so that the migration table, whose rows come from the target's template, can
still take new rows. The sequence reset then looks up each model's sequence through its auto-incrementing primary key
column, so it also works for a table that was renamed after it was created. If a model's auto-incrementing column has
no sequence, or the shard has no such table or column, the move stops with an error that names the column. It stops
before any sequence is reset.

The rows went through the triggers of the source schema when they were written, so the triggers of the target schema
do not fire for the rows that the move copies or retargets. A trigger that records history, for example, would
otherwise record every row a second time. Foreign keys are still checked, and the triggers are enabled again before the
move ends. A row of a public model that the retargeting copies to the target node is a new row there, so the triggers
of its table do fire.

Plugins can hook into the move, inside its transaction and once the schema exists on the target node:
``move_shard_to_node.move_context`` wraps the rest of the move, and ``pre_copy`` and ``post_copy`` run around the copy
and the retargeting. See :ref:`command_hooks`.

Reading the source's positions requires ``SELECT`` on every sequence of the source shard. If the role can use a
sequence but not read it, the move stops with a permission error, the same as cloning a schema does.

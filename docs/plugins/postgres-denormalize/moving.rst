============================
Moving shards and their data
============================

DjanQuiltDB's commands that move data keep the triggers from firing for the rows that they copy (see
:doc:`/operations/move_shard_to_node` and :doc:`/operations/move_data_to_shard`), so the copies arrive as they left. A
move does not repair a copy that was out of date in the source shard: it stays out of date in the target shard. To find
and correct such copies, run :doc:`the helpers <repairing>` on the target after the move:

.. code-block:: python

    from djanquiltdb.utils.postgres_denormalize import repair_shard, stale_rows

    if stale_rows(target_shard):
        repair_shard(target_shard)

A move does not change the rows that the copies are read from, so a copy that was correct before the move is still
correct after it.

============================
Moving shards and their data
============================

The events of a sharded model are rows in the same shard as the rows that they describe, so DjanQuiltDB's commands
that move data move the events too. A move only changes where the rows are stored, so it records no events for the rows
that it moves.

.. warning::

    To record no events, the commands keep triggers from firing for the rows that they move or purge, and not only the
    triggers of pghistory. ``move_shard_to_node`` disables every user trigger of the new schema while it copies and
    retargets the rows, so no trigger of the project runs for them either. ``move_data_to_shard`` and
    ``purge_shard_data`` run inside ``pgtrigger.ignore()``, which ignores every trigger that django-pgtrigger installs,
    the project's own included. A trigger that was created in another way, for example with ``RunSQL``, still fires for
    the copy and the delete.
    Check what the triggers of the project do before you move a shard or its data, for example whether one keeps a
    counter or a search column up to date, or rejects some writes.

Moving a shard to another node
==============================

:doc:`move_shard_to_node </operations/move_shard_to_node>` copies every table of the shard, the event tables included,
into a new schema on the other node. It runs with the triggers of that schema disabled, so the events keep their
``pgh_id``, context and creation time, and no event is recorded for the rows that the move writes. The triggers fire
again once the move is done.

The move retargets each foreign key to a public model to the row with the same natural key on the other node. That
includes the snapshot of such a foreign key in an event, so an event keeps describing the same public row. A snapshot
of a public row that was deleted after the event was recorded has no natural key to retarget by, so it keeps the id that
the row had on the source node. The other node may use that id for another row.

A public row that the other node does not have yet is copied there, if its model allows it. That row is new on the other
node, so the triggers of its table fire, and a public model that is tracked with ``track_public()`` records an insert
event for it there. When the model has ``auto_now`` or ``auto_now_add`` fields, the move then writes the values of the
source row to those fields with an update, so such a model also records an update event.

With a normalized context and a public ``Context`` table, the events of a moved shard keep their context: a context
that the other node does not have yet is copied there with the same id and timestamps.

Moving data to another shard
============================

:doc:`move_data_to_shard </operations/move_data_to_shard>` finds the rows of one root object with a collector that
follows every foreign key to a row that it found. An event has two kinds of foreign keys to the rows: ``pgh_obj``, to
the row that it describes, and a snapshot of each foreign key of the tracked model. So the collector finds the events of
each moved row, and also the events of other rows whose snapshot points at a moved row. The plugin takes part in the
move:

* once the rows are collected, the plugin keeps only the events whose ``pgh_obj`` is a moved row. The history of every
  other row stays with that row;
* the rows and their events are copied and deleted inside ``pgtrigger.ignore()``, so no event is recorded for the
  copy in the target shard or for the delete in the source shard;
* after the moved rows are deleted from the source shard, the plugin deletes their events there too. The relation from
  an event to its row has ``on_delete=DO_NOTHING``, so Django's collector, which deletes the rows, does not find the
  events.

Some events do not move:

* an event of a row that was deleted before the move, also when a snapshot of the event points at a moved row;
* an event of a row that belongs to another root object now, also when it belonged to the moved root object when the
  event was recorded;
* a context in a sharded ``Context`` table. An event of the moved rows keeps the id of its context, but the row of the
  context stays in the source shard.

An event model of a model that is tracked with ``obj_field=None``, such as a tracked many-to-many through model, has
no ``pgh_obj``. Its snapshots are the only link to the rows, so each of its events moves when a snapshot points at a
moved row.

The command does not change the ids of the rows that it moves, and that includes the ``pgh_id`` of the events. As for
every other model, rows that already have one of those ids in the target shard make the move fail.

Purging data
============

:doc:`purge_shard_data </operations/purge_shard_data>` deletes the rows of one root object from a shard, usually from
the source shard of a ``move_data_to_shard --no-delete``. The plugin takes part in the purge as it does in a move:

* once the rows are collected, the plugin keeps only the events whose ``pgh_obj`` is a purged row;
* the rows are deleted inside ``pgtrigger.ignore()``, so no delete event is recorded for them;
* after the rows are deleted, the plugin deletes the events of the purged rows. Those are the events whose ``pgh_obj``
  is a purged row, and for an event model without ``pgh_obj``, the events with a snapshot that points at a purged row.

When the event model has a ``pgh_obj``, the events of a row that was deleted before the purge, or that belongs to
another root object, stay in the shard.

================
Repairing copies
================

The triggers keep every copy up to date for every write that PostgreSQL sees. A write that the triggers do not see can
leave copies out of date: a ``DISABLE TRIGGER``, a dump that is restored without its triggers, or the replication of a
mirrored row to another node. django-postgres-denormalize finds such rows with ``drift_condition()`` and corrects them
with ``backfill_denormalized()``, but both work on the table of the active shard only. The plugin's helpers enter each
shard for you:

.. code-block:: python

    from djanquiltdb.utils.postgres_denormalize import repair_every_shard, repair_shard, stale_rows

    stale_rows(shard)  # {<class 'tenancy.models.Task'>: 3, <class 'tenancy.models.Comment'>: 12}
    repair_shard(shard)

    skipped = repair_every_shard()

``stale_rows(shard)`` counts, for each model, the rows whose copies differ from their lookup paths. A model whose copies
are all correct is left out, so an empty dict means that the shard needs no repair.

``repair_shard(shard)`` corrects the copies in one shard, in one transaction on the shard's connection. A row whose
copies are correct is not written, but each table is read in full to find the rows that differ.

``repair_every_shard()`` repairs every active shard, on every node, each in its own transaction, so a failure in one
shard leaves the shards before it repaired. A shard that is being created, moved or purged cannot be entered, so
``repair_every_shard()`` skips it, and returns the shards that it skipped. Run it again for those once they are active.

All three helpers take a list of models. By default, they cover ``sharded_leaves()``: every sharded model that declares
a denormalized field.


The shard of one root object
----------------------------

``use_shard_for()`` enters the shard of a root object, such as a tenant, by its mapping value. To repair that shard,
pass the shard itself:

.. code-block:: python

    from djanquiltdb.utils import get_shard_for

    repair_shard(get_shard_for('acme'))

``backfill_denormalized()`` corrects a whole table, so the repair covers every root object in that shard, and not only
this tenant. Inside a block of your own, you can also call django-postgres-denormalize's functions directly. Enter
the shard first, and then open the transaction, so that it runs on the shard's connection:

.. code-block:: python

    from django.db import transaction
    from djanquiltdb.utils import use_shard_for
    from postgres_denormalize.operations import backfill_denormalized

    with use_shard_for('acme'), transaction.atomic():
        backfill_denormalized(Task)

============
Installation
============

Install through DjanQuiltDB's extra::

    pip install djanquiltdb[postgres-denormalize]

The plugin itself needs no setup: DjanQuiltDB finds it through its entry point as soon as it is installed. The library
that it works with still needs its own setup. ``postgres_denormalize`` is a Django app, and goes in ``INSTALLED_APPS``
next to ``djanquiltdb`` and the ``pgtrigger`` app that it builds on::

    INSTALLED_APPS = (
        ...
        'djanquiltdb',
        'pgtrigger',
        'postgres_denormalize',
        ...
    )


Triggers in every schema
------------------------

django-postgres-denormalize keeps the copies up to date with django-pgtrigger triggers, so everything that :doc:`the
trigger documentation </database_objects/triggers>` says applies. The triggers reach the template and every shard
through migrations, so leave ``PGTRIGGER_MIGRATIONS`` on and ``PGTRIGGER_INSTALL_ON_MIGRATE`` off. DjanQuiltDB checks
both settings (``djanquiltdb.W001`` and ``djanquiltdb.W002``).

A trigger function names its tables without a schema, so each shard's triggers read and write that shard's tables. Keep
the whole lookup path of a copy among sharded models, and do not copy a column of a mirrored or public model:
:doc:`pitfalls` describes why.


SERIALIZABLE on every node
--------------------------

Below SERIALIZABLE, a write to a leaf row that runs at the same time as an update of its ancestor row can commit a copy
that differs from its lookup path. django-postgres-denormalize's system check ``postgres_denormalize.W303`` reports a
database that stores copies and runs below SERIALIZABLE, or ``postgres_denormalize.E303`` when ``ISOLATION_CHECK_LEVEL``
is ``'error'``. It asks the routers which databases store a leaf. While the system checks run, DjanQuiltDB's router
answers a node name for the node as a whole (see :doc:`/reference/checks`), so the check reports each node whose shards
store copies.

Set the isolation level in the ``OPTIONS`` of every node that stores copies, and retry the transactions that fail with a
serialization error:

.. code-block:: python

    from django.db.backends.postgresql.psycopg_any import IsolationLevel

    DATABASES = {
        'default': {
            ...,
            'OPTIONS': {'isolation_level': IsolationLevel.SERIALIZABLE},
        },
        'other': {
            ...,
            'OPTIONS': {'isolation_level': IsolationLevel.SERIALIZABLE},
        },
    }

If a lower level is a deliberate choice, set ``ISOLATION_CHECK_LEVEL`` to ``'off'``, and find the copies that differ
with ``stale_rows()`` (see :doc:`repairing`).


Backfilling in a migration
--------------------------

``BackfillDenormalized`` gives the routers the model's name as the ``model_name`` hint, so DjanQuiltDB's router reads
the sharding mode from the model, and the backfill of a sharded model runs in the template and in every shard without
any hints:

.. code-block:: python

    from postgres_denormalize.operations import BackfillDenormalized

    operations = [
        ...,
        BackfillDenormalized('task'),
    ]

``migrate`` runs the operation once for each schema, and the operation writes to the schema that ``migrate`` is on.

When the migration names the model with its app label, such as ``'tenancy.task'``, the operation gives no
``model_name``, because the router would pair it with the migration's own app label. Give the sharding mode yourself in
that case:

.. code-block:: python

    from djanquiltdb import ShardingMode

    BackfillDenormalized('tenancy.task', hints={'sharding_mode': ShardingMode.SHARDED})

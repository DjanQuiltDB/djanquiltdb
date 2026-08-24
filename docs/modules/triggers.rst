========
Triggers
========

A trigger is a database object rather than model state, so it is created through a hinted ``RunSQL`` in a migration,
the same way a view is (see :doc:`database_views`). What needs explaining on its own is how a trigger reaches a new
shard.

A shard is not migrated from empty, it is cloned from the template schema, and a trigger only keeps working in that
clone if both the relation it is attached to and the function it executes are re-pointed at the new schema.


How triggers are cloned
-----------------------

Cloning the triggers is the last step of cloning the template. It runs after the tables have been created and their
rows copied, and after the views have been recreated: a trigger needs its relation to exist, and going last means no
trigger of the new shard fires while the template's rows are being copied in.

Every relation in the template is covered: ordinary tables, partitioned tables, and views, the latter so that an
``INSTEAD OF`` trigger making a non-auto-updatable view writable comes along as well. Each trigger is read back with
``pg_get_triggerdef()`` while only the template schema is visible on the search path, so the relation in the ``ON``
clause and a trigger function living in the template print unqualified, while a function living elsewhere - typically
in the public schema - prints with its schema attached. Executing those statements with the new shard first on the
path then binds the unqualified names to the shard's own copies and leaves the qualified ones alone. Internal
triggers are skipped, since the ones Postgres creates to implement foreign keys arrive with the constraints
themselves.

This is the same visibility principle the rest of the clone uses for expressions and views, and like there, the
definition text is never rewritten: a ``WHEN`` clause or trigger argument holding a string literal survives
byte-identical, whatever it contains. The function placement that falls out is the rule :doc:`database_functions`
describes, and it holds for a trigger function as it does for any other: sharded ones are per shard, public ones
shared.

Triggers need this pass of their own because ``CREATE TABLE ... (LIKE ... INCLUDING ALL)``, which is what carries the
columns, defaults, indexes and constraints into the clone, does not copy them. For what cloning deliberately does not
carry, see :ref:`cloning_limitations`.


Using django-pgtrigger
----------------------

``django-pgtrigger`` works with DjanQuiltDB as long as it is used in migration mode, which is its default. It has been
tested at version 4.17.0.

In migration mode every trigger is written into a migration, so DjanQuiltDB's shard-aware ``migrate`` applies it to the
public schema, to the template schema and to every existing shard, exactly as it does for any other operation. Shards
created afterwards inherit their triggers from the template through the clone described above. The operations name the
model they belong to, so the router reads the sharding mode from that model and no ``sharding_mode`` hint is needed,
unlike a hand-written ``RunSQL``.

Leave both of its settings at their defaults:

.. code-block:: python

    PGTRIGGER_MIGRATIONS = True
    PGTRIGGER_INSTALL_ON_MIGRATE = False

pgtrigger creates the function backing a trigger unqualified, so it is created in whichever schema the connection is
pointed at and ends up in the template and in each shard, next to the tables it belongs to. The clone then binds each
cloned trigger to the shard's own copy of it. The small helper behind ``pgtrigger.ignore`` is qualified into the public
schema instead, so it is created once and shared by all of them.


Why the management commands do not work
---------------------------------------

Do not use ``pgtrigger install``, ``pgtrigger uninstall``, ``pgtrigger enable``, ``pgtrigger disable`` or
``pgtrigger prune``. They change the database directly instead of through a migration, and so never pass the shard-aware
migration manager that spreads a change across the schemas. Each acts on whatever single schema the connection's search
path points at, leaving the template and every other shard as they were, and any shard created afterwards is then cloned
from a template that never saw the change.

``pgtrigger ls`` is misleading rather than harmful, for the same reason: it reports on one schema, so it tells you
nothing about whether the others agree with it.

Add, remove or disable a trigger in a migration instead, and it lands everywhere it belongs.

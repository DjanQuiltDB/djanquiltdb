========
Tracking
========

The plugin's ``track()`` works like ``pghistory.track()``. It has the same signature and the same behavior, with one
difference: pghistory bases the generated event model on ``PGHISTORY_BASE_MODEL``, and the plugin chooses the base from
the sharding mode of the tracked model instead. The plugin's event bases inherit from the model that
``PGHISTORY_BASE_MODEL`` names, so that setting still applies. The events are then stored in the same place as the rows
that they describe:

* the events of a sharded model are stored inside the same shard;
* the events of a mirrored model are stored in a mirrored table, in the public schema of every node. The router sends
  each write of a mirrored model to the primary node, so the trigger writes the events there, and the replication that
  copies the tracked table to the other nodes has to copy the event table too;
* the events of a public model are stored in the public schema;
* the events of a model on a single node (see :doc:`/guides/sharding_modes`) are stored in the public schema of that
  node. The event model gets the node of the tracked model as the tracked model declares it, so the event model of a
  model on ``PRIMARY_NODE`` is on ``PRIMARY_NODE`` too.

To move a project from plain pghistory to the plugin, change the imports. The only requirement is that ``track()`` can
read the mode from the model when it runs, so the mode decorator goes below it::

    from djanquiltdb.decorators import pghistory, sharded_model


    @pghistory.track(exclude=['password'])
    @sharded_model()
    class User(models.Model):
        ...

The ``pghistory`` namespace above is served from ``djanquiltdb.decorators``, next to the model decorators. It is a
module, so the same code can also import the function by name::

    from djanquiltdb.decorators import sharded_model
    from djanquiltdb.decorators.pghistory import track


    @track(exclude=['password'])
    @sharded_model()
    class User(models.Model):
        ...

Neither way is preferred. ``@pghistory.track()`` looks like pghistory's own API. That keeps call sites familiar, but it
does not show that the tracking comes from a separate library through DjanQuiltDB. Importing the function by name does
show that. Choose one way for each project and use it consistently.

The tracking functions are deliberately not served directly from ``djanquiltdb.decorators``, unlike the placement
decorators of other plugins: the name ``track`` on its own says too little at a call site, and would sooner or later
collide with a name from another plugin.

Resolving the mode
==================

``track()`` reads the mode in the same way as the router, and also works for models that have no mode of their own:

* the marker of ``@sharded_model()``, ``@mirrored_model()``, ``@public_model()`` or
  ``@single_node_unsharded_model(node)``, when that decorator has already run;
* a ``QUILT_DB['OVERRIDE_SHARDING_MODE']`` entry, which takes precedence over the marker and also covers third-party
  models;
* a proxy model gets the mode of its concrete model, so a proxy of a third-party model is tracked like the model that it
  proxies;
* an auto-created model, such as a many-to-many through table, gets the mode of the model that caused its creation.

When none of these gives a mode, ``track()`` raises ``ImproperlyConfigured``.

The explicit tracking functions
===============================

``track_sharded()``, ``track_mirrored()`` and ``track_public()`` do not read the placement from the model. They
therefore work in either decorator order, and on models that have no declared mode::

    from djanquiltdb.decorators import pghistory


    @pghistory.track_sharded()
    class Comment(ThirdPartyComment):
        class Meta:
            proxy = True

Each of them uses its own event base as the default ``base_model``, and raises ``ImproperlyConfigured`` for a base of
another mode. Whenever the model has a mode, all the tracking functions check it against the base, so tracking a
mirrored model with ``track_sharded()`` raises ``ImproperlyConfigured``. When the mode decorator is above the tracking
decorator, the model has no mode yet when the tracking decorator runs, and the system check
``djanquiltdb_plugin_pghistory.E001`` reports the mismatch instead.

``create_event_model()``, the form of pghistory's API that is not a decorator, chooses its base in the same way.

Where the generated model is created
====================================

``app_label=`` generates the event model into another app, as in pghistory. The base still comes from the tracked model,
but migrations place the generated table according to what ``OVERRIDE_SHARDING_MODE`` says about the app that it is
generated into. If the settings give that app a different mode, the events would be written to a place where the base,
the queryset and the router do not expect them. The system check ``djanquiltdb_plugin_pghistory.E003`` reports this.

Custom base models
==================

``base_model=`` works as in pghistory, with one extra requirement: the base must inherit one of the plugin's event bases
(``ShardedEvent``, ``MirroredEvent``, ``PublicEvent``, ``SingleNodeEvent``), because those bases have the sharding mode
and the queryset::

    from djanquiltdb.decorators.pghistory import track
    from djanquiltdb.models.pghistory import ShardedEvent


    class AuditEvent(ShardedEvent):
        reviewed = models.BooleanField(null=True)

        class Meta:
            abstract = True


    @track(base_model=AuditEvent)
    @sharded_model()
    class Contract(models.Model):
        ...

As in pghistory, the class name of the base is part of the name of the generated model. Each of the plugin's own bases
is a class named ``Event``, which is why plain tracking generates a model named ``ContractEvent``, and the example above
generates ``ContractAuditevent``. Pass ``model_name=`` to choose a name explicitly.

A base that inherits ``SingleNodeEvent`` is only accepted for a model on a single node, because the event model gets
the node of the tracked model.

The database has to fill in an extra field on a base, because the trigger that writes the events only sets the snapshot
and ``pgh_*`` columns. The trigger does not apply a Django ``default``, so the column is ``NULL``, or the insert fails
when the column does not allow ``NULL``. Give the field a ``db_default``, or fill it in with a trigger, as
:doc:`customization` shows.

Public event models
===================

DjanQuiltDB requires every public model to define a natural key. The plugin's ``PublicEvent`` base uses the ``pgh_id``
of the event as the natural key, because an event has no other value that identifies it. DjanQuiltDB reads the natural
key from the first entry of ``unique_together``, so the plugin puts ``pgh_id`` there when the event model is created. It
only does so on the model's ``_meta``, so the migrations of the event model do not add a unique constraint next to its
primary key. A ``unique_together`` that a call site passes in ``meta`` follows ``pgh_id``, and is the only one that
the migrations create.

The ``pgh_id`` is both the natural key and the primary key of an event. This matters when you serialize events.
``dumpdata --natural-primary`` omits the primary key from each record, because the natural key is expected to find the
row again. Here the natural key *is* the primary key, so nothing is left to find the row with, and every load adds new
rows. Dump public event models without ``--natural-primary``. The records then include their ``pgh_id``, and loading
them again updates the rows in place.

Querying
========

Event models are queried like any other model in a sharded project. The plugin's bases use a queryset that keeps
pghistory's proxy fields working on Django 6, where the column aliasing of ``values()`` breaks the proxy-field CTE of
django-pghistory releases up to at least 3.9.

The queryset keeps three gaps of pghistory's CTE, which pghistory 3.9 has too. They only affect a query that selects
the proxy fields of an event model:

* when a tracked field has a ``db_column`` that is different from its name, the query fails with a database error that
  names the column;
* a proxy field that reads a key inside a key of the context metadata, such as ``pgh_context__baker__name``, returns
  the value JSON-encoded: ``'"ann"'`` and not ``'ann'``;
* the CTE renames the column of each proxy field and turns ``->`` into ``->>`` by replacing text in the whole SQL of
  the query. A key in the path of a proxy field that contains ``->``, or an underscore followed by the column of a
  proxy field, is changed too.

Aggregating across event models
-------------------------------

``pghistory.models.Events`` and its ``MiddlewareEvents`` proxy run each query as a ``UNION`` over the event tables that
they are told to span. That union is built as raw SQL and is not routed, and it names each table without a schema, so
the search path of the connection that runs it decides which rows it finds:

* inside :func:`~djanquiltdb.utils.use_shard`, it spans the event tables of that shard **and** the public ones. This is
  the only setup in which an aggregate across sharding modes works;
* the shard that it reads is the active shard, and not a shard that the query names;
* outside any shard, it can only span public and mirrored event models. Including a sharded event model raises a
  database error that names the missing table.

An aggregate over sharded events therefore only covers the active shard. To aggregate over the whole project, run the
aggregate once for each shard, usually with :func:`~djanquiltdb.utils.for_each_shard`.

The same applies to pghistory's admin. It is built on ``PGHISTORY_ADMIN_MODEL``, which is ``pghistory.Events`` by
default, so it runs this kind of query on every page. Public and mirrored event models show up wherever the admin is
used. A sharded event model only shows up when a shard is already active by the time the view runs.

:doc:`The admin shard switcher </contrib/admin_shard_switcher>` makes the chosen shard active for each admin request, so
pghistory's event pages list the events of that shard.

Statement-level triggers
========================

``level=pghistory.Statement``, at a call site or for the whole project through ``PGHISTORY_LEVEL``, works as in
pghistory, and the events are stored in the shard that has the rows.

The trigger SQL of statement-level tracking always calls pghistory's ``_pgh_attach_context()`` function, regardless of
whether the context is normalized or denormalized. At row level, the trigger calls it when the event model has a
foreign key to the ``Context`` table, so only row-level tracking with a denormalized context does not call it. The
``pghistory`` app's own migrations install the function, so it exists wherever :doc:`you placed that app
<installation>`: inside each shard when the app is sharded, and in the public schema when the app is public or
mirrored. Every search path ends with the public schema, so with a public app, the trigger of a model of every sharding
mode reaches the function. With a sharded app, only a shard can reach it, so a write to a public or mirrored model
whose trigger calls the function fails outside a shard, and the plugin reports ``djanquiltdb_plugin_pghistory.E004``.

Reverting and manual events
===========================

``Event.revert()`` works as in pghistory, but lets the router choose the connection: the write goes to the active shard
for a sharded model, and to the primary for a mirrored model. pghistory's own ``revert()`` writes to the ``default``
connection, which points at the public schema of a node and not at the shard that has the row. An explicit ``using=`` is
used as given.

``pghistory.create_event()`` does the same. pghistory writes a manual event through the ORM when its event model
records the context denormalized, and the router already chooses the connection then. When the event model has a
foreign key to the ``Context`` table, pghistory writes the event through its own SQL compiler on the ``default``
connection. The plugin replaces ``pghistory.create_event()`` when it is installed, so that without a ``using=``, the
event is written where the router writes the tracked row.

The plugin is installed when Django's app registry is ready. A module that imports the function by name before then,
with ``from pghistory import create_event``, such as a ``models.py``, keeps pghistory's own function. Call it as
``pghistory.create_event()``.

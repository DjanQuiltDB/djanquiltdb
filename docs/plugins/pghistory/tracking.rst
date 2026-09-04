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
* the events of a public model are stored in the public schema.

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

* the marker of ``@sharded_model()``, ``@mirrored_model()`` or ``@public_model()``, when that decorator has already run;
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
(``ShardedEvent``, ``MirroredEvent``, ``PublicEvent``), because those bases have the sharding mode and the queryset::

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

The database has to fill in an extra field on a base, because the trigger that writes the events only sets the snapshot
and ``pgh_*`` columns. The trigger does not apply a Django ``default``, so the column is ``NULL``, or the insert fails
when the column does not allow ``NULL``. Give the field a ``db_default``, or fill it in with a trigger, as
:doc:`customization` shows.

Public event models
===================

DjanQuiltDB requires every public model to define a natural key. The plugin's ``PublicEvent`` base answers with the
event's ``pgh_id``, and the tracking functions merge the ``unique_together`` backing it into the generated model's
meta whenever the call site does not bring its own.

Querying
========

Event models are queried like any other model in a sharded project: inside :func:`~djanquiltdb.utils.use_shard` for
sharded events, plainly for mirrored and public ones. The plugin's bases carry a queryset that resolves the
argument conflict between pghistory's ``EventQuerySet`` and DjanQuiltDB's sharded queryset wrapping, and keeps
pghistory's proxy fields resolving — including on Django 6, where the column aliasing of ``values()`` breaks the
proxy-field CTE of django-pghistory releases up to at least 3.9.

Reverting
=========

``Event.revert()`` works as in pghistory, but lets the router choose the connection: the write goes to the active shard
for a sharded model, and to the primary for a mirrored model. pghistory's own ``revert()`` writes to the ``default``
connection, which points at the public schema of a node and not at the shard that has the row. An explicit ``using=`` is
used as given.

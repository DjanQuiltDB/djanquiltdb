=============
API reference
=============

Once the plugin is installed, its API is served from inside ``djanquiltdb`` itself, so that a project never has to
import from the distribution package: the tracking functions from ``djanquiltdb.decorators.pghistory``, next to the
model decorators, and the event bases, ``create_event_model()`` and the queryset from ``djanquiltdb.models.pghistory``.

Both namespaces support importing the plugin's module and importing a single name from it, and neither way is
preferred::

    from djanquiltdb.decorators import pghistory        # the namespace, for @pghistory.track()
    from djanquiltdb.decorators.pghistory import track   # the function, imported by name

Everything below except ``EventQueryCompiler`` can also be imported from ``djanquiltdb_plugin_pghistory``, the
distribution package, which resolves its exports lazily, like the namespaces above. A type checker or an IDE cannot
follow either of them, so point it at the modules that define the names: ``djanquiltdb_plugin_pghistory.core`` for the
tracking functions and ``create_event_model()``, ``.sharded``, ``.mirrored`` and ``.public`` for the event bases, and
``.querysets`` for the queryset.


Tracking
========

.. currentmodule:: djanquiltdb.decorators.pghistory

.. autofunction:: track

.. autofunction:: track_sharded

.. autofunction:: track_mirrored

.. autofunction:: track_public


Event bases
===========

These abstract base models set the sharding mode of the event models that are generated from them. Each one is a class
named ``Event``, exported under the name below, so the models that pghistory generates from them are named after the
tracked model only.

.. currentmodule:: djanquiltdb.models.pghistory

.. class:: ShardedEvent

    The events are stored inside the same shard as the tracked rows.

.. class:: MirroredEvent

    The events are stored in a mirrored table, in the public schema of every node.

.. class:: PublicEvent

    The events are stored in the public schema. The ``pgh_id`` of an event is its natural key, which public models must
    have.

Event models
============

.. currentmodule:: djanquiltdb.models.pghistory

.. autofunction:: create_event_model

Querysets
=========

.. currentmodule:: djanquiltdb.models.pghistory

.. autoclass:: EventQuerySet

The compiler that the queryset uses is internal, and is only documented here because a custom queryset has to keep using
it, by inheriting from ``EventQuerySet``:

.. currentmodule:: djanquiltdb_plugin_pghistory.querysets

.. autoclass:: EventQueryCompiler


The plugin object
=================

.. currentmodule:: djanquiltdb_plugin_pghistory

.. autofunction:: install

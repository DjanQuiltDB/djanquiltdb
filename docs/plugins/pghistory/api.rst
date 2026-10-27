=============
API reference
=============

Once the plugin is installed, its API is served from inside ``djanquiltdb`` itself, so that a project never has to
import from the distribution package: the tracking functions from ``djanquiltdb.decorators.pghistory``, next to the
model decorators, the event bases, ``create_event_model()`` and the queryset from ``djanquiltdb.models.pghistory``, and
the mixin that the migrations of event models name from ``djanquiltdb.base.pghistory``.

Both namespaces support importing the plugin's module and importing a single name from it, and neither way is
preferred::

    from djanquiltdb.decorators import pghistory        # the namespace, for @pghistory.track()
    from djanquiltdb.decorators.pghistory import track   # the function, imported by name

The tracking functions, the event bases, ``create_event_model()`` and the queryset can also be imported from
``djanquiltdb_plugin_pghistory``, the distribution package, which resolves its exports lazily, like the namespaces
above. A type checker or an IDE cannot follow either of them, so point it at the modules that define the names:
``djanquiltdb_plugin_pghistory.core`` for the tracking functions and ``create_event_model()``, ``.sharded``,
``.mirrored``, ``.public`` and ``.single_node`` for the event bases, and ``.querysets`` for the queryset.


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

.. class:: SingleNodeEvent

    The events are stored in the public schema of the node of the tracked model, which must be on a single node. The
    tracking functions give each event model that node.

Event models
============

.. currentmodule:: djanquiltdb.models.pghistory

.. autofunction:: create_event_model

Querysets
=========

.. currentmodule:: djanquiltdb.models.pghistory

.. autoclass:: EventQuerySet

The queryset compiles its queries with a compiler of the plugin, which is internal. To keep using it, a custom queryset
has to inherit from ``EventQuerySet``.


Base classes
============

.. currentmodule:: djanquiltdb.base.pghistory

.. autoclass:: ShardRoutedRevert

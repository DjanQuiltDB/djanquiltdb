pghistory
=========
This plugin makes DjanQuiltDB and `django-pghistory <https://django-pghistory.readthedocs.io/>`_ work together: it
stores each event table in the same place as the table of the tracked model. Install it through an extra of
``djanquiltdb`` itself::

    pip install djanquiltdb[pghistory]

Plugin release |pghistory-version|.

Things to know
--------------

* Do not use the ``pgtrigger`` management commands that change the database directly. Triggers only reach the template
  schema and every shard through migrations. See :doc:`installation`.
* Record the context denormalized, with ``PGHISTORY_CONTEXT_FIELD = pghistory.ContextJSONField()``, and make the
  ``pghistory`` app public. No sharding mode of a normalized ``Context`` table suits the events of every model, and the
  plugin's system checks report every other setup. See :doc:`installation`.
* When the ``pghistory`` app is sharded, a write to a public or mirrored model whose trigger calls
  ``_pgh_attach_context()`` fails outside a shard. This is the case for statement-level tracking, and for an event
  model with a foreign key to the ``Context`` table. See :doc:`tracking`.
* ``pghistory.context()`` reaches the connection of every node, so a context that is entered before a shard is selected
  also reaches a shard on another node. See :doc:`installation`.
* Changing the sharding mode of the ``pghistory`` app in an existing project does not move its table and function. That
  takes a one-time migration. See :doc:`installation`.
* Apply the sharding mode decorator below ``track()``, so that ``track()`` can read the mode. See :doc:`tracking`.
* The events of a mirrored model are written on the primary node, so the replication that copies the tracked table to
  the other nodes has to copy the event table too. See :doc:`tracking`.
* An extra field on an event base needs a ``db_default``, or a trigger that fills it in, because the trigger that writes
  the events does not apply a Django ``default``. See :doc:`tracking`.
* Do not dump public event models with ``dumpdata --natural-primary``. Their natural key is their primary key, so every
  load of such a fixture adds the events again. See :doc:`tracking`.
* A query on pghistory's aggregate ``Events`` model, and so pghistory's admin, only covers the active shard and the
  public schema. Run it once for each shard to cover the whole project. See :doc:`tracking`.
* A query that selects the proxy fields of an event model fails when a tracked field has a ``db_column`` that is
  different from its name, and a proxy field that reads a key inside a key returns its value JSON-encoded. pghistory
  3.9 has these gaps too. See :doc:`tracking`.
* Moving a shard or its data, or purging data, records no events for those rows, because their triggers do not fire.
  That also holds for the project's own triggers: every one of them when a shard moves, and the ones that
  django-pgtrigger installs when data moves or is purged. See :doc:`moving`.
* When data moves to another shard or is purged, only the events of the moved or purged rows go with it. The events of
  rows that were deleted before, or that belong to another root object now, stay. See :doc:`moving`.
* A shard cannot move to another node while one of its events has a snapshot of a public row that was deleted since.
  See :doc:`moving`.
* ``Event.revert()`` and ``pghistory.create_event()`` write through the router, to the active shard or the primary
  node, and not to the ``default`` connection as pghistory's own do. Call ``pghistory.create_event()`` through the
  module, because a name imported before the app registry is ready is pghistory's own function. See :doc:`tracking`.
* pghistory does not inherit the ``Meta`` of an event base, so a trigger or a meta option for every event model has to
  come from a wrapper of ``track()``. See :doc:`customization`.
* A type checker cannot follow the names that the plugin serves lazily. Point it at the modules that define them. See
  :doc:`api`.


.. toctree::
    :maxdepth: 2

    installation
    tracking
    moving
    customization
    api
    changelog

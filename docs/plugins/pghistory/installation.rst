============
Installation
============

Install through DjanQuiltDB's extra::

    pip install djanquiltdb[pghistory]

The plugin itself needs no setup: DjanQuiltDB finds it through its entry point as soon as it is installed. The two
libraries that it connects still need their own setup. ``pghistory`` is a Django app, and goes in ``INSTALLED_APPS``
next to ``djanquiltdb`` and the ``pgtrigger`` app that it builds on::

    INSTALLED_APPS = (
        ...
        'djanquiltdb',
        'pgtrigger',
        'pghistory',
        ...
    )

django-pghistory writes events through django-pgtrigger triggers, so everything that :doc:`the trigger documentation
</database_objects/triggers>` says applies: triggers reach the template and every shard only through migrations, and the
``pgtrigger`` management commands that bypass migrations must not be used.

``pghistory.context()`` passes its metadata to the triggers through the database connection. The plugin makes it do this
on the connection of every node, so a context that is entered before a shard is selected, as pghistory's middleware
does, also reaches a shard on another node.

Recording the context
=====================

Use a **denormalized context**. It copies the metadata of ``pghistory.context()`` to each event row, so the events of
every model keep their context next to them::

    import pghistory

    PGHISTORY_CONTEXT_FIELD = pghistory.ContextJSONField()

pghistory's default, a normalized context, gives each event row a foreign key to a single ``Context`` table instead. No
sharding mode of that table suits the events of every model:

* A sharded ``Context`` table cannot be reached by the events of public and mirrored models.
* A public or mirrored ``Context`` table stores the context of the history of sharded models outside their shards.

Placing the context table
=========================

The ``pghistory`` app has a concrete ``Context`` model, also when no event model reads it. It has no sharding mode
unless the settings give it one, and without one, the plugin reports ``djanquiltdb_plugin_pghistory.W002``. Make it
public::

    QUILT_DB = {
        ...
        'OVERRIDE_SHARDING_MODE': {
            ...
            ('pghistory',): ShardingMode.PUBLIC,
        },
    }

The table has to be somewhere, and a project has one public schema on each node, but a schema for every shard.
``djanquiltdb_plugin_pghistory.W001`` reports a sharded or mirrored ``Context`` table that no event model has a foreign
key to, and asks you to make it public. A sharded table is created in the template schema and in every shard, and a
mirrored table suggests rows that are kept in sync across the nodes, while nothing reads them. The table is not always
empty: the trigger of a model that is tracked with ``level=pghistory.Statement`` writes a row to it inside a
``pghistory.context()`` block.

DjanQuiltDB requires a natural key of every public model. When the ``pghistory`` app is public, the plugin gives its
models their primary key as the natural key: the UUID ``id`` of ``Context``, and the ``pgh_slug`` of pghistory's
aggregate ``Events`` model. Like the natural key of the public event models, it is set on the models' ``_meta`` only, so
it needs no migration.

With a normalized context
-------------------------

When ``PGHISTORY_CONTEXT_FIELD``, or a ``context_field=`` at a call site, gives event models a foreign key to the
table, those event models have to be able to reach it. The plugin reports every such setup, so that you can move to a
denormalized context. The checks read the generated event models and not the setting, so they also see a
``context_field=`` that a call site passes, and the event models that a third-party app generates with plain
``pghistory.track()``.

* When the table is **public**, every event can reach it, but the context of sharded models is stored outside their
  shards, and the context rows of mirrored events are only written on the node where the tracked write happened.
  ``djanquiltdb_plugin_pghistory.W004`` reports this.
* When the table is **sharded**, *every* event model that has a foreign key to it must be sharded too. A public or
  mirrored event model with such a foreign key cannot reach the table, and the plugin reports
  ``djanquiltdb_plugin_pghistory.E002``, which names the event model. When every such event model is sharded, it
  works, and ``djanquiltdb_plugin_pghistory.W005`` reports it: the context is recorded per shard, so a
  ``pghistory.context()`` block that spans two shards writes a row in each of them, with the same id.
* When the table is **mirrored**, it works, but the event triggers write the context rows on the node where the tracked
  write happened. Nothing routes them to the primary, so the copies on the nodes stop matching.
  ``djanquiltdb_plugin_pghistory.W003`` reports this. This placement only works when your replication also covers the
  context table.

The trigger of an event model with such a foreign key calls pghistory's ``_pgh_attach_context()`` function, like a
statement-level trigger does. When the app is sharded, the function only exists in the shards, so the plugin reports
``djanquiltdb_plugin_pghistory.E004`` for each public or mirrored model whose triggers call it. See
:doc:`statement-level triggers <tracking>`.

Changing the placement of an existing project
---------------------------------------------

The router places pghistory's migrations by the ``OVERRIDE_SHARDING_MODE`` entry at the time they run. A project that
changes the entry later keeps the ``pghistory_context`` table and the ``_pgh_attach_context()`` function where its
migrations created them, and the triggers keep calling the old function. To move them, add a one-time migration to an
app of the project:

* Create the table and the function in their new place. ``Context.install_pgh_attach_context_func()`` creates the
  function on the connection that it is given.
* If any event model has a foreign key to the table, copy the existing context rows to the new place.
* Drop the table and the function from the template schema and from every shard, or from the public schema, depending
  on where they were. Qualify the drop with the schema, for example with ``current_schema()``. On the connection of a
  shard, the search path ends with the public schema, so an unqualified ``DROP ... IF EXISTS`` that finds nothing in
  the shard drops the public table or function instead.

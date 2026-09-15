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

Placing the context table
=========================

The ``pghistory`` app has a concrete ``Context`` model, which has no sharding mode unless the settings give it one.
Without one, the plugin reports ``djanquiltdb_plugin_pghistory.W002``::

    QUILT_DB = {
        ...
        'OVERRIDE_SHARDING_MODE': {
            ...
            ('pghistory',): ShardingMode.PUBLIC,
        },
    }

DjanQuiltDB requires a natural key of every public model. The plugin gives the models of the ``pghistory`` app their
primary key as the natural key: the UUID ``id`` of ``Context``, and the ``pgh_slug`` of pghistory's aggregate
``Events`` model. This needs no migration.

The placement only matters to the event models that have a foreign key to the table, and that depends on how
``PGHISTORY_CONTEXT_FIELD``, or a ``context_field=`` at a call site, records the context. The plugin registers system
checks for the placements that do not work, and for a placement that works but creates a table that nothing uses. The
checks read the generated event models and not the setting, so they also see a ``context_field=`` that a call site
passes.

**A denormalized context** copies the metadata to each event row, and never reads the table::

    import pghistory

    PGHISTORY_CONTEXT_FIELD = pghistory.ContextJSONField()

Any placement of ``Context`` then works. ``djanquiltdb_plugin_pghistory.W001`` reports a sharded or mirrored ``Context``
table that no event model reads, and recommends a public one, so that the project has one unused table, and not an
unused copy in every shard or on every node. Silence it if you want to keep the table where it is. The table is not
always empty: the trigger of a model that is tracked with ``level=pghistory.Statement`` writes a row to it inside a
``pghistory.context()`` block.

**A normalized context**, which is pghistory's default, gives each event row a foreign key to the table, so the events
have to be able to reach it:

* In the **public** schema, every event can reach it. Choose this placement unless you have a reason not to.
* When the table is **sharded**, *every* event model that has a foreign key to it must be sharded too. A public or
  mirrored event model with such a foreign key cannot reach the table, and the plugin reports
  ``djanquiltdb_plugin_pghistory.E002``, which names the event model. Note that the context is then recorded per shard:
  a ``pghistory.context()`` block that spans two shards writes a row in each of them, with the same id.
* When the table is **mirrored**, it works, but the event triggers write the context rows on the node where the tracked
  write happened. Nothing routes them to the primary, so the copies on the nodes stop matching.
  ``djanquiltdb_plugin_pghistory.W003`` reports this. This placement only works when your replication also covers the
  context table. Silence the check once it does.

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

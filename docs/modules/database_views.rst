==============
Database views
==============

This chapter is about views in the database. For the middleware that wraps a Django view in a sharding context, see
:doc:`views`.


Creating a view
---------------

A view is not model state, so it needs no ``SeparateDatabaseAndState``: a plain hinted ``RunSQL`` is enough.

.. code-block:: python

   operations = [
       migrations.RunSQL(
           'CREATE VIEW example_knight AS SELECT id, name, title AS rank FROM example_paladin;',
           reverse_sql='DROP VIEW example_knight;',
           hints={'sharding_mode': ShardingMode.SHARDED},
       )
   ]

The sharding mode goes in the ``hints`` dictionary, which ``RunSQL`` passes straight to the router's
``allow_migrate``; without it the router cannot tell where the operation belongs and raises a ``ProgrammingError``.
See :doc:`migrations`.


Where a view belongs
--------------------

With ``ShardingMode.SHARDED`` the view is created on the template schema and on every existing shard, and skipped on
the public schema. ``ShardingMode.PUBLIC`` and ``ShardingMode.MIRRORED`` put it on the public schema instead.

A view over sharded tables has to be sharded itself. A view's body is resolved when the view is *created*, binding it to
the tables of the schema it was created in. A public view over a sharded table would therefore read the public schema's
copy for ever, whichever shard the query came from. See :doc:`database_functions`.


What a new shard inherits
-------------------------

A shard is created by cloning the template schema rather than by migrating an empty one, so a sharded view reaches a
new shard without anything further being done about it.

Views and materialized views are **recreated from their definitions** rather than copied, which is what keeps a cloned
view tracking the tables of its own schema. The rebinding is done with the search path rather than by rewriting the
definition text: each definition is read back with only the template schema on the path, so that a reference within
that schema prints unqualified while a reference into another schema prints qualified, and it is then executed with
the new shard first on the path. A string literal that happens to hold a schema name survives untouched.

Creating them is retried until a full round adds nothing new, so a view selecting from another view needs no declared
order. If a round produces only errors and no progress, every error of that round is reported rather than whichever
view happened to fail last.

Two things that are not part of a view's definition are carried over separately. Its ``reloptions`` are re-emitted as
written, which is how ``WITH CHECK OPTION`` and ``security_barrier`` come along. Column defaults set with
``ALTER VIEW ... SET DEFAULT`` are read from the template and re-applied to the clone.

``move_sharded_models``, which converts an unsharded project by moving its tables onto a shard, recreates the
template's views on the target schema for the same reason a clone does: a view is not a table, so moving the tables
cannot carry it along. It then compares template and target as sorted name-and-kind pairs, so a view that arrives as
the wrong kind is caught rather than only a missing one.


Materialized views
------------------

A materialized view stores its rows, so a clone cannot simply track its sources the way a plain view does. It is
populated at clone time, from the tables that were just cloned, which means its contents are recomputed rather than
copied across. One that was deliberately left unpopulated is created ``WITH NO DATA`` and stays unpopulated.

``CREATE MATERIALIZED VIEW`` copies no indexes, so the template's are read back and recreated on the clone. That
matters beyond query speed: without a unique index a materialized view cannot be refreshed concurrently.

Refreshing
~~~~~~~~~~

The connection carries the helpers for it:

* ``refresh_materialized_views(names=None, schema_name=None)`` repopulates a schema's materialized views. Passing
  ``names`` is how a caller keeps deliberately unpopulated views unpopulated; leaving it out refreshes the schema's
  populated ones.
* ``get_populated_materialized_views(schema_name=None)`` returns the ones currently holding data.
* ``get_materialized_views_in_dependency_order(schema_name=None)`` returns them in the order they have to be
  refreshed in.

Each refresh is issued schema-qualified. A shard's search path covers the public schema as well as its own, so an
unqualified refresh could reach the wrong copy. The order is worked out over the whole view graph, plain views included,
because a materialized view may read another one *through* a plain view; only the materialized ends of such a chain can
be refreshed, but leaving the plain view out of the graph would lose the edge between them and let a view refresh
against rows that are about to change.

Moving data leaves a materialized view describing the state before the move, so the commands that move data refresh
them:

* ``move_data_to_shard`` refreshes **both** source and destination shards, each from its own populated set, after the
  rows have been deleted from the source. Refreshing the source while it still held the rows it was about to lose would
  leave its stored views describing a state that no longer exists.
* ``move_shard_to_node`` refreshes the target, using the *source's* populated set, because cloning populated the
  target's views while its tables were still empty.
* ``move_sharded_models`` refreshes the target shard and then the public schema.


Triggers on views
-----------------

An ``INSTEAD OF`` trigger, which is what makes a view that is not automatically updatable writable, is cloned along
with the view it is attached to. See :doc:`triggers`.


Renaming a table during a rolling deploy
----------------------------------------

A view is what makes a table rename possible without a gap: rename the table, then expose the old name as a view so
the previous release keeps reading and writing it, and drop the view in a later contract migration. A column alias
keeps such a view automatically updatable; the restriction is on expressions, not on renamed plain column references.


With the postgres-objects extra
-------------------------------

Everything above is the base library, where a view is hand-written SQL in a migration. The
``djanquiltdb[postgres-objects]`` extra offers the other route: declare the view as a class with
`django-postgres-objects <https://github.com/djanquiltdb/django-postgres-objects>`_ and let ``makemigrations`` write
the operations for it. See :ref:`postgres_objects_extra` for installing it and
:doc:`/plugins/postgres-objects/views` for the full reference.

.. code-block:: python

    # example/db_views.py
    from djanquiltdb.decorators import sharded_view
    from postgres_objects import View


    @sharded_view()
    class LoudCakes(View):
        sql = 'SELECT id, upper(name) AS name FROM example_cake'

``@sharded_view()``, ``@public_view()`` and ``@mirrored_view()`` say which schemas the view belongs in, matching the
model decorators. They work by putting a sharding mode in the declaration's ``router_hints``, which is the same hint
channel a hand-written ``RunSQL`` uses, so the placement rules above hold unchanged and there is no second mechanism.
A declaration left unannotated is public, so one written for a single-database project keeps working once that project
is sharded.

Placement is identical for the public and mirrored modes, since a view holds no rows of its own to distinguish them by.
What separates them is how a *materialized* view is refreshed from code: a mirrored one is refreshed on every node in
a single cascading transaction, a public one only on the connection in context, and a sharded one per shard.

To use the appropriate decorators with these declarations, install the ``djanquiltdb[postgres-objects]`` extra.

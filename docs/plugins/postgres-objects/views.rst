=====
Views
=====

.. warning::

   Only views declared with ``django-postgres-objects`` are supported by these decorators. Another library that tries to
   achieve something similar, `django-pgviews-redux <https://github.com/xelixdev/django-pgviews-redux>`_, is **not**
   supported. However, you can use it next to ``django-postgres-objects`` if you only manage functions with it, and do
   not apply these decorators to views from ``django-pgviews-redux``.

For existing views created with ``django-postgres-objects``, simply add the decorator, same as you do with models:

.. code-block:: python

    # example/db_views.py
    from djanquiltdb.decorators import sharded_view
    from postgres_objects import View


    @sharded_view()
    class LoudCakes(View):
        sql = 'SELECT id, upper(name) AS name FROM example_cake'

Point ``POSTGRES_OBJECTS['VIEWS_MODULE_PATH']`` at the module the views live in; see :doc:`installation`.


Where the view lives
--------------------

Three decorators, matching the function ones:

==================== ========================================================================================
``@public_view()``   Created in the public schema of every node, each copy standing on its own.
``@mirrored_view()`` Created in the public schema of every node, with the copies kept in step: a refresh visits every
                     node.
``@sharded_view()``  Created on the template schema and on every shard, and not in the public schema.
==================== ========================================================================================

A view over sharded tables has to be sharded itself. Note that this behavior differs substantially from that of a
function: a function's body resolves its table names at execution time, so a ``PUBLIC`` function reads whichever shard
the caller is pointed at. A view does not: its body is resolved when the view is *created*, binding it to the tables of
the schema it was created in. A public view over ``example_cake`` would therefore attempt to read a public schema's
table regardless of which shard the query came from (which would generally result in an error).

An unannotated declaration is public, exactly as for a function, so a declaration written for a single-database project
keeps working once that project is sharded.


What a new shard inherits
-------------------------

Nothing extra is needed to get a sharded view onto a new shard. A shard is created by cloning the template schema, and
the clone recreates views and materialized views from their definitions rather than copying them, so each shard's copy
tracks its own tables. DjanQuiltDB's ``move_sharded_models``, which converts an unsharded project by moving its tables
onto a shard, recreates the template's views there for the same reason: a view is not a table, so moving the tables
cannot carry it along. Both are described in DjanQuiltDB's own documentation, since they hold for any view, declared
or hand-made.

A materialized view inherits one thing more: whether it has been populated. The clone reads that from the template and
recreates a populated view with its rows recomputed from the shard's own freshly cloned tables, and recreates an
unpopulated one as unpopulated. So for a view declared ``with_data=False``, the first fill has to be the
``RefreshMaterializedView`` operation in the migration that creates it. The template schema is built by migrating, and
nothing calls ``refresh()`` while that happens.

If you leave the operation out, the view stays unpopulated on the template, and thus on every shard cloned from it, and
no later ``migrate`` will repair it: a clone inherits the applied-migration history along with everything else, so the
fill never comes up again. What this decides for a new shard is whether its copy can be *read*, not whether it holds
rows. A template's tables are empty, so neither the fill nor the clone's recompute puts anything in either copy; what
the fill changes is that PostgreSQL will read the view at all, which it refuses to do for one that has never been
refreshed.


Keeping materialized views current
----------------------------------

A materialized view stores its rows, so moving data around leaves it describing the state before the move. The
DjanQuiltDB commands that move data repopulate them afterwards:

- ``move_shard_to_node`` repopulates the target's materialized views from the copied rows.
- ``move_data_to_shard`` repopulates those of **both** shards, since the rows left one and arrived at the other.

Views the source schema deliberately leaves unpopulated stay unpopulated. A view is always refreshed after the ones it
reads, including where it reads them through a plain view, so a stack of materialized views comes out consistent.


Refreshing one yourself
~~~~~~~~~~~~~~~~~~~~~~~

Everything else that leaves a view stale is yours to say, and django-postgres-objects has the call for it:

.. code-block:: python

    from example.db_views import CakeTotals

    with use_shard(shard):
        CakeTotals.refresh(concurrently=True)

A refresh follows the connection in context, like every other read and write. So a ``@sharded_view()`` is refreshed
per shard, inside the ``use_shard`` block for the one whose rows moved, and refreshing it outside a shard context
fails on the public schema, where a sharded view does not exist, the same way a query against a sharded model
outside a context does.

A ``@mirrored_view()`` has copies on the public schema of every node, so one call visits them all, inside a single
cascading transaction: either every node's copy moves or none does. That is how any other mirrored write is propagated,
and it needs no context of its own.

A ``@public_view()`` has a copy on the public schema of every node too, but its copies are not treated as one object: a
refresh moves only the one on the connection in context, which is the right answer for a view whose sources differ per
node. That is the whole of the difference between the two annotations, and the reason to pick one over the other for a
materialized view.

Since a view that is not materialized has nothing to refresh, the difference between mirrored and public is purely a
statement of intent and nothing more.

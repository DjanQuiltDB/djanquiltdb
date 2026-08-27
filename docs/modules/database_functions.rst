==================
Database functions
==================

A Postgres function is a database object rather than model state, so Django's migration framework has no
representation for it and the base library creates it the way it creates any other database object: with a hinted
``RunSQL``. What a sharded project has to decide on top of that is which schemas the function should live in.


Creating a function
-------------------

.. code-block:: python

    from djanquiltdb import ShardingMode

    operations = [
        migrations.RunSQL(
            """
            CREATE FUNCTION alluppercase(input TEXT) RETURNS TEXT
            LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
            AS $$
                BEGIN
                    RETURN UPPER(input);
                END;
            $$;
            """,
            reverse_sql='DROP FUNCTION alluppercase(TEXT);',
            hints={'sharding_mode': ShardingMode.PUBLIC},
        )
    ]

The sharding mode goes in the ``hints`` dictionary, which ``RunSQL`` passes straight to the router's
``allow_migrate``; without it the router cannot tell where the operation belongs and raises a ``ProgrammingError``.
See :doc:`migrations`.

Anything that references the function has to be able to find it, so the migration creating it must run before the one
adding whatever calls it.


Where a function belongs
------------------------

``ShardingMode.PUBLIC`` creates the function in the public schema of every node. There is one copy per node and every
shard on that node shares it, because a shard's search path covers the public schema as well as its own, so an
unqualified call resolves from anywhere.

``ShardingMode.SHARDED`` creates it on the template schema and on every shard, and not in the public schema. Each
shard then calls its own copy, and the function is not reachable from the public schema at all.

A function in the public schema can still read sharded tables. As long as the table names in its body are not
schema-qualified, they resolve at execution time against the search path of whoever is calling, which is the shard in
context. Reach for ``SHARDED`` only when the *function itself* has to differ per shard; purely referencing sharded data
does not immediately mean you need to make the function sharded.


What a new shard inherits
-------------------------

A shard is created by cloning the template schema, and every function in the template is carried into the clone. Each
one is read back from the template and created again in the new schema, after the tables and foreign keys and before the
expressions, triggers and views that have to bind to the copies.

Unlike the view and expression passes, which rebind by search path, this one rewrites the definition text: occurrences
of the template schema's name are replaced with the new shard's. Keep the template schema name out of string literals
in a function that lives in the template schema, since a literal is rewritten along with everything else.

A function in the public schema is not touched by any of this. It is created once by its migration and every shard
goes on calling that one copy.

Procedures have no declarative route and no coverage of their own; write one with ``RunSQL`` as above and place it in
the public schema.


Where functions turn up
-----------------------

Three parts of the library lean on the placement rule above, and all three follow it identically: a ``SHARDED`` function
is copied into every shard and each shard uses its own copy, while a ``PUBLIC`` function has a single copy that every
shard shares:

* the function a stored generated column's expression calls, see :doc:`generated_columns`
* the function a trigger executes, see :doc:`triggers`
* the functions a view's body calls, see :doc:`database_views`


Calling a function from Django
------------------------------

Name the function unqualified, so that each shard resolves it through its own search path:

.. code-block:: python

    from django.db.models import F, Func

    Cake.objects.annotate(uppercased=Func(F('name'), function='alluppercase'))

Qualifying the call would pin it to whichever schema happened to build the query, which is exactly what a sharded
project does not want.


With the postgres-objects extra
-------------------------------

The ``djanquiltdb[postgres-objects]`` extra offers the alternative to hand-written SQL: declare the function as a
class with `django-postgres-objects <https://github.com/djanquiltdb/django-postgres-objects>`_ and let
``makemigrations`` write the operations that create, alter and remove it. See :ref:`postgres_objects_extra` for
installing it and :doc:`/plugins/postgres-objects/functions` for the full reference.

.. code-block:: python

    # example/functions.py
    from djanquiltdb.decorators import public_function
    from postgres_objects import Function


    @public_function()
    class AllUppercase(Function):
        arguments = 'input TEXT'
        returns = 'TEXT'
        volatility = 'IMMUTABLE'
        strict = True
        parallel = 'SAFE'
        body = """
            BEGIN
                RETURN UPPER(input);
            END;
        """

``@public_function()``, ``@mirrored_function()`` and ``@sharded_function()`` correspond to the sharding modes above.
They put the mode in the declaration's ``router_hints``, the same hint channel a hinted ``RunSQL`` uses, so the
placement rules are the ones already described. A declaration left unannotated is public, which is the right answer
for the common case and lets a declaration written for a single-database project keep working once that project is
sharded. For a function the public and mirrored modes place identically; the distinction only bites for a materialized
view, whose refresh differs between them.

The declaration is also callable, so the same class serves both the migration that creates the function and the
queries that call it, in place of the ``Func`` above.

To use the appropriate decorators with these declarations, install the ``djanquiltdb[postgres-objects]`` extra.

Migrations that were generated before the plugin was installed carry no placement hint. Unlike a hint-less
``RunSQL``, they do not fail: at apply time such an operation falls back to the default placement the plugin
installs, the public schema of every node - exactly what an unannotated declaration gets. Review pre-existing
migrations when adopting the extra on a project that already used django-postgres-objects, and regenerate them if
another placement is wanted.

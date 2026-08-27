=========
Functions
=========

For existing functions declared with ``django-postgres-objects``, simply add the decorator, same as you do with models:

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



Where the function lives
------------------------

Three decorators, matching the ones DjanQuiltDB already uses on models:

======================== ====================================================================================
``@public_function()``   Created in the public schema of every node. Functionally equivalent to mirrored, see below.
``@mirrored_function()`` Created in the public schema of every node. Functionally equivalent to public, see below.
``@sharded_function()``  Created on the template schema and on every shard, and not in the public schema.
======================== ====================================================================================

A public function and a mirrored one are placed identically. Both are allowed on the public schema of every node
and refused everywhere else, and a function holds no data, so nothing else is left for the two to differ by. The
two separate decorators are added to maintain symmetry with other objects and can be used to state intent, but otherwise
hold no meaningful difference.

A ``PUBLIC`` function is reachable from inside a shard: the search path covers the shard's own schema and the public
schema, so an unqualified call resolves. A ``SHARDED`` function is not reachable from the public schema.

Note that a function can refer to sharded tables even when it lives in the public schema. As long as those table names
are not schema-qualified in the function body, they resolve at execution time to the tables in whichever schema the
caller is pointed at. This means that you usually want to use ``@sharded_function()`` **only** when the
*function itself* has to differ per shard.

An unannotated declaration is public, which means declarations written for a single-database project keep working when
that project is sharded.

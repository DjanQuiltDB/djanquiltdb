from djanquiltdb import ShardingMode


def _annotate_object(cls, sharding_mode):
    """
    Mark a declared database object as belonging to a sharding mode.

    django-postgres-objects hands ``router_hints`` to ``allow_migrate`` for every operation on the object, so putting
    the sharding mode there routes it exactly the way a model of that mode is routed. It knows nothing about sharding
    itself; this is the whole of the seam between the two libraries.
    """
    cls.router_hints = {'sharding_mode': sharding_mode}

    return cls


def mirrored_function():
    """
    A decorator for marking a declared Postgres function as being mirrored across the various nodes.

    The function is created on the public schema of every node, which is where ``@public_function()`` puts it too: a
    function holds no data, so there is nothing for the two modes to differ by. Pick whichever names the intent, the
    way the model decorators do.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators import mirrored_function
            from postgres_objects import Function


            @mirrored_function()
            class AllUppercase(Function):
                arguments = 'input TEXT'
                returns = 'TEXT'
                volatility = 'IMMUTABLE'
                body = '''
                    BEGIN
                        RETURN UPPER(input);
                    END;
                '''
    """

    def configure(cls):
        return _annotate_object(cls, ShardingMode.MIRRORED)

    return configure


def public_function():
    """
    A decorator for marking a declared Postgres function as living in the public schema.

    This is what a function used by a stored generated column normally wants. The search path of a shard covers the
    shard's own schema and the public schema, so an unqualified call from inside a shard resolves.

    The function is created on the public schema of every node, since that is where every shard's search path can
    reach it. That is the same placement ``@mirrored_function()`` gives, and for a function the two are equivalent;
    what a mode records here is intent rather than a different set of schemas.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators import public_function
            from postgres_objects import Function


            @public_function()
            class AllUppercase(Function):
                arguments = 'input TEXT'
                returns = 'TEXT'
                volatility = 'IMMUTABLE'
                strict = True
                parallel = 'SAFE'
                body = '''
                    BEGIN
                        RETURN UPPER(input);
                    END;
                '''
    """

    def configure(cls):
        return _annotate_object(cls, ShardingMode.PUBLIC)

    return configure


def sharded_function():
    """
    A decorator for marking a declared Postgres function as being sharded.

    The function is created on the template schema and on every shard, and not on the public schema. Note that a
    ``PUBLIC`` function can already read sharded tables: as long as the table names in its body are not
    schema-qualified, they resolve at execution time to the tables of whichever schema the caller is pointed at. Reach
    for a sharded function when the *function itself* has to differ per shard.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators import sharded_function
            from postgres_objects import Function


            @sharded_function()
            class ShardTotal(Function):
                returns = 'BIGINT'
                body = '''
                    BEGIN
                        RETURN (SELECT COUNT(*) FROM example_cake);
                    END;
                '''
    """

    def configure(cls):
        return _annotate_object(cls, ShardingMode.SHARDED)

    return configure

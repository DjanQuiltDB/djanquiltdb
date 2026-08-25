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


def mirrored_view():
    """
    A decorator for marking a declared Postgres view as being mirrored across the various nodes.

    The view is created on the public schema of every node, which is where a view over mirrored tables belongs.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators import mirrored_view
            from postgres_objects import View


            @mirrored_view()
            class ActiveTypes(View):
                sql = 'SELECT id, name FROM example_type WHERE active'
    """

    def configure(cls):
        return _annotate_object(cls, ShardingMode.MIRRORED)

    return configure


def public_view():
    """
    A decorator for marking a declared Postgres view as living in the public schema.

    A view is created on the public schema of every node and reads the tables that live there. Those are the same
    schemas a mirrored view reaches; what PUBLIC says is that each copy stands on its own, over sources that may differ
    per node.

    Note that unlike a function, a public view is *not* a way to read sharded tables: a view's body is resolved when it
    is created, not when it is queried, so the tables it names are pinned to the schema it was created in. A view over
    sharded tables has to be sharded itself.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators import public_view
            from postgres_objects import View


            @public_view()
            class CakeTypeNames(View):
                sql = 'SELECT id, name FROM example_caketype'
    """

    def configure(cls):
        return _annotate_object(cls, ShardingMode.PUBLIC)

    return configure


def sharded_view():
    """
    A decorator for marking a declared Postgres view as being sharded.

    The view is created on the template schema and on every shard, and not on the public schema. This is what a view
    over sharded tables needs: each shard gets its own copy, reading the tables of the schema it was created in.

    :Example:
        .. code-block:: python

            from djanquiltdb.decorators import sharded_view
            from postgres_objects import View


            @sharded_view()
            class LoudCakes(View):
                sql = 'SELECT id, upper(name) AS name FROM example_cake'
    """

    def configure(cls):
        return _annotate_object(cls, ShardingMode.SHARDED)

    return configure

from postgres_objects import Function

from djanquiltdb.decorators import public_function, sharded_function

APP_LABEL = 'example'


@public_function()
class AllUppercase(Function):
    app_label = APP_LABEL
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


# The same function as AllUppercase but under its bare name: the generated-column tests reference it as
# public.alluppercase in DDL they build by hand, so the identifier is pinned with db_name. Only the identifier is
# pinned; naming the declaration 'alluppercase' as well would make this module declare two objects under that name.
@public_function()
class RawAllUppercase(Function):
    app_label = APP_LABEL
    db_name = 'alluppercase'
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


@sharded_function()
class ShardOnly(Function):
    app_label = APP_LABEL
    arguments = 'input TEXT'
    returns = 'TEXT'
    # Declared immutable so a generated column can call it: Postgres refuses a generation expression that is not.
    volatility = 'IMMUTABLE'
    strict = True
    parallel = 'SAFE'
    body = """
        BEGIN
            RETURN input;
        END;
    """


class Unannotated(Function):
    app_label = APP_LABEL
    arguments = 'input TEXT'
    returns = 'TEXT'
    body = """
        BEGIN
            RETURN input;
        END;
    """

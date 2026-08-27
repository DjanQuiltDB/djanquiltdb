# These declarations are fixtures, not schema: do not write them into a migration, however much `makemigrations`
# wants to. The tests apply the operations by hand and drop the objects again afterwards, so anything a migration
# created would be gone after the first test and the migrated state would be a lie for the rest of the run. The cases
# that do need a migration use the `declared` app instead, whose fixture migrations a test points MIGRATION_MODULES at.

from djanquiltdb.decorators import public_function, sharded_function
from postgres_objects import Function

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


# The same function as AllUppercase, but the one declaration here that sets db_name: it holds the other half of the
# naming rule, that an explicit db_name is the identifier verbatim while an unset one is derived from the app label.
# Only the identifier is overridden; naming the declaration 'alluppercase' as well would make this module declare two
# objects under that name.
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

from django.conf import settings
from django.db import DEFAULT_DB_ALIAS
from django.db.transaction import Atomic

from djanquiltdb.db import connections, connection, DefaultConnectionProxy
from djanquiltdb.postgresql_backend.base import DatabaseWrapper
from djanquiltdb.utils import transaction_for_nodes, get_connection_alias

"""
Both get_connection and atomic are copied from django native functions and altered.
atomic is monkey-patched in in sharding.apps._patch_transactions.
See django.db.transaction for their original.
"""


def get_connection(using=None):
    """
    Get a database connection by name, or the default database connection
    if no name is provided. This is a private API.
    """
    if using is None:
        using = connection
    if isinstance(using, DefaultConnectionProxy) or isinstance(using, DatabaseWrapper):
        return using
    else:
        return connections[using]


def atomic(using=None, savepoint=True, durable=False):
    # Bare decorator: @atomic -- although the first argument is called
    # `using`, it's actually the function being decorated.
    if callable(using):
        return atomic(using=None, savepoint=savepoint, durable=durable)(using)

    # Decorator: @atomic(...) or context manager: with atomic(...): ...
    if using:
        # If we specify which node to use, do not cascade with the primary node
        return Atomic(using, savepoint, durable=durable)

    primary_connection_name = settings.SHARDING.get('PRIMARY_DB_ALIAS', DEFAULT_DB_ALIAS)
    current_connection = get_connection(using)
    if get_connection_alias(current_connection) == primary_connection_name:
        # If we are on the default connection already, no need to do anything fancy
        return Atomic(using, savepoint, durable=durable)

    # So we are not on the primary connection.
    # Start a transaction on both the primary and our current connection
    return transaction_for_nodes(nodes=[primary_connection_name, current_connection])

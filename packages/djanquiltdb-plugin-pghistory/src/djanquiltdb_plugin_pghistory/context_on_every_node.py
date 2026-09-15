import contextlib

import pghistory.runtime
from django.db import connections
from djanquiltdb.utils import get_all_databases


class EveryNodeConnection:
    """
    Stands in for the connection that pghistory.context() installs its execute wrapper on.

    pghistory.runtime imports django.db.connection before DjanQuiltDB patches it, so pghistory.context() would only
    reach the connection of the default node. A write on another node would then record no context. This installs the
    wrapper on the connection of every node instead, so a context that is entered before a shard on another node is
    selected, as pghistory's middleware does, also reaches that shard.
    """

    @contextlib.contextmanager
    def execute_wrapper(self, wrapper):
        with contextlib.ExitStack() as stack:
            for node_name in get_all_databases():
                stack.enter_context(connections[node_name].execute_wrapper(wrapper))

            yield


def send_context_to_every_node():
    if not isinstance(pghistory.runtime.connection, EveryNodeConnection):
        pghistory.runtime.connection = EveryNodeConnection()

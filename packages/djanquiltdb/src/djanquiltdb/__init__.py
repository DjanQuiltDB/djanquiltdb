from enum import Enum

__version__ = '4.0.1'


default_app_config = 'djanquiltdb.apps.DjanQuiltDBConfig'


class ShardingMode(Enum):
    """
    Where a model's table lives, and which nodes may write to it.

    A model is given one of these by the decorators in :mod:`djanquiltdb.decorators`; an undecorated model has no
    mode at all and stays on the default node's public schema.
    """

    #: Public schema on every node, replicated: only the ``PRIMARY_DB_ALIAS`` node is writable.
    MIRRORED = 'M'
    #: Public schema on every node, each holding data of its own, all writable.
    PUBLIC = 'P'
    #: A schema per shard, on whichever node that shard lives on, all writable.
    SHARDED = 'S'


public_modes = (ShardingMode.MIRRORED, ShardingMode.PUBLIC)


class State(object):
    """
    Whether a shard may be routed to.

    A shard is created in ``MAINTENANCE`` and is moved to ``ACTIVE`` once its schema is ready. Commands that move or
    purge data put a shard back into maintenance for the duration, and routing to it raises ``StateException`` while
    it is there.
    """

    #: Reachable and writable.
    ACTIVE = 'A'
    #: Unreachable: being created, moved or purged.
    MAINTENANCE = 'M'


STATES = (
    (State.ACTIVE, 'Active'),
    (State.MAINTENANCE, 'Maintenance'),
)

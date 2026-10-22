from enum import Enum

__version__ = '4.4.0'


default_app_config = 'djanquiltdb.apps.DjanQuiltDBConfig'


class ShardingMode(Enum):
    """
    Where a model's table lives, and which nodes may write to it.

    A model is given one of these by the decorators in :mod:`djanquiltdb.decorators`, or by the
    ``OVERRIDE_SHARDING_MODE`` setting. A model without a mode fails the system check ``djanquiltdb.E004``.
    """

    #: Public schema on every node, replicated: only the ``PRIMARY_DB_ALIAS`` node is writable.
    MIRRORED = 'M'
    #: Public schema on every node, each holding data of its own, all writable.
    PUBLIC = 'P'
    #: A schema per shard, on whichever node that shard lives on, all writable.
    SHARDED = 'S'
    #: Public schema of one node only, which the model names. Strongly discouraged: see
    #: ``@single_node_unsharded_model()``.
    SINGLE_NODE_UNSHARDED = 'N'


public_modes = (ShardingMode.MIRRORED, ShardingMode.PUBLIC)


class PrimaryNode(Enum):
    """
    Names the node that ``PRIMARY_DB_ALIAS`` names, where DjanQuiltDB expects the alias of a node. The alias is looked
    up each time that it is used.
    """

    PRIMARY_NODE = 'primary node'


#: The node that ``PRIMARY_DB_ALIAS`` names, for ``@single_node_unsharded_model(node=PRIMARY_NODE)``.
PRIMARY_NODE = PrimaryNode.PRIMARY_NODE


def raise_for_invalid_node(node):
    """
    Raises TypeError unless ``node`` is the alias of a node, as a string, or ``PRIMARY_NODE``.
    """
    if node is not PRIMARY_NODE and not isinstance(node, str):
        raise TypeError('node must be the alias of a node or PRIMARY_NODE, not {!r}.'.format(node))


class SingleNodeUnsharded:
    """
    The value of an ``OVERRIDE_SHARDING_MODE`` entry that puts the models of the entry on one node, as
    ``@single_node_unsharded_model()`` does. ``single_node_unsharded()`` returns it.
    """

    def __init__(self, node):
        raise_for_invalid_node(node)
        self.node = node

    def __eq__(self, other):
        return isinstance(other, SingleNodeUnsharded) and other.node == self.node

    def __hash__(self):
        return hash(self.node)

    def __repr__(self):
        return 'single_node_unsharded(node={!r})'.format(self.node)


def single_node_unsharded(node):
    """
    The value for an ``OVERRIDE_SHARDING_MODE`` entry that puts the models of a third-party app on one node, as
    ``@single_node_unsharded_model(node)`` does for a model of the project. ``node`` is the alias of a node, or
    ``PRIMARY_NODE``. Strongly discouraged, see ``@single_node_unsharded_model()``::

        QUILT_DB = {
            'OVERRIDE_SHARDING_MODE': {
                ('some_app',): single_node_unsharded(node=PRIMARY_NODE),
            },
        }
    """
    return SingleNodeUnsharded(node)


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

from djanquiltdb import ShardingMode
from pghistory import config

from djanquiltdb_plugin_pghistory.base import ShardRoutedRevert
from djanquiltdb_plugin_pghistory.querysets import EventQuerySet


class Event(ShardRoutedRevert, config.base_model()):
    """
    The events of a model on a single node are stored on that node. The tracking functions give each event model the
    node of its tracked model, because ``@single_node_unsharded_model()`` needs a node, which this base cannot name.
    """

    objects = EventQuerySet.as_manager()

    class Meta:
        abstract = True


Event.__sharding_mode = ShardingMode.SINGLE_NODE_UNSHARDED

# The class is named Event, because pghistory names each generated event model after the tracked model followed by the
# name of its base, so tracking a model named Contract generates a model named ContractEvent.
SingleNodeEvent = Event

__all__ = ['SingleNodeEvent']

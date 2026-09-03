from djanquiltdb.decorators import sharded_model
from pghistory import config

from djanquiltdb_plugin_pghistory.base import ShardRoutedRevert
from djanquiltdb_plugin_pghistory.querysets import EventQuerySet


@sharded_model()
class Event(ShardRoutedRevert, config.base_model()):
    objects = EventQuerySet.as_manager()

    class Meta:
        abstract = True


# The class is named Event, because pghistory names each generated event model after the tracked model followed by the
# name of its base, so tracking a model named Contract generates a model named ContractEvent.
ShardedEvent = Event

__all__ = ['ShardedEvent']

from django.db import models
from djanquiltdb.decorators import public_model
from pghistory import config

from djanquiltdb_plugin_pghistory.base import ShardRoutedRevert
from djanquiltdb_plugin_pghistory.querysets import EventQuerySet


class EventManager(models.Manager.from_queryset(EventQuerySet)):
    def get_by_natural_key(self, pgh_id):
        return self.get(pgh_id=pgh_id)


@public_model()
class Event(ShardRoutedRevert, config.base_model()):
    """
    pghistory does not inherit the Meta of the base, so the tracking functions add the unique_together for the pgh_id
    natural key.
    """

    objects = EventManager()

    class Meta:
        abstract = True

    def natural_key(self):
        return (self.pgh_id,)


# The class is named Event, because pghistory names each generated event model after the tracked model followed by the
# name of its base, so tracking a model named Contract generates a model named ContractEvent.
PublicEvent = Event

__all__ = ['PublicEvent']

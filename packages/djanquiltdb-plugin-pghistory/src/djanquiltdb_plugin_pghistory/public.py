from django.db import models
from django.db.models.signals import class_prepared
from djanquiltdb.decorators import public_model
from pghistory import config

from djanquiltdb_plugin_pghistory.base import ShardRoutedRevert
from djanquiltdb_plugin_pghistory.natural_keys import make_the_primary_key_the_natural_key
from djanquiltdb_plugin_pghistory.querysets import EventQuerySet


class EventManager(models.Manager.from_queryset(EventQuerySet)):
    def get_by_natural_key(self, pgh_id):
        return self.get(pgh_id=pgh_id)


@public_model()
class Event(ShardRoutedRevert, config.base_model()):
    """
    The natural key of an event is its pgh_id, because an event has no other value that identifies it. pghistory does
    not inherit the Meta of the base, so each generated event model gets the pgh_id in its unique_together when it is
    created.

    The pgh_id only identifies an event on its own node, because each node generates its own. Nothing has a foreign key
    to an event, so no command looks an event up on another node by its natural key.
    """

    objects = EventManager()

    class Meta:
        abstract = True

    def natural_key(self):
        return (self.pgh_id,)


def make_pgh_id_the_natural_key_of_public_event_models(sender, **kwargs):
    if issubclass(sender, Event) and not sender._meta.proxy:
        make_the_primary_key_the_natural_key(sender)


# Django sends class_prepared for concrete models only.
class_prepared.connect(make_pgh_id_the_natural_key_of_public_event_models)

# The class is named Event, because pghistory names each generated event model after the tracked model followed by the
# name of its base, so tracking a model named Contract generates a model named ContractEvent.
PublicEvent = Event

__all__ = ['PublicEvent']

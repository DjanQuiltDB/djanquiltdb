"""
Routes the manual events of pghistory.create_event() through the router.

pghistory.create_event() writes an event whose model has a foreign key to the Context table through its own SQL
compiler, on the connection that its using argument names, and that argument defaults to the default alias. That alias
points at the public schema of a node, and not at the active shard, so the event of a sharded model would be written
outside its shard. An event without such a foreign key is written through the ORM, where the router already chooses.
"""

import pghistory
import pghistory.core
from django.db import router

#: pghistory's own create_event(), which create_event_through_the_router() calls.
create_event_of_pghistory = pghistory.core.create_event


def create_event_through_the_router(obj, *, label, using=None):
    """
    Calls pghistory's create_event(). Without a using argument, the event is written where the router writes the row
    that it describes: the shard of the row for a sharded model, and the primary node for a mirrored model. The plugin
    places each event model in the same way as its tracked model, so the router answers for both.
    """
    if using is None:
        using = router.db_for_write(type(obj), instance=obj)

    return create_event_of_pghistory(obj, label=label, using=using)


def route_manual_events_through_the_router():
    pghistory.create_event = create_event_through_the_router
    pghistory.core.create_event = create_event_through_the_router

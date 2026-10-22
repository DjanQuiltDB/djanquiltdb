"""
This package defines the plugin object, which DjanQuiltDB finds through the ``djanquiltdb.plugins`` entry-point group,
and exports the public API of the plugin. The names are resolved lazily, because DjanQuiltDB loads the entry point of
every installed plugin before the app registry is ready, and the event bases cannot be imported before then.
"""

from types import SimpleNamespace

from djanquiltdb_plugin_pghistory import base, data_moves, decorators, models

__version__ = '1.0.1'


def install():
    """
    Registers the plugin's system checks, gives the public models of the pghistory app the natural key that a public
    model needs, makes ``pghistory.context()`` reach every node, and routes the manual events of
    ``pghistory.create_event()`` through the router. DjanQuiltDB calls this once the app registry is ready, before it
    validates the public models.
    """
    from djanquiltdb_plugin_pghistory import checks  # noqa: F401
    from djanquiltdb_plugin_pghistory.context_on_every_node import send_context_to_every_node
    from djanquiltdb_plugin_pghistory.manual_events import route_manual_events_through_the_router
    from djanquiltdb_plugin_pghistory.natural_keys import add_natural_keys_to_public_pghistory_models

    route_manual_events_through_the_router()
    add_natural_keys_to_public_pghistory_models()
    send_context_to_every_node()


# The hooks below only keep the events with the rows that they describe. None of them keeps the triggers from firing
# while rows are moved or purged: DjanQuiltDB does that itself in move_shard_to_node, move_data_to_shard and
# purge_shard_data, so no event is recorded for those rows without the plugin needing a hook.
plugin = SimpleNamespace(
    base=base,
    decorators=decorators,
    models=models,
    install=install,
    move_data_to_shard=SimpleNamespace(
        post_collect=data_moves.keep_only_the_events_of_collected_rows,
        post_delete=data_moves.delete_moved_events,
    ),
    purge_shard_data=SimpleNamespace(
        post_collect=data_moves.keep_only_the_events_of_collected_rows,
        post_delete=data_moves.delete_events_of_purged_rows,
    ),
)

# __getattr__ below resolves every name in this list except install and plugin, from the module that serves it.
__all__ = [*models.__all__, *decorators.__all__, 'install', 'plugin']


def __getattr__(name):
    for served_module in (models, decorators):
        if name in served_module.__all__:
            return getattr(served_module, name)

    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

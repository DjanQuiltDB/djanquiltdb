"""
DjanQuiltDB serves this module as ``djanquiltdb.models.pghistory``. ``EventQueryCompiler`` is internal and is not
exported.

The names are resolved lazily, because this module can be imported before the app registry is ready, and the event bases
cannot be imported before then.
"""

from importlib import import_module

_DEFINING_MODULE_BY_NAME = {
    'EventQuerySet': 'djanquiltdb_plugin_pghistory.querysets',
    'MirroredEvent': 'djanquiltdb_plugin_pghistory.mirrored',
    'PublicEvent': 'djanquiltdb_plugin_pghistory.public',
    'ShardedEvent': 'djanquiltdb_plugin_pghistory.sharded',
    'SingleNodeEvent': 'djanquiltdb_plugin_pghistory.single_node',
    'create_event_model': 'djanquiltdb_plugin_pghistory.core',
}

# __getattr__ below resolves these names.
__all__ = [  # noqa: F822
    'EventQuerySet',
    'MirroredEvent',
    'PublicEvent',
    'ShardedEvent',
    'SingleNodeEvent',
    'create_event_model',
]


def __getattr__(name):
    if name not in _DEFINING_MODULE_BY_NAME:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

    return getattr(import_module(_DEFINING_MODULE_BY_NAME[name]), name)

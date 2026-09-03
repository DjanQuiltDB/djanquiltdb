"""
DjanQuiltDB serves this module as ``djanquiltdb.decorators.pghistory``. ``SERVE_UNQUALIFIED = False`` keeps these names
out of :mod:`djanquiltdb.decorators` itself, so that a call site such as ``@pghistory.track()`` shows which library does
the tracking, and a name as generic as ``track`` cannot collide with a name from another plugin.

The names are resolved lazily, because this module can be imported before the app registry is ready, and
django-pghistory's models cannot be imported before then.
"""

SERVE_UNQUALIFIED = False

# __getattr__ below resolves these names.
__all__ = [  # noqa: F822
    'track',
    'track_mirrored',
    'track_public',
    'track_sharded',
]


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

    from djanquiltdb_plugin_pghistory import core

    return getattr(core, name)

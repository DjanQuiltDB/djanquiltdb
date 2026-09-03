"""
This package defines the plugin object, which DjanQuiltDB finds through the ``djanquiltdb.plugins`` entry-point group,
and exports the public API of the plugin. The names are resolved lazily, because DjanQuiltDB loads the entry point of
every installed plugin before the app registry is ready, and the event bases cannot be imported before then.
"""

from types import SimpleNamespace

from djanquiltdb_plugin_pghistory import decorators, models

__version__ = '1.0.0'


def install():
    """
    Registers the plugin's system checks. DjanQuiltDB calls this once the app registry is ready.
    """
    from djanquiltdb_plugin_pghistory import checks  # noqa: F401


plugin = SimpleNamespace(decorators=decorators, models=models, install=install)

# __getattr__ below resolves every name in this list except install and plugin, from the module that serves it.
__all__ = [*models.__all__, *decorators.__all__, 'install', 'plugin']


def __getattr__(name):
    for served_module in (models, decorators):
        if name in served_module.__all__:
            return getattr(served_module, name)

    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

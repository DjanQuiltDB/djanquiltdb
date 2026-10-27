"""
The namespace that serves the classes of the plugins: a plugin's ``base`` module is served as
``djanquiltdb.base.<name>``. It holds the classes that a project subclasses or refers to, such as the base class of a
plugin's tasks. A plugin's functions, exceptions and constants are served from ``djanquiltdb.utils.<name>``.
"""

from djanquiltdb.plugins import install_plugin_importer, served_plugin_module

# A plugin's base module is served as the submodule djanquiltdb.base.<plugin>, and the import system only asks
# PluginImporter for a submodule when the parent module has a __path__. The list is empty, because no submodule is found
# by searching it.
__path__ = []

install_plugin_importer()


def __getattr__(name):
    """
    Serves the base module of a plugin under the plugin's registered name, for plain attribute access.
    :class:`~djanquiltdb.plugins.PluginImporter` handles import statements.
    """
    module = served_plugin_module(__name__, name)

    if module is None:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

    return module

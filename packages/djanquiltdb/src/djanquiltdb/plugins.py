"""
Discovery of library plugins, and the four namespaces that serve their API.

A plugin is a separate distribution that registers itself under the ``djanquiltdb.plugins`` entry-point group and
carries glue for one declaration library. The registry is resolved lazily, on first use rather than at app-load time:
a project's declaration modules may be imported while the app registry is still assembling, before any
``AppConfig.ready()`` has run, and the decorator re-export in :mod:`djanquiltdb.decorators` has to be able to answer
then.

A plugin's ``base``, ``decorators``, ``models`` and ``utils`` modules are served as ``djanquiltdb.base.<name>``,
``djanquiltdb.decorators.<name>``, ``djanquiltdb.models.<name>`` and ``djanquiltdb.utils.<name>``, as the plugin's own
module objects and not as copies. :class:`PluginImporter` handles import statements, and the ``__getattr__`` of each of
the four core modules handles plain attribute access.
"""

import contextlib
import sys
from functools import cache
from importlib import import_module
from importlib.abc import Loader, MetaPathFinder
from importlib.metadata import entry_points
from importlib.util import spec_from_loader

from django.core.exceptions import ImproperlyConfigured

#: The entry-point group that a plugin registers itself under. The plugin is served under its registered name. The entry
#: point loads an object that has an ``install()`` callable, which is called when the app registry is ready. The object
#: can also have the attributes that are named in ``_PLUGIN_ATTRIBUTE_BY_NAMESPACE``.
PLUGIN_GROUP = 'djanquiltdb.plugins'


def _distribution_name(entry_point):
    """
    Returns the name of the distribution that registers an entry point, or the entry point's value when the metadata has
    no distribution.
    """
    dist = getattr(entry_point, 'dist', None)

    return entry_point.value if dist is None else dist.name


class _DuplicatedPluginName:
    """
    Stands in for the entry points of a name that two distributions register. A plugin is imported under its
    registered name, and picking one of them would serve the imports of one plugin from the package of the other, so
    neither is loaded, and a lookup of the name raises ImproperlyConfigured.
    """

    def __init__(self, name, first, second):
        self.message = (
            f'Two distributions register a djanquiltdb plugin named {name!r}: {_distribution_name(first)} and '
            f'{_distribution_name(second)}. Uninstall one of them.'
        )


@cache
def _entry_points_by_name():
    """
    Returns the entry point of every registered plugin, keyed by the name that it registers. A name that two
    distributions register maps to a :class:`_DuplicatedPluginName`.

    The result is cached, so the entry points are read once per process, also when a name is duplicated.
    """
    by_name = {}

    for entry_point in entry_points(group=PLUGIN_GROUP):
        registered = by_name.get(entry_point.name)

        if registered is None:
            by_name[entry_point.name] = entry_point
        elif not isinstance(registered, _DuplicatedPluginName):
            by_name[entry_point.name] = _DuplicatedPluginName(entry_point.name, registered, entry_point)

    return by_name


def raise_for_duplicate_plugin_names():
    """
    Raises ImproperlyConfigured when two distributions register a plugin under the same name. DjanQuiltDB calls this
    when the app registry is ready, so the project does not start with a duplicated name.
    """
    for registered in _entry_points_by_name().values():
        if isinstance(registered, _DuplicatedPluginName):
            raise ImproperlyConfigured(registered.message)


@cache
def load_plugins():
    """
    Load every installed plugin, once per process. A name that two distributions register is left out, see
    :func:`raise_for_duplicate_plugin_names`.
    """
    return tuple(
        registered.load()
        for registered in _entry_points_by_name().values()
        if not isinstance(registered, _DuplicatedPluginName)
    )


def plugin_hooks(name):
    """
    Returns the hook that each loaded plugin has under ``name``, in the order in which the plugins are loaded. A name
    has the form ``<command>.<hook>``, and is read from the plugin object as an attribute path, so each command is a
    section of its own, such as ``plugin.move_data_to_shard.pre_copy``. A plugin without the hook takes no part.

    The hooks that DjanQuiltDB calls are listed in the Plugins section of the API reference.
    """
    hooks = []

    for plugin in load_plugins():
        hook = plugin

        for attribute in name.split('.'):
            hook = getattr(hook, attribute, None)

        if callable(hook):
            hooks.append(hook)

    return hooks


def run_plugin_hooks(name, *args, **kwargs):
    """
    Calls the hook that each loaded plugin has under ``name`` with the given arguments.
    """
    for hook in plugin_hooks(name):
        hook(*args, **kwargs)


@contextlib.contextmanager
def entered_plugin_hook_contexts(name, *args, **kwargs):
    """
    Enters the context manager that the hook of each loaded plugin under ``name`` returns for the given arguments, in
    the order in which the plugins are loaded, and exits them in reverse order after the block.
    """
    with contextlib.ExitStack() as stack:
        for hook in plugin_hooks(name):
            stack.enter_context(hook(*args, **kwargs))

        yield


def load_plugin_by_name(name):
    """
    Loads the plugin that is registered under ``name``, or returns None if no plugin is registered under that name.
    Raises ImproperlyConfigured when two distributions register that name.
    """
    registered = _entry_points_by_name().get(name)

    if registered is None:
        return None

    if isinstance(registered, _DuplicatedPluginName):
        raise ImproperlyConfigured(registered.message)

    return registered.load()


#: Maps each core module that serves a namespace to the plugin attribute that it serves. A plugin that has none of these
#: attributes is still loaded, but nothing is served for it in these namespaces.
_PLUGIN_ATTRIBUTE_BY_NAMESPACE = {
    'djanquiltdb.base': 'base',
    'djanquiltdb.decorators': 'decorators',
    'djanquiltdb.models': 'models',
    'djanquiltdb.utils': 'utils',
}


def served_plugin_module(namespace, name):
    """
    Returns the module that the plugin registered as ``name`` serves under ``namespace``, or None when no plugin serves
    that name there.

    The core modules and :class:`PluginImporter` all resolve names through this function, so plain attribute access
    and an import statement always give the same result. Names that start with an underscore are never served.
    """
    attribute = _PLUGIN_ATTRIBUTE_BY_NAMESPACE.get(namespace)

    if attribute is None or name.startswith('_'):
        return None

    plugin = load_plugin_by_name(name)

    return None if plugin is None else getattr(plugin, attribute, None)


class _AliasLoader(Loader):
    """
    A loader that serves a module that is already imported under a second name.

    ``create_module`` returns the existing module, so the plugin's module is not executed again and its classes exist
    only once. The import system then sets the spec of the alias on that module, so ``exec_module`` puts the original
    spec and loader back.
    """

    def __init__(self, module):
        self._module = module
        self._spec = module.__spec__
        self._loader = module.__loader__

    def create_module(self, spec):
        return self._module

    def exec_module(self, module):
        module.__spec__ = self._spec
        module.__loader__ = self._loader


class PluginImporter(MetaPathFinder):
    """
    Serves ``<core module>.<name>`` from the plugins, for each namespace in ``_PLUGIN_ATTRIBUTE_BY_NAMESPACE``.
    """

    def find_spec(self, name, path=None, target=None):
        for namespace in _PLUGIN_ATTRIBUTE_BY_NAMESPACE:
            prefix = f'{namespace}.'

            if not name.startswith(prefix):
                continue

            plugin_name, _, subpath = name[len(prefix) :].partition('.')
            module = served_plugin_module(namespace, plugin_name)

            if module is None:
                return None

            # A dotted name below the plugin name is served as the submodule of the plugin's own package, because
            # finding it through the __path__ of the alias would import a second copy of it.
            if subpath:
                submodule_name = f'{module.__name__}.{subpath}'

                try:
                    module = import_module(submodule_name)
                except ModuleNotFoundError as error:
                    # A finder returns None for a module that does not exist. An error about another module, which the
                    # submodule imports, is an error of the submodule, and is raised.
                    if error.name == submodule_name or submodule_name.startswith(f'{error.name}.'):
                        return None

                    raise

            return spec_from_loader(name, _AliasLoader(module))

        return None


def install_plugin_importer():
    """
    Adds :class:`PluginImporter` to ``sys.meta_path``, if it is not there yet.

    Each core module that serves a namespace calls this, because any one of them can be imported first.
    """
    if not any(isinstance(finder, PluginImporter) for finder in sys.meta_path):
        sys.meta_path.insert(0, PluginImporter())

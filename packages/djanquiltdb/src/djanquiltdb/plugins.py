"""
Discovery of library plugins.

A plugin is a separate distribution that registers itself under the ``djanquiltdb.plugins`` entry-point group and
carries glue for one declaration library. The registry is resolved lazily, on first use rather than at app-load time:
a project's declaration modules may be imported while the app registry is still assembling, before any
``AppConfig.ready()`` has run, and the decorator re-export in :mod:`djanquiltdb.decorators` has to be able to answer
then.
"""

from functools import cache
from importlib.metadata import entry_points

#: The entry-point group a plugin registers under. What name it registers is informational; the loaded object is what
#: counts. It exposes a ``decorators`` module, served through :mod:`djanquiltdb.decorators`, and an ``install()``
#: callable, run when the app registry is ready.
PLUGIN_GROUP = 'djanquiltdb.plugins'


@cache
def load_plugins():
    """
    Load every installed plugin, once per process.
    """
    return tuple(entry_point.load() for entry_point in entry_points(group=PLUGIN_GROUP))

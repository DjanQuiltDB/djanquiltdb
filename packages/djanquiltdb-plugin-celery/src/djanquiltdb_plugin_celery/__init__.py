"""
This package defines the plugin object, which DjanQuiltDB finds through the ``djanquiltdb.plugins`` entry-point group,
and exports the public API of the plugin. DjanQuiltDB serves the decorators as ``djanquiltdb.decorators.celery``.
"""

from types import SimpleNamespace

from djanquiltdb_plugin_celery import decorators
from djanquiltdb_plugin_celery.decorators import task_for_active_shard, task_for_node, task_for_shard

__version__ = '1.0.0'


def install():
    """
    Registers the plugin's system checks. DjanQuiltDB calls install() once the app registry is ready.
    """
    from djanquiltdb_plugin_celery import checks  # noqa: F401


plugin = SimpleNamespace(decorators=decorators, install=install)

__all__ = ['install', 'plugin', 'task_for_active_shard', 'task_for_node', 'task_for_shard']

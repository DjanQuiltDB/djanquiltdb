"""
This package defines the plugin object, which DjanQuiltDB finds through the ``djanquiltdb.plugins`` entry-point group,
and exports the public API of the plugin. DjanQuiltDB serves the decorators as ``djanquiltdb.decorators.celery``, and
the helpers as ``djanquiltdb.utils.celery``.
"""

from types import SimpleNamespace

from djanquiltdb_plugin_celery import decorators, utils
from djanquiltdb_plugin_celery.decorators import (
    task_for_active_shard,
    task_for_every_node,
    task_for_every_shard,
    task_for_node,
    task_for_shard,
)
from djanquiltdb_plugin_celery.utils import (
    aggregate_per_shard,
    fan_out,
    fan_out_to_nodes,
    fanout_shards,
    per_node_signatures,
    per_shard_signatures,
)

__version__ = '1.0.0'


def install():
    """
    Registers the plugin's system checks. DjanQuiltDB calls install() once the app registry is ready.
    """
    from djanquiltdb_plugin_celery import checks  # noqa: F401


plugin = SimpleNamespace(decorators=decorators, utils=utils, install=install)

__all__ = [
    'aggregate_per_shard',
    'fan_out',
    'fan_out_to_nodes',
    'fanout_shards',
    'install',
    'per_node_signatures',
    'per_shard_signatures',
    'plugin',
    'task_for_active_shard',
    'task_for_every_node',
    'task_for_every_shard',
    'task_for_node',
    'task_for_shard',
]

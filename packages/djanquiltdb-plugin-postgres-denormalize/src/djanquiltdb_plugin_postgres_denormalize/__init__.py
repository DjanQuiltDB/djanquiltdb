"""
This package defines the plugin object, which DjanQuiltDB finds through the ``djanquiltdb.plugins`` entry-point group,
and exports the public API of the plugin. DjanQuiltDB serves the helpers as ``djanquiltdb.utils.postgres_denormalize``.
"""

from types import SimpleNamespace

from djanquiltdb_plugin_postgres_denormalize import utils
from djanquiltdb_plugin_postgres_denormalize.utils import repair_every_shard, repair_shard, sharded_leaves, stale_rows

__version__ = '1.0.0'


def install():
    """
    DjanQuiltDB calls this once the app registry is ready. The plugin has nothing to set up: the system checks for the
    copies come from DjanQuiltDB and from django-postgres-denormalize itself.
    """

    pass


plugin = SimpleNamespace(utils=utils, install=install)

__all__ = ['install', 'plugin', 'repair_every_shard', 'repair_shard', 'sharded_leaves', 'stale_rows']

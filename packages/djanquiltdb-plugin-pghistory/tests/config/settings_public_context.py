"""
Settings for the run of the test suite in which pghistory's Context table is public, as the documentation recommends.
Everything else comes from config.settings.
"""

from djanquiltdb import ShardingMode

from config.settings import *  # noqa: F403
from config.settings import INSTALLED_APPS, QUILT_DB

# The triggers of the public and mirrored models of this app need a Context table that is not sharded.
INSTALLED_APPS = (*INSTALLED_APPS, 'unsharded_context_example')

QUILT_DB = {
    **QUILT_DB,
    'OVERRIDE_SHARDING_MODE': {**QUILT_DB['OVERRIDE_SHARDING_MODE'], ('pghistory',): ShardingMode.PUBLIC},
}

# The example apps give event models a foreign key to the Context table, to cover the normalized context.
SILENCED_SYSTEM_CHECKS = ['djanquiltdb_plugin_pghistory.W004']

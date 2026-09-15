"""
Settings for the run of the test suite in which pghistory's Context table is mirrored. Everything else comes from
config.settings_public_context.
"""

from djanquiltdb import ShardingMode

from config.settings_public_context import *  # noqa: F403
from config.settings_public_context import QUILT_DB

QUILT_DB = {
    **QUILT_DB,
    'OVERRIDE_SHARDING_MODE': {**QUILT_DB['OVERRIDE_SHARDING_MODE'], ('pghistory',): ShardingMode.MIRRORED},
}

# The example apps give event models a foreign key to the Context table, to cover the normalized context.
SILENCED_SYSTEM_CHECKS = ['djanquiltdb_plugin_pghistory.W003']

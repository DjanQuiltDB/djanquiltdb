"""
The suite runs once for each sharding mode of pghistory's Context table: sharded (config.settings), public
(config.settings_public_context) and mirrored (config.settings_mirrored_context). A test that only holds for some of
them runs only in those runs.
"""

from unittest import skipUnless

from django.conf import settings


def context_table_placement():
    """
    Returns the sharding mode that the settings of this run give pghistory's Context model.
    """
    return settings.QUILT_DB['OVERRIDE_SHARDING_MODE'][('pghistory',)]


def run_only_when_the_context_table_is(*modes):
    return skipUnless(
        context_table_placement() in modes,
        "pghistory's Context table is not {modes} in this run of the suite".format(
            modes=' or '.join(mode.name for mode in modes)
        ),
    )

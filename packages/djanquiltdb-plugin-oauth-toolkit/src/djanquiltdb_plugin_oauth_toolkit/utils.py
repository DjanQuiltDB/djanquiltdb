"""
The helpers of the plugin. DjanQuiltDB serves this module as ``djanquiltdb.utils.oauth_toolkit``, and the
``OAUTH2_PROVIDER`` settings name the token generators by their paths in that module.
"""

from djanquiltdb_plugin_oauth_toolkit.generators import (
    ShardSelectorError,
    active_shard_selector,
    active_shard_selectors,
    generate_access_token,
    generate_refresh_token,
)
from djanquiltdb_plugin_oauth_toolkit.identifiers import (
    generate_client_id_for_selector,
    generate_token_for_selector,
    parse_client_id,
    parse_token,
)

__all__ = [
    'ShardSelectorError',
    'active_shard_selector',
    'active_shard_selectors',
    'clear_expired_for_all_shards',
    'generate_access_token',
    'generate_client_id_for_selector',
    'generate_refresh_token',
    'generate_token_for_selector',
    'parse_client_id',
    'parse_token',
]


def clear_expired_for_all_shards(ignore_maintenance=False):
    """
    Runs the toolkit's ``clear_expired()`` in every active shard. ``clear_expired()`` deletes the expired tokens and
    grants of the shard that it runs in. A shard in maintenance is skipped unless ``ignore_maintenance`` is True, so
    that its expired tokens are deleted on a later run.

    A shard in which ``clear_expired()`` fails with a ``DatabaseError`` does not stop the other shards. After the last
    shard, the errors are raised together as an ``ExceptionGroup`` whose message names the shards that failed, and each
    error has a note that names its shard.

    :param bool ignore_maintenance: False by default. When True, ``clear_expired()`` also runs in the shards that are in
        maintenance.

    :returns: A tuple of two lists: the shards that ``clear_expired()`` ran in, and the shards that were skipped.
    """
    from django.db import DatabaseError
    from djanquiltdb.utils import State, StateException, for_each_shard, use_shard
    from oauth2_provider.models import clear_expired

    cleared_shards = []
    skipped_shards = []
    failed_shards = []
    errors = []

    def clear_expired_for_shard(shard):
        if shard.state != State.ACTIVE and not ignore_maintenance:
            skipped_shards.append(shard)
            return

        try:
            with use_shard(shard, active_only_schemas=not ignore_maintenance):
                clear_expired()
        except StateException:
            # The shard went into maintenance after the shard registry was read.
            skipped_shards.append(shard)
            return
        except DatabaseError as error:
            error.add_note('In shard {}.'.format(shard.alias))
            failed_shards.append(shard)
            errors.append(error)
            return

        cleared_shards.append(shard)

    for_each_shard(clear_expired_for_shard)

    if errors:
        raise ExceptionGroup(
            'clear_expired() failed in shard(s) {}. The other shards were cleared.'.format(
                ', '.join(shard.alias for shard in failed_shards)
            ),
            errors,
        )

    return cleared_shards, skipped_shards

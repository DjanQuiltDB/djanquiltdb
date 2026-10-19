"""
The helpers of the plugin. DjanQuiltDB serves this module as ``djanquiltdb.utils.oauth_toolkit``, and the
``OAUTH2_PROVIDER`` settings name the token generators by their paths in that module.
"""

from djanquiltdb_plugin_oauth_toolkit.generators import (
    ShardSelectorError,
    active_shard_selector,
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
    'generate_access_token',
    'generate_client_id_for_selector',
    'generate_refresh_token',
    'generate_token_for_selector',
    'parse_client_id',
    'parse_token',
]

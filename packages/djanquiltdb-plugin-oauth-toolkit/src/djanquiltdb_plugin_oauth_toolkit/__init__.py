"""
This package exports the public API of the plugin, and defines the plugin object that DjanQuiltDB finds through the
``djanquiltdb.plugins`` entry-point group. DjanQuiltDB serves the client id generator as
``djanquiltdb.base.oauth_toolkit``, the middleware as ``djanquiltdb.middleware.oauth_toolkit`` and the helpers as
``djanquiltdb.utils.oauth_toolkit``.
"""

from types import SimpleNamespace

from djanquiltdb_plugin_oauth_toolkit import base, middleware, utils
from djanquiltdb_plugin_oauth_toolkit.base import ShardSelectorClientIdGenerator
from djanquiltdb_plugin_oauth_toolkit.middleware import OAuthToolkitUseShardForMiddleware
from djanquiltdb_plugin_oauth_toolkit.utils import (
    ShardSelectorError,
    active_shard_selector,
    clear_expired_for_all_shards,
    generate_access_token,
    generate_client_id_for_selector,
    generate_refresh_token,
    generate_token_for_selector,
    parse_client_id,
    parse_token,
)

__version__ = '1.0.0'


def install():
    """
    Registers the system checks of the plugin, and connects ``refuse_foreign_client_id()`` as a ``pre_save`` receiver of
    the toolkit's application model. That receiver refuses a client id of another shard. DjanQuiltDB calls this function
    when the app registry is ready.
    """
    from django.db.models.signals import pre_save
    from oauth2_provider.settings import oauth2_settings

    from djanquiltdb_plugin_oauth_toolkit import checks  # noqa: F401
    from djanquiltdb_plugin_oauth_toolkit.generators import refuse_foreign_client_id

    pre_save.connect(
        refuse_foreign_client_id,
        sender=oauth2_settings.APPLICATION_MODEL,
        dispatch_uid='djanquiltdb_plugin_oauth_toolkit.refuse_foreign_client_id',
    )


plugin = SimpleNamespace(base=base, middleware=middleware, utils=utils, install=install)

__all__ = [
    'OAuthToolkitUseShardForMiddleware',
    'ShardSelectorClientIdGenerator',
    'ShardSelectorError',
    'active_shard_selector',
    'clear_expired_for_all_shards',
    'generate_access_token',
    'generate_client_id_for_selector',
    'generate_refresh_token',
    'generate_token_for_selector',
    'install',
    'parse_client_id',
    'parse_token',
    'plugin',
]

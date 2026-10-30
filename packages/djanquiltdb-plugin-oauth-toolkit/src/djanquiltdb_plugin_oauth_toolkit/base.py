"""
The classes of the plugin that a project refers to. DjanQuiltDB serves this module as
``djanquiltdb.base.oauth_toolkit``, and ``OAUTH2_PROVIDER['CLIENT_ID_GENERATOR_CLASS']`` names the client id generator
by its path in that module. The client id generator is defined in :mod:`djanquiltdb_plugin_oauth_toolkit.generators`.
"""

from djanquiltdb_plugin_oauth_toolkit.generators import ShardSelectorClientIdGenerator

__all__ = ['ShardSelectorClientIdGenerator']

============
Installation
============

Install through DjanQuiltDB's extra::

    pip install djanquiltdb[oauth-toolkit]

DjanQuiltDB finds the plugin through its entry point as soon as it is installed. Then set up django-oauth-toolkit as
`its documentation <https://django-oauth-toolkit.readthedocs.io/en/latest/install.html>`_ describes, with these changes.

The toolkit's models are sharded, so that each tenant has its own applications, grants and tokens. The project needs a
``MAPPING_MODEL``, because the shard selector is the mapping value of a tenant (see :doc:`identifiers`). This example
maps tenants onto shards by slug, so ``SHARD_SELECTOR_REGEX`` matches slugs:

.. code-block:: python

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.TenantShard',
        'SHARD_SELECTOR_REGEX': '[-a-z0-9]+',
        'OVERRIDE_SHARDING_MODE': {
            ('oauth2_provider',): ShardingMode.SHARDED,
        },
    }

Name the generators of the plugin in the settings of the toolkit:

.. code-block:: python

    OAUTH2_PROVIDER = {
        'CLIENT_ID_GENERATOR_CLASS': 'djanquiltdb.base.oauth_toolkit.ShardSelectorClientIdGenerator',
        'ACCESS_TOKEN_GENERATOR': 'djanquiltdb.utils.oauth_toolkit.generate_access_token',
        'REFRESH_TOKEN_GENERATOR': 'djanquiltdb.utils.oauth_toolkit.generate_refresh_token',
    }

Put the middleware of the plugin in ``MIDDLEWARE`` in place of ``UseShardForMiddleware``. Put it after
``SessionMiddleware``, because it reads the shard selector of the session too, and before the toolkit's
``OAuth2TokenMiddleware`` and ``OAuth2ExtraTokenMiddleware``, because they look up tokens:

.. code-block:: python

    MIDDLEWARE = [
        'django.contrib.sessions.middleware.SessionMiddleware',
        'djanquiltdb.middleware.oauth_toolkit.OAuthToolkitUseShardForMiddleware',
        'django.contrib.auth.middleware.AuthenticationMiddleware',
        'oauth2_provider.middleware.OAuth2TokenMiddleware',
    ]

The session backend of DjanQuiltDB (see :ref:`session_backend`) keeps each session in the shard of the tenant of the
user, with the same shard selector in the session key.

Add ``djanquiltdb_plugin_oauth_toolkit`` to ``INSTALLED_APPS``, before ``oauth2_provider``, so that the plugin's
``cleartokens`` command replaces the toolkit's (see :doc:`commands`):

.. code-block:: python

    INSTALLED_APPS = [
        ...
        'djanquiltdb_plugin_oauth_toolkit',
        'oauth2_provider',
        ...
    ]


Settings
--------

The optional ``QUILT_OAUTH_TOOLKIT`` setting is a dict whose keys override these defaults:

.. code-block:: python

    QUILT_OAUTH_TOOLKIT = {
        # The text at the start of every client id and token, before the shard selector.
        'IDENTIFIER_PREFIX': 'O',
        # The text after the shard selector of a client id, before its random part.
        'CLIENT_ID_DELIMITER': 'I',
        # The text after the shard selector of an access or refresh token, before its random part.
        'TOKEN_DELIMITER': 'T',
    }

A shard selector matches :ref:`QUILT_DB['SHARD_SELECTOR_REGEX'] <shard_selector_regex_setting>`, as the shard selector
of a session key does. See :doc:`identifiers` for the rules that the prefix and the delimiters follow. If you use the
device flow, read the warning on :doc:`identifiers` about the characters that a mapping value can have.


System checks
-------------

=============================================== =====================================================================
``djanquiltdb_plugin_oauth_toolkit.E001``       ``QUILT_OAUTH_TOOLKIT`` has a key that the plugin does not read.
``djanquiltdb_plugin_oauth_toolkit.E002``       ``QUILT_OAUTH_TOOLKIT`` is not a dict.
``djanquiltdb_plugin_oauth_toolkit.E003``       The prefix, a delimiter or the shard selector regex breaks a rule of
                                                :doc:`identifiers`.
``djanquiltdb_plugin_oauth_toolkit.E004``       ``oauth2_provider`` is not installed, the project has no
                                                ``MAPPING_MODEL``, or a model of the toolkit is not sharded.
``djanquiltdb_plugin_oauth_toolkit.E005``       ``CLIENT_ID_GENERATOR_CLASS``, ``ACCESS_TOKEN_GENERATOR`` or
                                                ``REFRESH_TOKEN_GENERATOR`` does not name a generator of the plugin.
``djanquiltdb_plugin_oauth_toolkit.E006``       ``MIDDLEWARE`` does not have ``OAuthToolkitUseShardForMiddleware`` or a
                                                subclass of it.
``djanquiltdb_plugin_oauth_toolkit.E007``       ``MIDDLEWARE`` has ``OAuth2TokenMiddleware`` or
                                                ``OAuth2ExtraTokenMiddleware`` before the middleware of the plugin, or
                                                ``SessionMiddleware`` after it.
``djanquiltdb_plugin_oauth_toolkit.E008``       ``OAUTH2_PROVIDER['CIMD_ENABLED']`` is on. The client id of a Client ID
                                                Metadata Document is a URL, which has no shard selector.
``djanquiltdb_plugin_oauth_toolkit.W001``       ``OAUTH2_PROVIDER['DCR_ENABLED']`` is on. Dynamic client registration
                                                registers an application in the shard of the session, and its
                                                registration access tokens have no shard selector.
``djanquiltdb_plugin_oauth_toolkit.W002``       ``INSTALLED_APPS`` does not list ``djanquiltdb_plugin_oauth_toolkit``
                                                before ``oauth2_provider``, so ``cleartokens`` is the command of the
                                                toolkit, which fails outside a shard.
=============================================== =====================================================================

E006 and E007 only look at the entries of ``MIDDLEWARE`` that import. Django reports an entry that does not import
when it loads the request handler.


The resource server mode
------------------------

With ``RESOURCE_SERVER_INTROSPECTION_URL``, the toolkit asks another server about a token that it does not know, and
stores the answer as a token in the active shard. The middleware names the shard from the token of the request, so this
mode only works when the other server issues tokens of this plugin, with the same settings.

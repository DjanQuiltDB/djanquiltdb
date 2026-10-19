=============
API reference
=============

Once the plugin is installed, DjanQuiltDB serves its API from inside ``djanquiltdb`` itself. It serves the client id
generator from ``djanquiltdb.base.oauth_toolkit``, the middleware and its sources from
``djanquiltdb.middleware.oauth_toolkit``, and the token generators and helpers from ``djanquiltdb.utils.oauth_toolkit``.
The ``MIDDLEWARE`` and ``OAUTH2_PROVIDER`` settings name them by these paths, and both ways of importing are supported::

    from djanquiltdb.utils import oauth_toolkit               # the namespace
    from djanquiltdb.utils.oauth_toolkit import parse_token   # a function, imported by name

The same names can also be imported from ``djanquiltdb_plugin_oauth_toolkit``, the distribution package. A type
checker or an IDE cannot follow the served namespaces, so point it at the modules that define them:
``djanquiltdb_plugin_oauth_toolkit`` ``.generators`` for the generators, ``.middleware`` for the middleware,
``.sources`` for the sources, and ``.identifiers`` for the helpers of the format.


Middleware
==========

.. currentmodule:: djanquiltdb.middleware.oauth_toolkit

.. autoclass:: OAuthToolkitUseShardForMiddleware
    :members: get_session_shard_selector, error_response

.. autodata:: SHARD_SELECTOR_SOURCES_BY_URL_NAME
    :no-value:


Sources
=======

.. currentmodule:: djanquiltdb.middleware.oauth_toolkit

.. autoclass:: FoundShardSelector

.. autoexception:: ShardSelectorRequestError

.. autofunction:: selector_from_bearer_header

.. autofunction:: selector_from_access_token_parameter

.. autofunction:: selector_from_basic_auth_header

.. autofunction:: selector_from_client_id_parameter

.. autofunction:: selector_from_path_client_id

.. autofunction:: selector_from_id_token_hint


Generators
==========

.. currentmodule:: djanquiltdb.base.oauth_toolkit

.. autoclass:: ShardSelectorClientIdGenerator
    :members: hash

.. currentmodule:: djanquiltdb.utils.oauth_toolkit

.. autofunction:: generate_access_token

.. autofunction:: generate_refresh_token

.. autofunction:: active_shard_selector

.. autoexception:: ShardSelectorError


Identifiers
===========

.. currentmodule:: djanquiltdb.utils.oauth_toolkit

.. autofunction:: generate_client_id_for_selector

.. autofunction:: parse_client_id

.. autofunction:: generate_token_for_selector

.. autofunction:: parse_token


Expired tokens
==============

.. currentmodule:: djanquiltdb.utils.oauth_toolkit

.. autofunction:: clear_expired_for_all_shards

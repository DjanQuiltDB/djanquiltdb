oauth-toolkit
=============
This plugin makes DjanQuiltDB and `django-oauth-toolkit <https://django-oauth-toolkit.readthedocs.io/>`_ work together.
Its client ids and tokens have the mapping value of their tenant in them. That is the value that the mapping table of
the project maps onto the shard of the tenant (see the mapping table in :doc:`/getting_started/overview`). Its
middleware enters that shard before the toolkit looks up a client id or a token. Install it through an extra of
``djanquiltdb`` itself::

    pip install djanquiltdb[oauth-toolkit]

Plugin release |oauth-toolkit-version|.

.. toctree::
    :maxdepth: 2

    installation
    identifiers
    middleware
    commands
    api
    changelog

For another OAuth2 or API token library, see :doc:`/guides/oauth2`.


Things to know
--------------

* The shard selector of a client id or a token is the mapping value of its tenant, such as the slug of the tenant, in
  plain text. It only names the shard in which to look up the identifier. The toolkit looks up a token by a checksum
  of the whole token, so a token whose shard selector is changed is not found in the other shard. See
  :doc:`identifiers`.
* A client id or a token keeps the mapping value that it was issued with. When the mapping value of a tenant changes,
  such as a slug that is renamed, its client ids and tokens name no shard any more. See :doc:`identifiers`.
* A request with a token of a tenant that has no mapping row gets a 401 from the plugin, and a request with a token
  that does not exist in the shard of an existing tenant gets the toolkit's own answer, such as a 403 from
  ``ProtectedResourceView``. So the responses tell whether a tenant exists. ``error_response()`` can give both the same
  answer. See :doc:`middleware`.
* The middleware replaces ``UseShardForMiddleware``. A request without a client id or a token runs in the shard of its
  session, and a request whose client id or token names another shard than its session gets a 400. See
  :doc:`middleware`.
* An application has to be instantiated inside the shard of its tenant, because the toolkit generates its client
  id when the application is instantiated. An application whose client id is set by hand to one of another shard
  cannot be saved. See :doc:`identifiers`.
* A client id is 42 characters plus the shard selector. The toolkit's ``DeviceGrant.client_id`` has room for 100
  characters, so a shard selector for the device flow can have up to 58 characters. See :doc:`identifiers`.
* The device flow relies on the pattern of your shard selectors. Its pages read the client id from a URL path with
  Django's ``slug`` converter, so while the device flow is configured, every mapping value has to keep to the
  characters that the warning on :ref:`QUILT_DB['SHARD_SELECTOR_REGEX'] <shard_selector_regex_setting>` strongly
  recommends. The plugin refuses to generate a client id for a mapping value with another character. See
  :doc:`identifiers`.
* The device code, the user code of the device flow and the authorization code have no shard selector, because each of
  them travels with a client id or is entered in a session. See :doc:`identifiers`.
* The middleware only reads a form-encoded body, as the OAuth RFCs ask. A client id or a token in a JSON or multipart
  body names no shard. See :doc:`middleware`.
* The plugin replaces the toolkit's ``cleartokens`` command with one that clears the expired tokens of every active
  shard. With ``--ignore-maintenance``, it also clears those of the shards in maintenance. The toolkit's own command
  fails, because the tables of the toolkit are sharded. List
  ``djanquiltdb_plugin_oauth_toolkit`` before ``oauth2_provider`` in ``INSTALLED_APPS``, so that Django runs the plugin's
  command. See :doc:`commands`.
* Client ID Metadata Documents (``CIMD_ENABLED``) are not supported, because their client ids are URLs. Dynamic client
  registration (``DCR_ENABLED``) registers an application in the shard of the session, and its registration access
  tokens have no shard selector. The resource server mode of the toolkit (``RESOURCE_SERVER_INTROSPECTION_URL``) only
  works with an authorization server that issues tokens of this plugin. See :doc:`installation`.
* A shard in maintenance gives a 503, through ``QUILT_DB['STATE_EXCEPTION_VIEW']`` when it is set. Point that setting at
  a view that renders JSON for API clients. See :doc:`middleware`.
* The plugin is tested with django-oauth-toolkit 3.4 and oauthlib 4.0.
* A type checker cannot follow the names that DjanQuiltDB serves. Point it at the modules that define them. See
  :doc:`api`.

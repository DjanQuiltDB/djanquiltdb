=====================
OAuth2 and API tokens
=====================

An OAuth2 provider or an API token library looks up every client id and token by its value. On a sharded project, the
tables of the library are sharded, so a request has to be inside the right shard before the library runs its first
query, and nothing in the request names a shard by itself. A token request has client credentials, and a request for a
resource has a token.

For django-oauth-toolkit, DjanQuiltDB has an official plugin, which does what this page describes. See
:doc:`/plugins/oauth-toolkit/index`. The other libraries have no plugin, so if your project uses one, build these pieces
yourself. This page outlines what to consider. It is not a recipe.


The shard selector
------------------

The mapping table of a project maps a value of each tenant, its mapping value, onto the shard that has the data of the
tenant (see :doc:`/guides/mapping_model`). The session backend of DjanQuiltDB shows the
pattern (see :ref:`session_backend`). A session key has the mapping value of its tenant in it, the shard selector, and
a middleware reads the shard selector back out of the key before the session is loaded. A client id or a token can have
a shard selector in the same way.

* Use the mapping value, and match it with :ref:`QUILT_DB['SHARD_SELECTOR_REGEX'] <shard_selector_regex_setting>`, so
  that the session keys and the other identifiers agree on what a shard selector is.
* Put a prefix before the shard selector and a delimiter after it, and pick a delimiter that the regex does not match,
  so that the end of the shard selector is clear.
* Keep the whole random part that the library generates. The shard selector adds length to an identifier, and no
  entropy.
* Check that the longer identifier fits every column and every URL path converter that has it, and that it only has
  characters that are allowed in Basic authentication and in URLs, such as the characters that the warning on
  :ref:`QUILT_DB['SHARD_SELECTOR_REGEX'] <shard_selector_regex_setting>` strongly recommends.
* A shard selector in plain text tells everyone who sees an identifier which tenant it belongs to, and a sequential
  mapping value tells them how many tenants there are.
* An identifier keeps the mapping value that it was issued with, so changing the mapping value of a tenant, such as
  renaming its slug, makes its identifiers name no shard.


Generating identifiers
----------------------

* Find every hook that the library has for generating a client id and a token, and every path that creates one without
  a hook, such as an admin form, a management command or a registration endpoint.
* A generator reads the shard selector of the active shard, so an identifier has to be created inside the shard of its
  tenant.
* Refuse to save an identifier that is set by hand with the shard selector of another shard, because no request could
  name its shard.
* An identifier that always travels with another identifier, such as an authorization code with its client id, needs
  no shard selector of its own. An identifier that a person types, such as the user code of the device flow, should
  stay short.


Reading identifiers back
------------------------

* List every place where the library reads a client id or a token (e.g. headers, a form body, the query string, a URL
  path or the claims of a JWT). Read the shard selector from the same places, in the same way, and leave the request
  unchanged for the library.
* Subclass ``BaseUseShardForMiddleware`` (see :ref:`use_shard_middleware`), and put it before the middleware of the
  library and before anything else that queries the database.
* A request can have a session and a client id or a token. Make them agree, because the user of the session is stored
  in the shard of the session.
* Treat a session whose mapping value the mapping table no longer has as ``UseShardForMiddleware`` does: let the
  request go on without a shard.
* The token that an endpoint acts on, such as the token to revoke or to introspect, is not a shard selector. Look it up
  in the shard of the client, where a token of another shard is not found.
* Answer an unknown shard selector with the error that the library gives an unknown client id or token. The mapping
  model raises ``DoesNotExist`` for a mapping value that it does not have.
* Only read a form-encoded body in a middleware. Parsing a multipart body there stops a view from setting its own
  upload handlers.


Signed tokens
-------------

A JWT can have the shard selector in a claim. A middleware that checks the signature before it enters the shard reads
a shard selector that nobody can change, which an identifier in plain text does not give. The refresh, verify and
revoke endpoints of a JWT library usually take the token in the body.


Hashed tokens
-------------

A library that stores only a hash of a token looks up the token by the hash of the whole token, so a shard selector in
the token keeps working. Never store or look up a token without its shard selector.


Sharded tables and cleanup
--------------------------

* Shard the tables of the library with ``OVERRIDE_SHARDING_MODE`` (see :doc:`/reference/settings`).
* A cleanup command of the library runs outside a shard, so its queries reach the public schema of the primary node,
  which has no sharded tables, and it fails. Replace it with a command of the same name in an app that comes before the
  library's in ``INSTALLED_APPS``, and run the cleanup in every active shard with ``for_each_shard()``.


Clients and providers
---------------------

A social login library, or another OAuth2 client, receives tokens that another server issues, so those tokens have no
shard selector. The shard of such a request comes from its session or its host.


Notes on other libraries
------------------------

Authlib
    Its Django integration has no models: the project defines them from Authlib's mixins, and generates client ids
    itself. A token generator gets the client of the request, which has the shard selector in its client id. Its
    resource protector only reads the ``Authorization`` header. Its JWT access tokens (RFC 9068) can have a claim with
    the shard selector.

djangorestframework-simplejwt
    It has no client ids. A custom ``TokenObtainPairSerializer.get_token()`` adds a claim with the shard selector, and a
    middleware can validate the token before it enters the shard and before the user is looked up. Shard its outstanding
    and blacklisted token tables.

django-allauth
    Its OpenID Connect provider has adapter methods that generate client ids and hash tokens. As a social login client,
    it receives tokens from other servers.

django-rest-knox
    A token is a prefix and a random part, and ``LoginView.get_token_prefix()`` can set the prefix for each request.
    The token is looked up by its first characters, and then compared by a digest of the whole token.

Django REST framework's TokenAuthentication
    A key is 40 characters, so a shard selector needs a token model of the project's own with a longer key.

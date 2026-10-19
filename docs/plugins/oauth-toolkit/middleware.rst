==========
Middleware
==========

``OAuthToolkitUseShardForMiddleware`` enters the shard of each request, as ``UseShardForMiddleware`` does. Put it in
``MIDDLEWARE`` in place of ``UseShardForMiddleware``. It reads the shard selector from the client id or the token of the
request, before the toolkit looks up the client id or the token. The middleware never changes the request, so the
toolkit reads the same client id or token afterwards.


Where the shard selector comes from
-----------------------------------

On each URL of the toolkit, the middleware reads the shard selector from the places where the toolkit reads a client id
or a token on that URL. The URLs are known by their names in the ``oauth2_provider`` namespace.

.. list-table::
    :header-rows: 1
    :widths: 45 55

    * - URL names
      - Sources, in this order
    * - ``token``, ``revoke-token``, ``device-authorization``
      - The client id of the Basic authorization header, and the ``client_id`` parameter
    * - ``introspect``
      - The client id of the Basic authorization header, the ``client_id`` parameter, the Bearer authorization header,
        and the ``access_token`` parameter
    * - ``authorize``
      - The ``client_id`` parameter
    * - ``device-confirm``, ``device-grant-status``, ``dcr-register-management``
      - The client id in the URL path
    * - ``rp-initiated-logout``
      - The ``client_id`` parameter, and the audience of the ``id_token_hint`` parameter
    * - ``device``, the application and token management pages, ``dcr-register``
      - The Bearer authorization header
    * - The discovery, JWKS and metadata documents
      - None
    * - Every other URL of the toolkit and of the project
      - The Bearer authorization header, and the ``access_token`` parameter

A parameter comes from the form-encoded body of a POST, and from the query string of another method. The
``access_token`` parameter is only read when the request has no ``Authorization`` header, as oauthlib reads it, and it
is not read from the query string when ``COMPLIANT_BCP_RFC9700_ACCESS_TOKEN_TRANSPORT`` is on. The audience of an
``id_token_hint`` is read without checking the signature, as the toolkit reads it, and the toolkit then checks the
signature inside the shard.

The token that an endpoint acts on is not a source (e.g. the token to revoke or to introspect, a refresh token, an
authorization code or a device code). The toolkit looks up such a token in the shard of the client, and a token of
another shard is not found there. Introspecting it gives ``{"active": false}``, revoking it gives an empty 200, and
refreshing with it gives ``invalid_grant``, as the RFCs ask for a token that the client does not know.


The session
-----------

A request without a client id or a token runs in the shard of its session. The middleware reads the shard selector of
the session from the attribute of the session that ``QUILT_DB['SESSION_SHARD_SELECTOR_KEY']`` names, as
``UseShardForMiddleware`` does.

When a request has a session and a client id or a token, both have to name the same shard. The user of the session is
stored in the shard of the session, so a request in another shard would act for another user of that shard.

A session whose shard selector the mapping table no longer has leaves the request without a shard, as with
``UseShardForMiddleware``, and ``djanquiltdb.sessions`` reads such a session as empty. On ``token``, ``revoke-token``
and ``introspect``, which need a shard, such a request gets the error of a request without a client id.


Errors
------

The middleware answers a request that names no shard that it can run in, as the toolkit answers a client id or a token
that it does not know:

.. list-table::
    :header-rows: 1
    :widths: 60 10 30

    * - Request
      - Status
      - ``error``
    * - A token that is not of the format, or names a tenant that has no mapping row
      - 401
      - ``invalid_token``
    * - A client id on ``token``, ``revoke-token`` or ``introspect`` that is not of the format, or names a tenant that
        has no mapping row
      - 401
      - ``invalid_client``
    * - A client id on another URL that is not of the format, or names a tenant that has no mapping row
      - 400
      - ``invalid_request``
    * - Two sources, or a source and the session, that name different shards
      - 400
      - ``invalid_request``
    * - ``token`` or ``revoke-token`` without a client id
      - 401
      - ``invalid_client``
    * - ``introspect`` without credentials or a token
      - 403
      - ``access_denied``
    * - A tenant whose shard or mapping row is in maintenance
      - 503
      - ``QUILT_DB['STATE_EXCEPTION_VIEW']``

The body is JSON with ``error`` and ``error_description``, and a 401 has the ``WWW-Authenticate`` header that oauthlib
sends.

A request for a URL of the project with a Bearer value of another kind, such as a token of another identity provider,
gets the 401 too. To avoid that, subclass the middleware and read other sources on the URLs of the project.


Hooks
-----

A subclass can change three things:

``error_response(request, error)``
    Returns the response for an error. ``error`` has the attributes ``error``, ``status_code``, ``description`` and
    ``source``.

``get_session_shard_selector(request)``
    Returns the shard selector of the session as a string, or None.

``shard_selector_sources_by_url_name`` and ``default_shard_selector_sources``
    The sources of each URL name of the toolkit, and of every other URL. Each source is a function of the request and
    of its resolver match. ``djanquiltdb.middleware.oauth_toolkit`` serves the sources of the plugin (see :doc:`api`).

.. code-block:: python

    from django.shortcuts import render
    from djanquiltdb.middleware.oauth_toolkit import OAuthToolkitUseShardForMiddleware


    class ApiUseShardForMiddleware(OAuthToolkitUseShardForMiddleware):
        def error_response(self, request, error):
            return render(request, 'errors/no_shard.html', {'error': error}, status=error.status_code)


The admin shard switcher
------------------------

The admin shard switcher (see :doc:`/contrib/admin_shard_switcher`) has its own middleware. Put it after this middleware
and after ``AuthenticationMiddleware`` in ``MIDDLEWARE``, as you would put it after ``UseShardForMiddleware``. It enters
the shard that the admin picks for the admin's URLs.

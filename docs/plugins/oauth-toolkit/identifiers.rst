=====================
Client ids and tokens
=====================

The mapping table of a project maps a value of each tenant, its mapping value, onto the shard that has the data of the
tenant (see :doc:`/guides/mapping_model`). The examples on this page use the mapping model ``TenantShard`` of the
mapping model guide, which maps the slug of each tenant onto its shard, with ``SHARD_SELECTOR_REGEX`` set to
``'[-a-z0-9]+'``.

A client id or a token of the plugin has the mapping value of its tenant in it, which this documentation calls the
shard selector. A session key of the ``djanquiltdb.sessions`` backend has a shard selector too. The identifier has four
parts: a prefix, the shard selector, a delimiter and a random part. For the tenant ``acme``, with the default settings:

=================== ======================================================== ===============================
Identifier          Format                                                   Example
=================== ======================================================== ===============================
Client id           ``O``, the shard selector, ``I``, 40 random characters   ``OacmeIxM2k...``
Access token        ``O``, the shard selector, ``T``, 30 random characters   ``OacmeT8fQz...``
Refresh token       ``O``, the shard selector, ``T``, 30 random characters   ``OacmeTbW1r...``
=================== ======================================================== ===============================

The random parts have the lengths that the toolkit and oauthlib generate, so the shard selector adds length to an
identifier, and the random part keeps all of its entropy.

The toolkit stores and looks up an identifier as a whole, and it looks up a token by a checksum of the whole token. The
shard selector is not signed, because it only names the shard in which to look up the identifier. When someone changes
the shard selector of a token, the toolkit does not find the token in the other shard, and answers as it answers any
unknown token.

The device code, the user code of the device flow and the authorization code have no shard selector. A device code and
an authorization code travel with the client id, and a user enters a user code while logged in, in the shard of the
session. A user code is typed by a person, so it is short on purpose.


Creating an application
-----------------------

The toolkit generates a client id when an application is instantiated without one, so instantiate an application inside
the shard of its tenant. ``use_shard_for()`` enters the shard of a mapping value:

.. code-block:: python

    from djanquiltdb.utils import use_shard_for
    from oauth2_provider.models import get_application_model

    with use_shard_for('acme'):
        application = get_application_model().objects.create(
            name='Integration',
            client_type='confidential',
            authorization_grant_type='client-credentials',
        )

Outside a shard, instantiating an application raises ``ShardSelectorError``. Inside a shard that ``use_shard()``
entered, the shard selector is the mapping value of the only mapping row of the shard, and a shard with more than one
mapping row has no shard selector.

An application cannot be saved with a client id whose shard selector is not the one of the active shard. The admin and
the toolkit's ``createapplication --client-id`` allow a client id that is set by hand, and the plugin refuses such a
client id when the application is saved, because no request could name its shard.

The toolkit's application registration pages run in the shard of the session of the user, so they generate client ids
of that shard.


The settings of the format
--------------------------

``QUILT_OAUTH_TOOLKIT`` sets the prefix and the two delimiters, and :ref:`QUILT_DB['SHARD_SELECTOR_REGEX']
<shard_selector_regex_setting>` sets the regex that a shard selector matches. The settings follow these rules, which the
check ``djanquiltdb_plugin_oauth_toolkit.E003`` reports:

* The prefix and the delimiters are strings that are not empty.
* The prefix and the delimiters only have the characters ``[-A-Za-z0-9_]``. The toolkit reads the client id of the
  device flow from a URL path with Django's ``slug`` converter, and Basic authentication separates the client id from
  the secret at a colon.
* The shard selector regex does not match a delimiter, so that the end of a shard selector is clear. With the default
  delimiters ``I`` and ``T``, a regex of upper case letters needs other delimiters.

.. warning::

    The device flow relies on the pattern of your shard selectors. The toolkit reads the client id of its device flow
    pages from a URL path with Django's ``slug`` converter. If your project uses the device flow, which it does when
    ``OAUTH2_PROVIDER['OAUTH_DEVICE_VERIFICATION_URI']`` is set, limit every mapping value to the characters that the
    warning on :ref:`QUILT_DB['SHARD_SELECTOR_REGEX'] <shard_selector_regex_setting>` strongly recommends. For the
    device flow this is required, not only recommended.

    While the device flow is configured, the plugin refuses to generate a client id for a mapping value with another
    character, and raises ``ImproperlyConfigured`` with the reason, so an application of that tenant cannot be created.
    The check ``djanquiltdb_plugin_oauth_toolkit.E003`` cannot read a set of characters off a regex, so the mapping
    value is only checked when a client id is generated for it. Every other flow sends the client id percent-encoded in
    a form body, a query string or a Basic authorization header, and works with any mapping value.

A token travels in a Bearer authorization header, which RFC 6750 limits to letters, digits and the characters
``-._~+/``, with ``=`` only at the end. A mapping value with another character, such as a space, a colon or ``@``,
gives tokens outside that limit, which a client or a proxy can refuse to send. The characters that the warning on
:ref:`QUILT_DB['SHARD_SELECTOR_REGEX'] <shard_selector_regex_setting>` strongly recommends are inside that limit.

The client ids and tokens that a project has issued keep their format and their shard selector. Changing a setting
makes the identifiers of the old format name no shard, and so does changing the mapping value of a tenant, such as
renaming its slug.

.. _session_backend:

========
Sessions
========

If your sharded project uses sessions, use DjanQuiltDB's session backend, ``djanquiltdb.sessions``, which keeps each
session in the shard of its tenant. :doc:`/getting_started/installation` includes its settings; this guide describes
them in full.

Django's session backends that use the database keep every session in one table. In a sharded project, the session is
what tells the project which shard a request belongs to, so the session is read before any shard is active. A session
table in the shards cannot be read then, and a session table outside them keeps the sessions of every tenant in one
place, on one node. DjanQuiltDB refuses Django's ``cached_db`` backend at startup when the user model is sharded or
mirrored.


How a session finds its shard
-----------------------------

Each session has a shard selector: the mapping value of the tenant whose shard the session is in, such as the slug of
the tenant. The backend puts the selector in the session key. A session key is the text
``S<selector>K<random part>``, signed with Django's signing module and the salt ``djanquiltdb.sessions``, so the backend
reads the selector from the cookie alone, before any shard is active, and nobody can change the selector without
breaking the signature. The signature expires after ``SESSION_COOKIE_AGE``, as the cookie does.

The backend enters ``use_shard_for(selector)`` for every read and write of a session. The selector is a mapping value,
not the id of a shard, so the backend needs a mapping model (see :doc:`/guides/mapping_model`).

A session key that the backend cannot use reads as a new, empty session, as Django's database backend reads a key that
it does not find: a key whose signature is wrong or has expired, a key that holds no selector that matches
``SHARD_SELECTOR_REGEX``, such as the keys made before that setting or ``SESSION_KEY_DELIMITER`` changed, and a key
whose selector the mapping table no longer has. The visitor is anonymous, and Django's ``SessionMiddleware`` deletes the
cookie.

The backend extends Django's ``cached_db`` backend, and reads sessions through the cache as that backend does. A session
key is unique across shards, because it holds the selector, so the cache needs no changes.

The async methods of the session, such as ``aget()`` and ``aflush()``, raise ``NotImplementedError``: Django's backend
reads and writes the database in them without the sync methods that enter the shard (see :doc:`/guides/async`).


.. _session_backend_setup:

Setting it up
-------------

The backend stores sessions in a model of the project: a sharded subclass of ``BaseQuiltSession``, which is Django's
``AbstractBaseSession`` with room for the longer session keys of the backend::

    # myapp/models.py
    from djanquiltdb.decorators import sharded_model
    from djanquiltdb.models import BaseQuiltSession


    @sharded_model()
    class QuiltSession(BaseQuiltSession):
        class Meta:
            app_label = 'myapp'

Run ``python manage.py makemigrations`` to create its table. Then name it in ``QUILT_SESSIONS``, and make the backend
the ``SESSION_ENGINE``::

    # settings.py
    from djanquiltdb import ShardingMode

    INSTALLED_APPS = [
        ...
        'django.contrib.sessions',
        ...
    ]

    SESSION_ENGINE = 'djanquiltdb.sessions'

    QUILT_SESSIONS = {
        'SESSION_MODEL': 'myapp.models.QuiltSession',
    }

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.TenantShard',
        # The mapping model of this example maps tenants by slug. See "The format of the shard selector" below.
        'SHARD_SELECTOR_REGEX': '[-a-z0-9]+',
        'OVERRIDE_SHARDING_MODE': {
            ...
            ('sessions',): ShardingMode.SHARDED,
        },
    }

    MIDDLEWARE = [
        ...
        'django.contrib.sessions.middleware.SessionMiddleware',
        'djanquiltdb.middleware.UseShardForMiddleware',
        ...
    ]

Keep ``django.contrib.sessions`` in ``INSTALLED_APPS``: Django's ``clearsessions`` command is part of it (see
:ref:`expired_sessions`). The backend does not use the ``Session`` model of that app, but every model needs a sharding
mode (the system check ``djanquiltdb.E004``, see :doc:`/reference/checks`). ``('sessions',): ShardingMode.SHARDED``
gives it one, and leaves an empty table in each shard.

A project that moves to DjanQuiltDB while it still uses Django's database session backend can keep the table of that
backend on one node with ``single_node_unsharded()`` (see :doc:`/guides/sharding_modes`) until it switches.

:doc:`/reference/settings` describes the settings of ``QUILT_SESSIONS``.


Setting the shard selector at login
-----------------------------------

A new session has no selector until you give it one. Set it when the user logs in, before ``login()``::

    # views.py
    from django.contrib.auth import authenticate, login

    from djanquiltdb.utils import use_shard_for


    def login_view(request):
        tenant_slug = ...  # For example from a field of the login form, or from the subdomain.

        with use_shard_for(tenant_slug):
            user = authenticate(request, username=..., password=...)
            if user is not None:
                request.session.shard_selector = tenant_slug
                login(request, user)
        ...

``login()`` gives the session a new key, and the backend builds that key with the selector in it, so the selector has to
be set first.

``shard_selector`` is an attribute of the session, not an item in it: ``request.session['shard_selector']`` is an
ordinary session value, which the backend does not read. Setting the attribute raises ``ImproperlyConfigured`` when the
value does not match ``SHARD_SELECTOR_REGEX``. The selector that the backend reads back from a session key is text, also
when the mapping values are numbers: ``42`` comes back as ``'42'``.

The backend reads and writes each session in the shard of its selector, so it cannot save a session that has none: the
save raises the ``DoesNotExist`` of the mapping model. Do not write to the session of a visitor before the selector is
set. That includes Django features that write to the session of an anonymous visitor, such as ``CSRF_USE_SESSIONS`` and
the ``SessionStorage`` of ``django.contrib.messages``.


The format of the shard selector
--------------------------------

The selector has to match ``SHARD_SELECTOR_REGEX`` in ``QUILT_DB``, which is ``'[0-9]+'`` by default and matches the
values of a mapping field that has integer ids. Change it when the mapping values are something else. For the slugs of
tenants, for example::

    QUILT_DB = {
        ...
        'SHARD_SELECTOR_REGEX': '[-a-z0-9]+',
    }

Every identifier that has a shard selector, such as a session key, uses this one regular expression. See
:ref:`shard_selector_regex_setting` for the characters that mapping values are strongly recommended to keep to, and for
the deprecated ``QUILT_SESSIONS['SHARD_SELECTOR_REGEX']``.

That regular expression matches lowercase slugs. A mapping field whose slugs can have uppercase letters needs one that
matches those, and then another delimiter than ``K`` too.

``SESSION_KEY_DELIMITER`` separates the selector from the random part of a session key, and is ``'K'`` by default. The
backend has to tell where the selector ends, so the regular expression must not match the delimiter, or a selector
followed by the delimiter. A regular expression that matches ``K`` needs another delimiter. The backend raises
``ImproperlyConfigured`` when the two overlap.


Selecting the shard of a request
--------------------------------

``djanquiltdb.middleware.UseShardForMiddleware`` enters the shard of the selector of the session of each request (see
:ref:`use_shard_middleware`). It reads the attribute of the session that ``SESSION_SHARD_SELECTOR_KEY`` in ``QUILT_DB``
names, which is ``shard_selector`` by default. A session without a selector leaves the request without a shard, and so
does a session whose selector the mapping table no longer has.

The admin shard switcher lets staff enter another shard than the one of their session (see
:doc:`/contrib/admin_shard_switcher`).


.. _expired_sessions:

Expired sessions
----------------

Django's ``clearsessions`` command works with this backend: it walks every ``ACTIVE`` shard and deletes the expired
sessions in it. Shards that are in maintenance, because they are being moved, are skipped, and their expired sessions
are deleted on the next run.


Moving data
-----------

``move_data_to_shard`` moves the rows that it finds from the root object through relations (see
:doc:`/operations/move_data_to_shard`). A session has no relation to them, so the sessions of a moved mapping value stay
in the source shard, where ``clearsessions`` deletes them once they expire. From then on the backend looks for the
sessions of that value in the target shard, so its users have to log in again.

``move_shard_to_node`` moves the whole schema of a shard, sessions included, so its users stay logged in.

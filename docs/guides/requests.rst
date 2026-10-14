=======================
Requests and middleware
=======================

This chapter is about Django views. For views in the database, see :doc:`/database_objects/views`.

To prevent the need to use ``with use_shard():`` in every view your project has, this library provides a middleware to
do that for you.

The ``UseShardMiddleware`` will enable a ``use_shard`` context manager in ``process_request`` and close it in
``process_response`` or ``process_exception``.

.. image:: /images/view_flow.svg
   :alt: DjanQuiltDB request flow
   :align: center

Since it inherits ``ExceptionMiddlewareMixin`` it will return a 503 response if the shard required is in a non-active
state.


.. _exception_middleware:

Handling unavailable shards
---------------------------

The ``djanquiltdb.middleware.ExceptionMiddlewareMixin`` class allows you to deal with exceptions raised by accessing
unavailable shards. It is not required, but recommended to add it to the middleware settings.

The middleware returns a 503 response when a shard availability error or a database connection error pops up during
view processing. You can also tell it to render a specific view instead. To do that set ``STATE_EXCEPTION_VIEW``
(for shard state errors) and/or ``CONNECTION_EXCEPTION_VIEW`` (for connection errors) in the ``QUILT_DB`` setting to
a view of your choice e.g.::

    MIDDLEWARE = [
        (...)
        'djanquiltdb.middleware.ExceptionMiddlewareMixin',
        (...)
    ]

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'STATE_EXCEPTION_VIEW': 'myapp.views.ShardExceptionView',
        'CONNECTION_EXCEPTION_VIEW': 'myapp.views.ConnectionExceptionView',
    }

.. _use_shard_middleware:

Selecting a shard per request
-----------------------------

The ``djanquiltdb.middleware.BaseUseShardMiddleware`` class extends ``ExceptionMiddlewareMixin`` and adds the option
to wrap views in a ``use_shard`` context manager. This prevents the need to take note of sharding in each of your
views.

How the middleware determines which shard to use is up to you however. To use the ``BaseUseShardMiddleware`` you have
to extend it and implement its ``get_shard_id()`` method yourself.

Don't forget you can assign your own view as error page like in :ref:`exception_middleware`.

.. code-block:: python

    # settings.py
    MIDDLEWARE = [
        (...)
        'django.contrib.sessions.middleware.SessionMiddleware',
        'middleware.UseShardMiddleware',
        (...)
    ]

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'STATE_EXCEPTION_VIEW': 'myapp.views.ShardExceptionView'
    }

    # middleware.py
    class UseShardMiddleware(BaseUseShardMiddleware):
        def get_shard_id(self, request):
            # A common way is to alter the login flow to set the shard_id in the session.
            return request.session.get('shard_id')


    # views.py
    from django.contrib.auth import views as django_auth_views

    # Example way of obtaining a shard_id is to add an additional input field in the login form.
    # Use the additional slug field to get the shard_id and save that to the session.
    def login_view(request):
        response = django_auth_views.login(request=request)

        if request.user.is_authenticated():
            slug = request.POST.get('slug')
            request.session['shard_id'] = get_shard_for_slug(slug).id

        return response

If you are using a mapping model in your project, you can also use the ``BaseUseShardForMiddleware`` in the same
fashion as the ``BaseUseShardMiddleware``. The only difference is that you now have to define the
``get_mapping_value()`` method instead of the ``get_shard_id()``.

.. code-block:: python

    # settings.py
    MIDDLEWARE = [
        (...)
        'django.contrib.sessions.middleware.SessionMiddleware',
        'middleware.UseShardForMiddleware',
        (...)
    ]

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.ShardMappingModel',
        'STATE_EXCEPTION_VIEW': 'myapp.views.ShardExceptionView',
    }

    # middleware.py
    class UseShardForMiddleware(BaseUseShardForMiddleware):
        def get_mapping_value(self, request):
            # A common way is to alter the login flow to set the mapping value in the session.
            return request.session.get('mapping_value')


.. _session_backend:

Storing the shard selector in the session
-----------------------------------------

If you want to store the value on the session, you can also use ``djanquiltdb.middleware.UseShardMiddleware`` or
``djanquiltdb.middleware.UseShardForMiddleware`` directly; these assume the shard selector value is stored on the
session with the key defined in the ``SESSION_SHARD_SELECTOR_KEY`` setting. The default is ``shard_selector``, which is
compatible with the standard ``djanquiltdb.sessions`` backend. To configure this session backend, you need to make
the following changes (assuming here that the session storage is under an app called ``users``):

.. code-block:: python

    # settings.py
    SESSION_ENGINE = 'djanquiltdb.sessions'

    QUILT_SESSIONS = {
        # Required: Configure the session model
        'SESSION_MODEL': 'users.models.QuiltSession',
        # Optional: Regex pattern for validating shard selector values
        'SHARD_SELECTOR_REGEX': '[0-9]+',
        # Optional: Delimiter used in session keys to separate shard selector from session key (default: 'K')
        'SESSION_KEY_DELIMITER': 'K',
    }

    # Optional: Customize the session key used by middleware to store shard selector (default: 'shard_selector')
    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'SESSION_SHARD_SELECTOR_KEY': 'shard_selector',  # Optional, defaults to 'shard_selector'
    }

    MIDDLEWARE = [
        (...)
        'django.contrib.sessions.middleware.SessionMiddleware',
        'djanquiltdb.middleware.UseShardForMiddleware',
        (...)
    ]

    # users/models.py
    from djanquiltdb.models import BaseQuiltSession

    @sharded_model()
    class QuiltSession(BaseQuiltSession):
        class Meta:
            app_label = 'users'

You would only need to change the ``SHARD_SELECTOR_REGEX`` if the primary key of your shard or mapping value is not a
number. For example, if your shard selectors are UUIDs, you might use:
``'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'``. If your regular expression supports string values
that may include ``K``, you also have to adjust the ``SESSION_KEY_DELIMITER`` to make sure it is a value that can't be
matched by the regular expression.

Django's ``clearsessions`` management command works with this backend: it walks every ``ACTIVE`` shard and deletes the
expired sessions it holds. Shards that are in maintenance (mid-move, being purged) are skipped and picked up on the
next run.

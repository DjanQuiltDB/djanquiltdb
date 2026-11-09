Admin shard switcher
====================
The ``djanquiltdb.contrib.quilt_admin`` package adds a shard switcher dropdown to the Django admin interface,
allowing administrators to easily switch between shards while browsing the admin.

.. note::

    This module does not provide a sharding-aware login method. You can either rely on your application's own login
    page for this after you've made it sharding-aware, or implement a custom one for the admin, as shown below.

::

    from django.contrib import admin
    from django.contrib.auth.forms import AuthenticationForm
    from django.core.exceptions import ValidationError
    from django.db import OperationalError

    from djanquiltdb.utils import StateException, use_shard_for
    from myapp.models import TenantShard

    class CustomAdminAuthenticationForm(AuthenticationForm):
        shard_identifying_field = ...

        def clean(self):
            shard_selector = ...

            try:
                # Assuming you're using the djanquiltdb.sessions backend, see the Sessions guide
                if self.request is not None:
                    self.request.session.shard_selector = shard_selector

                    with use_shard_for(shard_selector):
                        return super().clean()
            except (TenantShard.DoesNotExist, StateException, OperationalError):
                raise ValidationError(self.error_messages['invalid_login'], code='invalid_login')

    admin.site.login_form = CustomAdminAuthenticationForm
    admin.site.login_template = 'admin/custom_admin_login.html'

To enable this feature, add the package to your ``INSTALLED_APPS``::

    INSTALLED_APPS = (
        'django.contrib.admin',
        ...
        'djanquiltdb.contrib.quilt_admin',
        ...
    )

Add the appropriate middleware after the base ``UseShardMiddleware``/``UseShardForMiddleware`` and after the
``AuthenticationMiddleware``. If you are working with a mapping model, this should be
``MappingValueAdminOverrideMiddleware``, otherwise ``ShardIdAdminOverrideMiddleware``::

    MIDDLEWARE = [
        (...)
        'django.contrib.sessions.middleware.SessionMiddleware',
        'djanquiltdb.middleware.UseShardForMiddleware',
        (...)
        'django.contrib.auth.middleware.AuthenticationMiddleware',
        'djanquiltdb.contrib.quilt_admin.middleware.MappingValueAdminOverrideMiddleware',
        (...)
    ]


Add the context processor to your template settings::

    TEMPLATES = [
        {
            ...
            'OPTIONS': {
                'context_processors': [
                    ...
                    'djanquiltdb.contrib.quilt_admin.context_processors.admin_shard_context',
                ],
            },
        },
    ]

Include the admin URLs in your main URLconf. The URLs should be included under the ``admin/`` path::

    from django.contrib import admin
    from django.urls import include, path

    urlpatterns = [
        path('admin/', include('djanquiltdb.contrib.quilt_admin.urls')),
        path('admin/', admin.site.urls),
        ...
    ]

.. note::

    The order matters: include the shard switcher URLs first, then include ``admin.site.urls``. The admin ends its URLs
    with a pattern that matches every other path under ``admin/``, so it answers ``admin/switch-shard/`` with a 404 when
    you include ``admin.site.urls`` first.

Once installed, a dropdown will appear in the top right corner of the Django admin interface (next to the user links).
The dropdown lists all available shards and allows you to switch between them. The selected shard is stored in the
session and will be used by your shard middleware for subsequent requests.

The dropdown shows:

- Shard alias
- Node name and schema name (e.g., ``default|shard_1_schema``)
- Current selection is highlighted

The dropdown is sorted by the mapping value, or by the shard alias if your project has no mapping model. To sort it by
something else, set ``QUILT_ADMIN['SHARD_SWITCHER_SORT_KEY']`` to one of these:

- ``'mapping_value'`` (the default): the value of the mapping field, such as the slug of a tenant. If your project has
  no mapping model, the shard alias is used.
- ``'shard_alias'``: the alias of the shard.
- ``'schema_name'``: the schema name of the shard.
- A function, or its dotted path, that returns the sort key of an option.

The three built-in choices ignore the case of strings, and compare other values, such as integer mapping values, as
they are. An option whose sort field is empty comes last, such as a mapping row without a shard when you sort by shard
alias. The shard switcher calls your own function as ``fn(value, shard, mapping)`` for each option, with the same
arguments as ``format_override_option()`` of the shard selector class. If your project has no mapping model, ``value``
is the id of the shard and ``mapping`` is ``None``::

    # myapp/admin.py
    def shard_state_sort_key(value, shard, mapping):
        return (shard.state, shard.alias.casefold())

    # settings.py
    QUILT_ADMIN = {
        'SHARD_SWITCHER_SORT_KEY': 'myapp.admin.shard_state_sort_key',
    }

Selecting no shard clears the shard selection and returns to the default behavior.

The shard switcher lists every mapping by default. To limit a user to some of the tenants, for example by the user's
permissions or by a field of the mapping, subclass the shard selector class, override
``switchable_mappings(request, queryset)``, and point ``QUILT_ADMIN['SHARD_SELECTOR_CLASS']`` at the subclass.
``switchable_mappings()`` receives a queryset of all mappings and returns a queryset of the mappings that the user of
the request may switch to. While it runs, ``request.user`` is the logged-in user, also when the admin shows another
tenant, and the user's own shard is active, so a query on a sharded model reads the data of the user's own shard. Return
a queryset without a slice, because the admin filters and orders it::

    # myapp/admin.py
    from djanquiltdb.contrib.quilt_admin.shard_selector import AdminShardSelector

    class RegionAdminShardSelector(AdminShardSelector):
        @classmethod
        def switchable_mappings(cls, request, queryset):
            if request.user.is_superuser:
                return queryset
            return queryset.filter(region=request.user.region)

    # settings.py
    QUILT_ADMIN = {
        'SHARD_SELECTOR_CLASS': 'myapp.admin.RegionAdminShardSelector',
    }

The shard switcher lists only the mappings that ``switchable_mappings()`` returns. The switch view responds with 403
Forbidden to a mapping value that exists but that ``switchable_mappings()`` does not return.
``MappingValueAdminOverrideMiddleware`` responds with 403 Forbidden to an admin request for such a mapping value,
whether the value comes from the session or from a subclass that overrides ``get_mapping_value()``, so a user who loses
access to a tenant can neither see nor change its data from the next request on. When the value comes from the session,
the middleware also clears the override and adds a message that says why, so the next admin page opens in the user's
own shard. A request to switch shards or to log out goes on without the override, so that the user can pick another
tenant or log out.

If your project has no mapping model, override ``switchable_shards(request, queryset)``. It receives a queryset of all
shards and returns a queryset of the shards that the user of the request may switch to. The shard switcher, the
switch view and ``ShardIdAdminOverrideMiddleware`` use it in the same way.

If you are not using a default ``UseShardMiddleware``/``UseShardForMiddleware`` or have a non-standard session backend
(i.e. not ``djanquiltdb.sessions``, see :doc:`/guides/sessions`), it might be necessary to customize
``AdminOverrideMiddleware``, the setting ``OVERRIDE_SHARD_SELECTOR_KEY`` under ``QUILT_ADMIN``, or define a custom
``SHARD_SELECTOR`` class that subclasses ``djanquiltdb.contrib.quilt_admin.shard_selector.BaseAdminShardSelector``. If
this is required, refer to the code for more details and reference implementations.

If your project serves the Django admin under a strict Content Security Policy (e.g. ``script-src 'nonce-…'``
without ``'unsafe-inline'``), set ``QUILT_ADMIN['USE_CSP_NONCE'] = True``. The shard switcher will then emit
``<script nonce="{{ csp_nonce }}">`` for the small inline script that wires up the dropdown, relying on the
``csp_nonce`` template context variable provided by Django 6.0+'s ``django.template.context_processors.csp`` or
the ``django-csp`` package. The default is ``False``, which emits a plain ``<script>`` tag without a ``nonce``
attribute; this is the right choice for projects that don't use CSP or that allow ``'unsafe-inline'``.

Please be aware that if you run a multi-node setup and are relying on Python logic in your application to propagate
changes to each node individually, and you are exposing mirrored models in the admin, you need to make sure that your
admin classes use this custom save logic so that they keep the nodes consistent.

At this time there is no separate node switcher. If you want to view public models on various nodes (i.e. models that
exist in the ``public`` schema but are not mirrored) you need to switch to viewing a shard that exists on the node for
which you want to view the public model in question, since this will switch the context to that node.

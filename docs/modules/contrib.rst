Contrib packages
================

The djanquiltdb library comes with some optional packages that can be installed as apps in `INSTALLED_APPS`. The packages
are to be found in `djanquiltdb/contrib`.

Authentication
--------------
Django comes with their `django.contrib.auth` package, that adds authentication models to the application. It also has a
`createsuperuser` management command, that offers a simple way of creating a superuser from the command line. This
command doesn't work with the sharding library, so the sharding library also offers an authentication package that
overwrites the `createsuperuser` command. It's important that this package comes before `django.contrib.auth` in
`INSTALLED_APPS`::

    INSTALLED_APPS = (
        ...
        'djanquiltdb.contrib.quilt_auth',
        'django.contrib.auth',
        ...
        'djanquiltdb',
        ...
    )

The command automatically detects whether your user model is a sharded model or a mirrored model, and offers
different arguments to create a superuser accordingly. When your user model is a sharded model, it offers you the
option to specify a database (`--database`) and a schema name (`--schema-name`). Both are required. When your use model
is a mirrored model, it only offers you the option to specify a database (`--database`). The difference is that it also
gives you the option to pass 'all' as `--database` option, which will create a superuser on all databases. This is also
the default option.

Note that options that Django's `createsuperuser` gives us, are still available on the command the sharding library
offers.

Admin Shard Switcher
---------------------
The `django_sharding.contrib.quilt_admin` package adds a shard switcher dropdown to the Django admin interface,
allowing administrators to easily switch between shards while browsing the admin.

Note: this module does not provide a sharding-aware login method. You can either rely on your application's own
login page for this after you've made it sharding-aware, or implement a custom one for the admin::

    from django.contrib import admin
    from django.contrib.auth.forms import AuthenticationForm

    class CustomAdminAuthenticationForm(AuthenticationForm):
        shard_identifying_field = ...

        def clean(self)
            shard_selector = ...

            try:
                # Assuming you're using django_sharding.sessions backend
                if self.request is not None:
                    self.request.session.shard_selector = shard_selector

                    with use_shard_for(shard_selector):
                        return super().clean()
            except (OrganizationShard.DoesNotExist, StateException, OperationalError):  # type: ignore[attr-defined]
                raise ValidationError(self.error_messages['invalid_login'], code='invalid_login')

    admin.site.login_form = CustomAdminAuthenticationForm
    admin.site.login_template = 'admin/custom_admin_login.html'

To enable this feature, add the package to your `INSTALLED_APPS`::

    INSTALLED_APPS = (
        'django.contrib.admin',
        ...
        'django_sharding.contrib.quilt_admin',
        ...
    )

Add the appropriate middleware after the base `UseShardMiddleware`/`UseShardForMiddleware` and after the
`AuthenticationMiddleware`. If you are working with a mapping model, this should be
`MappingValueAdminOverrideMiddleware`, otherwise `ShardIdAdminOverrideMiddleware`::

    MIDDLEWARE_CLASSES = (
        (...)
        'django.contrib.sessions.middleware.SessionMiddleware',
        'django_sharding.middleware.UseShardForMiddleware',
        (...)
        'django.contrib.auth.middleware.AuthenticationMiddleware',
        'django_sharding.contrib.quilt_admin.middleware.MappingValueAdminOverrideMiddleware',
        (...)
    )


Add the context processor to your template settings::

    TEMPLATES = [
        {
            ...
            'OPTIONS': {
                'context_processors': [
                    ...
                    'django_sharding.contrib.quilt_admin.context_processors.admin_shard_context',
                ],
            },
        },
    ]

Include the admin URLs in your main URLconf. The URLs should be included under the ``admin/`` path::

    from django.contrib import admin
    from django.urls import include, path

    urlpatterns = [
        path('admin/', admin.site.urls),
        path('admin/', include('django_sharding.contrib.quilt_admin.urls')),
        ...
    ]

Note: The order matters - include ``admin.site.urls`` first, then include the shard switcher URLs.

Once installed, a dropdown will appear in the top right corner of the Django admin interface (next to the user links).
The dropdown lists all available shards and allows you to switch between them. The selected shard is stored in the session
and will be used by your shard middleware for subsequent requests.

The dropdown shows:
- Shard alias
- Node name and schema name (e.g., "default|org_1_schema")
- Current selection is highlighted

Selecting no shard clears the shard selection and returns to the default behavior.

If you are not using a default `UseShardMiddleware`/`UseShardForMiddleware` or have a non-standard session backend
(i.e. not `django_sharding.sessions`), it might be necessary to customize the `AdminOverrideMiddleware`, the setting
`OVERRIDE_SHARD_SELECTOR_KEY` under `QUILT_ADMIN`, or define a custom `SHARD_SELECTOR` class that subclasses
`django_sharding.contrib.admin.shard_selector.BaseAdminShardSelector`. If this is required, refer to the code for
more details and reference implementations.

Please be aware that if you run a multi-node setup and are relying on Python logic in your application to propagate
changes to each node individually, and you are exposing mirrored models in the admin, you need to make sure that your
admin classes use this custom save logic so that they keep the nodes consistent.

At this time there is no separate node switcher. If you want to view public models on various nodes (i.e. models that
exist in the `public` schema but are not mirrored) you need to switch to viewing a shard that exists on the node for
which you want to view the public model in question, since this will switch the context to that node.

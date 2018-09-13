Contrib packages
================

The sharding library comes with some optional packages that can be installed as apps in `INSTALLED_APPS`. The packages
are to be found in `sharding/contrib`.

Authentication
--------------
Django comes with their `django.contrib.auth` package, that adds authentication models to the application. It also has a
`createsuperuser` management command, that offers a simple way of creating a superuser from the command line. This
command doesn't work from scratch with the sharding library, so the sharding library also offers an authentication
package that overwrites the `createsuperuser` command. It's important that this package comes before
`django.contrib.auth` in `INSTALLED_APPS`::

    INSTALLED_APPS = (
        ...
        'sharding.contrib.authentication',
        'django.contrib.auth',
        ...
        'sharding',
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

============
Installation
============

These steps set up a Django project with DjanQuiltDB, in this order: install the package, configure the project, write
the models that the configuration names, and migrate. Each step links to the guide that covers it in full.

.. warning::

    DjanQuiltDB is not async-safe. If your project has async views, async middleware or Channels consumers, read
    :doc:`/guides/async` first.

.. _`install`:

Installing djanquiltdb
----------------------

The stable version is available from PyPI::

    pip install djanquiltdb

If you want to install development version (unstable), you can do so doing::

    pip install git+ssh://git@github.com/djanquiltdb/djanquiltdb.git@master#egg=djanquiltdb

Or, if you'd like to install the development version as a git repository (so you can ``git pull`` updates), use the
``-e`` flag with ``pip install``, like so::

    pip install -e git+ssh://git@github.com/djanquiltdb/djanquiltdb.git@master#egg=djanquiltdb


Configuration
-------------

Configure the project before you write models or make migrations: where a model's table goes, and so what its migrations
do, depends on this configuration. For a project that ``django-admin startproject`` created, with an app ``myapp`` that
holds the models of the next step, these are the settings::

    from djanquiltdb import ShardingMode

    INSTALLED_APPS = [
        'django.contrib.admin',
        # Optional but recommended: a shard switcher in the admin. See "Admin shard switcher".
        'djanquiltdb.contrib.quilt_admin',
        # Optional but recommended: a createsuperuser that works with shards. List it before django.contrib.auth.
        # See "Authentication".
        'djanquiltdb.contrib.quilt_auth',
        'django.contrib.auth',
        'django.contrib.contenttypes',
        # Optional but recommended: unused by the session backend, but it has the clearsessions command.
        # See "Sessions".
        'django.contrib.sessions',
        'django.contrib.messages',
        'django.contrib.staticfiles',
        # Required.
        'djanquiltdb',
        'myapp',
    ]

    # Required: every node uses the DjanQuiltDB backend. See "Settings".
    DATABASES = {
        'default': {
            'ENGINE': 'djanquiltdb.postgresql_backend',
            ...
        },
    }

    # Required: the router sends each query to the right node and schema. See "Settings".
    DATABASE_ROUTERS = ['djanquiltdb.router.DynamicDbRouter']

    QUILT_DB = {
        # Required: the registry of the shards. See "The Shard model".
        'SHARD_CLASS': 'myapp.models.Shard',
        # Optional, but strongly recommended for convenience: finds the shard of a tenant by a value of the tenant, and
        # lets several tenants share a shard. Required when using the session backend. See "The mapping model".
        'MAPPING_MODEL': 'myapp.models.TenantShard',
        # Optional: the regex that every shard selector matches. A shard selector is the mapping value inside an
        # identifier, such as a session key. The default matches integer mapping values, and this example maps tenants
        # by slug. See "Settings".
        'SHARD_SELECTOR_REGEX': '[-a-z0-9]+',
        # Required as soon as the project has mirrored models, as auth and contenttypes are here. See "Settings".
        'PRIMARY_DB_ALIAS': 'default',
        # Required: every model needs a sharding mode, and these give one to the models of Django's own apps. Any
        # other app with models needs an entry too. See "Sharding modes".
        'OVERRIDE_SHARDING_MODE': {
            ('auth',): ShardingMode.MIRRORED,
            ('contenttypes',): ShardingMode.MIRRORED,
            ('admin', 'logentry'): ShardingMode.SHARDED,
            # Required while django.contrib.sessions is installed. See "Sessions".
            ('sessions',): ShardingMode.SHARDED,
        },
    }

    # Required for a project with sessions: keeps each session in the shard of its tenant. See "Sessions".
    SESSION_ENGINE = 'djanquiltdb.sessions'

    QUILT_SESSIONS = {
        # Required: the session model of the project. See "Sessions".
        'SESSION_MODEL': 'myapp.models.QuiltSession',
    }

    MIDDLEWARE = [
        'django.middleware.security.SecurityMiddleware',
        'django.contrib.sessions.middleware.SessionMiddleware',
        # Optional but recommended: enters the shard of the tenant of the session for each request, after
        # SessionMiddleware. See "Requests and middleware".
        'djanquiltdb.middleware.UseShardForMiddleware',
        'django.middleware.common.CommonMiddleware',
        'django.middleware.csrf.CsrfViewMiddleware',
        'django.contrib.auth.middleware.AuthenticationMiddleware',
        # Optional but recommended, with quilt_admin: enters the shard that is selected in the admin, after
        # AuthenticationMiddleware. See "Admin shard switcher".
        'djanquiltdb.contrib.quilt_admin.middleware.MappingValueAdminOverrideMiddleware',
        'django.contrib.messages.middleware.MessageMiddleware',
        'django.middleware.clickjacking.XFrameOptionsMiddleware',
    ]

    TEMPLATES = [
        {
            'BACKEND': 'django.template.backends.django.DjangoTemplates',
            'DIRS': [],
            'APP_DIRS': True,
            'OPTIONS': {
                'context_processors': [
                    'django.template.context_processors.request',
                    'django.contrib.auth.context_processors.auth',
                    'django.contrib.messages.context_processors.messages',
                    # Optional but recommended, with quilt_admin: draws the shard switcher. See "Admin shard switcher".
                    'djanquiltdb.contrib.quilt_admin.context_processors.admin_shard_context',
                ],
            },
        },
    ]

And in the project's ``urls.py``, the URLs of the shard switcher after those of the admin::

    from django.contrib import admin
    from django.urls import include, path

    urlpatterns = [
        path('admin/', admin.site.urls),
        # Optional but recommended, with quilt_admin: after admin.site.urls. See "Admin shard switcher".
        path('admin/', include('djanquiltdb.contrib.quilt_admin.urls')),
    ]

What each part does:

* **The backend and the router.** Every node is a database that uses ``djanquiltdb.postgresql_backend``, and
  ``DynamicDbRouter`` sends each query to the right node and schema. :doc:`/reference/settings` describes these and
  every other setting.
* **SHARD_CLASS** names the Shard model, the registry of the shards, which the next step writes (see
  :doc:`/guides/shard_model`).
* **MAPPING_MODEL** names the mapping model, which records the shard of each tenant. It is optional, but strongly
  recommended for convenience: code finds the shard of a tenant by a value of the tenant, such as its slug, several
  tenants can share a shard, and one tenant can move to another shard (see :doc:`/guides/mapping_model`). It is required
  when using the session backend, because that finds the shard of a session through it.
* **SHARD_SELECTOR_REGEX** is the regex that every shard selector matches. A shard selector is the mapping value of a
  tenant inside an identifier, such as a session key, and it names the shard of the identifier. The default,
  ``'[0-9]+'``, matches integer mapping values only, and the mapping model of the next step maps tenants by slug, so it
  is set here. It is strongly recommended to limit mapping values to those allowed by Django's ``slug`` path converter,
  see :ref:`QUILT_DB['SHARD_SELECTOR_REGEX'] <shard_selector_regex_setting>`.
* **PRIMARY_DB_ALIAS** names the node that writes to mirrored models go to. A project with mirrored models has to set
  it, and ``auth`` and ``contenttypes`` are mirrored here.
* **OVERRIDE_SHARDING_MODE** gives the models of Django's own apps a sharding mode. Every model needs one (see
  :doc:`/guides/sharding_modes`): the models of the project get theirs from a decorator in the next step. Any other app
  with models needs an entry too, and the system check ``djanquiltdb.E004`` names the models that have no mode (see
  :doc:`/reference/checks`).
* **SESSION_ENGINE and QUILT_SESSIONS.** Django's session backends that use the database keep the sessions of all
  tenants in one table, which a sharded project cannot read before it knows the shard, so use ``djanquiltdb.sessions``
  instead. It keeps each session in the shard of its tenant, in a session model of the project: a
  subclass of ``BaseQuiltSession``, which the project can extend with fields of its own. ``SESSION_MODEL`` names it, and
  is the one key of ``QUILT_SESSIONS`` without a default; the others fall back on their defaults (see
  :doc:`/reference/settings`). Keep ``django.contrib.sessions`` installed for its ``clearsessions`` command. Your login
  flow has to tell each session which tenant it belongs to, as :doc:`/guides/sessions` describes.
* **UseShardForMiddleware** enters the shard of the tenant of the session for each request, so views need not enter it
  themselves (see :doc:`/guides/requests`). Put it after ``SessionMiddleware``, which it reads the tenant from.
* **djanquiltdb.contrib.quilt_admin** adds a shard switcher to the Django admin, so that staff can browse the data of
  any shard. ``MappingValueAdminOverrideMiddleware`` enters the shard that is selected; put it after
  ``AuthenticationMiddleware``. The context processor and the URLs draw the switcher. The admin's own login is not
  sharding-aware, so have staff log in through the login of your project, or give the admin a login form that sets the
  shard selector (see :doc:`/contrib/admin_shard_switcher`).
* **djanquiltdb.contrib.quilt_auth** replaces Django's ``createsuperuser`` command with one that works with shards. List
  it before ``django.contrib.auth`` (see :doc:`/contrib/authentication`).


Creating models
---------------

The configuration names three models of ``myapp``: the Shard model, the mapping model and the session model.

``myapp/models.py``::

    from django.db import models

    from djanquiltdb import STATES, State
    from djanquiltdb.decorators import mirrored_model, shard_mapping_model, sharded_model
    from djanquiltdb.models import BaseQuiltSession, BaseShard, MappingQuerySet


    # Required: the registry of the shards, which SHARD_CLASS names. See "The Shard model".
    @mirrored_model()
    class Shard(BaseShard):
        class Meta:
            app_label = 'myapp'


    # Optional, but strongly recommended for convenience: the shard of each tenant, which MAPPING_MODEL names. Required
    # when using the session backend. See "The mapping model".
    @mirrored_model()
    @shard_mapping_model(mapping_field='tenant_slug')
    class TenantShard(models.Model):
        shard = models.ForeignKey(Shard, on_delete=models.CASCADE)
        tenant_slug = models.SlugField(unique=True)
        state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

        objects = MappingQuerySet.as_manager()


    # Required for a project with sessions: the session model, which SESSION_MODEL names. It can have fields of its
    # own. See "Sessions".
    @sharded_model()
    class QuiltSession(BaseQuiltSession):
        class Meta:
            app_label = 'myapp'

:doc:`/guides/shard_model`, :doc:`/guides/mapping_model` and :doc:`/guides/sessions` describe each of them. Give each
of your other models a decorator with its sharding mode, such as ``@sharded_model()`` for the data of a tenant (see
:doc:`/guides/sharding_modes`).


Migrating
---------

Make the migrations of the models, and apply them::

    python manage.py makemigrations
    python manage.py migrate

``migrate`` migrates the public schema of every node, and every shard. Before it applies the first migration, it creates
the template schema of every node, which new shards are cloned from (see :doc:`/guides/template_schema`).
:doc:`/guides/migrations` describes how migrations run in a sharded project.


Creating a shard
----------------

The project is ready for its first shard and tenant::

    from djanquiltdb import State
    from djanquiltdb.utils import use_shard_for

    from myapp.models import Shard, TenantShard

    shard = Shard.objects.create(alias='shard_1', node_name='default', schema_name='shard_1', state=State.ACTIVE)
    TenantShard.objects.create(shard=shard, tenant_slug='acme')

    with use_shard_for('acme'):
        ...  # Queries of sharded models run in the shard of the tenant acme.

:doc:`/guides/queries` describes how to run queries in a shard, and :doc:`/guides/sessions` how a request finds the
shard of its tenant.


Optional extras
---------------

The base library covers models fully with decorators, and leaves database *objects* (views, functions, triggers) to be
managed with hand-written ``RunSQL`` in a migration.

If you wish to manage database triggers, we recommend
`django-pgtrigger <https://github.com/AmbitionEng/django-pgtrigger>`_. See :doc:`/database_objects/triggers` for more
information.

.. _`celery_extra`:

``celery``
~~~~~~~~~~

The ``celery`` extra adds `Celery <https://docs.celeryq.dev/>`_ tasks that work with sharding::

    pip install djanquiltdb[celery]

It installs a plugin that makes this library work with Celery. The plugin's task decorators run a task in the shard that
the keyword arguments of the task identify, or in the shard that is active where the task is sent. A task whose shard is
in maintenance is retried, and a task can run once in every shard or on every node. See :doc:`the plugin's documentation
</plugins/celery/index>`.

.. _`oauth_toolkit_extra`:

``oauth-toolkit``
~~~~~~~~~~~~~~~~~

The ``oauth-toolkit`` extra adds OAuth2 client ids and tokens that work with sharding::

    pip install djanquiltdb[oauth-toolkit]

It installs a plugin that makes this library work with `django-oauth-toolkit
<https://django-oauth-toolkit.readthedocs.io/>`_. The client ids and tokens of the plugin have the shard selector of
their shard in them, and its middleware enters that shard before the toolkit looks up a client id or a token. See
:doc:`the plugin's documentation </plugins/oauth-toolkit/index>`, and :doc:`/guides/oauth2` for other OAuth2 and API
token libraries.

.. _`pghistory_extra`:

``pghistory``
~~~~~~~~~~~~~

The ``pghistory`` extra adds model history that works with sharding::

    pip install djanquiltdb[pghistory]

It installs a plugin that makes this library work with `django-pghistory <https://django-pghistory.readthedocs.io/>`_.
The plugin's ``track()`` works like ``pghistory.track()``, and places each event model in the same place as the tracked
model. See :doc:`the plugin's documentation </plugins/pghistory/index>`.

.. _`postgres_denormalize_extra`:

``postgres-denormalize``
~~~~~~~~~~~~~~~~~~~~~~~~

The ``postgres-denormalize`` extra adds helpers for the copies that `django-postgres-denormalize
<https://django-postgres-denormalize.readthedocs.io/>`_ keeps::

    pip install djanquiltdb[postgres-denormalize]

django-postgres-denormalize keeps copies of related columns up to date with triggers, and its functions that find and
correct out-of-date copies work on the active shard only. The plugin's helpers enter one shard or every shard for you.
See :doc:`the plugin's documentation </plugins/postgres-denormalize/index>`.

.. _`postgres_objects_extra`:

``postgres-objects``
~~~~~~~~~~~~~~~~~~~~

The ``postgres-objects`` extra adds the declarative alternative for views and functions::

    pip install djanquiltdb[postgres-objects]

It installs a plugin carrying the glue between this library and `django-postgres-objects
<https://github.com/djanquiltdb/django-postgres-objects>`_, which declares Postgres views and functions as classes and
lets ``makemigrations`` manage them. That library works on its own against a single database and needs nothing from
here; what a sharded project adds is the question of which schemas each object belongs in, and the plugin provides
decorators for that purpose analogous to the model decorators in the base library. They are importable from
``djanquiltdb.decorators`` beside the model decorators.

See :doc:`/database_objects/views`, :doc:`/database_objects/functions` and :doc:`/database_objects/generated_columns`
for what the extra changes in each case, and :doc:`the plugin's documentation </plugins/postgres-objects/index>` for the
rest.

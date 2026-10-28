.. The logo needs an absolute URL: this README is the PyPI long description, where a relative path renders broken.

.. image:: https://raw.githubusercontent.com/DjanQuiltDB/djanquiltdb/master/assets/icon-128.png
    :alt: DjanQuiltDB
    :width: 128

DjanQuiltDB - Django Database Sharding
======================================

.. image:: https://img.shields.io/pypi/v/djanquiltdb.svg
    :target: https://pypi.org/project/djanquiltdb/
    :alt: PyPI

.. image:: https://img.shields.io/pypi/pyversions/djanquiltdb.svg
    :target: https://pypi.org/project/djanquiltdb/
    :alt: Supported Python versions

.. image:: https://img.shields.io/pypi/frameworkversions/django/djanquiltdb.svg
    :target: https://pypi.org/project/djanquiltdb/
    :alt: Supported Django versions

.. image:: https://img.shields.io/badge/postgres-17%20%7C%2018-4169e1?logo=postgresql&logoColor=white
    :target: https://www.postgresql.org/
    :alt: Supported PostgreSQL versions

.. image:: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml
    :alt: CI

.. image:: https://readthedocs.org/projects/djanquiltdb/badge/?version=latest
    :target: https://djanquiltdb.readthedocs.io/
    :alt: Documentation

.. image:: https://img.shields.io/pypi/l/djanquiltdb.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/blob/master/LICENSE
    :alt: BSD-3-Clause licence

**DjanQuiltDB** is an extension to the Django web framework that provides helper functions to split a database based on
top level hierarchy. It is specifically designed to support horizontal sharding not just within a single database
cluster, but also across multiple database clusters, thus allowing you to scale database capacity both vertically and
horizontally as your dataset grows.

This library attempts to combine the best of as many worlds as possible. Tenant-specific data is kept in tenant-specific
PostgreSQL schemas, while data that is tenant-agnostic or shared can be kept in public schemas, so as to deduplicate.
There are helpers to split data off from one shard to another, to migrate a shard across nodes, to write to every node
at once, etc.

As the name suggests, this approach provides an interface to data, that may in reality be scattered across various
schemas in various database clusters, and presents it as a coherent and easily accessible patchwork of tables, resulting
in a database resembling a quilt.

Full documentation is at https://djanquiltdb.readthedocs.io/.

Installation
============

DjanQuiltDB is not async-safe. If your project has async views, async middleware or Channels consumers, read the `async
guide <https://djanquiltdb.readthedocs.io/en/latest/guides/async.html>`_ first.

::

    pip install djanquiltdb

The `installation guide <https://djanquiltdb.readthedocs.io/en/latest/getting_started/installation.html>`_ describes
each step below in full. In ``settings.py``, for an app ``myapp`` that has the models of the next step::

    from djanquiltdb import ShardingMode

    INSTALLED_APPS = [
        ...
        'djanquiltdb',
        'myapp',
    ]

    # Every node uses the DjanQuiltDB backend.
    DATABASES = {
        'default': {
            'ENGINE': 'djanquiltdb.postgresql_backend',
            ...
        },
    }

    # The router sends each query to the right node and schema.
    DATABASE_ROUTERS = ['djanquiltdb.router.DynamicDbRouter']

    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.TenantShard',
        # The node that writes to mirrored models go to.
        'PRIMARY_DB_ALIAS': 'default',
        # Every model needs a sharding mode, and these give one to the models of Django's own apps.
        'OVERRIDE_SHARDING_MODE': {
            ('auth',): ShardingMode.MIRRORED,
            ('contenttypes',): ShardingMode.MIRRORED,
            ('admin', 'logentry'): ShardingMode.SHARDED,
            ('sessions',): ShardingMode.PUBLIC,
        },
    }

In ``myapp/models.py``, the Shard model is the registry of the shards, and the mapping model records the shard of each
tenant::

    from django.db import models

    from djanquiltdb import STATES, State
    from djanquiltdb.decorators import mirrored_model, shard_mapping_model
    from djanquiltdb.models import BaseShard, MappingQuerySet


    @mirrored_model()
    class Shard(BaseShard):
        class Meta:
            app_label = 'myapp'


    @mirrored_model()
    @shard_mapping_model(mapping_field='tenant_slug')
    class TenantShard(models.Model):
        shard = models.ForeignKey(Shard, on_delete=models.CASCADE)
        tenant_slug = models.SlugField(unique=True)
        state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

        objects = MappingQuerySet.as_manager()

Give each of your other models a decorator with its sharding mode, such as ``@sharded_model()`` for the data of a
tenant. A model without a sharding mode fails the system checks. Then make the migrations, and apply them::

    python manage.py makemigrations
    python manage.py migrate

``migrate`` migrates the public schema of every node, and every shard. Create the first shard and tenant, and run
queries in the shard of the tenant::

    from djanquiltdb import State
    from djanquiltdb.utils import use_shard_for

    from myapp.models import Shard, TenantShard

    shard = Shard.objects.create(alias='shard_1', node_name='default', schema_name='shard_1', state=State.ACTIVE)
    TenantShard.objects.create(shard=shard, tenant_slug='acme')

    with use_shard_for('acme'):
        ...  # Queries of sharded models run in the shard of the tenant acme.

The installation guide also sets up a session backend that keeps each session in the shard of its tenant, a middleware
that enters that shard for each request, a shard switcher in the admin, and a ``createsuperuser`` command that works
with shards.

To run Celery tasks in a shard, or once in every shard or on every node, install the extra::

    pip install djanquiltdb[celery]

To issue django-oauth-toolkit client ids and tokens that name their shard, and enter that shard for each request,
install the extra::

    pip install djanquiltdb[oauth-toolkit]

To track model history with django-pghistory, install the extra::

    pip install djanquiltdb[pghistory]

To find and correct the out-of-date copies that django-postgres-denormalize keeps, in one shard or in every shard,
install the extra::

    pip install djanquiltdb[postgres-denormalize]

To declare Postgres views and functions as classes, and place them with sharding decorators, install the extra::

    pip install djanquiltdb[postgres-objects]

The `settings reference <https://djanquiltdb.readthedocs.io/en/latest/reference/settings.html>`_ describes every
setting.

Requirements
============

* Python 3.14
* Django 6.0 or 6.1
* PostgreSQL 17 or 18

Development
===========

This package is developed in the `DjanQuiltDB repository <https://github.com/DjanQuiltDB/djanquiltdb>`_ beside its
in-tree plugins, each shipped as a distribution of its own.

Code style
----------

Ruff handles both linting and formatting. Enable the git hook once per clone::

    pre-commit install

The same checks run over the whole tree with::

    pre-commit run --all-files

Tests
-----

Tox, run from the repository root, runs the suite against every Django and PostgreSQL version the package claims. It
needs the PostgreSQL containers in the repository's ``docker-compose.yml``; see ``DOCKER.md`` at the repository root::

    docker compose run --rm test tox -m core

The suite runs without any placement plugin installed: plugins carry suites of their own.

The test project reads its connection strings from ``packages/djanquiltdb/tests/secrets.json`` when that file exists,
which is how a run outside the containers is pointed at your own databases::

    cp packages/djanquiltdb/tests/secrets.json.example packages/djanquiltdb/tests/secrets.json

Building
--------

::

    docker compose run --no-deps --rm test tox -e core-build

The sdist and wheel are placed in ``packages/djanquiltdb/dist/`` and checked with ``twine``.

Documentation
-------------

The documentation lives in ``docs/`` at the repository root, shared with the in-tree plugins. Build it with::

    docker compose run --no-deps --rm test tox -e docs

Attribution
===========

This is an independently maintained fork of the patchman-django-sharding library originally created and maintained by
Patchman B.V. (2017-2023) and Cloud Linux Software, Inc. (2023-2025).

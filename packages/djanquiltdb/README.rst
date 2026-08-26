DjanQuiltDB - Django Database Sharding
======================================

.. image:: https://img.shields.io/pypi/v/djanquiltdb.svg
    :target: https://pypi.org/project/djanquiltdb/
    :alt: PyPI

.. image:: https://img.shields.io/pypi/pyversions/djanquiltdb.svg
    :target: https://pypi.org/project/djanquiltdb/
    :alt: Supported Python versions

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
There are helpers to split data off from one shard to another, to migrate a shard across nodes, to synchronize changes
across nodes, etc.

As the name suggests, this approach provides an interface to data, that may in reality be scattered across various
schemas in various database clusters, and presents it as a coherent and easily accessible patchwork of tables, resulting
in a database resembling a quilt.

Full documentation is at https://djanquiltdb.readthedocs.io/.

Installation
============

::

    pip install djanquiltdb

Add it to ``INSTALLED_APPS``::

    INSTALLED_APPS = (
        ...
        'djanquiltdb',
        ...
    )

Point every connection at the bundled backend, and routing at the router::

    DATABASES = {
        'default': dj_database_url.parse(DATABASE_URL, engine='djanquiltdb.postgresql_backend'),
    }

    DATABASE_ROUTERS = ['djanquiltdb.router.DynamicDbRouter']

Then declare a shard model of your own and name it in the ``QUILT_DB`` setting::

    # myapp/models.py
    from djanquiltdb.models import BaseShard


    class Shard(BaseShard):
        class Meta:
            app_label = 'myapp'

    # settings.py
    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
    }

Models are placed with the decorators in ``djanquiltdb.decorators``; an undecorated model stays on the default node's
public schema. ``manage.py migrate`` is shard-aware, and applies each migration wherever its models live.

To declare Postgres views and functions the same way, install the extra::

    pip install djanquiltdb[postgres-objects]

See the `installation docs <https://djanquiltdb.readthedocs.io/en/latest/modules/installation.html>`_ for the full set
of settings, including the template schema every node needs, the mapping model, the session backend and the middleware.

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

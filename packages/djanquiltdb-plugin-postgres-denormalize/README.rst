.. The logo needs an absolute URL: this README is the PyPI long description, where a relative path renders broken.

.. image:: https://raw.githubusercontent.com/DjanQuiltDB/djanquiltdb/master/assets/icon-128.png
    :alt: DjanQuiltDB
    :width: 128

=======================================
djanquiltdb-plugin-postgres-denormalize
=======================================

.. image:: https://img.shields.io/pypi/v/djanquiltdb-plugin-postgres-denormalize.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-postgres-denormalize/
    :alt: PyPI

.. image:: https://img.shields.io/pypi/pyversions/djanquiltdb-plugin-postgres-denormalize.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-postgres-denormalize/
    :alt: Supported Python versions

.. image:: https://img.shields.io/pypi/frameworkversions/django/djanquiltdb-plugin-postgres-denormalize.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-postgres-denormalize/
    :alt: Supported Django versions

.. image:: https://img.shields.io/badge/postgres-17%20%7C%2018-4169e1?logo=postgresql&logoColor=white
    :target: https://www.postgresql.org/
    :alt: Supported PostgreSQL versions

.. image:: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml
    :alt: CI

.. image:: https://readthedocs.org/projects/djanquiltdb/badge/?version=latest
    :target: https://djanquiltdb.readthedocs.io/en/latest/plugins/postgres-denormalize/index.html
    :alt: Documentation

.. image:: https://img.shields.io/pypi/l/djanquiltdb-plugin-postgres-denormalize.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/blob/master/LICENSE
    :alt: BSD-3-Clause licence

`django-postgres-denormalize <https://django-postgres-denormalize.readthedocs.io/>`_ stores copies of related columns
in ordinary columns, and keeps them up to date with Postgres triggers. `DjanQuiltDB
<https://github.com/djanquiltdb/djanquiltdb>`_ shards a database across schemas and nodes. This library finds and
corrects the copies that are out of date, in one shard or in every shard::

    from djanquiltdb.utils.postgres_denormalize import repair_every_shard, stale_rows

    stale_rows(shard)  # {<class 'Task'>: 3}
    repair_every_shard()

Install it through DjanQuiltDB's extra::

    pip install djanquiltdb[postgres-denormalize]

The `installation guide
<https://djanquiltdb.readthedocs.io/en/latest/plugins/postgres-denormalize/installation.html>`_ describes each step in
full. ``postgres_denormalize`` and the ``pgtrigger`` app that it builds on go in ``INSTALLED_APPS``. Run every node that
stores copies at SERIALIZABLE, because at a lower level a copy can differ from its lookup path, and retry the
transactions that fail with a serialization error::

    # settings.py
    from django.db.backends.postgresql.psycopg_any import IsolationLevel

    INSTALLED_APPS = [
        ...
        'djanquiltdb',
        'pgtrigger',
        'postgres_denormalize',
        ...
    ]

    DATABASES = {
        'default': {
            ...,
            'OPTIONS': {'isolation_level': IsolationLevel.SERIALIZABLE},
        },
    }

Do not copy a column of a mirrored or public model. The triggers of such a copy only reach the current shard, and fail
outside one.

Full documentation is at https://djanquiltdb.readthedocs.io/en/latest/plugins/postgres-denormalize/index.html.

Development
===========

This package is developed in the `DjanQuiltDB repository <https://github.com/DjanQuiltDB/djanquiltdb>`_ beside
``djanquiltdb`` itself. Tests run through tox against the Postgres containers in the repository's
``docker-compose.yml``; see ``DOCKER.md`` at the repository root::

    docker compose run --rm test tox

Requirements
------------

* Python 3.14
* Django 6.0 or 6.1
* PostgreSQL 17 or 18
* djanquiltdb 4.3.0 or newer 4.x
* django-postgres-denormalize 1.0.0 or newer 1.x

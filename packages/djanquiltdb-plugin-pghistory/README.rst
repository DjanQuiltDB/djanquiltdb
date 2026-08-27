.. The logo needs an absolute URL: this README is the PyPI long description, where a relative path renders broken.

.. image:: https://raw.githubusercontent.com/DjanQuiltDB/djanquiltdb/master/assets/icon-128.png
    :alt: DjanQuiltDB
    :width: 128

============================
djanquiltdb-plugin-pghistory
============================

.. image:: https://img.shields.io/pypi/v/djanquiltdb-plugin-pghistory.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-pghistory/
    :alt: PyPI

.. image:: https://img.shields.io/pypi/pyversions/djanquiltdb-plugin-pghistory.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-pghistory/
    :alt: Supported Python versions

.. image:: https://img.shields.io/pypi/frameworkversions/django/djanquiltdb-plugin-pghistory.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-pghistory/
    :alt: Supported Django versions

.. image:: https://img.shields.io/badge/postgres-17%20%7C%2018-4169e1?logo=postgresql&logoColor=white
    :target: https://www.postgresql.org/
    :alt: Supported PostgreSQL versions

.. image:: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml
    :alt: CI

.. image:: https://readthedocs.org/projects/djanquiltdb/badge/?version=latest
    :target: https://djanquiltdb.readthedocs.io/en/latest/plugins/pghistory/index.html
    :alt: Documentation

.. image:: https://img.shields.io/pypi/l/djanquiltdb-plugin-pghistory.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/blob/master/LICENSE
    :alt: BSD-3-Clause licence

`django-pghistory <https://django-pghistory.readthedocs.io/>`_ records model changes in event tables through Postgres
triggers. `DjanQuiltDB <https://github.com/djanquiltdb/djanquiltdb>`_ shards a database across schemas and nodes. This
library makes the two work together: its ``track()`` works like ``pghistory.track()``, and places each generated event
model in the same place as the tracked model, so the history of a sharded model stays inside its shard::

    # example/models.py
    from djanquiltdb.decorators import pghistory, sharded_model


    @pghistory.track(exclude=['password'])
    @sharded_model()
    class User(models.Model):
        ...

The ``pghistory`` namespace is importable from ``djanquiltdb.decorators`` beside the model decorators, as above; the
same functions are also importable directly from ``djanquiltdb_plugin_pghistory``. The sharding mode is read from the
tracked model, so the mode decorator goes below ``@track()``. Models that cannot carry the decorator, such as
third-party ones, resolve through ``QUILT_DB['OVERRIDE_SHARDING_MODE']``, and the explicit forms ``track_sharded``,
``track_mirrored`` and ``track_public`` state the placement at the call site instead. ``create_event_model()`` is
mirrored the same way.

Install it through DjanQuiltDB's extra::

    pip install djanquiltdb[pghistory]

Full documentation is at https://djanquiltdb.readthedocs.io/en/latest/plugins/pghistory/index.html.

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
* djanquiltdb 4.2.0 or newer 4.x
* django-pghistory 3.9.2 or newer 3.x

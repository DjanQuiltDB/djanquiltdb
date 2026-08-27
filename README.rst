.. image:: https://raw.githubusercontent.com/DjanQuiltDB/djanquiltdb/master/assets/icon-128.png
    :alt: DjanQuiltDB
    :width: 128

DjanQuiltDB
===========

.. image:: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml
    :alt: CI

.. image:: https://readthedocs.org/projects/djanquiltdb/badge/?version=latest
    :target: https://djanquiltdb.readthedocs.io/
    :alt: Documentation

.. image:: https://img.shields.io/pypi/v/djanquiltdb.svg
    :target: https://pypi.org/project/djanquiltdb/
    :alt: djanquiltdb on PyPI

.. image:: https://img.shields.io/pypi/pyversions/djanquiltdb.svg
    :target: https://pypi.org/project/djanquiltdb/
    :alt: Supported Python versions

.. image:: https://img.shields.io/pypi/frameworkversions/django/djanquiltdb.svg
    :target: https://pypi.org/project/djanquiltdb/
    :alt: Supported Django versions

.. image:: https://img.shields.io/badge/postgres-17%20%7C%2018-4169e1?logo=postgresql&logoColor=white
    :target: https://www.postgresql.org/
    :alt: Supported PostgreSQL versions

.. image:: https://img.shields.io/pypi/l/djanquiltdb.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/blob/master/LICENSE
    :alt: BSD-3-Clause licence

**DjanQuiltDB** is an extension to the Django web framework that provides helper functions to split a database based on
top level hierarchy. It is specifically designed to support horizontal sharding not just within a single database
cluster, but also across multiple database clusters, thus allowing you to scale database capacity both vertically and
horizontally as your dataset grows.

This repository develops the library and its plugins, each shipped as a distribution of its own:

* ``packages/djanquiltdb``: the core library, published as `djanquiltdb`::

      pip install djanquiltdb

* ``packages/djanquiltdb-plugin-postgres-objects``: sharding decorators for the PostgreSQL functions and views
  `django-postgres-objects <https://github.com/djanquiltdb/django-postgres-objects>`_ declares, installed through the
  core library's extra::

      pip install djanquiltdb[postgres-objects]

DjanQuiltDB discovers plugins through the ``djanquiltdb.plugins`` entry-point group, so plugins can also be developed
outside this repository.

The documentation for the library and every in-tree plugin builds from ``docs/`` and is published at
https://djanquiltdb.readthedocs.io/.

Development
===========

Enable the git hook once per clone::

    pre-commit install

Everything else runs through tox in Docker; see ``DOCKER.md``::

    docker compose run --rm test tox

Each package carries its own README, changelog and version, and is released independently.

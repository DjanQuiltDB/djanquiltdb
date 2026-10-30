.. The logo needs an absolute URL: this README is the PyPI long description, where a relative path renders broken.

.. image:: https://raw.githubusercontent.com/DjanQuiltDB/djanquiltdb/master/assets/icon-128.png
    :alt: DjanQuiltDB
    :width: 128

===================================
djanquiltdb-plugin-postgres-objects
===================================

.. image:: https://img.shields.io/pypi/v/djanquiltdb-plugin-postgres-objects.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-postgres-objects/
    :alt: PyPI

.. image:: https://img.shields.io/pypi/pyversions/djanquiltdb-plugin-postgres-objects.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-postgres-objects/
    :alt: Supported Python versions

.. image:: https://img.shields.io/pypi/frameworkversions/django/djanquiltdb-plugin-postgres-objects.svg
    :target: https://pypi.org/project/djanquiltdb-plugin-postgres-objects/
    :alt: Supported Django versions

.. image:: https://img.shields.io/badge/postgres-17%20%7C%2018-4169e1?logo=postgresql&logoColor=white
    :target: https://www.postgresql.org/
    :alt: Supported PostgreSQL versions

.. image:: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml
    :alt: CI

.. image:: https://readthedocs.org/projects/djanquiltdb/badge/?version=latest
    :target: https://djanquiltdb.readthedocs.io/en/latest/plugins/postgres-objects/index.html
    :alt: Documentation

.. image:: https://img.shields.io/pypi/l/djanquiltdb-plugin-postgres-objects.svg
    :target: https://github.com/DjanQuiltDB/djanquiltdb/blob/master/LICENSE
    :alt: BSD-3-Clause licence

`django-postgres-objects <https://github.com/djanquiltdb/django-postgres-objects>`_ declares Postgres functions and
views as classes and lets ``makemigrations`` manage them. `DjanQuiltDB <https://github.com/djanquiltdb/djanquiltdb>`_
shards a database across schemas and nodes. This library serves as a compatibility layer between them, allowing you to
mark declarative functions and views as sharded::

    # example/functions.py
    from djanquiltdb.decorators import public_function
    from postgres_objects import Function


    @public_function()
    class AllUppercase(Function):
        arguments = 'input TEXT'
        returns = 'TEXT'
        volatility = 'IMMUTABLE'
        strict = True
        parallel = 'SAFE'
        body = """
            BEGIN
                RETURN UPPER(input);
            END;
        """

Install it through DjanQuiltDB's extra::

    pip install djanquiltdb[postgres-objects]

The `installation guide <https://djanquiltdb.readthedocs.io/en/latest/plugins/postgres-objects/installation.html>`_
describes each step in full. ``postgres_objects`` goes in ``INSTALLED_APPS``, and the ``POSTGRES_OBJECTS`` setting names
the module of each app that declares its functions and the module that declares its views::

    # settings.py
    INSTALLED_APPS = [
        ...
        'djanquiltdb',
        'postgres_objects',
        ...
    ]

    POSTGRES_OBJECTS = {
        'FUNCTIONS_MODULE_PATH': 'functions',
        'VIEWS_MODULE_PATH': 'db_views',
    }

The decorators are importable from ``djanquiltdb.decorators`` beside the decorators of the models. Functions get
``@public_function()``, ``@mirrored_function()`` and ``@sharded_function()``, and views get ``@public_view()``,
``@mirrored_view()`` and ``@sharded_view()``. A declaration without one of these decorators is placed in the public
schema of every node, and fails the system check ``djanquiltdb_plugin_postgres_objects.E001``, so that every
declaration says where it belongs.

Full documentation is at https://djanquiltdb.readthedocs.io/en/latest/plugins/postgres-objects/index.html.

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
* djanquiltdb 4.5.0 or newer 4.x
* django-postgres-objects 1.0.0 or newer 1.x

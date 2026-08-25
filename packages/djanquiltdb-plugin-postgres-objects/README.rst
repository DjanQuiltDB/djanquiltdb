===================================
djanquiltdb-plugin-postgres-objects
===================================

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

The decorators are importable from ``djanquiltdb.decorators`` beside the model ones, as above.

Functions get ``@public_function``, ``@mirrored_function`` and ``@sharded_function``; views get ``@public_view``,
``@mirrored_view`` and ``@sharded_view``. An unannotated declaration is placed in the public schema, so declarations
written for a single-database project keep working once that project is sharded.

Development
===========

This package is developed in the `DjanQuiltDB repository <https://github.com/DjanQuiltDB/djanquiltdb>`_ beside
``djanquiltdb`` itself. Tests run through tox against the Postgres containers in the repository's
``docker-compose.yml``; see ``DOCKER.md`` at the repository root::

    docker compose run --rm test tox

Requirements
------------

* Python 3.14
* Django 6.0
* PostgreSQL 17 or 18
* djanquiltdb 4.x
* django-postgres-objects 1.x

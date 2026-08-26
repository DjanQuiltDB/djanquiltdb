DjanQuiltDB
===========

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

The documentation for the library and every in-tree plugin builds from ``docs/``.

Development
===========

Enable the git hook once per clone::

    pre-commit install

Everything else runs through tox in Docker; see ``DOCKER.md``::

    docker compose run --rm test tox

Each package carries its own README, changelog and version, and is released independently.

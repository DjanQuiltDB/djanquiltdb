DjanQuiltDB - Django Database Sharding
======================================

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

Development
===========

To setup your development environment it is important to install the development requirements first::

    pip install -e .[dev]

Next, copy the example secrets file in the ``djanquiltdb`` directory and adjust the parameters::

    cp secrets.json.example secrets.json

Tests
-----

Tox, run from the repository root, runs the test suite against multiple Python and Django versions::

    tox -m core

Building
--------

Building a distribution requires the ``build`` package (``pip install build``).
To build both the source distribution and the wheel, simply run::

    python -m build

The results are then placed in the /dist folder

Documentation
-------------

Documentation can be found in the `/docs` directory at the repository root, shared with the in-tree plugins. Build
it from there with::

    make -C docs html

Attribution
===========

This is an independently maintained fork of the patchman-django-sharding library originally created and maintained by
Patchman B.V. (2017-2023) and Cloud Linux Software, Inc. (2023-2025).
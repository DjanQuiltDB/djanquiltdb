django-sharding
===============

**django-sharding** is an extension to the Django web framework that provides
helper functions to split a database based on top level hierarchy.

Development
===========

To setup your development environment it is important to install the development requirements first::

    pip install -e .[dev]

Next, copy the example environment file and adjust their parameters::

    cp .env.example .env

Tests
-----

Tox can be used to run the test suite against multiple Python and Django versions::

    tox

Building
--------

To build the library, simply run::

    python setup.py build

Documentation
-------------

Documentation can be found in the `/docs` directory. Build the documentation with::

    python setup.py build_sphinx

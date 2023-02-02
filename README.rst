patchman-django-sharding
========================

**patchman-django-sharding** is an extension to the Django web framework that provides
helper functions to split a database based on top level hierarchy.

Development
===========

To setup your development environment it is important to install the development requirements first::

    pip install -e .[dev]

Next, copy the example secrets file in the ``patchman_django_sharding`` directory and adjust the parameters::

    cp secrets.json.example secrets.json

Tests
-----

Tox can be used to run the test suite against multiple Python and Django versions::

    tox

Building
--------

To build the library, simply run::

    python setup.py build

And to make a distribution, run ::

    python setup.py sdist

The result is then bound in the /dist folder

Documentation
-------------

Documentation can be found in the `/docs` directory. Build the documentation with::

    python setup.py build_sphinx

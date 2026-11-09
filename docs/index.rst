DjanQuiltDB - Django Database Sharding
======================================
Easy horizontal database sharding that allows for scaling and faster queries.

Release |version|.

.. container:: badges

    .. image:: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml/badge.svg
        :target: https://github.com/DjanQuiltDB/djanquiltdb/actions/workflows/ci.yml
        :alt: CI

    .. image:: https://img.shields.io/codecov/c/github/DjanQuiltDB/djanquiltdb/master?flag=core
        :target: https://app.codecov.io/gh/DjanQuiltDB/djanquiltdb
        :alt: Coverage

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


.. toctree::
    :maxdepth: 2
    :caption: Getting started

    getting_started/overview
    getting_started/installation

.. toctree::
    :maxdepth: 2
    :caption: Guides

    guides/sharding_modes
    guides/shard_model
    guides/mapping_model
    guides/template_schema
    guides/queries
    guides/transactions
    guides/requests
    guides/sessions
    guides/oauth2
    guides/async
    guides/forms
    guides/migrations
    guides/fixtures

.. toctree::
    :maxdepth: 2
    :caption: Database objects

    database_objects/views
    database_objects/functions
    database_objects/triggers
    database_objects/generated_columns

.. toctree::
    :maxdepth: 2
    :caption: Shard operations

    operations/align_shards_with_template
    operations/move_data_to_shard
    operations/move_shard_to_node
    operations/purge_shard_data

Contrib packages are optional Django apps that ship inside ``djanquiltdb``, under ``djanquiltdb.contrib``. Each one is
enabled by adding it to ``INSTALLED_APPS``.

.. toctree::
    :maxdepth: 2
    :caption: Contrib packages

    contrib/authentication
    contrib/admin_shard_switcher

Optional features are plugins: distributions that are installed separately, through an extra of ``djanquiltdb``.
DjanQuiltDB finds them through the ``djanquiltdb.plugins`` entry-point group.

.. toctree::
    :maxdepth: 2
    :caption: Integrations

    plugins/celery/index
    plugins/oauth-toolkit/index
    plugins/pghistory/index
    plugins/postgres-denormalize/index
    plugins/postgres-objects/index

.. toctree::
    :maxdepth: 2
    :caption: Reference

    reference/commands
    reference/settings
    reference/checks
    reference/api
    reference/changelog


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

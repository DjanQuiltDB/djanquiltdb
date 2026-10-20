DjanQuiltDB - Django Database Sharding
======================================
Easy horizontal database sharding that allows for scaling and faster queries.


Release |version|.


.. toctree::
    :maxdepth: 2
    :caption: Getting started

    getting_started/overview
    getting_started/installation

.. toctree::
    :maxdepth: 2
    :caption: Guides

    guides/sharding_modes
    guides/queries
    guides/requests
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
    plugins/pghistory/index
    plugins/postgres-denormalize/index
    plugins/postgres-objects/index

.. toctree::
    :maxdepth: 2
    :caption: Reference

    reference/settings
    reference/checks
    reference/api
    reference/changelog


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

DjanQuiltDB - Django Database Sharding
======================================
Easy horizontal database sharding that allows for scaling and faster queries.


Release |version|.


The User Guide
--------------

.. toctree::
    :maxdepth: 2

    modules/overview
    modules/installation
    modules/sharding_modes
    modules/queries
    modules/views
    modules/forms
    modules/decorators
    modules/utils
    modules/connection
    modules/migrations
    modules/database_views
    modules/database_functions
    modules/triggers
    modules/generated_columns
    modules/fixtures
    modules/celery
    modules/commands
    modules/contrib


Plugins
-------

Optional features ship as plugins: separately installed distributions DjanQuiltDB discovers through the
``djanquiltdb.plugins`` entry-point group. Each is installed through an extra of ``djanquiltdb`` itself.

.. toctree::
    :maxdepth: 2

    plugins/postgres-objects/index


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

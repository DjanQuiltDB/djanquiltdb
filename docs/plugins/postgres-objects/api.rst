=============
API reference
=============

The placement decorators are importable from ``djanquiltdb.decorators`` beside the model ones once the plugin is
installed; the module below is where they are defined.


Decorators
==========

.. currentmodule:: djanquiltdb_plugin_postgres_objects.decorators

Functions
---------

.. autofunction:: public_function

.. autofunction:: mirrored_function

.. autofunction:: sharded_function

.. autofunction:: single_node_unsharded_function

Views
-----

.. autofunction:: public_view

.. autofunction:: mirrored_view

.. autofunction:: sharded_view

.. autofunction:: single_node_unsharded_view

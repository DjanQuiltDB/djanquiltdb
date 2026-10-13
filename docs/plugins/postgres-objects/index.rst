postgres-objects
================
Compatibility layer between DjanQuiltDB and `django-postgres-objects
<https://github.com/djanquiltdb/django-postgres-objects>`_, allowing marking declarative functions and views as
sharded. Installed through an extra of ``djanquiltdb`` itself::

    pip install djanquiltdb[postgres-objects]

Plugin release |postgres-objects-version|.

Things to know
--------------

* Migrations that were generated before the plugin was installed have no placement, so they create their objects in
  the public schema of every node. Review them, and regenerate the ones that should not be public. See
  :doc:`installation`.
* A function or view without a placement decorator is public. See :doc:`functions` and :doc:`views`.
* A sharded function cannot be reached from the public schema. A public function can read sharded tables, as long as it
  does not qualify their names with a schema. See :doc:`functions`.
* Only the views of django-postgres-objects are supported, and not those of django-pgviews-redux. See :doc:`views`.
* A view that reads sharded tables has to be sharded too, because a view is bound to its tables when it is created. See
  :doc:`views`.
* A materialized view that is declared with ``with_data=False`` needs a ``RefreshMaterializedView`` in the migration
  that creates it. Without one, it cannot be read in the template schema and in every shard that is cloned from it. See
  :doc:`views`.
* ``move_shard_to_node`` and ``move_data_to_shard`` refresh the materialized views of the shards that they change.
  After any other change, refresh a materialized view yourself. A sharded view can only be refreshed inside a shard, and
  a mirrored view is refreshed on every node. See :doc:`views`.
* A ``postgres_objects.GeneratedField`` calculates its stored values again when its function changes. For a public
  function in a sharded column, the function changes in the public schema, and the values in each shard. See
  :doc:`generated_columns`.
* Cloning a schema for a new shard does not copy aggregates, row-level security policies, or the grants and comments of
  views and functions. See :ref:`cloning_limitations`.


.. toctree::
    :maxdepth: 2

    installation
    functions
    views
    generated_columns
    api
    changelog

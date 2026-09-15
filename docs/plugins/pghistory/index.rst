pghistory
=========
This plugin makes DjanQuiltDB and `django-pghistory <https://django-pghistory.readthedocs.io/>`_ work together: it
stores each event table in the same place as the table of the tracked model. Install it through an extra of
``djanquiltdb`` itself::

    pip install djanquiltdb[pghistory]

Plugin release |pghistory-version|.

Things to know
--------------

* Do not use the ``pgtrigger`` management commands that change the database directly. Triggers only reach the template
  schema and every shard through migrations. See :doc:`installation`.
* Give the ``pghistory`` app a sharding mode, preferably public, because its ``Context`` table exists even when no
  event model uses it. With a normalized context, a sharded or mirrored ``Context`` table has drawbacks, which the
  plugin's system checks report. See :doc:`installation`.
* ``pghistory.context()`` reaches the connection of every node, so a context that is entered before a shard is selected
  also reaches a shard on another node. See :doc:`installation`.
* Changing the sharding mode of the ``pghistory`` app in an existing project does not move its table and function. That
  takes a one-time migration. See :doc:`installation`.
* Apply the sharding mode decorator below ``track()``, so that ``track()`` can read the mode. See :doc:`tracking`.
* The events of a mirrored model are written on the primary node, so the replication that copies the tracked table to
  the other nodes has to copy the event table too. See :doc:`tracking`.
* An extra field on an event base needs a ``db_default``, or a trigger that fills it in, because the trigger that writes
  the events does not apply a Django ``default``. See :doc:`tracking`.
* ``Event.revert()`` writes through the router, to the active shard or the primary node, and not to the ``default``
  connection as pghistory's own does. See :doc:`tracking`.
* pghistory does not inherit the ``Meta`` of an event base, so a trigger or a meta option for every event model has to
  come from a wrapper of ``track()``. See :doc:`customization`.
* A type checker cannot follow the names that the plugin serves lazily. Point it at the modules that define them. See
  :doc:`api`.


.. toctree::
    :maxdepth: 2

    installation
    tracking
    customization
    api

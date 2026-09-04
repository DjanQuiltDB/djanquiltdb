============
Installation
============

Install through DjanQuiltDB's extra::

    pip install djanquiltdb[pghistory]

The plugin itself needs no setup: DjanQuiltDB finds it through its entry point as soon as it is installed. The two
libraries that it connects still need their own setup. ``pghistory`` is a Django app, and goes in ``INSTALLED_APPS``
next to ``djanquiltdb`` and the ``pgtrigger`` app that it builds on::

    INSTALLED_APPS = (
        ...
        'djanquiltdb',
        'pgtrigger',
        'pghistory',
        ...
    )

django-pghistory writes events through django-pgtrigger triggers, so everything :doc:`the trigger documentation
</database_objects/triggers>` says applies: triggers reach the template and every shard exclusively through
migrations, and the ``pgtrigger`` management commands that bypass migrations stay off limits.

Two settings need a value that survives sharding, and the plugin registers system checks that warn when they do not
hold:

* ``PGHISTORY_CONTEXT_FIELD`` must denormalize the context into the event rows
  (``djanquiltdb_plugin_pghistory.W001``). pghistory's default is a foreign key to its context table, and an event
  table inside a shard cannot hold a foreign key to a table outside it::

      import pghistory

      PGHISTORY_CONTEXT_FIELD = pghistory.ContextJSONField()

* The ``pghistory`` app carries a concrete ``Context`` model, and nothing places it unless the settings say where it
  goes (``djanquiltdb_plugin_pghistory.W002``). With the denormalized context field the table stays unused, but it
  still has to land somewhere::

      QUILT_DB = {
          ...
          'OVERRIDE_SHARDING_MODE': {
              ...
              ('pghistory',): ShardingMode.SHARDED,
          },
      }

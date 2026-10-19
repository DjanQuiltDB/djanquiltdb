postgres-denormalize
====================
This plugin makes DjanQuiltDB and `django-postgres-denormalize <https://django-postgres-denormalize.readthedocs.io/>`_
work together: it finds and corrects the stored copies that are out of date, in one shard or in every shard. Install it
through an extra of ``djanquiltdb`` itself::

    pip install djanquiltdb[postgres-denormalize]

Plugin release |postgres-denormalize-version|.

.. toctree::
    :maxdepth: 2

    installation
    repairing
    pitfalls
    moving
    api
    changelog


Things to know
--------------

* The triggers of django-postgres-denormalize reach the template schema and every shard through migrations, like any
  other django-pgtrigger trigger. ``BackfillDenormalized`` needs no hints: it runs in the template and in every shard.
  See :doc:`installation`.
* Run every node that stores copies at SERIALIZABLE. django-postgres-denormalize's ``postgres_denormalize.W303``
  reports each node whose shards store copies and that runs at a lower level. See :doc:`installation`.
* ``backfill_denormalized()`` corrects the table of the active shard only. ``repair_shard()`` and
  ``repair_every_shard()`` enter each shard for you. See :doc:`repairing`.
* **Do not copy a column of a mirrored or public model.** Its cascade and fill triggers only reach the current shard, and
  fail outside one. DjanQuiltDB's system check ``djanquiltdb.E001`` reports both. See :doc:`pitfalls`.
* Moving a shard or its data does not fire the triggers, so a copy that is out of date stays out of date in its new
  shard. See :doc:`moving`.

======================================
Do not copy mirrored or public columns
======================================

A sharded model can have a foreign key to a mirrored or public model, and django-postgres-denormalize accepts a copy of
a column through such a key. So you might be tempted to copy a column of a mirrored or public model, such as the name
of a task type:

.. code-block:: python

    from djanquiltdb.decorators import mirrored_model, sharded_model
    from postgres_denormalize.fields import DenormalizedField


    @mirrored_model()
    class TaskType(models.Model):
        name = models.CharField(max_length=100)


    @sharded_model()
    class Task(models.Model):
        type = models.ForeignKey(TaskType, on_delete=models.PROTECT)
        type_name = DenormalizedField('type__name')

**Do not do this.** Only one of the three triggers works. The set trigger on ``Task`` works: a shard's search path ends
with ``public``, so the trigger finds the ``TaskType`` row in the ``public`` schema of the shard's node. The cascade
trigger, which runs when a ``TaskType`` row changes, and the fill trigger, which runs when a ``TaskType`` row is
created, do not work. ``migrate`` installs both on the ``TaskType`` table in ``public``, and both name the table of
``Task`` without a schema, so PostgreSQL finds that table through the search path of the session that writes the
``TaskType`` row:

* **A write inside a shard on the primary node corrects that shard only.** Renaming a task type inside
  ``use_shard(shard)`` rewrites the tasks of that shard. The tasks in every other shard keep the old name, and nothing
  reports it.
* **A write anywhere else fails.** Outside a shard, the search path has no table of ``Task``. Inside a shard on another
  node, the router sends the write to the ``public`` schema of the primary node, where the search path has no table of
  ``Task`` either. In both cases, creating or renaming a task type raises ``relation "tenancy_task" does not exist`` and
  rolls back.
* **A replicated write runs no trigger.** DjanQuiltDB does not copy mirrored rows to the other nodes. When logical
  replication copies them, PostgreSQL runs no ordinary triggers for the replicated rows, so the shards on those nodes
  keep the old name.
* **The batch helpers do not reach every shard.** django-postgres-denormalize's ``ignore_cascades()`` and
  ``defer_cascades()`` correct the copies on the one database that the router chooses for the model that changes. Inside
  a shard on the primary node, they correct that shard only. Anywhere else, their correction raises the same error, and
  the change rolls back.
* **A shard without ``public`` breaks the set trigger.** Inside ``use_shard(shard, include_public=False)``, the search
  path has no ``TaskType`` table, so every insert of a task, and every update that changes its type, raises an error.

Copy from sharded models only, so that the whole lookup path stays inside the shard. To show a column of a mirrored or
public model, read it through the foreign key, for example with ``Task.objects.select_related('type')``.

DjanQuiltDB's system check reports the cascade trigger and the fill trigger each as ``djanquiltdb.E001``, so a project
with such a copy does not start (see :doc:`/database_objects/triggers`). There is deliberately no setting that allows
one copy.


If a project already has such a copy
------------------------------------

Remove the copy, or replace it with a column of a sharded model. Until then, the project has to list
``djanquiltdb.E001`` in ``SILENCED_SYSTEM_CHECKS``, which silences the check for every trigger. It then has to skip the
cascade trigger when it changes the column, and the fill trigger when it creates a row, and repair every shard
afterwards:

.. code-block:: python

    import pgtrigger
    from djanquiltdb.utils.postgres_denormalize import repair_every_shard

    with pgtrigger.ignore(
        'tenancy.TaskType:denormalize_cascade_tenancy_task',
        'tenancy.TaskType:denormalize_fill_tenancy_task',
    ):
        TaskType.objects.filter(pk=task_type.pk).update(name='Defect')
        TaskType.objects.create(name='Chore')

    repair_every_shard()

Each shard is repaired from the ``TaskType`` row in its own node's ``public`` schema, so run the repair after the change
has reached every node. Until the repair reaches a shard, its tasks have the old name.

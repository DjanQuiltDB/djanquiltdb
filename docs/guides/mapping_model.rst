=================
The mapping model
=================

A row of the Shard model describes a schema (see :doc:`/guides/shard_model`). A row of the mapping model describes a
tenant: the mapping model is a table with one row for each tenant, which names the shard that the data of the tenant is
in. They are separate tables because a shard can hold more than one tenant.


Why a mapping model
-------------------

* **Several tenants on one shard.** Small tenants can share a shard, a large tenant can have a shard of its own, and
  tenants that stopped using the project can move to an archive shard together. The Shard model knows which shards
  there are; the mapping model knows which tenant is on which of them.
* **Tenants are found by a value of their own.** Code enters the shard of a tenant with its mapping value, such as the
  slug of the tenant from the URL, a subdomain or the login form: ``use_shard_for('acme')``. It never needs the id of a
  shard, which says where data is stored, not whose data it is.
* **A tenant can move.** ``move_data_to_shard`` moves the data of one tenant to another shard, and points its row at
  that shard. Code finds the tenant by the same slug before and after the move.
* **Each tenant has a state.** A move puts the row of the tenant in maintenance while it runs, and the other tenants of
  both shards keep working.

The mapping model is optional. A project without one enters shards by the shard itself or by its id, with
``use_shard()``. That suits a project with one tenant per shard that names the shard after the tenant, or a project
that keeps the shard id where it needs it. The session backend ``djanquiltdb.sessions`` finds the shard of a session
through the mapping model, so a project without one handles its sessions itself (see :doc:`/guides/sessions`).


Defining the mapping model
--------------------------

The mapping model needs three fields:

* ``shard``: a foreign key to the Shard model.
* ``state``: a ``CharField`` with ``max_length=1`` and ``choices=STATES``, from ``djanquiltdb``.
* The mapping field, whose name ``@shard_mapping_model(mapping_field=...)`` gives. It has the mapping value of the row,
  such as the slug of the tenant.

Decorate it with ``@shard_mapping_model()`` and with ``@mirrored_model()``, give it ``MappingQuerySet`` as its manager,
and name it in the ``MAPPING_MODEL`` setting (see :doc:`/reference/settings`)::

    from django.db import models

    from djanquiltdb import STATES, State
    from djanquiltdb.decorators import mirrored_model, shard_mapping_model
    from djanquiltdb.models import MappingQuerySet


    @mirrored_model()
    @shard_mapping_model(mapping_field='tenant_slug')
    class TenantShard(models.Model):
        shard = models.ForeignKey('myapp.Shard', on_delete=models.CASCADE)
        tenant_slug = models.SlugField(unique=True)
        state = models.CharField(choices=STATES, max_length=1, default=State.ACTIVE)

        objects = MappingQuerySet.as_manager()

        class Meta:
            app_label = 'myapp'

    # settings.py
    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
        'MAPPING_MODEL': 'myapp.models.TenantShard',
    }

When you put a tenant on a shard, create its row::

    TenantShard.objects.create(shard=shard, tenant_slug='acme')

A project has one mapping model at most: the decorator raises ``ImproperlyConfigured`` on a second one.

The mapping model is mirrored, like the Shard model, so that every node has the mapping table. The system check
``djanquiltdb.E003`` reports a mapping model that is not mirrored (see :doc:`/reference/checks`), and
:ref:`making_an_existing_model_mirrored` describes how a project with more than one node gets there. Like every mirrored
model, it is written on the primary node, and you have to keep the copies on the other nodes the same.

The mapping field
~~~~~~~~~~~~~~~~~

The mapping field is usually a plain value, such as the slug of a tenant. A value that people and URLs use, rather than
a number, is easy to tell apart from the id of a shard. A foreign key from the mapping table to a sharded model is not
possible, because the mapping table is in the public schema and the rows of a sharded model are in
the schemas of the shards. The mapping field can be a foreign key to a public or mirrored model, which every node has.
``use_shard_for()`` and ``get_shard_for()`` then take a model instance as well as the value that the field stores: the
primary key of the instance, or the field that ``to_field`` names on the foreign key.

``route_to_primary_db``
~~~~~~~~~~~~~~~~~~~~~~~

``@shard_mapping_model()`` reads the mapping table on the primary node by default. The mapping table changes during
``move_data_to_shard``, and the primary node has a change before the other nodes do, so a lookup on the primary node
finds the shard that the command moved the data to as soon as the command is done.

With ``route_to_primary_db=False``, the mapping table is read on the active node, like every other mirrored model.
The copy of the active node can be behind the primary node, as the replication lag section of
:doc:`/operations/move_data_to_shard` describes. The mapping model is mirrored, so it is written on the primary node
either way.


Finding and entering the shard of a value
-----------------------------------------

``use_shard_for(value)`` enters the shard of a mapping value, as ``use_shard(shard)`` enters a shard::

    from djanquiltdb.utils import use_shard_for

    with use_shard_for('acme'):
        ...  # Queries of sharded models run in the shard of the tenant acme.

``get_shard_for(value)`` returns the shard without entering it. Both raise ``StateException`` when the row or its shard
is in maintenance, unless ``active_only_schemas=False`` (``active_only=False`` for ``get_shard_for()``), and the
``DoesNotExist`` of the mapping model when the table has no row for the value. ``get_shard_for(value, field=...)``
looks up the row on another unique field of the mapping model than the mapping field.

``ShardOptions.from_shard(shard, check_active_mapping_values=True)`` refuses a shard that has a row in maintenance,
also when the shard itself is active.

``UseShardForMiddleware`` enters the shard of the mapping value of each request (see :doc:`/guides/requests`), and the
admin shard switcher lists the mapping values when the project has a mapping model (see
:doc:`/contrib/admin_shard_switcher`).

The queryset
~~~~~~~~~~~~

``MappingQuerySet`` adds these methods to the manager of the mapping model:

* ``for_target(value, field=None)``: the row of a mapping value, looked up on the mapping field or on ``field``.
* ``for_shard(shard)``: the rows of a shard.
* ``in_maintenance()``: the rows that are in maintenance, or whose shard is.
* ``active()``: the rows that are active, and whose shard is active.


Locks and the move commands
---------------------------

``use_shard_for()`` takes two shared advisory locks while it is entered: the lock of the shard, as ``use_shard()``
takes, and the lock of the mapping value. The name of the lock of a mapping value has the value that the mapping field
stores, so a model instance and its stored value take the same lock.

The move commands take the exclusive locks of what they change, so they wait for the ``use_shard_for()`` contexts that
are entered, and the contexts that are entered later wait for the command:

* ``move_data_to_shard`` takes the exclusive lock of the mapping value that it moves, puts its row in maintenance,
  copies the data, and then points the row at the target shard and gives it back its state. The other values of both
  shards stay active. See :doc:`/operations/move_data_to_shard`.
* ``move_shard_to_node`` takes the exclusive lock of the shard and of each of its mapping values, and puts the shard and
  its rows in maintenance until the shard is on its new node. See :doc:`/operations/move_shard_to_node`.

After a ``use_shard_for()`` gets its locks, it reads the row of its value again, and raises ``StateException`` when a
move command changed the row in the meantime.

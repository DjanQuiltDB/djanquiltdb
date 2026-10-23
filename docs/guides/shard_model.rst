===============
The Shard model
===============

A shard is a schema on a node, where the data of one or more tenants lives. The Shard model is the registry of the
shards: each row names the node and the schema of one shard, and every query that DjanQuiltDB routes to a shard starts
from that row.


Defining the Shard model
------------------------

Define the Shard model as a subclass of ``BaseShard``, decorate it with ``@mirrored_model()``, and name it in the
``SHARD_CLASS`` setting (see :doc:`/reference/settings`)::

    from djanquiltdb.decorators import mirrored_model
    from djanquiltdb.models import BaseShard


    @mirrored_model()
    class Shard(BaseShard):
        class Meta:
            app_label = 'myapp'

    # settings.py
    QUILT_DB = {
        'SHARD_CLASS': 'myapp.models.Shard',
    }

``BaseShard`` gives it four fields:

* ``alias``: a unique name for the shard, for people and for the management commands.
* ``node_name``: the alias of the node in ``DATABASES`` that has the schema of the shard.
* ``schema_name``: the name of the schema. A node has each schema name once, so ``node_name`` and ``schema_name``
  together are unique.
* ``state``: whether the shard can be used. See `States`_.

The project can add fields of its own. The Shard model is mirrored, so that every node has the registry. The system
check ``djanquiltdb.E003`` reports a Shard model that is not mirrored (see :doc:`/reference/checks`).


Creating a shard
----------------

Saving a new shard creates its schema, as a clone of the template schema of its node (see
:doc:`/guides/template_schema`)::

    from djanquiltdb import State

    shard = Shard.objects.create(alias='shard_1', node_name='default', schema_name='shard_1', state=State.ACTIVE)

Without a ``node_name``, the shard goes to the node that the ``NEW_SHARD_NODE`` setting names. A shard starts in
``MAINTENANCE`` unless it is created with another state, so give it ``State.ACTIVE`` when it is created, or once it is
ready to be used.


States
------

A shard is ``ACTIVE`` or in ``MAINTENANCE``. Entering a shard that is in maintenance raises ``StateException``, and
the request middleware turns that into a 503 response (see :ref:`exception_middleware`). ``move_shard_to_node`` puts a shard
in maintenance while it moves the shard. ``move_data_to_shard`` does the same to its source shard, or, with a mapping
model, only to the tenants that it moves (see :doc:`/operations/move_data_to_shard`).


Entering a shard
----------------

``use_shard()`` enters a shard, so that the queries of sharded models run in its schema::

    from djanquiltdb.utils import use_shard

    with use_shard(shard):
        ...  # Queries of sharded models run in the schema of the shard.

``shard.use()`` does the same. ``use_shard(node_name=..., schema_name=...)`` enters a schema without looking up a
shard. :doc:`/guides/queries` describes these, and the other ways to run queries in a shard.


Deleting a shard
----------------

``shard.delete()`` deletes the row of the shard, and leaves its schema and the data in it.
``shard.delete(delete_from_db=True)`` drops the schema as well.


Shards and tenants
------------------

A shard does not say which tenants it holds. A project that keeps one tenant on each shard can name the shard after its
tenant. To put several tenants on one shard, or to find the shard of a tenant by a value of the tenant itself, add a
mapping model (see :doc:`/guides/mapping_model`).

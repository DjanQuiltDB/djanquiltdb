==============
Sharding modes
==============

To define which table ends up in which schema we offer three decorators designed to be placed upon a model definition.
The decorator also influences how its queries get routed:

* ``@sharded_model()``: This model will have its table migrated to the sharded schemas of all nodes. Queries will be
  routed according to the prevailing sharding context.

* ``@public_model()``: This model will have its table migrated to the public schemas of all nodes, and the table on each
  node has rows of its own. Queries will be routed according to the prevailing sharding context.

* ``@mirrored_model()``: This model will have its table migrated to the public schemas of all nodes. The write queries
  for this model will be routed to the primary node. The read queries will be routed according to the prevailing
  sharding context.

Special decorator:

* ``@shard_mapping_model(mapping_field='user_id')``: The mapping table is optional. When defined it allows usages
  of helper functions to select shards based on the mapping values. Specifically the field designated as mapping field
  in the decorator. Decorate the mapping model, like the Shard model, with ``@mirrored_model()`` as well. See
  :doc:`/guides/mapping_model`.

:ref:`More info <api_decorators>` on decorators and how to use them.

The models of a third-party app get their mode from the ``OVERRIDE_SHARDING_MODE`` setting (see
:doc:`/reference/settings`), which maps an app, or a model of an app, to a mode. These are the modes for the apps with
models that a new Django project has in ``INSTALLED_APPS``::

    from djanquiltdb import ShardingMode

    QUILT_DB = {
        ...
        'OVERRIDE_SHARDING_MODE': {
            ('auth',): ShardingMode.MIRRORED,
            ('contenttypes',): ShardingMode.MIRRORED,
            ('admin', 'logentry'): ShardingMode.SHARDED,
            # Unused with the djanquiltdb.sessions backend, see the Sessions guide.
            ('sessions',): ShardingMode.PUBLIC,
        },
    }

The permissions of ``auth`` refer to the content types, so both are mirrored. The admin log is sharded, so each shard
keeps the log of the changes made in it, and its foreign key to the user also works when the project has a sharded user
model. :ref:`session_backend_setup` explains the mode of ``sessions``. These are the settings of
:doc:`/getting_started/installation`.

Every model needs a mode. A model without one fails the system check ``djanquiltdb.E004`` (see
:doc:`/reference/checks`).


Models on a single node
-----------------------

``@single_node_unsharded_model(node)`` puts a model on one node only, and opts it out of ``djanquiltdb.E004``. It is
strongly discouraged: it is a convenience for a project that moves an app without sharding to DjanQuiltDB one model
at a time, and you take on the risks below.

* The table of the model is in the public schema of ``node`` only. ``node`` is the alias of a node, or
  ``PRIMARY_NODE`` (``from djanquiltdb import PRIMARY_NODE``) for the node that ``PRIMARY_DB_ALIAS`` names.
* Every query of the model goes to that node, whichever node or shard is active.
* Only a model on the same node can refer to it, and it can only refer to a public model, a mirrored model or a model on
  the same node. Every other foreign key, one-to-one or many-to-many field to or from it fails the system check
  ``djanquiltdb.E005``.
* Each model on a single node gets the warning ``djanquiltdb.W004``. Silence it with
  ``SILENCED_SYSTEM_CHECKS = ['djanquiltdb.W004']``.

A model of a third-party app gets the same placement from ``single_node_unsharded(node)`` in
``OVERRIDE_SHARDING_MODE``::

    from djanquiltdb import PRIMARY_NODE, single_node_unsharded

    QUILT_DB = {
        ...
        'OVERRIDE_SHARDING_MODE': {
            ('some_app',): single_node_unsharded(node=PRIMARY_NODE),
        },
    }

A model without a mode has its table on the node that is named ``default``, so ``node='default'`` keeps its table where
it is. Unlike a model without a mode, whose queries go to the active node and
fail on every other node, a model on a single node is queried on its node from everywhere. ``node=PRIMARY_NODE`` puts
the table on another node when ``PRIMARY_DB_ALIAS`` does not name ``default``, so move the table there before the
project uses it.

A project that still uses Django's database session backend while it moves to DjanQuiltDB can keep the table of that
backend on one node with ``('sessions',): single_node_unsharded(node=...)`` until it switches to the
``djanquiltdb.sessions`` backend (see :doc:`/guides/sessions`).

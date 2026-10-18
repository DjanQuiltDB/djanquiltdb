=======
Queries
=======

The meat of using this library is defining 'on what shard you are working on'. By default your queries are routed to
the primary node, with only the public schema selected. So only public and mirrored models are visible.
All others will result in a form of 'table does not exist' error.

Overriding a connection
-----------------------
Database connections are overridden by the library. This happens by monkeypatching them upon startup.
(See ``djanquiltdb.apps._patch_connections``). Remember that there is always an open connection to each node Django knows.
For each of those connection we set our current active shard, if there is any.
There are various ways to influence the active shard, and there are ways to route queries specifically,
if you want to bypass this.

The sharding context
--------------------
The most common way to set the active shard on a connection is by setting a sharding context.

.. code-block:: python

  from djanquiltdb.utils import use_shard

  with use_shard(shard_object):
      User.objects.all()

Within a ``use_shard`` context manager, the active shard will be set to the given shard.

.. code-block:: python

  User.objects.all()  # Table does not exist error           # shard context: None

  with use_shard(Shard.objects.get(id=1)):                   # shard context: <shard 1>
      User.objects.all()  # returns values

      with use_shard(Shard.objects.get(id=2)):               # shard context: <shard 2>
          User.objects.all()  # returns different values
                                                             # shard context: returns to <shard 1>
      User.objects.all()  # returns same as first
                                                             # shard context: returns to None
  User.objects.all()  # Table does not exist error

What a sharding context accepts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In the examples given so far, we pass a Shard model instance to set up a sharding context, for example
``with use_shard(Shard.objects.get(id=1)):`` or ``User.objects.using(Shard.objects.get(id=1)).all()``.
This is not the only way to tell the router which shard we want. Whatever value these functions are given, they create
a ``ShardOptions`` object from it, which the router uses to direct the query to the correct place.

``use_shard`` accepts:

* a Shard object:

  .. code-block:: python

    shard = Shard.objects.create(alias='org_1', node_name='default', schema_name='org_1_schema')
    with use_shard(shard):
        (...)

* a node name and a schema name:

  .. code-block:: python

    with use_shard(node_name='default', schema_name='org_1_schema'):
        (...)

``use_shard_for`` takes the value of the mapping field of a mapping model, and looks up its shard:

.. code-block:: python

  shard = Shard.objects.create(alias='org_1', node_name='default', schema_name='org_1_schema')
  OrganizationShard.objects.create(shard=shard, organization_id=1)
  with use_shard_for(1):
      (...)

``.using()`` and ``connections`` accept more forms, such as a ``'<node_name>|<schema_name>'`` string and a
``ShardOptions`` object, as described below.

.. note:: :ref:`More info <api_utils>` on helper functions to easily create a sharding context.


Object context persistence
--------------------------
Objects received keep their sharding context information on itself (Saved on ``<model instance>._state.db``).
This is useful, for any function you call on it will be performed within the sharding context of that object.

.. code-block:: python

  with use_shard(Shard.objects.get(id=1)):
      user = User.objects.get(id=1)

  # Outside of a sharding context
  user.name = 'Bob'
  user.save()  # Write is performed on shard-1

Unless this is explicitly overwritten:

.. code-block:: python

  with use_shard(Shard.objects.get(id=1)):
      user = User.objects.get(id=1)

  # Outside of a sharding context
  user.name = 'Bob'
  user.save(using=Shard.objects.get(id=2))  # Write is performed on shard-2


Connections
-----------

The sharding library is mostly about switching and targeting a specific connection to a schema. This can be done by
``use_shard``, which internally picks a connection to a shard and remembers this connection within the contextmanager.
You can get that connection also without ``use_shard`` by importing ``django.db.connections``. In order to target a
specific schema, you have multiple options.

The ``connections`` accept multiple aliases:

* A string ``<node_name>``
* A string ``<node_name>|<schema_name>``
* A tuple ``(<node_name>, <schema_name>)``
* A shard model instance
* A ``ShardOptions`` instance

Public schema
~~~~~~~~~~~~~

Just as without the sharding library, one can target a specific connection by calling ``connections`` with the database
alias. This will return the connection to the public schema of that database.

.. code-block:: python

    connections['default']  # Returns a connection to default's public schema
    connections['other']  # Returns a connection to other's public schema


Other schema
~~~~~~~~~~~~

The easiest way to connect to a different schema is by specifying the node name and the schema name divided by a pipe.
This will select the public schema as well, together with the targeted schema.

.. code-block:: python

    connections['default|template']  # Returns a connection to default's public schema and template schema
    connections['other|foo_schema']  # Returns a connection to other's public schema and foo_schema


Tuple
~~~~~

The same as with the string of node name and schema name divided by a pipe, it's also possible to target a schema by
providing a tuple consisting of node name and schema name.

.. code-block:: python

    connections[('default', 'template')]  # Returns a connection to default's public schema and template schema
    connections[('other', 'foo_schema')]  # Returns a connection to other's public schema and foo_schema


Shard
~~~~~

Most of the time you want to target a schema that's defined by a shard instance. Connections also accept a shard
instance, that will be used to determine the node name and schema name which we want to connect to.

.. code-block:: python

    from djanquiltdb.utils import get_shard_class


    shard = get_shard_class().objects.get(node_name='default', schema_name='foo_schema')
    connections[shard]  # Returns a connection to default's public schema and foo_schema


ShardOptions
~~~~~~~~~~~~

The most advanced way to get a connection is with ``ShardOptions``, which is also used internally by ``use_shard``.


.. code-block:: python

    from djanquiltdb.options import ShardOptions
    from djanquiltdb.utils import get_shard_class


    # Returns a connection to default's public and foo_schema. Equivalent to connections['default|foo_schema']
    shard_options = ShardOptions(node_name='default', schema_name='foo_schema')
    connections[shard_options]

    # Returns a connection to default's public and foo_schema. Equivalent to connections[shard]
    shard = get_shard_class().objects.get(node_name='default', schema_name='foo_schema')
    shard_options = ShardOptions(node_name='default', schema_name='foo_schema', shard_id=shard.id)
    connections[shard_options]

    # Returns a connection to default's public and foo_schema, but also saves the mapping value as an option.
    mapping_model = get_mapping_class()
    mapping_object = mapping_model.objects.filter(shard__node_name='default', shard__schema_name='foo_schema').first()
    mapping_value = getattr(mapping_object, mapping_model.mapping_field)

    shard_options = ShardOptions(node_name='default', schema_name='foo_schema', shard_id=mapping_object.shard.id,
                                 mapping_value=mapping_value)
    connections[shard_options]


``.using()``
------------
When using the Django ORM, you can specify which node you want the query to perform on using ``.using('node-name')``.
The library extends this behavior to allow for shard selection this way as well.
``User.objects.using(Shard.objects.get(id=1)).all()`` routes the resulting query to go to the shard we want.
This overrides any prevailing sharding context.

All the connection aliases described before can also be used in a ``QuerySet``'s ``using`` method. This allows you to
perform a query on a shard without having to use ``use_shard``. This can be useful in templates, where you define the
schema the query needs to run on in the view, and it will run the query as soon as it gets evaluated in the template in
the correct schema. All the examples below will run the query in the ``foo_schema`` (and the ``public`` schema).


.. code-block:: python

    from djanquiltdb.options import ShardOptions
    from djanquiltdb.utils import get_shard_class

    Foo.objects.using('default').all()
    Foo.objects.using('default|foo_schema').all()
    Foo.objects.using(('default', 'foo_schema')).all()

    shard = get_shard_class().objects.get(node_name='default', schema_name='foo_schema')
    Foo.objects.using(shard).all()

    shard_options = ShardOptions(node_name='default', schema_name='foo_schema')
    Foo.objects.using(shard_options).all()

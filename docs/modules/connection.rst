==========
Connection
==========

The sharding library is mostly about switching and targeting a specific connection to a schema. This can be done by
``use_shard``, which internally picks a connection to a shard and remembers this connection within the contextmanager.
You can get that connection also without ``use_shard`` by importing ``django.db.connections``. In order to target a
specific schema, you have multiple options.

Aliases
-------

The ``connections`` accept multiple aliases:

* A string ``<node_name>>``
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

    from sharding.utils import get_shard_class


    shard = get_shard_class().objects.get(node_name='default', schema_name='foo_schema')
    connections[shard]  # Returns a connection to default's public schema and foo_schema


ShardOptions
~~~~~~~~~~~~

The most advanced way to get a connection is with ``ShardOptions``, which is also used internally by ``use_shard``.


.. code-block:: python

    from sharding.options import ShardOptions
    from sharding.utils import get_shard_class


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

Using
-----

All the aliases described before can also be used in a ``QuerySet``'s ``using`` method. This allows you to perform a
query on a shard without having to use ``use_shard``. This can be useful in templates, where you define the schema the
query needs to run on in the view, and it will run the query as soon as it gets evaluated in the template in the correct
schema. All the examples below will run the query in the `foo_schema` (and the `public` schema).


.. code-block:: python

    from sharding.options import ShardOptions
    from sharding.utils import get_shard_class

    Foo.objects.using('default').all()
    Foo.objects.using('default|foo_schema').all()
    Foo.objects.using(('default', 'foo_schema')).all()

    shard = get_shard_class().objects.get(node_name='default', schema_name='foo_schema')
    Foo.objects.using(shard).all()

    shard_options = ShardOptions(node_name='default', schema_name='foo_schema')
    Foo.objects.using(shard_options).all()

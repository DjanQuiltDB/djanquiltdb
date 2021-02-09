=======
Queries
=======

The meat of using this library is defining 'on what shard you are working on'. on default your queries are routed to
the primary node, with only the public schema selected. To only public and mirrored models are visible.
All others will result in a a form of 'table does not exist' error.

Overriding a connection
-----------------------
Database connections are overridden by the library. This happens by monkeypatching them upon startup.
(See sharding.apps._patch_connections). Remember that there is always an open connection to each node Django knows.
For each of those connection we set our current active shard, if there is any.
There are various ways to influence the active shard, and there are ways to route queries specifically,
if you want to bypass this.

The sharding context
--------------------
The most common way to set the active shard on a connection is by setting a sharding context.

.. code-block:: python

  from sharding.utils import use_shard

  with use_shard(shard_object):
      User.objects.all()

Within a `use_shard` context manager, the active shard will be set to the given shard.

.. code-block:: python

  #		                                        Connection
  User.objects.all()  # Table does not exist error	shard context: None

  with use_shard(Shard.objects.get(id=1)):		#  shard context: <shard 1>
      User.objects.all()  # returns values

      with use_shard(Shard.objects.get(id=2)):		   #  shard context: <shard 2>
          user.objects.all()  # returns different values
                                                        #  shard context: returns to <shard 1>
      User.objects.all()  # returns same as first
                                                     #  shard context: returns to None
  User.objects.all()  # Table does not exist error


.using
------
When using the Django ORM, you can specify which node you want the query to perform on using `.using('node-name')`.
The library extends this behavior to allow for shard selection this way as well.
`User.objects.using(Shard.objects.get(id=1)).all()` routes the resulting query to go to the shard we want.
This ovewrite any prevailing sharding context.

Object context persistence
--------------------------
Objects received keep their sharding context information on itself (Saved on `<model instance>._state.db`).
This is useful, for any function you call on it will be performed within the sharding context of that object.

.. code-block:: python

  with use_shard(Shard.objects.get(id=1)):
      user = User.objects.get(id=1)

  # Outside of a sharding context
  user.name = 'Bob'
  user.save()  # Write is performed on shard-1

Unless this is explicitly overwritten

.. code-block:: python

  with use_shard(Shard.objects.get(id=1)):
    user = User.objects.get(id=1)
      # Outside of a sharding context
      user.name = 'Bob'
      user.save(using=Shard.objects.get(id=2))  # Write is performed on shard-2


Sharding Options
----------------
In the examples given so far, we pass a Shard model instance to functions available to use to set up a sharding context.
`with use_shard(Shard.objects.get(id=1)):` or `User.objects.using(Shard.objects.get(id=1)).all()` for example.
This is far from the only way to tell the router which shard we want.
All these functions interpret the given value and create a ShardObjects object from it. This object is what is
used by the router to direct the query to the correct place.
The following values are supported by the ShardingOptions parser:
* string representation: `'<node_name>|<schema_name>'`

.. code-block:: python

  Shard.objects.create(node_name='default', schema_name='org_1_schema')
    with use_shard('default|user_1_schema'):
          (...)


* node and schema arguments: `(node_name=<>, schema_name=<>)`:

.. code-block:: python

  Shard.objects.create(node_name='default', schema_name='org_1_schema')
  with use_shard(node_name='default', schema_name='user_1_schema'):
      (...)

* Shard object: `<Shard object>`:

.. code-block:: python

  shard_1 = Shard.objects.create(node_name='default', schema_name='org_1_schema')
  with use_shard(shard_1):
    (...)

* ShardOptions object itself.

.. code-block:: python

  user = User.objects.using('default|org_1_schema').get(id=1)
  with use_shard(user._state.db):
    (...)

* Shard alias

.. code-block:: python

  shard = Shard.objects.create(node_name='default', schema_name='org_1_schema', alias='my_shard')
  with use_shard(alias='my_shard'):
    (...)

* Shard id

.. code-block:: python

  shard = Shard.objects.create(node_name='default', schema_name='org_1_schema', alias='my_shard')
  with use_shard(alias=shard.id):
    (...)

* Mapping value

.. code-block:: python

  shard = Shard.objects.create(node_name='default', schema_name='org_1_schema')
  use_shard = UserShard.objects.create(shard=shard, user_id=1, slug='my_user')
  with use_shard(mapping_value='my_user'):
    (...)

.. note:: `More info <utils.html>`__  on helper functions to easily create a sharding context.

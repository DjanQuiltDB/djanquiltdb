==================
Move Data To Shard
==================

Introduction
============

Ideally you take sharding in consideration at the moment you first conceptualize the project. And you might start to use this library from day one. In that case, your data will always be created on the shards you want them to, and add shards as you go.

If you have not, and discover after years your project needs sharding: you are faced with some problems.
One of which is, all the data is in a single place: the default database on the public schema.
The `move_models_to_schema` command looks after that, and will relocate all sharded tables from the public schema to a single sharded schema.
Next is to move data away from that 'main' schema, and into their own schemas. After all, you won't see any benefit from sharding if you use only a single shard.
This can be done with the move_data_to_shard command.


Command usage
=============

Root-object
-----------
With a 'root-object' we mean the top-level object of a data structure. This can be a User, an Organization, a Server, whatever tops your data structure. You often want to move this root object and all data that hangs beneath this over to another shard. That is exactly what this command does.

Steps
-----
The command will do its thing in several steps:

1. Create a data set based on the root-object given.

2. Puts the source shard, or relevant mapping object (if any) into MAINTENANCE mode.

3. Copy the data from the source shard to the target shard.

4. Check if the data is copied completely and it is the same as one on the source shard.

5. Delete the data from the source shard.

6. Refresh the materialized views of both shards, so neither is left describing the state before the move.

7. Return both shards back to their original state (probably ACTIVE).


Locking and concurrency
-----------------------
Be precise about what the command protects. With a mapping model configured, only the mapping objects being moved are
locked (exclusively) and put into MAINTENANCE - the source and target shards themselves stay ACTIVE, and other
tenants on either shard keep working throughout. Without a mapping model, the source shard is locked and put into
maintenance; the target shard never is. Requests holding a shard's shared advisory lock inside a transaction finish
before the exclusive lock is granted.

Because the target shard stays live, its sequences may advance while the move runs; the sequence reset at the end
only ever moves a sequence forward, never back, so a concurrent writer's ids are not reissued. Id *collisions* with
pre-existing target data remain the operator's responsibility, as described under get_target_shard below.

Two more operational notes. The rows deleted from the source in step 5 are the rows just copied to the target, so the
``pre_delete``/``post_delete`` signals are disconnected during that deletion - a handler cleaning up external
resources keyed by those rows would destroy things the moved copy still references. And the copy buffers each table's
export in memory, so peak memory use is proportional to the largest table being moved.


get_target_shard
----------------

By default, the command assumes the target shard exists and won't suffer from receiving the data.
Since it just copies the data over, no modification is done on the ids found within that data. So if there already exists data on the target shard that shares ids, you will get in trouble.

We advise you to move data over to a new, empty shard. Or know for sure that there won't be id collisions.
If you want the command to create the target shard for you, you will discover it cannot do that out of the box. The library does not know what naming scheme you use, and what fields your Shard model might have in addition to the base. And if you use mapping models, there are even more considerations.

To solve this, you can easily extend the command and override the function it calls to retrieve its target:


.. code-block:: python

    from example.models import Shard
    from djanquiltdb.management.commands.move_data_to_shard import Command as BaseCommand
    from djanquiltdb.utils import get_new_shard_node


    class Command(BaseCommand):
        def get_target_shard(self, root_object, options):
            """
            Create a new shard to receive the data.
            """
            return Shard.objects.create(alias='new Shard', node_name=get_new_shard_node(), organization=root_object)


Options
-------
Most options the command recognizes are mandatory.
Example: ``move_data_to_shard --source-shard-alias earth --target-shard-alias mars --model-name example.organization --root-object-id 1``

``--source-shard-alias``
~~~~~~~~~~~~~~~~~~~~~~~~
The ``--source-shard-alias`` argument must be the name of the shard the data is currently located.

``--target-shard-alias``
~~~~~~~~~~~~~~~~~~~~~~~~
The ``--target-shard-alias`` argument must be the name of the shard that will receive the data.

``--model-name``
~~~~~~~~~~~~~~~~
The ``--model-name`` argument must contain the app name and the model name of the model the root_object belongs to, separated by a dot.
Eg. ``--model-name example.Organization``

``--root-object-id``
~~~~~~~~~~~~~~~~~~~~
The ``--root-object-id`` argument must be the id of the root_object.

``--use-original-collector``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``--reuse-simple-collector-for-delete`` argument forces the command to reuse to data gathered for the copy step for deletion as well.
During the data collection for copy step, it uses the SimpleCollector within this library. This collector does not follow delete constrains or patterns (such as on_delete=CASCADE or on_delete=PROTECTED) to get the complete picture.
Per default it will run Django's delete collector to collect data to delete from the source shard.
Naturally, this collector does take constrains into account and will not delete data that might be used by objects outside the collected objects.
Setting this argument to true will allow the command to use the same data to delete from the source shard. Be careful when using this: for it might remove data used by other objects. But not using it leads to duplication of that data, since it will get copied and not deleted.

``--quiet``
~~~~~~~~~~~
The ``--quiet`` allows you to silence the output of the command. It will normally notify the user what it is doing by printing the step it is performing and listing per model that it is copying data.

``--no-input``
~~~~~~~~~~~~~~
The ``--no-input`` argument allows you to suppress the confirmation the command will prompt.

``--no-delete``
~~~~~~~~~~~~~~~
The ``--no-delete`` argument will make sure that the data you copy to the target shard will remain on the source shard.
This allows you to confirm that the data has been copied correctly, before deleting the data from the source shard.
When using a mapping model, reverting back to the old data is as simple as pointing the shard in the mapping table to
the source shard.

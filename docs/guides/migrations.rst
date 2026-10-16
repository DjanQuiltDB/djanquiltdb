==========
Migrations
==========

Since we work with multiple nodes and multiple schemas, performing migrations is more involved.

* Public and mirrored models are always migrated to the public schema of each node.
* Sharded models are migrated to each (non-public) schema of all nodes.
* Default models (those that are neither public, mirrored nor sharded) only end up on the public schema of the default
  node.

Determining where models go is done by the ``DynamicDbRouter`` on basis of the decorators set on the model definitions.

This library overrides the ``migrate`` management command with one that executes the migration on each shard of each
node, so running ``migrate`` as usual just works. It is the only migration command; there is nothing else to call.

Making migrations
-----------------
Creating migrations is done as usual. Since it is executed for each shard on each node, you do not have to use
``use_shard`` in data migrations.

Data migrations
~~~~~~~~~~~~~~~
A ``RunPython`` operation is run once for every schema. For every schema, ``migrate`` renders the historical models it
passes as the ``apps`` argument. This is Django-native behavior and completely safe, but can be slow on a larger
schema count. Particularly in tests, this can cause a lot of overhead which is unnecessary since each schema can be
expected to start at the same state. Set ``QUILT_DB['SHARED_TEST_MIGRATION_STATES']`` to ``True`` to make building a new
test database faster: this will make ``migrate`` render the historical models once per migration and hand the public
and template schemas the same model classes.

With that setting on, a data migration must not keep state on those classes or on their managers. Keep what it needs
for a schema in local variables of its function instead:

.. code-block:: python

   def set_default_type(apps, schema_editor):
       Cake = apps.get_model('example', 'Cake')
       CakeType = apps.get_model('example', 'CakeType')

       # Wrong: the next schema would find this schema's CakeType here, and write its id into its own rows.
       # Cake._default_type = CakeType.objects.get(name='plain')

       # Right: look it up again for every schema.
       default_type = CakeType.objects.get(name='plain')
       Cake.objects.filter(type=None).update(type=default_type)

There is a limited safety check in the command to make sure you don't unwittingly cause errors due to such code. If a
data migration adds, removes or replaces an attribute of a historical model class (or of one of its managers), the
change will be undone and the migration will fail for that schema with a ``HistoricalModelsChanged`` error naming the
attribute. This is an indication that you should leave the setting off.

.. warning::

   While the check is intended to catch obvious errors, it cannot guarantee an exhaustive safety audit. The check only
   compares the attributes of the historical model classes and of their managers. State that a data migration keeps
   elsewhere about those classes carries over to the next schema without failing it, such as:

   * a change made inside an attribute that was already there, such as an item added to a dictionary on the class;
   * anything kept on a model's ``_meta``;
   * a cache keyed by the class, such as a function decorated with ``functools.lru_cache`` that is passed the model,
     or a module-level dictionary;
   * a signal receiver connected with a historical model as its ``sender``;
   * an attribute set on the ``apps`` registry the data migration is handed.

   Only turn the setting on when the project's data migrations rely on none of these.

   When in doubt, leave the setting off.


As the speed gains are most useful for local development (with repeated, possibly targeted runs) it is recommended to
disable the setting in CI environments where the relative speed gains are marginal. This also ensures that in case you
do have any code violating the constraints above, it will still be caught somewhere.


Calling migrate
---------------

``migrate`` does two things regarding the way it applies migrations:

Determine migration state
~~~~~~~~~~~~~~~~~~~~~~~~~

Like the original, the command has to determine what the current state of the database is, to draft a list of
migrations to execute: the migration plan.
Since we have multiple shards, it has to check the current state on each of them, in case they are not in sync with
each other.
After it has done this, it looks for the 'least migrated' state: the state with the least migrations performed.
It makes this the start point, and creates a plan from that point to the target state (either the latest or a target
given as argument).

Applying migrations to each shard
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Now we have a migration plan, the command executes it one migration at a time, and applies each migration to every
schema before it starts the next one. For each migration, it first applies it to the public and template schemas of
every node. After that, it applies it to the shards, one node after the other.

If a schema already has the migration, the command skips that schema.

If the migration fails on a schema, the command writes the error and still applies the migration to all remaining
schemas, on every node. Then it stops before the next migration and exits with status 1, so later migrations are not
applied to any schema. Fix the migration, or roll it back on the schemas that have it, and run ``migrate`` again; the
schemas that already have the migration are skipped.

.. image:: /images/migration_flow.svg
   :scale: 100%
   :alt: DjanQuiltDB migration flow
   :align: center

The error handling is the main reason the ``migrate`` command goes migration by migration, instead of running all the
migrations on one node before it moves on to the next node: it keeps every schema at the same migration as much as
possible.

Options
-------
The sharded ``migrate`` command extends Django's normal ``migrate`` command. Thus it knows the same arguments.

``--database``
~~~~~~~~~~~~~~
The ``--database`` argument defaults to ``all``. But you can provide a name (as listed in the database connections in
settings) if you want to migrate a single node.
Example: ``migrate --database hoth``

``--schema-name``
~~~~~~~~~~~~~~~~~
``--schema-name`` (or ``-s``) migrates a single schema, named as in the database, on each node ``--database`` selects:
``public``, the template schema, or the schema of a shard. A shard's schema has to exist as a shard on every selected
node, so name its node with ``--database``.
For example: ``migrate --schema-name public`` or ``migrate --database hoth --schema-name rebellious_shard``

With ``--schema-name``, an error stops the command at once.

Router considerations
---------------------

The ``DynamicDbRouter`` will ensure the tables will only be created on the schemas they should be created on.
To do that, it looks at the sharding mode of models during ``allow_migrate``.

This can be a problem if the migration to be executed no longer has a model known to Django; most likely because
you removed it and the migration is to remove the table as well.
Any migration operation related to a model of which we cannot confirm the sharding mode is NOT executed on the database.
This is no problem for models that have been migrated in the past, or when a model is only created and removed in old
migrations and you perform them from scratch.

If you want to remove a model, use ``migrations.SeparateDatabaseAndState`` to remove it from the state and use
``migrations.RunSQL`` with a hint to remove the tables from the database.


.. code-block:: python

   from django.db import migrations

   from djanquiltdb import ShardingMode


   class Migration(migrations.Migration):

       dependencies = [
           ('example', '0001_initial'),
       ]

       operations = [
           migrations.SeparateDatabaseAndState(
               state_operations=[
                   migrations.DeleteModel('Knights'),
               ],
               database_operations=[
                   migrations.RunSQL(
                       'DROP TABLE example_knights CASCADE;',
                       hints={'sharding_mode': ShardingMode.SHARDED},
                   ),
               ]
           )
       ]

Note that the sharding mode goes in the ``hints`` dictionary. ``RunSQL`` and ``RunPython`` pass it straight to the
router's ``allow_migrate``, and without it the router cannot tell where the operation belongs and raises a
``ProgrammingError``.

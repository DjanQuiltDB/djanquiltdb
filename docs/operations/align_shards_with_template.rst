==========================
Align Shards With Template
==========================

Introduction
============
This command makes existing shards match the template schema of their node, the same way that cloning the template
sets up a new shard. Run it once after upgrading to 4.1.0 or later. A shard that was cloned with an earlier version can
differ from its template in these ways:

* a serial sequence is not owned by its column. ``pg_get_serial_sequence()`` then does not find it, Django's migrations
  skip it when they change the column (for example from ``AutoField`` to ``BigAutoField``), and dropping the table does
  not drop the sequence;
* a sequence has the default data type, start, increment, bounds, cache size and cycling instead of the template's, or
  is logged while the template's is unlogged;
* a primary key or identity sequence is named after its table instead of having the template's name. This happens when
  a table is renamed;
* a serial column still takes its values from the template's sequence, which is the case for shards cloned before
  4.0.1. Every insert on the shard then advances the template's sequence, and the shard's own sequence is not used.

A serial column that takes its values from the template's sequence is changed to take them from the shard's matching
sequence. Before that, the shard's sequence is moved forward to the position of the template's sequence, which returned
every value the column has, unless the shard's sequence is already past it. Other than that, the command never moves a
sequence, and it never changes data. A shard that already matches its template is not changed, so running the command
again is harmless.

.. warning::
   The command gives every shard sequence that differs from the template the template's settings. If a shard's
   sequences were given their own settings by hand, such as an increment or range unique to that shard, those settings
   are lost. Check the ``--dry-run`` output first if any shard was set up that way.

Command usage
=============

Steps
-----
#. Collect the shards of the selected nodes from the shard registry;

#. Check that every selected node has a template schema and every selected shard has its schema, and stop before
   changing anything if one is missing;

#. Unless ``--dry-run`` is provided, install the functions that list the statements in the ``public`` schema of each
   node, once per node and in a separate transaction;

#. For each shard, in the order of their schema names and in one transaction: take the shard's advisory lock, compare
   the shard with the template of its node, print the statements that align it and, unless ``--dry-run`` is provided,
   run them.

Because of the shard's lock, the command waits for a ``move_shard_to_node`` or ``move_data_to_shard`` of that shard to
finish. Shards are aligned whatever their state, the same as ``migrate`` migrates them. The statements take short-lived
locks on the tables and sequences they change, so run the command when the shards are quiet.

A rename can clash with a name that is already in use. A shard can have a relation with the name that the template
gives to one of the shard's primary keys or identity sequences, such as a sequence left behind by a table that was
dropped before that name was used. A table can also have another constraint with the name that the template gives to
its primary key. Without ``--skip-name-clashes``, a clash stops the command before the shard is changed, with an error
that lists the clash and the shards that were not aligned. A statement that fails, for example because a shard's
sequence is already past the template's bound, rolls back that shard's alignment and stops the command in the same way.
The shards aligned before it stay aligned.

Options
-------
All options of the command are optional.
::

  manage.py align_shards_with_template --database=default --dry-run

``--database``
~~~~~~~~~~~~~~
The node whose shards are aligned. Defaults to all nodes.

``--schema-name``, ``-s``
~~~~~~~~~~~~~~~~~~~~~~~~~
The schema name of the one shard to align. When empty, all shards of the selected nodes are aligned.

``--dry-run``
~~~~~~~~~~~~~
Print the statements that align each shard without running them. The statements are listed by functions that the
command installs in the ``public`` schema, the same functions that cloning a schema installs. So a dry run needs the
same privileges as a real run. A dry run installs the functions in each shard's transaction, which is rolled back, so
the functions in the database are unchanged afterwards.

``--skip-name-clashes``
~~~~~~~~~~~~~~~~~~~~~~~
Skip each rename to a name that is already in use, by another relation of the shard or by another constraint of the
table, and align the rest of the shard. Each skipped rename is printed under the shard, and the command continues with
the next shard::

  Renames skipped in default|north because the name is already in use:
      north.gizmo_id_seq already exists, so gizmo_id_seq1 cannot be renamed to it

The object keeps its current name. To give it the template's name, rename or drop the relation that has the name, and
run the command again.

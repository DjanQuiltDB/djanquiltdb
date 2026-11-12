===================
Management commands
===================

DjanQuiltDB replaces some of Django's management commands under their own names, so that they run on every node and in
every schema of the project instead of on one connection. This page lists them. The commands that DjanQuiltDB adds for
shard operations have pages of their own: :doc:`/operations/align_shards_with_template`,
:doc:`/operations/move_data_to_shard`, :doc:`/operations/move_shard_to_node` and :doc:`/operations/purge_shard_data`.


flush
-----

``flush`` empties the tables of every schema of every node, as Django's ``flush`` empties the tables of one database.
On each node, it flushes the template schema first, then every shard that the shard registry has on the node, and the
public schema last, because rows of a shard can point at public rows. It asks for a confirmation once, for the whole
run::

    python manage.py flush

``--database`` names one node to flush. Without it, every node is flushed. ``--schema-name`` names one schema to flush
on the node, or on every node without ``--database``::

    python manage.py flush --database default --schema-name acme

A shard in maintenance is not flushed. Pass ``--ignore-maintenance`` to flush such a shard too::

    python manage.py flush --ignore-maintenance

.. _ignore_maintenance_warning:

.. warning::

    A new shard is in maintenance until you activate it, and ``move_shard_to_node`` and ``move_data_to_shard`` put a
    shard in maintenance while they copy its data (see :doc:`/guides/shard_model`). With ``--ignore-maintenance``, a
    command runs in such a shard while a move is copying, checking or deleting its data, and ``flush`` then destroys
    the data that the move is about to copy or has just checked. Wait for a running move to finish before you pass the
    flag. The same applies to every other command that takes ``--ignore-maintenance``, such as the ``cleartokens``
    command of a plugin.

A shard that cannot be flushed does not stop the other schemas. ``flush`` goes on with the next shard, the public schema
of the node and the other nodes, and then exits with an error that names each shard that it could not flush and why. A
shard in maintenance is named in that error too, when ``--ignore-maintenance`` is not passed.


sqlflush
--------

``sqlflush`` prints the SQL that ``flush`` would run, for each schema in the same order. It takes the same
``--database``, ``--schema-name`` and ``--ignore-maintenance`` options as ``flush``. Like ``flush``, it skips a shard in
maintenance, prints the SQL of the other schemas, and then exits with an error that names the shards that it skipped.


migrate
-------

``migrate`` applies each migration to the public schema, the template schema and every shard of every node, one
migration at a time, and it is the only migration command of a sharded project. :doc:`/guides/migrations` describes how
it plans and applies the migrations, what it does when a migration fails on a schema, and its ``--database`` and
``--schema-name`` options.


loaddata and dumpdata
---------------------

``loaddata`` loads a fixture into the schema that each entry's ``_schema`` property names, and ``dumpdata`` writes
fixtures with that property. :doc:`/guides/fixtures` describes the property, the special schema names and the
``--database`` option.


createsuperuser
---------------

The contrib app ``djanquiltdb.contrib.quilt_auth`` replaces ``createsuperuser`` with a command that creates the
superuser in a shard that you name when your user model is sharded, and on every node when it is mirrored. See
:doc:`/contrib/authentication`.

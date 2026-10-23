===================
The template schema
===================

Every node has a template schema: a schema with the tables of the sharded models, and no data of any tenant. A new shard
on a node starts as a copy of the template schema of that node. The schema is named ``template``, unless the
``TEMPLATE_NAME`` setting names another (see :doc:`/reference/settings`).


What it is for
--------------

Creating a shard is a copy, not a run of the migrations. ``Shard.objects.create(...)`` clones the template schema of
the node of the shard into a new schema, so a new shard does not replay the migration history of the project.

``migrate`` keeps the template schemas up to date: it applies each migration to the public schema and to the template
schema of every node first, and to the shards after that (see :doc:`/guides/migrations`). A shard created after a
migration has its changes from the start.


Creating it
-----------

``migrate`` creates the template schema of every node that it migrates and that has none, before it applies any
migration, and then migrates it along with the public schemas. A new project, a test database and a node that was just
added to ``DATABASES`` get their template schemas from the first ``migrate`` of that node, without further setup.
``migrate --database`` creates the template schema of that node only. ``migrate --schema-name`` creates no template
schema, unless it names the template schema.

``QUILT_DB['AUTO_CREATE_TEMPLATE_SCHEMA'] = False`` turns that off, for a project that creates its template schemas
itself. ``create_template_schema()`` creates the template schema of one node, and does nothing when the node has one
already::

    from djanquiltdb.utils import create_template_schema

    create_template_schema(node_name='default')

By default it also migrates the new schema. ``migrate=False`` leaves that to the next ``migrate``.

.. warning::

   Older versions of this documentation told a project to connect a ``pre_migrate`` receiver of its own that creates
   the template schemas. With ``AUTO_CREATE_TEMPLATE_SCHEMA`` on, remove that receiver, or set the setting to
   ``False`` to keep it. Running both risks unexpected behavior.


Changing the template schema
----------------------------

Change the template schema through migrations, so that every shard gets the same change. Rows can be put in the
template schema with a data migration, or with ``loaddata`` and the name of the template schema (see
:doc:`/guides/fixtures`); a shard created afterwards gets a copy of them.

Shards that were cloned with a version before 4.1.0 can differ from the template schema in their sequences.
``align_shards_with_template`` makes them match it (see :doc:`/operations/align_shards_with_template`).


.. _cloning_limitations:

What schema cloning carries, and what it does not
-------------------------------------------------

A new shard is created by cloning the template schema: tables with their rows, sequences and identity positions,
constraints, indexes, defaults and generated columns, plain and materialized views, functions and triggers all carry
over, rebound to the new schema where they should be. Everything a Django migration puts into the template arrives in
the clone. The known limitations, which matter once objects reach the template by other routes than migrations:

* **Partitioned tables** are not understood: the parent and each partition clone as independent plain tables, and the
  rows arrive twice - once through the parent, once through the partition.
* **Mixed-case table names** (a quoted ``db_table``) abort the clone; Django's conventional lowercase names are safe.
* **Aggregates** are skipped: Postgres cannot render their definitions the way it renders functions.
* **Function bodies are never rewritten.** An unqualified reference in a body resolves through the caller's search
  path at runtime, which is what binds each shard's copy to its own tables; a reference that explicitly qualifies the
  template schema keeps pointing at the template after the clone. Qualify deliberately, or not at all.
* **Schema-local types and domains** are not cloned; a cloned column keeps using the template's type, and dropping the
  template schema with ``CASCADE`` would then reach into every shard.
* **Row-level security policies, grants and comments** on views and functions are not carried (table and column
  comments are). A security policy on a template table is silently absent from its clones.
* **Standalone sequences** are recreated with their value but not their parameters - ``INCREMENT``, ``MINVALUE``,
  ``CYCLE`` and ``CACHE`` are lost. Sequences behind serial and identity columns are unaffected.
* ``truncate_all_tables()`` on the connection truncates the schema the connection is on, not the schema named in its
  argument.
* The reserved-name check refuses creating or deleting a schema named literally ``default``, ``public``,
  ``information_schema`` or a ``pg_``-prefixed name; other connection aliases are not reserved.

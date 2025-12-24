========
Fixtures
========

The Django Sharding library extends Django's ``loaddata`` management command to support loading fixtures into specific PostgreSQL schemas. This allows you to load test data or initial data into different shards in a single fixture file. By extension, loading fixtures in a Django test case also supports this feature.

Introduction
============

When working with sharded databases, you often need to load data into multiple schemas (shards). The sharding-aware fixture loader allows you to specify which schema each fixture entry should be loaded into using the ``_schema`` property.

The _schema Property
====================

Each entry in your fixture file can include an optional ``_schema`` property that specifies which PostgreSQL schema the entry should be loaded into.

If an entry does not have a ``_schema`` property, it will be loaded into the default schema specified by the database connection you're using (typically the ``public`` schema).

Supported Formats
=================

The ``_schema`` property is supported for:

* **JSON** fixtures (``.json`` files)
* **YAML** fixtures (``.yaml`` or ``.yml`` files)

Other fixture formats (such as XML) will be loaded normally without schema awareness.

Usage
=====

Basic Usage
-----------

To load fixtures with schema awareness, use the ``loaddata`` command as you normally would:

.. code-block:: bash

    python manage.py loaddata my_fixtures.json

The command will automatically detect entries with the ``_schema`` property and load them into the appropriate schemas.

Specifying a Database Connection
---------------------------------

You can specify which database connection (node) to use with the ``--database`` option:

.. code-block:: bash

    python manage.py loaddata my_fixtures.json --database default|public

The format is ``node_name|schema_name``, where:
* ``node_name`` is the database connection name from your settings
* ``schema_name`` is the default schema to use for entries without an explicit ``_schema`` property

Example Fixture Files
=====================

JSON Example
------------

Here's an example JSON fixture file that loads data into multiple schemas:

.. code-block:: json

    [
      {
        "model": "example.Organization",
        "pk": 1,
        "fields": {
          "name": "The Empire"
        },
        "_schema": "empire_schema"
      },
      {
        "model": "example.User",
        "pk": 1,
        "fields": {
          "name": "Sheev Palpatine",
          "email": "s.palpatine@sith.sw",
          "organization": 1
        },
        "_schema": "empire_schema"
      },
      {
        "model": "example.Organization",
        "pk": 2,
        "fields": {
          "name": "The Rebel Alliance"
        },
        "_schema": "alliance_schema"
      },
      {
        "model": "example.User",
        "pk": 2,
        "fields": {
          "name": "Mon Mothma",
          "email": "m.mothma@alliance.sw",
          "organization": 2
        },
        "_schema": "alliance_schema"
      },
      {
        "model": "example.Type",
        "pk": 1,
        "fields": {
          "name": "Leader"
        }
      }
    ]

In this example:
* The first two entries (Organization and User) are loaded into the ``empire_schema`` shard
* The next two entries are loaded into the ``alliance_schema`` shard
* The last entry (Type) has no ``_schema`` property, so it's loaded into the default schema

YAML Example
------------

The same fixture can be written in YAML format:

.. code-block:: yaml

    - model: example.Organization
      pk: 1
      fields:
        name: The Empire
      _schema: empire_schema
    - model: example.User
      pk: 1
      fields:
        name: Sheev Palpatine
        email: s.palpatine@sith.sw
        organization: 1
      _schema: empire_schema
    - model: example.Organization
      pk: 2
      fields:
        name: The Rebel Alliance
      _schema: alliance_schema
    - model: example.User
      pk: 2
      fields:
        name: Mon Mothma
        email: m.mothma@alliance.sw
        organization: 2
      _schema: alliance_schema
    - model: example.Type
      pk: 1
      fields:
        name: Leader

Special Schema Names
====================

Public Schema
-------------

To load data into the public schema, use ``public`` as the ``_schema`` value:

.. code-block:: json

    {
      "model": "example.Type",
      "pk": 1,
      "fields": {
        "name": "admin"
      },
      "_schema": "public"
    }

Template Schema
---------------

You can also load data into the template schema by using the template schema name. The template schema name is determined by your Django Sharding configuration.

Schema Creation
==============

The fixture loader will automatically:

1. Ensure the template schema exists and has all migrations applied
2. Create any shard schemas that don't exist yet (cloning them from the template schema)
3. Load entries into their respective schemas

This means you don't need to manually create schemas before loading fixtures - the loader handles this for you.

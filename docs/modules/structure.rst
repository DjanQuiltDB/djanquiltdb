=========
Structure
=========

Structure of Django-sharding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. image:: django_sharding_schema.svg
   :scale: 100 %
   :alt: Django Sharding schema
   :align: center

Nodes
~~~~~

Nodes are entries in the DATABASES section of the Django settings. The names you give them there are the node names.
.. code-block:: python

    DATABASES = {
        'default': {
            'ENGINE': 'sharding.postgresql_backend',
            'NAME': 'primary_database',
            'USER': 'mydatabaseuser',
            'PASSWORD': 'mypassword',
            'HOST': '127.0.0.1',
            'PORT': '5432'},
        'second': {
            'ENGINE': 'sharding.postgresql_backend',
            'NAME': 'secondary_database',
            'USER': 'mydatabaseuser',
            'PASSWORD': 'mypassword',
            'HOST': '127.0.0.1',
            'PORT': '5433'  # same server, different pSQL installation},
        }
    }

Shards
~~~~~~

A Shard is a combination of a node_name and schema_name. We list all shards in the Shard model.
When an entry is made to the Shard model a schema is made on the Node.

When we 'use' a Shard, we route the queries to the correct Node and set the connection's search path to the Shard's schema_name.

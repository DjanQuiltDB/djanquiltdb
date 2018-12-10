================
Purge Shard Data
================

Introduction
============
This command allows you to delete the data for a specific object and shard and is usually used after using
``move_data_to_shard`` with the ``--no-delete`` option.

Command usage
=============

Steps
-----
#. Collect objects;

#. Request user confirmation unless ``--noinput`` is provided;

#. Delete objects.


Options
-------
The command takes required positional arguments and optional options.
::

  manage.py purge_shard_data shard1 example.Organization 1 --mapping-field=uuid --simple-collector

``shard_alias``
~~~~~~~~~~~~~~~
The name of the shard the data is located.

``model_name``
~~~~~~~~~~~~~~
The app name and the model name of the model the object belongs to as dot notation. Eg. ``example.Organization``

``mapping_value``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The mapping value of the object.

``--mapping-field``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The field to map the mapping value to on the model and defaults to ``id``. Take for example this Django model:

.. code-block:: python

    class Organization(models.Model):
        uuid = UUIDField()

If ``model_name`` is set to ``example.Organization``, ``mapping_value`` is a UUID and ``--mapping-field`` is set to
``uuid``, it will select the organization object with that UUID.

``--simple-collector``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The default behaviour is to use Django's ``django.contrib.admin.utils.NestedObjects`` collector to collect data, but
this collector doesn't follow ``on_delete=SET_NULL`` relations. If this option is passed, the
``harding.collector.SimpleCollector`` is used which does follow these relations.

``--verbosity``
~~~~~~~~~~~~~~~~~~~~~~~
Verbosity level; 0=minimal output, 1=normal output, 2=verbose output, 3=very verbose output.

``--noinput``
~~~~~~~~~~~~~~
Do NOT prompt the user for input of any kind.

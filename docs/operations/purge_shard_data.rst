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

The delete runs in one transaction, with the ``pre_delete`` and ``post_delete`` signals disconnected. Plugins can hook
into the purge: ``purge_shard_data.post_collect`` runs once the rows are collected, and can leave rows out of the
purge. ``purge_context`` wraps the delete, and ``pre_delete`` and ``post_delete`` run before and after it, inside the
same transaction. See :ref:`command_hooks`.

Options
-------
The command takes these options:

.. code-block:: console

  manage.py purge_shard_data --shard-alias shard1 --model-name example.Organization --object-value 1 --simple-collector

``--shard-alias``, ``-s``
~~~~~~~~~~~~~~~~~~~~~~~~~
The alias of the shard the data is located on.

``--model-name``, ``-m``
~~~~~~~~~~~~~~~~~~~~~~~~
The app name and the model name of the model the object belongs to as dot notation, e.g. ``example.Organization``.

``--object-value``, ``-o``
~~~~~~~~~~~~~~~~~~~~~~~~~~
The value of the object field of the object whose data is purged. The object field defaults to ``id``, so this is
usually the primary key value, such as the ``id`` of an ``example.Organization``.

``--object-field``
~~~~~~~~~~~~~~~~~~
The field to map the object value to on the model and defaults to ``id``. Take for example this Django model:

.. code-block:: python

    class Organization(models.Model):
        uuid = UUIDField()

If ``--model-name`` is set to ``example.Organization``, ``--object-value`` is a UUID and ``--object-field`` is set to
``uuid``, it will select the organization object with that UUID.

``--simple-collector``
~~~~~~~~~~~~~~~~~~~~~~
The default behaviour is to use Django's ``django.contrib.admin.utils.NestedObjects`` collector to collect data, but
this collector doesn't follow ``on_delete=SET_NULL`` relations. If this option is passed, the
``djanquiltdb.collector.SimpleCollector`` is used which does follow these relations.

``--verbosity``
~~~~~~~~~~~~~~~
Verbosity level; 0=minimal output, 1=normal output, 2=verbose output, 3=very verbose output.

``--noinput``, ``--no-input``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Do NOT prompt the user for input of any kind and assume "yes" on all questions.

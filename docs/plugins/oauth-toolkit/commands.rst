========
Commands
========

The plugin replaces django-oauth-toolkit's ``cleartokens`` command with one that deletes the expired tokens and grants in
every active shard, on every node::

    python manage.py cleartokens

The toolkit's own ``cleartokens`` runs outside a shard, so its queries reach the public schema of the primary node. The
tables of the toolkit are sharded, so that schema has none of them, and the command fails with a ``ProgrammingError``.

Django runs the command of the app that comes first in ``INSTALLED_APPS``, so list the plugin's app before the
toolkit's:

.. code-block:: python

    INSTALLED_APPS = [
        ...
        'djanquiltdb_plugin_oauth_toolkit',
        'oauth2_provider',
        ...
    ]

The check ``djanquiltdb_plugin_oauth_toolkit.W002`` reports an ``INSTALLED_APPS`` that does not list them in this order
(see :doc:`installation`).

The command runs the toolkit's ``clear_expired()`` once in each shard, with the toolkit's settings for the batches. It
warns when ``REFRESH_TOKEN_EXPIRE_SECONDS`` is not set, as the toolkit's command does.

A shard that is in maintenance is skipped, and its expired tokens are deleted on a later run. The command names each
shard that it skipped. To also clear the shards that are in maintenance, pass ``--ignore-maintenance``, as you would to
``flush`` (see :doc:`/reference/commands`)::

    python manage.py cleartokens --ignore-maintenance

.. warning::

    A new shard is in maintenance until you activate it, and ``move_shard_to_node`` and ``move_data_to_shard`` put a
    shard in maintenance while they copy its data. With ``--ignore-maintenance``, the command deletes tokens in such a
    shard while a move is copying or checking them, and the copy then differs from the source. Wait for a running move
    to finish before you pass the flag. See :ref:`the warning on --ignore-maintenance <ignore_maintenance_warning>`
    of the core ``flush`` command.

A shard in which ``clear_expired()`` fails does not stop the other shards. The command goes on with the next shard, and
exits with an error that names each shard that failed, once the last shard is done.

For a scheduler that calls Python, such as a periodic Celery task, ``clear_expired_for_all_shards()`` does the same
work. It returns the shards that it cleared and the shards that it skipped, and it raises an ``ExceptionGroup`` with
the error of each shard that failed, after the last shard. Its ``ignore_maintenance`` argument does what
``--ignore-maintenance`` does:

.. code-block:: python

    from djanquiltdb.utils.oauth_toolkit import clear_expired_for_all_shards

    cleared_shards, skipped_shards = clear_expired_for_all_shards()

    # Also clears the shards that are in maintenance.
    clear_expired_for_all_shards(ignore_maintenance=True)

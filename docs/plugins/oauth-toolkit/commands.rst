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
``flush``::

    python manage.py cleartokens --ignore-maintenance

The same work is available as a function, for a scheduler that calls Python, such as a periodic Celery task. It returns
the shards that it cleared and the shards that it skipped. Its ``ignore_maintenance`` argument does what
``--ignore-maintenance`` does:

.. code-block:: python

    from djanquiltdb.utils.oauth_toolkit import clear_expired_for_all_shards

    cleared_shards, skipped_shards = clear_expired_for_all_shards()

    # Also clears the shards that are in maintenance.
    clear_expired_for_all_shards(ignore_maintenance=True)

"""
The classes of the plugin that a project refers to. DjanQuiltDB serves this module as ``djanquiltdb.base.pghistory``.
"""


class ShardRoutedRevert:
    """
    Defaults the ``using`` of ``revert()`` to None, so the router chooses the connection. pghistory defaults it to the
    default alias, which points at the public schema of a node, and not at the active shard.

    Every event base inherits from this class, so ``makemigrations`` writes it into the bases of each event model. Its
    ``__module__`` is the served path, so that a migration imports it from ``djanquiltdb.base.pghistory`` rather than
    from the distribution package. Migrations that name ``djanquiltdb_plugin_pghistory.base.ShardRoutedRevert`` keep
    working, because this module still defines it.
    """

    __module__ = 'djanquiltdb.base.pghistory'

    def revert(self, using=None):
        return super().revert(using=using)


__all__ = ['ShardRoutedRevert']

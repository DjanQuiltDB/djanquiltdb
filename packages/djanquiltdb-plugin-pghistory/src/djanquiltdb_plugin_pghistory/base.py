class ShardRoutedRevert:
    """
    Defaults the ``using`` of ``revert()`` to None, so the router chooses the connection. pghistory defaults it to the
    default alias, which points at the public schema of a node, and not at the active shard.
    """

    def revert(self, using=None):
        return super().revert(using=using)

"""
A replacement for the utils module of an installed plugin, which only checks that its objects are served unchanged
through the import system.
"""


def fixture_helper():
    """
    Stands in for a plugin export that is a plain function, such as a helper that runs in every shard.
    """

    pass


__all__ = ['fixture_helper']

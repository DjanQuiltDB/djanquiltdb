"""
A replacement for the base module of an installed plugin, which only checks that its objects are served unchanged
through the import system.
"""


class FixtureBase:
    """
    Stands in for a plugin export that is a class to subclass, such as the base class of a plugin's tasks.
    """


__all__ = ['FixtureBase']

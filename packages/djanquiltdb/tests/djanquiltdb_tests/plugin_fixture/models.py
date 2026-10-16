"""
A replacement for the models module of an installed plugin. It has no Django models, because the tests only check that
its objects are served unchanged through the import system.
"""


class FixtureEvent:
    """
    Stands in for a plugin export that is not a decorator, such as a base class for event models.
    """

    pass


__all__ = ['FixtureEvent']

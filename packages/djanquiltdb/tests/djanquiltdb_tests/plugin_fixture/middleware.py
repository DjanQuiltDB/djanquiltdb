"""
A replacement for the middleware module of an installed plugin. The tests use it to check that the import system
serves its objects unchanged, both to import statements and to ``import_string()``, which Django uses to load a
``MIDDLEWARE`` entry.
"""

from django.utils.deprecation import MiddlewareMixin


class FixtureMiddleware(MiddlewareMixin):
    """
    Stands in for a plugin export that is a middleware class, such as one that enters the shard of a request.
    """

    pass


__all__ = ['FixtureMiddleware']

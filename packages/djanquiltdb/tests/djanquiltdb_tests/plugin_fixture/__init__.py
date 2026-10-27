"""
A replacement for an installed plugin. It is not registered as an entry point, and the tests patch the installed entry
points to return it.
"""

from types import SimpleNamespace

from djanquiltdb_tests.plugin_fixture import base, decorators, models, utils


def install():
    pass


plugin = SimpleNamespace(base=base, decorators=decorators, models=models, utils=utils, install=install)

__all__ = ['install', 'plugin']

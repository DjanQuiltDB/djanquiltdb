"""
The plugin object DjanQuiltDB discovers through the ``djanquiltdb.plugins`` entry-point group.
"""

from types import SimpleNamespace

__version__ = '1.0.0'


def install():
    """
    Run when the app registry is ready.
    """


plugin = SimpleNamespace(install=install)

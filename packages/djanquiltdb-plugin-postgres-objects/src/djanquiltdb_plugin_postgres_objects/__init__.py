"""
The plugin object DjanQuiltDB discovers through the ``djanquiltdb.plugins`` entry-point group.

It exposes two things. ``decorators`` is the module DjanQuiltDB serves from ``djanquiltdb.decorators``, so the
placement decorators read the same as the model ones. ``install()`` runs when the app registry is ready, and gives
declared objects their default placement.
"""

from types import SimpleNamespace

from djanquiltdb_plugin_postgres_objects import decorators

__version__ = '1.0.0'


def install():
    """
    Give declared database objects a default placement.

    django-postgres-objects manages Postgres objects that are not tables, and has no notion of placement: a declaration
    carries no routing hints unless something puts them there, which is exactly right for the single-database projects
    it is also meant to serve. Inside a sharded project the sensible default is the public schema, since that is where
    a function called by a stored generated column belongs and it stays reachable from every shard's search path.

    The placement decorators set the hints per declaration and so override this.
    """
    from djanquiltdb import ShardingMode
    from postgres_objects.base import DeclarativeObject

    if not DeclarativeObject.router_hints:
        DeclarativeObject.router_hints = {'sharding_mode': ShardingMode.PUBLIC}


plugin = SimpleNamespace(decorators=decorators, install=install)

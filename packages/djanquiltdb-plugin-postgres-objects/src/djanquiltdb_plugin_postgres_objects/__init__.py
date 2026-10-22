"""
The plugin object DjanQuiltDB discovers through the ``djanquiltdb.plugins`` entry-point group.

It exposes two things. ``decorators`` is the module DjanQuiltDB serves from ``djanquiltdb.decorators``, so the
placement decorators read the same as the model ones. ``install()`` runs when the app registry is ready, registers the
plugin's system checks, and gives the operations of migrations without placement hints their default placement.
"""

from types import SimpleNamespace

from djanquiltdb_plugin_postgres_objects import decorators

__version__ = '1.0.1'


def install():
    """
    Register the plugin's system checks, and give the operations of migrations without placement hints a default
    placement.

    django-postgres-objects manages Postgres objects that are not tables, and has no notion of placement: a declaration
    carries no routing hints unless something puts them there, which is exactly right for the single-database projects
    it is also meant to serve. A migration that was written before this plugin was installed has operations without
    hints, and they fall back to the hints of ``DeclarativeObject``. The default is the public schema, since that is
    where a function called by a stored generated column belongs and it stays reachable from every shard's search path.

    A declaration inherits that default too, so the system check ``djanquiltdb_plugin_postgres_objects.E001`` reports a
    declaration without a placement decorator.
    """
    from djanquiltdb import ShardingMode
    from postgres_objects.base import DeclarativeObject

    from djanquiltdb_plugin_postgres_objects import checks  # noqa: F401

    if not DeclarativeObject.router_hints:
        DeclarativeObject.router_hints = {'sharding_mode': ShardingMode.PUBLIC}


plugin = SimpleNamespace(decorators=decorators, install=install)

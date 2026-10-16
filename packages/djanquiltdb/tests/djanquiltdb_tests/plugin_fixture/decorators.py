"""
A replacement for the decorators module of an installed plugin. It does not set ``SERVE_UNQUALIFIED``, so its public
names are also served from :mod:`djanquiltdb.decorators`.
"""

shiny_decorator = object()

_private_helper = object()

from django.db import connections


class DefaultConnectionProxy(object):
    """
    Proxy for accessing the default DatabaseWrapper object's attributes. Taken from django.db.DefaultConnectionProxy.
    """

    @property
    def db_alias(self):
        from djanquiltdb.router import get_active_connection

        return get_active_connection()

    def __getattr__(self, item):
        return getattr(connections[self.db_alias], item)

    def __setattr__(self, name, value):
        return setattr(connections[self.db_alias], name, value)

    def __delattr__(self, name):
        return delattr(connections[self.db_alias], name)

    def __eq__(self, other):
        return connections[self.db_alias] == other

    def __ne__(self, other):
        return connections[self.db_alias] != other


connection = DefaultConnectionProxy()

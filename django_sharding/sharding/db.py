from django.db import connections, DEFAULT_DB_ALIAS


class DefaultConnectionProxy(object):
    """
    Proxy for accessing the default DatabaseWrapper object's attributes. Taken from django.db.DefaultConnectionProxy.
    """

    @property
    def db_alias(self):
        from sharding.utils import THREAD_LOCAL  # Prevent cyclic imports

        return getattr(THREAD_LOCAL, 'DB_OVERRIDE', None) and THREAD_LOCAL.DB_OVERRIDE[-1] or DEFAULT_DB_ALIAS

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

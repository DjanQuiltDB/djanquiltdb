import functools
import itertools
from unittest import mock

from django.db import DEFAULT_DB_ALIAS, connections
from django.test import TestCase, TransactionTestCase

from djanquiltdb.router import DynamicDbRouter, set_active_connection


class CleanShardingArtifactsMixin:
    @classmethod
    def _fixture_setup(cls):
        """
        Save the names of the schemas that exist on start of the test case.
        Django 6.0 calls _fixture_setup as a classmethod from _pre_setup.
        """
        # Can't access instance methods here, so we'll do this in setUp instead
        super()._fixture_setup()

    def setUp(self):
        """
        Save the names of the schemas that exist on start of the test case.
        """
        if not hasattr(self, '_initial_schemas'):
            self._initial_schemas = {}
            for db_name in self._databases_names():
                connection_ = connections[db_name]
                self._initial_schemas[db_name] = {s[0] for s in connection_.get_all_pg_schemas()}
        super().setUp()

    def _post_teardown(self):
        """
        Remove all the schemas that exist now, but didn't at the start of the test case.
        """
        super()._post_teardown()

        # Only clean up if _initial_schemas was initialized
        if hasattr(self, '_initial_schemas'):
            for db_name in self._databases_names():
                connection_ = connections[db_name]

                for schema in itertools.chain.from_iterable(connection_.get_all_pg_schemas()):
                    if schema not in self._initial_schemas[db_name]:
                        connection_.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format(schema))


class ResetConnectionTestCaseMixin:
    """
    Makes sure that at the end of each test (and as fallback, at the beginning of each test) the connection is set to
    public.
    """

    @classmethod
    def _pre_setup(cls):
        # Django 6.0 calls _pre_setup as a classmethod from setUpClass
        # We can't reset connections at class level, so this is a no-op
        # The actual reset happens in setUp() for each test instance
        super()._pre_setup()

    def setUp(self):
        # Reset connections at the start of each test
        self._reset_connections_to_public()
        super().setUp()

    def _post_teardown(self):
        self._reset_connections_to_public()
        super()._post_teardown()

    def _reset_connections_to_public(self):
        set_active_connection(DEFAULT_DB_ALIAS)


class ShardingTestCase(ResetConnectionTestCaseMixin, TestCase):
    available_apps = ['djanquiltdb', 'example']
    databases = '__all__'  # To make sure cleanup will be done on all databases

    # @classmethod
    # def _enter_atomics(cls):
    #     return {}
    #
    # def _fixture_teardown(self):
    #     pass
    #
    # @classmethod
    # def _rollback_atomics(cls, atomics):
    #     pass


class ShardingTransactionTestCase(ResetConnectionTestCaseMixin, CleanShardingArtifactsMixin, TransactionTestCase):
    available_apps = ['djanquiltdb', 'example']
    databases = '__all__'  # To make sure cleanup will be done on all databases


def disable_db_reconnect():
    def outer(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            with mock.patch('django.db.backends.postgresql.base.DatabaseWrapper.is_usable', return_value=True):
                return func(*args, **kwargs)

        return inner

    return outer


class OverrideMirroredRoutingMixin:
    """
    Since the sharding library doesn't actually have replication, we need to be able to write to MIRRORED models on all
    nodes in our tests. To accomplish this we override the strict db_for_write function that does the write routing
    with non MIRRORED enforcing db_for_read. This is restored at cleanup.
    """

    def reset_router_override(self):
        DynamicDbRouter.db_for_write = self.old_db_for_write

    def setUp(self):
        super().setUp()
        self.old_db_for_write = DynamicDbRouter.db_for_write
        self.addCleanup(self.reset_router_override)
        DynamicDbRouter.db_for_write = DynamicDbRouter.db_for_read

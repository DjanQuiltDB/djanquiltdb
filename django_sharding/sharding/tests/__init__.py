import functools
import itertools
from unittest import mock

from django.db import connections, DEFAULT_DB_ALIAS
from django.test import TestCase, TransactionTestCase

from sharding.router import set_active_connection, DynamicDbRouter


class CleanShardingArtifactsMixin:
    def _fixture_setup(self):
        """
        Save the names of the schemas that exist on start of the test case.
        """
        self._initial_schemas = {}
        for db_name in self._databases_names():
            connection_ = connections[db_name]
            self._initial_schemas[db_name] = {s[0] for s in connection_.get_all_pg_schemas()}

        super()._fixture_setup()

    def _post_teardown(self):
        """
        Remove all the schemas that exist now, but didn't at the start of the test case.
        """
        super()._post_teardown()

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
    def _pre_setup(self):
        self._reset_connections_to_public()
        super()._pre_setup()

    def _post_teardown(self):
        self._reset_connections_to_public()
        super()._post_teardown()

    def _reset_connections_to_public(self):
        set_active_connection(DEFAULT_DB_ALIAS)


class ShardingTestCase(ResetConnectionTestCaseMixin, TestCase):
    available_apps = ['sharding', 'example']
    multi_db = True  # To make sure cleanup will be done on all databases

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
    available_apps = ['sharding', 'example']
    multi_db = True  # To make sure cleanup will be done on all databases


def disable_db_reconnect():
    def outer(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            with mock.patch('django.db.backends.postgresql_psycopg2.base.DatabaseWrapper.is_usable',
                            return_value=True):
                return func(*args, **kwargs)
        return inner
    return outer


class OverrideMirroredRoutingMixin:
    """
    Since the sharding library doesn't actually have replication, we need to write to all nodes manually.
    To accomplish this we must override the strict Mirrored mode routing
    """
    def reset_router_override(self):
        DynamicDbRouter.db_for_write = self.old_db_for_write

    def setUp(self):
        super().setUp()
        self.old_db_for_write = DynamicDbRouter.db_for_write
        self.addCleanup(self.reset_router_override)
        DynamicDbRouter.db_for_write = DynamicDbRouter.db_for_read

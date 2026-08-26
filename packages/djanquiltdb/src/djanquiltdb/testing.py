"""
Test harness made available to both this library and plugins for DjanQuiltDB.

``available_apps`` defaults to the two apps a minimal sharded test project installs, this library and an ``example``
app carrying the Shard model. A project whose apps are named differently, or whose cases need more of the registry,
should override it on the test case.
"""

import functools
import itertools
from unittest import mock

from django.db import DEFAULT_DB_ALIAS, connections
from django.test import TestCase, TransactionTestCase

from djanquiltdb.router import DynamicDbRouter, set_active_connection


class CleanShardingArtifactsMixin:
    """
    Drops every schema a test created but did not remove.

    A test that creates a shard leaves a schema behind that no transaction rollback undoes, so without this the next
    test starts against a database the previous one shaped.
    """

    @classmethod
    def _pre_setup(cls):
        """
        Save the names of the schemas that exist at the start of the test.

        Recorded here rather than in setUp, and on the class rather than on the instance, because Django 6.0 calls both
        this and _fixture_setup as classmethods. A subclass that overrides setUp without calling super() would otherwise
        leave the teardown below with nothing to compare against, and every schema the test created would survive it
        silently.
        """
        cls._initial_schemas = {
            db_name: {schema for (schema,) in connections[db_name].get_all_pg_schemas()}
            for db_name in cls._databases_names()
        }
        super()._pre_setup()

    def _post_teardown(self):
        """
        Remove all the schemas that exist now, but didn't at the start of the test.
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

    @classmethod
    def _pre_setup(cls):
        # Django 6.0 calls _pre_setup as a classmethod from setUpClass
        # We can't reset connections at class level, so this is a no-op
        # The actual reset happens in setUp() for each test instance
        super()._pre_setup()

    def setUp(self):
        """Point the connection at the public schema before the test body runs."""
        # Reset connections at the start of each test
        self._reset_connections_to_public()
        super().setUp()

    def _post_teardown(self):
        self._reset_connections_to_public()
        super()._post_teardown()

    def _reset_connections_to_public(self):
        set_active_connection(DEFAULT_DB_ALIAS)


class ShardingTestCase(ResetConnectionTestCaseMixin, TestCase):
    """
    ``TestCase`` for a sharded project: every node is in ``databases``, and each test starts on the public schema.

    Everything runs in a transaction that is rolled back, so a test that has to create a shard for real wants
    :class:`ShardingTransactionTestCase` instead.
    """

    available_apps = ['djanquiltdb', 'example']
    databases = '__all__'  # To make sure cleanup will be done on all databases


class ShardingTransactionTestCase(ResetConnectionTestCaseMixin, CleanShardingArtifactsMixin, TransactionTestCase):
    """
    ``TransactionTestCase`` for a sharded project, for tests that create shards or commit for real.

    Schemas the test leaves behind are dropped afterwards. ``available_apps`` restricts the migration graph, so a
    test needing a table from a third-party app has to widen it.
    """

    available_apps = ['djanquiltdb', 'example']
    databases = '__all__'  # To make sure cleanup will be done on all databases


def skip_without_virtual_generated_column_support(func):
    """Skip the decorated test unless the default connection is PostgreSQL 18 or later."""

    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        if not connections[DEFAULT_DB_ALIAS].features.supports_virtual_generated_columns:
            self.skipTest('Virtual generated columns require PostgreSQL 18 or later.')

        return func(self, *args, **kwargs)

    return inner


def disable_db_reconnect():
    """
    Decorator factory holding every connection usable for the duration, so Django does not quietly reconnect.

    A test that makes a connection fail on purpose needs the failure to stay; without this Django opens a fresh
    connection and the test passes against a healthy database.
    """

    def outer(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            with mock.patch('django.db.backends.postgresql.base.DatabaseWrapper.is_usable', return_value=True):
                return func(*args, **kwargs)

        return inner

    return outer


class OverrideMirroredRoutingMixin:
    """
    Since our test environment doesn't have data replication configured between nodes, we need something to simulate it
    for writing to MIRRORED models on all nodes in our tests. To accomplish this we override the strict db_for_write
    function that does the write routing with non MIRRORED enforcing db_for_read. This is restored at cleanup.
    """

    def reset_router_override(self):
        """Put the router's real ``db_for_write`` back. Registered as a cleanup, so it runs even on failure."""
        DynamicDbRouter.db_for_write = self.old_db_for_write

    def setUp(self):
        """Point mirrored writes at whichever node is active, rather than only at the primary."""
        super().setUp()
        self.old_db_for_write = DynamicDbRouter.db_for_write
        self.addCleanup(self.reset_router_override)
        DynamicDbRouter.db_for_write = DynamicDbRouter.db_for_read

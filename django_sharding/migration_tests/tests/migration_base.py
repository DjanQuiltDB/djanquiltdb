# Taken from the Django source: https://github.com/django/django/blob/stable/1.8.x/tests/migrations/test_base.py
from unittest import mock

import os

from django.core.management import get_commands
from django.db import connections
from django.db.migrations.recorder import MigrationRecorder
from django.utils._os import upath

from sharding.db import connection
from sharding.tests.utils import ShardingTestCase
from sharding.utils import create_template_schema


class MigrationTestBase(ShardingTestCase):
    """
    Contains an extended set of asserts for testing migrations and schema operations.
    """

    available_apps = ['migration_tests', 'sharding']
    test_dir = os.path.abspath(os.path.dirname(upath(__file__)))

    def setUp(self):
        super().setUp()

        self.mock_router = mock.patch('sharding.utils.DynamicDbRouter.allow_migrate').start()
        self.addCleanup(mock.patch.stopall)

        commands = get_commands()
        commands['migrate_shards'] = 'sharding'

        with mock.patch('django.core.management.get_commands', return_value=commands):
            create_template_schema()  # The template won't have any migration applied to it initially
            create_template_schema('other')  # The template won't have any migration applied to it initially

    def tearDown(self):
        # Reset applied-migrations state.
        for connection_name in connections:
            con = connections[connection_name]
            recorder = MigrationRecorder(con)
            recorder.migration_qs.all().delete()
        super().tearDown()

    def get_table_description(self, table):
        with connection.cursor() as cursor:
            return connection.introspection.get_table_description(cursor, table)

    def assertTableExists(self, table):
        with connection.cursor() as cursor:
            self.assertIn(table, connection.introspection.table_names(cursor))

    def assertTableNotExists(self, table):
        with connection.cursor() as cursor:
            self.assertNotIn(table, connection.introspection.table_names(cursor))

    def assertColumnExists(self, table, column):
        self.assertIn(column, [c.name for c in self.get_table_description(table)])

    def assertColumnNotExists(self, table, column):
        self.assertNotIn(column, [c.name for c in self.get_table_description(table)])

    def assertColumnNull(self, table, column):
        self.assertEqual([c.null_ok for c in self.get_table_description(table) if c.name == column][0], True)

    def assertColumnNotNull(self, table, column):
        self.assertEqual([c.null_ok for c in self.get_table_description(table) if c.name == column][0], False)

    def assertIndexExists(self, table, columns, value=True):
        with connection.cursor() as cursor:
            self.assertEqual(
                value,
                any(
                    c['index']
                    for c in connection.introspection.get_constraints(cursor, table).values()
                    if c['columns'] == list(columns)
                ),
            )

    def assertIndexNotExists(self, table, columns):
        return self.assertIndexExists(table, columns, False)

    def assertFKExists(self, table, columns, to, value=True):
        with connection.cursor() as cursor:
            self.assertEqual(
                value,
                any(
                    c['foreign_key'] == to
                    for c in connection.introspection.get_constraints(cursor, table).values()
                    if c['columns'] == list(columns)
                ),
            )

    def assertFKNotExists(self, table, columns, to, value=True):
        return self.assertFKExists(table, columns, to, False)

from unittest import mock

from django.db import connection
from django.test import TestCase

from sharding.utils import create_schema_on_node


class PostgresBackendTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.addCleanup(self.clean_up)

    def clean_up(self):
        # drop test_schema if it is created to make the state clean for the next test
        if connection.get_ps_schema('test_schema'):
            connection.cursor().execute("DROP SCHEMA {};".format('test_schema'))

    @mock.patch('django.db.backends.postgresql_psycopg2.base.DatabaseWrapper.close')
    def test_close(self, mock_close):
        """
        Case: Call connection.close().
        Expected: connection.search_path_set to be set to false.
        """
        connection.close()
        self.assertTrue(mock_close.called)
        self.assertFalse(connection.search_path_set)

    @mock.patch('django.db.backends.postgresql_psycopg2.base.DatabaseWrapper.rollback')
    def test_rollback(self, mock_rollback):
        """
        Case: Call connection.rollback().
        Expected: connection.search_path_set to be set to false.
        """
        connection.rollback()
        self.assertTrue(mock_rollback.called)
        self.assertFalse(connection.search_path_set)

    def test_set_schema(self):
        """
        Case: Call connection.set_schema.
        Expected: connection.schema_name to be set and connection.search_path_set to be set to false.
        """
        connection.set_schema('test_schema')  # First set to something, we can't be sure our starting position is clean
        self.assertEqual(connection.schema_name, 'test_schema')
        connection.set_schema('other_schema')
        self.assertEqual(connection.schema_name, 'other_schema')
        self.assertFalse(connection.search_path_set)

    def test_set_schema_to_public(self):
        """
        Case: Call connection.set_schema_to_public.
        Expected: connection.schema_name to be set to None and connection.search_path_set to be set to false.
        """
        connection.set_schema('test_schema')  # First set to something, we can't be sure our starting position is clean
        self.assertEqual(connection.schema_name, 'test_schema')
        connection.set_schema_to_public()
        self.assertEqual(connection.schema_name, None)
        self.assertFalse(connection.search_path_set)

    def test_get_schema(self):
        """
        Case: Call connection.get_schema.
        Expected: Returned the schema name
        """
        connection.set_schema('test_schema')
        self.assertEqual(connection.get_schema(), 'test_schema')

    def test_get_ps_schema_with_existing_schema(self):
        """
        Case: Call connection.set_ps_schema with an existing schema name.
        Expected: Receive string 'test_schema'.
        """
        create_schema_on_node('test_schema', 'default', migrate=False)  # no need to migrate for this test
        self.assertEqual(connection.get_ps_schema('test_schema'), 'test_schema')

    def test_get_ps_schema_with_unexisting_schema(self):
        """
        Case: Call connection.set_ps_schema with an unexisting schema name.
        Expected: Receive None.
        """
        self.assertIsNone(connection.get_ps_schema('test_schema'))

from unittest import mock

from django.db import connection
from django.test import SimpleTestCase, override_settings
from psycopg2 import InternalError

from sharding.postgresql_backend.base import get_validated_schema_name
from sharding.utils import create_schema_on_node, create_template_schema
from sharding.tests.utils import ShardingTestCase


class GetValidatedSchemaNameTestCase(SimpleTestCase):
    def test_valid_name(self):
        """
        Case: Call get_validated_schema_name with a valid name.
        Expected: The same value returned.
        """
        self.assertEqual(get_validated_schema_name('valid_name'), 'valid_name')

    def test_non_string(self):
        """
        Case: Call get_validated_schema_name with None.
        Expected: A ValueError raised. (not a string)
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name(None)

    def test_illegal_string(self):
        """
        Case: Call get_validated_schema_name with a string of invalid structure.
        Expected: A ValueError raised.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('DROP * FROM')

    @override_settings(SHARDING={'TEMPLATE_NAME': 'template', 'SHARD_CLASS': 'shardingtest.models.Shard'})
    def test_template_name(self):
        """
        Case: Call get_validated_schema_name with the same name as the default template.
        Expected: A ValueError raised.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('template')

    @override_settings(SHARDING={'TEMPLATE_NAME': 'not-template', 'SHARD_CLASS': 'shardingtest.models.Shard'})
    def test_other_template_name(self):
        """
        Case: Call get_validated_schema_name with the same name as the set template.
        Expected: A ValueError raised.
        """
        self.assertEqual(get_validated_schema_name('template'), 'template')

    def test_public(self):
        """
        Case: Call get_validated_schema_name with 'public'.
        Expected: A ValueError raised.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('public')

    def test_information_schema(self):
        """
        Case: Call get_validated_schema_name with 'information_schema'.
        Expected: A ValueError raised.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('information_schema')

    def test_startswith_pg(self):
        """
        Case: Call get_validated_schema_name with a value starting with 'pg_'
        Expected: A ValueError raised.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('pg_12')

    @override_settings(SHARDING={'TEMPLATE_NAME': 'template', 'SHARD_CLASS': 'shardingtest.models.Shard'})
    def test_is_template(self):
        """
        Case: Call get_validated_schema_name with a template name, while is_template set.
        Expected: A ValueError raised.
        """
        self.assertEqual(get_validated_schema_name('template', is_template=True), 'template')


class PostgresBackendTestCase(ShardingTestCase):
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
        Case: Call connection.get_ps_schema with an existing schema name.
        Expected: Receive string 'test_schema'.
        """
        create_schema_on_node('test_schema', 'default', migrate=False)  # no need to migrate for this test
        self.assertEqual(connection.get_ps_schema('test_schema'), 'test_schema')

    def test_get_ps_schema_with_unexisting_schema(self):
        """
        Case: Call connection.get_ps_schema with an nonexisting schema name.
        Expected: Receive None.
        """
        self.assertIsNone(connection.get_ps_schema('test_schema'))

    def test_set_clone_function(self):
        """
        Case: Call connection.set_clone_function
        Expected: The clone_schema function to be defined on our pSQL connection
        """
        cursor = connection.cursor()
        connection.set_clone_function(cursor)
        try:
            # this will error if the function does not exists.
            cursor.execute("SELECT pg_get_functiondef('clone_schema(text, text)'::regprocedure);")
            self.assertTrue(cursor.fetchall()[0][0])
        except InternalError:
            # we need to rollback in case of a pSQL error, since we are in a transaction.
            cursor.execute("ROLLBACK;")
            self.fail("PostgreSQL internal error")

    def test_clone_schema(self):
        """
        Case: Call connection.migrate_schema
        Expected: The given schema to have correct table headers.
        """
        create_template_schema('default')
        connection.create_schema('test_schema')

        cursor = connection.cursor()
        connection.clone_schema('template', 'test_schema')
        cursor.execute("SELECT pg_get_functiondef('clone_schema(text, text)'::regprocedure);")
        self.assertTrue(cursor.fetchall()[0][0])

        cursor = connection.cursor()
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'template';")
        template_tables = [table[1] for table in cursor.fetchall()]
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'test_schema';")
        new_schema_tables = [table[1] for table in cursor.fetchall()]

        self.assertCountEqual(template_tables, new_schema_tables)

    def test_clone_schema_wo_template(self):
        """
        Case: Call connection.migrate_schema with missing template schema
        Expected: An error to be raised
        """
        connection.create_schema('test_schema')

        with self.assertRaises(ValueError):
            connection.clone_schema('template', 'test_schema')

    def test_clone_schema_wo_target(self):
        """
        Case: Call connection.migrate_schema with missing target schema
        Expected: An error to be raised
        """
        create_template_schema('default')

        with self.assertRaises(ValueError):
            connection.clone_schema('template2', 'test_schema')

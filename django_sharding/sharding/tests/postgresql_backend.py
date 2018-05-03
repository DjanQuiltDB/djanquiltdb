from unittest import mock

from django.db import connection, ProgrammingError
from django.test import override_settings, TestCase
from psycopg2 import InternalError

from example.models import Type, Organization, User, Statement, Shard
from sharding import State
from sharding.postgresql_backend.base import get_validated_schema_name
from sharding.utils import create_schema_on_node, create_template_schema, use_shard
from sharding.tests.utils import ShardingTestCase


class GetValidatedSchemaNameTestCase(TestCase):
    def test_valid_name(self):
        """
        Case: Call get_validated_schema_name with a valid name.
        Expected: The same value returned.
        """
        self.assertEqual(get_validated_schema_name('valid_name'), 'valid_name')

    def test_non_string(self):
        """
        Case: Call get_validated_schema_name with None.
        Expected: A ValueError raised (not a string).
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

    @override_settings(SHARDING={'TEMPLATE_NAME': 'template', 'SHARD_CLASS': 'example.models.Shard'})
    def test_template_name(self):
        """
        Case: Call get_validated_schema_name with the same name as the default template.
        Expected: A ValueError raised.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('template')

    @override_settings(SHARDING={'TEMPLATE_NAME': 'not-template', 'SHARD_CLASS': 'example.models.Shard'})
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
        Expected: A ValueError raised, because 'information_schema' is a blacklisted schema name.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('information_schema')

    def test_default(self):
        """
        Case: Call get_validated_schema_name with 'default'.
        Expected: A ValueError raised, because 'default' is a blacklisted schema name.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('default')

    def test_startswith_pg(self):
        """
        Case: Call get_validated_schema_name with a value starting with 'pg_'.
        Expected: A ValueError raised, because we do not allow schema names to start with the postgresql namespace.
        """
        with self.assertRaises(ValueError):
            get_validated_schema_name('pg_12')

    @override_settings(SHARDING={'TEMPLATE_NAME': 'template', 'SHARD_CLASS': 'example.models.Shard'})
    def test_is_template(self):
        """
        Case: Call get_validated_schema_name with a template name, while is_template set.
        Expected: A ValueError raised, we don't want shards to bear the 'template' name.
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
        Expected: connection.schema_name to be set, connection.search_path_set to be set to false
                  and include_public_schema to be set to True.
        """
        connection.set_schema('test_schema')  # First set to something, we can't be sure our starting position is clean.
        self.assertEqual(connection.schema_name, 'test_schema')
        self.assertTrue(connection.include_public_schema)
        connection.set_schema('other_schema')
        self.assertEqual(connection.schema_name, 'other_schema')
        self.assertFalse(connection.search_path_set)
        self.assertTrue(connection.include_public_schema)

    def test_set_schema_include_public(self):
        """
        Case: Call connection.set_schema with include_public=False
        Expected: connection.schema_name to be set to None, connection.search_path_set to be set to false and
                  include_public_schema to be set to False.
        """
        connection.set_schema('test_schema')  # First set to something, we can't be sure our starting position is clean.
        self.assertEqual(connection.schema_name, 'test_schema')
        self.assertTrue(connection.include_public_schema)
        connection.set_schema('other_schema', include_public=False)
        self.assertEqual(connection.schema_name, 'other_schema')
        self.assertFalse(connection.search_path_set)
        self.assertFalse(connection.include_public_schema)

    def test_set_schema_to_public(self):
        """
        Case: Call connection.set_schema_to_public.
        Expected: connection.schema_name to be set to None and connection.search_path_set to be set to false.
        """
        connection.set_schema('test_schema')  # First set to something, we can't be sure our starting position is clean.
        self.assertEqual(connection.schema_name, 'test_schema')
        connection.set_schema_to_public()
        self.assertEqual(connection.schema_name, 'public')
        self.assertFalse(connection.search_path_set)

    def test_get_schema(self):
        """
        Case: Call connection.get_schema.
        Expected: Returned the schema name.
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
        Case: Call connection.set_clone_function.
        Expected: The clone_schema function to be defined on our pSQL connection.
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
        Case: Call connection.migrate_schema.
        Expected: The given schema to have correct table headers and sequencers.
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

        # Get sequencer names and start value
        cursor.execute("SELECT sequence_name, start_value FROM information_schema.sequences "
                       "WHERE sequence_schema = 'test_schema';")
        new_sequencers = cursor.fetchall()
        self.assertCountEqual(new_sequencers,
                              [('{}_id_seq'.format(table_name), '1') for table_name in new_schema_tables])

        # Check if the new tables have the new sequences assigned
        cursor.execute("SELECT column_name, column_default FROM information_schema.columns "
                       "WHERE table_schema = 'template' AND column_default LIKE 'nextval(%::regclass)';")
        template_defaults = [column[1] for column in cursor.fetchall()]
        cursor.execute("SELECT column_name, column_default FROM information_schema.columns "
                       "WHERE table_schema = 'test_schema' AND column_default LIKE 'nextval(%::regclass)';")
        new_schema_defaults = [column[1] for column in cursor.fetchall()]
        self.assertNotEqual(template_defaults, new_schema_defaults)
        self.assertCountEqual(new_schema_defaults,
                              ["nextval('test_schema.example_organization_id_seq'::regclass)",
                               "nextval('test_schema.example_suborganization_id_seq'::regclass)",
                               "nextval('test_schema.example_user_id_seq'::regclass)",
                               "nextval('test_schema.example_statement_id_seq'::regclass)",
                               "nextval('test_schema.django_migrations_id_seq'::regclass)"])

    def test_sequencers_of_cloned_schema(self):
        """
        Case: Create two shards and write similar data to both shards.
        Expected: Each schema to have their own sequences and thus we get the same ids across shards.
        """
        create_template_schema('default')
        shard_1 = Shard.objects.create(alias='org_1_shard', schema_name='org_1_shard', node_name='default',
                                       state=State.ACTIVE)
        shard_2 = Shard.objects.create(alias='org_2_shard', schema_name='org_2_shard', node_name='default',
                                       state=State.ACTIVE)
        with use_shard(shard_1):
            organization_1 = Organization.objects.create(name='The Boris Corp')
            user_1 = User.objects.create(name='Boris', email='boris@gast.bv', organization=organization_1)
        with use_shard(shard_2):
            organization_2 = Organization.objects.create(name='The Sjonnie Corp')
            user_2 = User.objects.create(name='Boris', email='boris@gast.bv', organization=organization_2)

        with use_shard(shard_1):
            user_3 = User.objects.create(name='Sjonnie', email='sjonnie@gast.bv', organization=organization_1)
        with use_shard(shard_2):
            user_4 = User.objects.create(name='Sjonnie', email='sjonnie@gast.bv', organization=organization_2)

        self.assertEqual(user_1.id, 1)  # Sequence starts at 1
        self.assertEqual(user_1.id, user_2.id)  # Both on different schema's, both new sequencers.
        self.assertEqual(user_3.id, user_4.id)  # Both on different schema's, continuation of above sequencer.
        self.assertEqual(organization_1.id, 1)  # Sequence starts at 1
        self.assertEqual(organization_1.id, organization_2.id)  # different schema's, both new sequencers
        self.assertNotEqual(user_1.id, user_3.id)  # Both on same schema
        self.assertNotEqual(user_2.id, user_4.id)  # Both on same schema

    def test_clone_schema_wo_template(self):
        """
        Case: Call connection.migrate_schema with missing template schema.
        Expected: An error to be raised.
        """
        connection.create_schema('test_schema')

        with self.assertRaises(ValueError):
            connection.clone_schema('template', 'test_schema')

    def test_clone_schema_wo_target(self):
        """
        Case: Call connection.migrate_schema with missing target schema.
        Expected: An error to be raised.
        """
        create_template_schema('default')

        with self.assertRaises(ValueError):
            connection.clone_schema('template2', 'test_schema')

    def test_flush_schema(self):
        """
        Case: Create a template schema and call 'flush_schema' on it.
        Expected: We end up with an empty schema. Stripped from all tables and sequencers.
        """
        create_template_schema('default')
        with use_shard(node_name='default', schema_name='template') as env:
            self.assertNotEqual(connection.get_all_table_headers(schema_name='template'), [])
            self.assertNotEqual(connection.get_all_table_sequences(schema_name='template'), [])
            env.connection.flush_schema(schema_name='template')
            self.assertEqual(connection.get_all_table_headers(schema_name='template'), [])
            self.assertEqual(connection.get_all_table_sequences(schema_name='template'), [])

    def test_get_schema_for_model(self):
        """
        Case: Call get_schema_for_model for a model.
        Expected: The correct schema name to be returned.
        """
        self.assertEquals(connection.get_schema_for_model(Type), [('public',)])

    def test_reset_sequence_for_local_field(self):
        """
        Case: Call reset_sequence for a two models with only local fields,
        Expected: The correct statement to be formulated and executed.
        """
        mock_cursor = mock.Mock()
        mock_cursor.execute = mock.Mock()
        connection.reset_sequence(_cursor=mock_cursor, model_list=[Organization, Statement])
        mock_cursor.execute.assert_called_once_with(
            'SELECT setval(\'example_organization_id_seq\', coalesce(max("id"), 1), max("id") IS NOT null) '
            'FROM "example_organization";\n'
            'SELECT setval(\'example_statement_id_seq\', coalesce(max("id"), 1), max("id") IS NOT null) '
            'FROM "example_statement"')

    def test_reset_sequence_for_m2m_field(self):
        """
        Case: Call reset_sequence for a model with a many-to-many field.
        Expected: The correct statement to be formulated and executed.
        """
        mock_cursor = mock.Mock()
        mock_cursor.execute = mock.Mock()
        connection.reset_sequence(_cursor=mock_cursor, model_list=[User])
        mock_cursor.execute.assert_called_once_with(
            'SELECT setval(\'example_user_id_seq\', coalesce(max("id"), 1), max("id") IS NOT null) '
            'FROM "example_user"')


class CursorTestCase(TestCase):
    def test_select_schema_operation(self):
        """
        Case: Use 'use_shard' on a normal connection.
        Expected: get_ps_schema to be called (part of setting the search_path).
        """
        with mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.get_ps_schema') as mock_get_ps_schema:
            with use_shard(node_name='default', schema_name='public'):
                with connection.cursor() as cursor:
                    cursor.execute('SELECT * FROM example_type;')  # some query
                    self.assertEqual(mock_get_ps_schema.call_count, 1)

    def test_no_db_operation(self):
        """
        Case: Use 'use_shard' on a __no_db__ connection.
        Expected: get_ps_schema to be NOT called (part of setting the search_path).
        """
        with mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.get_ps_schema') as mock_get_ps_schema:
            with use_shard(node_name='default', schema_name='public'):
                with connection._nodb_connection.cursor() as cursor:
                    with self.assertRaises(ProgrammingError):
                        cursor.execute('SELECT * FROM example_type;')  # some query that will fail on __no_db__.
                    self.assertEqual(mock_get_ps_schema.call_count, 0)

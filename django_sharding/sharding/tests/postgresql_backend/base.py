from contextlib import contextmanager
from unittest import mock

from django.db import connections, IntegrityError, InterfaceError
from django.db.backends.base.base import BaseDatabaseWrapper
from django.test import override_settings
from psycopg2 import InternalError

from example.models import Type, Organization, User, Statement, Shard
from sharding import State
from sharding.db import connection
from sharding.options import ShardOptions
from sharding.postgresql_backend.base import get_validated_schema_name, PUBLIC_SCHEMA_NAME, \
    DatabaseWrapper, ShardDatabaseWrapper
from sharding.postgresql_backend.utils import LockCursorWrapperMixin
from sharding.tests import ShardingTransactionTestCase, ShardingTestCase, disable_db_reconnect
from sharding.utils import create_schema_on_node, create_template_schema, use_shard, get_template_name


class GetValidatedSchemaNameTestCase(ShardingTestCase):
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


class PostgresBackendTestCase(ShardingTransactionTestCase):
    @mock.patch('django.db.backends.postgresql_psycopg2.base.DatabaseWrapper.close')
    def test_close(self, mock_close):
        """
        Case: Call connection.close().
        Expected: connection.search_path_set to be set to false.
        """
        connection.close()
        self.assertTrue(mock_close.called)
        self.assertEqual(connection.current_search_paths, [PUBLIC_SCHEMA_NAME])

    @mock.patch('django.db.backends.postgresql_psycopg2.base.DatabaseWrapper.rollback')
    def test_rollback(self, mock_rollback):
        """
        Case: Call connection.rollback().
        Expected: connection.search_path_set to be set to false.
        """
        connection.rollback()
        self.assertTrue(mock_rollback.called)
        self.assertEqual(connection.current_search_paths, [PUBLIC_SCHEMA_NAME])

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

    @staticmethod
    def get_oid(schema_name, table_name, cursor):
        """
        Return internal id for tables to use in queries to gather metadata for them
        """
        cursor.execute("""SELECT c.oid
                          FROM pg_catalog.pg_class c
                          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                          WHERE n.nspname = %s
                          AND c.relname = %s
                          AND c.relkind = 'r' -- only tables;""", [schema_name, table_name])
        return cursor.fetchall()[0][0]

    def test_clone_schema_table_attributes(self):
        """
        Case: Call connection.migrate_schema.
        Expected: The given schema to have the same tables and all the table's info is the same.
                  This include indexes, sequences, constraints and foreign-key constraints
        """
        create_template_schema('default')
        connection.create_schema('test_schema')
        connection.clone_schema('template', 'test_schema')

        cursor = connection.cursor()
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'template';")
        template_tables = [table[1] for table in cursor.fetchall()]
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'test_schema';")
        new_schema_tables = [table[1] for table in cursor.fetchall()]

        self.assertCountEqual(template_tables, new_schema_tables)

        # These queries are based on the queries ran by Postgres when executing '\d table_name'. You can reveal
        # these by running `psql -E`.
        info_queries = {
            'table info':
                """SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules,
                       c.relrowsecurity, c.relforcerowsecurity, c.relhasoids, '', c.reltablespace,
                     CASE WHEN c.reloftype = 0 THEN ''
                       ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident
                   FROM pg_catalog.pg_class c
                     LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
                   WHERE c.oid = %s""",
            'field info':
                """SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
                     (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                        FROM pg_catalog.pg_attrdef d
                        WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
                     a.attnotnull, a.attnum,
                     (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
                        WHERE c.oid = a.attcollation AND t.oid = a.atttypid
                        AND a.attcollation <> t.typcollation) AS attcollation,
                     NULL AS indexdef,
                     NULL AS attfdwoptions
                   FROM pg_catalog.pg_attribute a
                     WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
                   ORDER BY a.attnum;""",
            'field constraints':
                """SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid,
                     pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), pg_catalog.pg_get_constraintdef(con.oid, true),
                     contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace
                   FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
                     LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid
                       AND contype IN ('p','u','x')) --- primary key constraint, unique constraint and exclusion constr.
                   WHERE c.oid = %s AND c.oid = i.indrelid AND i.indexrelid = c2.oid
                   ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;""",
            'unknown constraints':
                """SELECT r.conname, pg_catalog.pg_get_constraintdef(r.oid, true)
                   FROM pg_catalog.pg_constraint r
                   WHERE r.conrelid = %s AND r.contype = 'c' ORDER BY 1;""",
            'foreign key constraints':
                """SELECT conname, pg_catalog.pg_get_constraintdef(r.oid, true) as condef
                   FROM pg_catalog.pg_constraint r
                   WHERE r.conrelid = %s AND r.contype = 'f' ORDER BY 1;"""
        }

        for table_name in template_tables:
            oid_template = self.get_oid('template', table_name, connection.cursor())
            oid_test_schema = self.get_oid('test_schema', table_name, connection.cursor())

            for name, query in info_queries.items():
                with use_shard(node_name='default', schema_name='template') as env:
                    cursor = env.connection.cursor()
                    cursor.execute(query, [oid_template])
                    template_result = next(iter(cursor.fetchall()), [])  # Get the first element, or empty list
                with use_shard(node_name='default', schema_name='test_schema') as env:
                    cursor = env.connection.cursor()
                    cursor.execute(query, [oid_test_schema])
                    test_schema_result = next(iter(cursor.fetchall()), [])  # Get the first element, or empty list

                # Replace schema names to a generic name, so they can be compared.
                self.assertCountEqual(
                    list(map(lambda i: i.replace('test_schema', 'schema') if type(i) == 'string' else i,
                             test_schema_result)),
                    list(map(lambda i: i.replace('template', 'schema') if type(i) == 'string' else i,
                             template_result)),
                    '{} does not appear to be cloned successfully'.format(name))

    def test_clone_schema_sequences(self):
        """
        Case: Call connection.migrate_schema.
        Expected: The given schema to have correct sequences.
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
        new_sequences = cursor.fetchall()
        self.assertCountEqual(new_sequences,
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
                               "nextval('test_schema.example_cake_id_seq'::regclass)",
                               "nextval('test_schema.example_user_cake_id_seq'::regclass)",
                               "nextval('test_schema.example_statement_type_id_seq'::regclass)",
                               "nextval('test_schema.django_migrations_id_seq'::regclass)"])

    def test_sequences_of_cloned_schema(self):
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
        self.assertEqual(user_1.id, user_2.id)  # Both on different schema's, both new sequences.
        self.assertEqual(user_3.id, user_4.id)  # Both on different schema's, continuation of above sequencer.
        self.assertEqual(organization_1.id, 1)  # Sequence starts at 1
        self.assertEqual(organization_1.id, organization_2.id)  # different schema's, both new sequences
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
        Expected: We end up with an empty schema. Stripped from all tables and sequences.
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

    def test_get_schema_for_sequence(self):
        """
        Case: Call get_schema_for_sequence for a sequence name.
        Expected: The correct schema name to be returned.
        """
        self.assertEquals(connection.get_schema_for_sequence('example_type_id_seq'), [('public',)])

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

    def test_is_public_schema(self):
        """
        Case: Test connection.is_public_schema()
        Expected: Returns False when the schema is not the public schema and returns True if the schema is the public
                  schema
        """
        connection.schema_name = 'test_schema'
        self.assertFalse(connection.is_public_schema())

        connection.schema_name = PUBLIC_SCHEMA_NAME
        self.assertTrue(connection.is_public_schema())

    def test_delete_schema(self):
        """
        Case: Create a schema and delete it after with connection.delete_schema
        Expected: Schema is deleted from the database
        """
        connection.create_schema('test_schema')
        self.assertIsNotNone(connection.get_ps_schema('test_schema'))  # Check if schema exists

        connection.delete_schema('test_schema')
        self.assertIsNone(connection.get_ps_schema('test_schema'))  # Schema does not exist anymore

    @mock.patch('django.db.backends.postgresql_psycopg2.base.DatabaseWrapper.is_usable', return_value=False)
    def test_reconnect_on_error(self, mock_is_usable):
        """
        Case: Call for a cursor while the connection has errors or not
        Expected: self.is_usable() and self.close() to be called if needed
        """
        with self.subTest('Errors occured'):
            connection.errors_occurred = True

            with mock.patch.object(connection, 'close') as mock_close:
                connection.cursor()

            mock_is_usable.assert_called_once_with()
            mock_close.assert_called_once_with()

        with self.subTest('No errors occured'):
            connection.errors_occurred = False
            mock_is_usable.reset_mock()

            with mock.patch.object(connection, 'close') as mock_close:
                connection.cursor()

            self.assertFalse(mock_is_usable.called)
            self.assertFalse(mock_close.called)

    @mock.patch('django.db.backends.postgresql_psycopg2.base.DatabaseWrapper.is_usable')
    def test_reconnect_on_stale_connection(self, mock_is_usable):
        """
        Case: Call for a cursor while the connection had errors and the connection is stale or not
        Expected: self.is_usable() to be used and self.close() to be called if needed
        """
        connection.errors_occurred = True
        with self.subTest('Connection stale'):
            mock_is_usable.return_value = False

            with mock.patch.object(connection, 'close') as mock_close:
                connection.cursor()

            mock_is_usable.assert_called_once_with()
            mock_close.assert_called_once_with()

        with self.subTest('Connection open'):
            mock_is_usable.reset_mock()
            mock_is_usable.return_value = True

            with mock.patch.object(connection, 'close') as mock_close:
                connection.cursor()

            mock_is_usable.assert_called_once_with()
            self.assertFalse(mock_close.called)

    def test_dropped_connection(self):
        """
        Case: Forcefully disconnect the database connection and perform a query.
        Expected: Database connection to be remade and the query executed without a problem.
        """
        def disconnect():
            """
            Perform a SQL query that will disconnect all current connections to this database.
            """
            from psycopg2 import OperationalError as OperationalError1
            from django.db.utils import OperationalError as OperationalError2

            with use_shard(node_name='default', schema_name='public') as env:
                cursor = env.connection._get_cursor()  # Force the creation of a new cursor
                try:
                    cursor.execute('select pg_terminate_backend(pid) from pg_stat_activity where '
                                   'datname=%s;', [env.connection.settings_dict['NAME']])
                except (InterfaceError, OperationalError1, OperationalError2):
                    # We know this will raise errors.
                    # Disconnecting the connection from a connection does not pass silently.
                    pass

        create_template_schema('default')
        shard = Shard.objects.create(alias='org_1_shard', schema_name='org_1_shard', node_name='default',
                                     state=State.ACTIVE)

        with self.subTest('With reconnecting logic enabled'):
            with use_shard(shard):
                organization = Organization.objects.create(name='Nail!')
                organization.refresh_from_db()

                disconnect()

                # This should use the refreshed connection and raise no errors.
                organization.refresh_from_db()

        with self.subTest('With reconnecting logic disabled'):
            """
            Should this test ever fail, that means that either the disconnect query does not work, or Django/psycopg
            reconnects for us. The latter might lead to us removing the reconnect feature added in SHARDING-90.
            """
            with use_shard(shard):
                organization = Organization.objects.create(name='Nail!')
                organization.refresh_from_db()

                # Tell the connectionwrapper the connection is always usuable, so it won't see it is disconnected,
                # and thus will no reconnecting when needed.
                with mock.patch('django.db.backends.postgresql_psycopg2.base.DatabaseWrapper.is_usable',
                                return_value=True):
                    disconnect()

                    with self.assertRaisesMessage(InterfaceError, 'connection already closed'):
                        organization.refresh_from_db()


class CursorTestCase(ShardingTestCase):
    def close_connections(self):
        if hasattr(self, 'connection'):
            self.connection.close()

    def setUp(self):
        super().setUp()
        self.addCleanup(self.close_connections)

        create_template_schema()

        # Create a new connection that we can safely play with
        self.connection = DatabaseWrapper(connections['default'].settings_dict, connections['default'].alias)

        # And ask for a new cursor on the default connection to make sure that our current search path is the public
        # schema only
        with connections['default'].cursor():
            self.assertEqual(connections['default'].current_search_paths, [PUBLIC_SCHEMA_NAME])

        self.shard_options = ShardOptions(node_name='default', schema_name='template')
        self.template_connection = ShardDatabaseWrapper(self.connection, self.shard_options)

    @contextmanager
    def assertSearchPathChanged(self, connection_, old_search_paths, new_search_paths):
        """
        Asserts whether the search path has been changed, get_ps_schema is called and cursor.execute() is called with
        the correct arguments to change the search path in the database.
        """
        self.assertEqual(connection_.current_search_paths, old_search_paths)
        with mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.get_ps_schema') as mock_get_ps_schema, \
                mock.patch.object(connection_, 'connection') as mock_connection:
            yield

        self.assertTrue(mock_get_ps_schema.called)
        self.assertEqual(connection_.current_search_paths, new_search_paths)

        mock_connection.cursor.return_value.execute.assert_called_once_with(
            'SET search_path = {}'.format(','.join(new_search_paths))
        )

        self.assertEqual(mock_connection.cursor.return_value.close.call_count, 3)
        mock_connection.cursor.return_value.close.assert_has_calls([
            mock.call(),  # Cursor for get_ps_schema
            mock.call(),  # Cursor for setting the search path
            mock.call(),  # Actual cursor inside the context manager
        ])

    @contextmanager
    def assertSearchPathNotChanged(self, connection_, old_search_paths):
        """
        Asserts whether the search path has not been changed, get_ps_schema is not called and cursor.execute() is not
        called to change the search path in the database.
        """
        self.assertEqual(connection_.current_search_paths, old_search_paths)
        with mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.get_ps_schema') as mock_get_ps_schema, \
                mock.patch.object(connection_, 'connection') as mock_connection:
            yield

        self.assertFalse(mock_get_ps_schema.called)
        self.assertEqual(connection_.current_search_paths, old_search_paths)
        self.assertFalse(mock_connection.cursor.return_value.execute.called)

    @disable_db_reconnect()  # Disable the reconnect logic, to prevent it making queries
    def test_select_schema_operation(self):
        """
        Case: While the connection's current search path is 'public' only, get a cursor for a connection to the template
              schema
        Expected: current_search_path of connection set to ['template', 'public'], get_ps_schema called and the search
                  path in the database correctly set
        """
        with self.assertSearchPathChanged(self.template_connection, ['public'], ['template', 'public']):
            with self.template_connection.cursor():
                pass

    @disable_db_reconnect()  # Disable the reconnect logic, to prevent it making queries
    def test_dont_include_public_schema(self):
        """
        Case: While the connection's current search path is 'public' only, get a cursor for a connection to the template
              schema
        Expected: current_search_path of connection set to ['template', 'public'], get_ps_schema called and the search
                  path in the database correctly set
        """
        shard_options = ShardOptions(node_name='default', schema_name='template', include_public=False)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)

        with self.assertSearchPathChanged(connection_, ['public'], ['template']):
            with connection_.cursor():
                pass

    def test_no_db_operation(self):
        """
        Case: While the connection's current search path is 'public' only, get a cursor for a nodb connection
        Expected: Search path not changed while getting a new cursor
        """
        with self.assertSearchPathNotChanged(self.connection, ['public']):
            with self.connection._nodb_connection.cursor():
                pass

    @disable_db_reconnect()  # Disable the reconnect logic, to prevent it making queries
    def test_search_path_equal(self):
        """
        Case: While the connection's current search path is already 'template' and 'public', get a cursor for the
              template schema
        Expected: Search path not changed because it was already the correct search path
        """
        self.template_connection.current_search_paths = ['template', 'public']
        with self.assertSearchPathNotChanged(self.template_connection, ['template', 'public']):
            with self.template_connection.cursor():
                pass

    def test_schema_does_not_exist(self):
        """
        Case: Get a new cursor for a connection to a schema that does not exists
        Expected: IntegrityError raised, because the schema does not exists
        """
        shard_options = ShardOptions(node_name='default', schema_name='foo')
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)
        with self.assertRaisesMessage(IntegrityError, "Schema '{}' does not exist.".format('foo')):
            with connection_.cursor():
                pass

    @mock.patch.object(DatabaseWrapper, '_cursor')
    def test_cursor(self, mock_cursor):
        """
        Case: Call connection's `cursor` method
        Expected: Return value of `_cursor` returned
        """
        self.assertEqual(self.connection.cursor(), mock_cursor.return_value)
        mock_cursor.assert_called_once_with()

    @mock.patch.object(DatabaseWrapper, 'ensure_connection')
    @mock.patch.object(DatabaseWrapper, 'create_cursor')
    @mock.patch.object(DatabaseWrapper, '_prepare_cursor')
    def test_get_cursor(self, mock_prepare_cursor, mock_create_cursor, mock_ensure_connection):
        """
        Case: Call DatabaseWrapper._get_cursor() with `skip_lock` being False.
        Expected: `ensure_connection` called, `create_cursor` called (with the `name` passed in Django 1.11+) and
                  `_prepare_cursor` called with the return value of `create_cursor` and `skip_lock` being False.
        """
        name = 'foo'
        skip_lock = False

        self.assertEqual(self.connection._get_cursor(name, skip_lock=skip_lock), mock_prepare_cursor.return_value)

        mock_ensure_connection.assert_called_once_with()

        mock_create_cursor.assert_called_once_with(name)

        mock_prepare_cursor.assert_called_once_with(mock_create_cursor.return_value, skip_lock=skip_lock)

    @mock.patch.object(DatabaseWrapper, 'ensure_connection')
    @mock.patch.object(DatabaseWrapper, 'create_cursor')
    @mock.patch.object(DatabaseWrapper, '_prepare_cursor')
    def test_get_cursor_skip_lock(self, mock_prepare_cursor, mock_create_cursor, mock_ensure_connection):
        """
        Case: Call DatabaseWrapper._get_cursor() with `skip_lock` being True.
        Expected: `ensure_connection` called, `create_cursor` called (with the `name` passed in Django 1.11+) and
                  `_prepare_cursor` called with the return value of `create_cursor` and `skip_lock` being True.
        """
        name = 'foo'
        skip_lock = True

        self.assertEqual(self.connection._get_cursor(name, skip_lock=skip_lock), mock_prepare_cursor.return_value)

        mock_ensure_connection.assert_called_once_with()

        # Named cursors is only a thing in Django 1.11+
        mock_create_cursor.assert_called_once_with(name)

        mock_prepare_cursor.assert_called_once_with(mock_create_cursor.return_value, skip_lock=skip_lock)

    @mock.patch.object(DatabaseWrapper, 'validate_thread_sharing')
    @mock.patch.object(DatabaseWrapper, 'make_cursor')
    @mock.patch.object(DatabaseWrapper, 'queries_logged', False)
    def test_prepare_cursor(self, mock_make_cursor, mock_validate_thread_sharing):
        """
        Case: Call DatabaseWrapper._get_cursor() with `queries_logged` being False.
        Expected: Returns the return value of `make_cursor` and calls `validate_thread_sharing`.
        """
        cursor = mock.Mock()
        skip_lock = mock.Mock()

        self.assertEqual(self.connection._prepare_cursor(cursor, skip_lock), mock_make_cursor.return_value)

        mock_validate_thread_sharing.assert_called_once_with()
        mock_make_cursor.assert_called_once_with(cursor, skip_lock=skip_lock)

    @mock.patch.object(DatabaseWrapper, 'validate_thread_sharing')
    @mock.patch.object(DatabaseWrapper, 'make_debug_cursor')
    @mock.patch.object(DatabaseWrapper, 'queries_logged', True)
    def test_prepare_cursor_queries_logged(self, mock_make_debug_cursor, mock_validate_thread_sharing):
        """
        Case: Call DatabaseWrapper._get_cursor() with `queries_logged` being True.
        Expected: Returns the return value of `make_debug_cursor` and calls `validate_thread_sharing`.
        """
        cursor = mock.Mock()
        skip_lock = mock.Mock()

        self.assertEqual(self.connection._prepare_cursor(cursor, skip_lock), mock_make_debug_cursor.return_value)

        mock_validate_thread_sharing.assert_called_once_with()
        mock_make_debug_cursor.assert_called_once_with(cursor, skip_lock=skip_lock)


class AdvisoryLockingTestCase(ShardingTransactionTestCase):
    def close_connections(self):
        if hasattr(self, 'connection2'):
            self.connection2.close()

    def setUp(self):
        super().setUp()
        self.addCleanup(self.close_connections)

        self.connection1 = connections['default']

        # Create second connection to the same database
        self.connection2 = DatabaseWrapper(self.connection1.settings_dict, self.connection1.alias)

    @staticmethod
    def get_lock(connection_, key):
        key = LockCursorWrapperMixin.get_int_from_key(key)
        cursor = connection_.cursor()
        cursor.execute('SELECT pg_try_advisory_lock({});'.format(key))
        return cursor.fetchall()[0][0]

    @mock.patch('django.db.backends.utils.CursorWrapper.execute')
    def test_acquire_shard_lock(self, mock_execute):
        """
        Case: Call acquire_advisory_lock for a shared lock.
        Expected: The correct SQL to be executed.
        """
        self.connection1.acquire_advisory_lock(key='test', shared=True)

        mock_execute.assert_called_once_with(
            'SELECT pg_advisory_lock_shared(%s);', [LockCursorWrapperMixin.get_int_from_key('test')])

    @mock.patch('django.db.backends.utils.CursorWrapper.execute')
    def test_acquire_exclusive_lock(self, mock_execute):
        """
        Case: Call acquire_advisory_lock for an exclusive lock.
        Expected: The correct SQL to be executed.
        """
        self.connection1.acquire_advisory_lock(key='test', shared=False)

        mock_execute.assert_called_once_with(
            'SELECT pg_advisory_lock(%s);', [LockCursorWrapperMixin.get_int_from_key('test')])

    @mock.patch('django.db.backends.utils.CursorWrapper.execute')
    def test_release_shard_lock(self, mock_execute):
        """
        Case: Call release_advisory_lock for a shared lock.
        Expected: The correct SQL to be executed.
        """
        self.connection1.release_advisory_lock(key='test', shared=True)

        mock_execute.assert_called_once_with(
            'SELECT pg_advisory_unlock_shared(%s);', [LockCursorWrapperMixin.get_int_from_key('test')])

    @mock.patch('django.db.backends.utils.CursorWrapper.execute')
    def test_release_exclusive_lock(self, mock_execute):
        """
        Case: Call release_advisory_lock for an exclusive lock.
        Expected: The correct SQL to be executed.
        """
        self.connection1.release_advisory_lock(key='test', shared=False)

        mock_execute.assert_called_once_with(
            'SELECT pg_advisory_unlock(%s);', [LockCursorWrapperMixin.get_int_from_key('test')])

    def test_shared_blocks_exclusive_lock(self):
        """
        Case: Set a shared advisory lock and then try to set an exclusive one.
        Expected: Exclusive lock not given at first, but is given when the shared lock is released.
        """
        self.connection1.acquire_advisory_lock(key='test', shared=True)
        self.assertFalse(self.get_lock(self.connection2, 'test'))

        self.connection1.release_advisory_lock(key='test', shared=True)
        self.assertTrue(self.get_lock(self.connection2, 'test'))

    def test_locks_for_two_keys_dont_block(self):
        """
        Case: Calling for two exclusive locks on different keys.
        Expected: Both locks to be given.
        """
        self.connection1.acquire_advisory_lock(key='test', shared=False)
        self.connection2.acquire_advisory_lock(key='test2', shared=False)

        self.assertTrue(self.get_lock(self.connection1, 'test'))
        self.assertTrue(self.get_lock(self.connection2, 'test2'))

    def test_nested_locks(self):
        """
        Case: Have multiple advisory locks with the same key, release one, and release the other after
        Expected: While only one lock has been released, it's still not possible to get an exclusive lock
        """
        self.connection1.acquire_advisory_lock(key='test', shared=True)
        self.connection1.acquire_advisory_lock(key='test', shared=True)

        # We have two advisory locks set now, so we can't get an exclusive lock on a different connection
        self.assertFalse(self.get_lock(self.connection2, 'test'))

        # Release one
        self.connection1.release_advisory_lock(key='test', shared=True)

        # Meaning that we still have one advisory lock set, so we still can't get an exclusive lock on a different
        # connection
        self.assertFalse(self.get_lock(self.connection2, 'test'))

        # Release the second lock
        self.connection1.release_advisory_lock(key='test', shared=True)

        # Now all locks are released, so we can get an exclusive lock now
        self.assertTrue(self.get_lock(self.connection2, 'test'))


class AdvisoryLockingIntegrationTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        self.shard = Shard.objects.create(node_name='default', schema_name='test_schema', alias='test',
                                          state=State.ACTIVE)

    @mock.patch.object(LockCursorWrapperMixin, 'acquire_advisory_lock')
    @mock.patch.object(LockCursorWrapperMixin, 'release_advisory_lock')
    def test_lock_use_shard(self, mock_release_advisory_lock, mock_acquire_advisory_lock):
        """
        Case: Retrieve an object in a use_shard context
        Expected: Acquiring and releasing an advisory lock only done once
        """
        with self.shard.use():
            Organization.objects.create(name='Hogwarts')

        mock_acquire_advisory_lock.assert_called_once_with('shard_{}'.format(self.shard.id), shared=True)
        mock_release_advisory_lock.assert_called_once_with('shard_{}'.format(self.shard.id), shared=True)

    @mock.patch.object(LockCursorWrapperMixin, 'acquire_advisory_lock')
    @mock.patch.object(LockCursorWrapperMixin, 'release_advisory_lock')
    def test_lock_on_execute(self, mock_release_advisory_lock, mock_acquire_advisory_lock):
        """
        Case: Retrieve an object with the using method
        Expected: Acquiring and releasing an advisory lock only done once
        """
        Organization.objects.using(self.shard).create(name='Hogwarts')

        mock_acquire_advisory_lock.assert_called_once_with('shard_{}'.format(self.shard.id), shared=True)
        mock_release_advisory_lock.assert_called_once_with('shard_{}'.format(self.shard.id), shared=True)


class ShardDatabaseWrapperTestCase(ShardingTransactionTestCase):
    def close_connections(self):
        if hasattr(self, 'connection'):
            self.connection.close()

    def setUp(self):
        super().setUp()
        self.addCleanup(self.close_connections)

        create_template_schema()

        self.shard = Shard.objects.create(node_name='default', schema_name='test_schema', alias='test',
                                          state=State.ACTIVE)

        # Create a new connection that we can safely play with
        self.connection = DatabaseWrapper(connections['default'].settings_dict, connections['default'].alias)

    def test_proxy_fields(self):
        """
        Case: Setting and getting an attribute that's listed in _PROXY_FIELDS is proxied to the main connection.
        Expected: The fields are proxied to the main connection.
        """
        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)

        for field in ShardDatabaseWrapper._PROXY_FIELDS:
            # First set the value on the ShardDatabaseWrapper instance and check whether the field is changed on the
            # main connection. Basically tests __setattr__.
            value = mock.MagicMock()
            setattr(connection_, field, value)
            self.assertIs(getattr(connection_._main_connection, field), value)

            # And next set it on the main connection and check whether the value on the ShardDatabaseWrapper is proxied
            # to the main connection. Basically tests __getattribute__.
            other_value = mock.MagicMock()
            setattr(self.connection, field, other_value)
            self.assertIs(getattr(connection_._main_connection, field), other_value)

    def test_expected_proxy_fields(self):
        """
        Case: Check whether the ShardDatabaseWrapper._PROXY_FIELDS are the same as the fields defined in
              BaseDatabaseWrapper's init fields, including the current_search_path, but excluding:
                * alias
                * client
                * creation
                * features
                * introspection
                * ops
                * validation
        Expected: List is as we expected
        Note: if in future versions of Django the fields we define in BaseDatabaseWrapper changes, this test will tell
              us. We want to proxy all those fields (except for the alias).
        """
        exclude_classes = ['client', 'creation', 'features', 'introspection', 'ops', 'validation']

        class DummyDatabaseWrapper(BaseDatabaseWrapper):
            pass

        for exclude_class in exclude_classes:
            setattr(DummyDatabaseWrapper, '{}_class'.format(exclude_class), mock.Mock())

        proxy_fields = list(DummyDatabaseWrapper({}).__dict__.keys())
        proxy_fields.remove('alias')

        for exclude_class in exclude_classes:
            if exclude_class in proxy_fields:
                proxy_fields.remove(exclude_class)

        proxy_fields.append('current_search_paths')

        self.assertCountEqual(ShardDatabaseWrapper._PROXY_FIELDS, proxy_fields)

    def test_current_search_paths(self):
        """
        Case: Initialize a new ShardDatabaseWrapper and after ask for a cursor
        Expected: After initializing the new ShardDatabaseWrapper, the `current_search_paths` of the main connection has
                  not been altered to the new schema in ShardDatabaseWrapper. Only after asking for a cursor, the
                  `current_search_paths` has been altered.
        """
        current_search_paths = [PUBLIC_SCHEMA_NAME, get_template_name()]
        self.connection.current_search_paths = current_search_paths

        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)

        self.assertEqual(connection_.current_search_paths, current_search_paths)
        self.assertEqual(self.connection.current_search_paths, current_search_paths)

        connection_.cursor()

        new_current_search_paths = [self.shard.schema_name, PUBLIC_SCHEMA_NAME]

        self.assertEqual(connection_.current_search_paths, new_current_search_paths)
        self.assertEqual(self.connection.current_search_paths, new_current_search_paths)

    def test_alias(self):
        """
        Case: Get the alias of a ShardDatabaseWrapper
        Expected: Returns the node name and the schema name divided by a pipe
        """
        options = {'node_name': 'default', 'schema_name': self.shard.schema_name}
        shard_options = ShardOptions(**options)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)

        self.assertEqual(connection_.alias, '{node_name}|{schema_name}'.format(**options))

    def test_change_alias(self):
        """
        Case: Change the alias of a ShardDatabaseWrapper
        Expected: ValueError raised, because the alias is managed by the main connection
        """
        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)

        with self.assertRaisesMessage(ValueError, 'The alias is managed by the main connection and cannot be changed.'):
            connection_.alias = 'other'

    @mock.patch.object(ShardDatabaseWrapper, 'acquire_advisory_lock')
    def test_acquire_locks(self, mock_acquire_advisory_lock):
        """
        Case: Acquire lock on a ShardDatabaseWrapper while having a shard_id set on ShardOptions
        Expected: Lock keys from the ShardOptions used to call acquire_advisory_lock
        """
        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name, shard_id=self.shard.id)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)
        connection_.acquire_locks()
        mock_acquire_advisory_lock.assert_called_once_with('shard_{}'.format(self.shard.id), shared=True)

    @mock.patch.object(ShardDatabaseWrapper, 'acquire_advisory_lock')
    def test_acquire_locks_with_mapping_value(self, mock_acquire_advisory_lock):
        """
        Case: Acquire lock on a ShardDatabaseWrapper while having a shard_id and a mapping value set on ShardOptions
        Expected: Lock keys from the ShardOptions used to call acquire_advisory_lock
        """
        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name, shard_id=self.shard.id,
                                     mapping_value=42)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)
        connection_.acquire_locks()
        self.assertEqual(mock_acquire_advisory_lock.call_count, 2)
        mock_acquire_advisory_lock.assert_has_calls([
            mock.call('shard_{}'.format(self.shard.id), shared=True),
            mock.call('mapping_42', shared=True),
        ])

    @mock.patch.object(ShardDatabaseWrapper, 'release_advisory_lock')
    def test_release_locks(self, mock_release_advisory_lock):
        """
        Case: Release lock on a ShardDatabaseWrapper while having a shard_id set on ShardOptions
        Expected: Lock keys from the ShardOptions used to call release_advisory_lock
        """
        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name, shard_id=self.shard.id)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)
        connection_.release_locks()
        mock_release_advisory_lock.assert_called_once_with('shard_{}'.format(self.shard.id), shared=True)

    @mock.patch.object(ShardDatabaseWrapper, 'release_advisory_lock')
    def test_release_locks_with_mapping_value(self, mock_release_advisory_lock):
        """
        Case: Release lock on a ShardDatabaseWrapper while having a shard_id and a mapping value set on ShardOptions
        Expected: Lock keys from the ShardOptions used to call release_advisory_lock
        """
        shard_options = ShardOptions(node_name='default', schema_name=self.shard.schema_name, shard_id=self.shard.id,
                                     mapping_value=42)
        connection_ = ShardDatabaseWrapper(self.connection, shard_options)
        connection_.release_locks()
        self.assertEqual(mock_release_advisory_lock.call_count, 2)
        mock_release_advisory_lock.assert_has_calls([
            mock.call('shard_{}'.format(self.shard.id), shared=True),
            mock.call('mapping_42', shared=True),
        ])

    def test_lock_on_execute(self):
        """
        Case: Initialize the ShardDatabaseWrapper for multiple combinations of options
        Expected: If we are not in a use_shard context, set lock to True and have lock keys, then `lock_on_execute`
                  returns True. It returns False otherwise.
        """
        dont_lock_on_execute = [
            {'lock': False},
            {'lock': True, 'use_shard': True, 'shard_id': self.shard.id},
            {'lock': True, 'use_shard': False},
        ]

        lock_on_execute = [
            {'lock': True, 'use_shard': False, 'shard_id': self.shard.id},
            {'lock': True, 'use_shard': False, 'shard_id': self.shard.id, 'mapping_value': 42},
            {'lock': True, 'use_shard': False, 'mapping_value': 42},  # Unlikely, but possible
        ]

        for options in dont_lock_on_execute:
            shard_options = ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name,
                                         **options)
            connection_ = ShardDatabaseWrapper(self.connection, shard_options)
            self.assertFalse(connection_.lock_on_execute)

        for options in lock_on_execute:
            shard_options = ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name,
                                         **options)
            connection_ = ShardDatabaseWrapper(self.connection, shard_options)
            self.assertTrue(connection_.lock_on_execute)

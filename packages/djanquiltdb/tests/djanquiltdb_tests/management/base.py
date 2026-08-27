from unittest import mock

from django.conf import settings
from django.core.management import CommandError, call_command

from djanquiltdb import State
from djanquiltdb.management.base import get_databases_and_schema_from_options, shard_table_exists
from djanquiltdb.utils import create_template_schema, get_all_databases
from djanquiltdb_tests import ShardingTestCase
from example.models import Shard


class ShardTableExistsTestCase(ShardingTestCase):
    def test_exists(self):
        """
        Case: Check if the shard table exists for a node we know the shard table exists
        Expected: Returns True
        """
        self.assertTrue(shard_table_exists('default'))

    def test_not_exists(self):
        """
        Case: Check if the shard table exists for a node we know the shard table does exists
        Expected: Returns False
        """
        # Remove the shard table from public by migration to zero
        call_command('migrate', 'example', 'zero', database='default', verbosity=0)
        self.assertFalse(shard_table_exists('default'))


class GetDatabasesAndSchemaFromOptionsTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        create_template_schema('other')

        Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default', state=State.ACTIVE)

    @mock.patch('djanquiltdb.management.base.shard_table_exists', return_value=True)
    def test_shard_check_reads_the_primary_alias(self, mock_shard_table_exists):
        """
        Case: PRIMARY_DB_ALIAS points at the other node while a schema name needs validating.
        Expected: The shard-table pre-flight check consults the currently configured primary node.
        """
        quilt_db = dict(settings.QUILT_DB, PRIMARY_DB_ALIAS='other')
        with self.settings(QUILT_DB=quilt_db):
            with self.assertRaises(CommandError):
                # The shard does not exist on 'other', so the deeper check refuses - all this test pins
                # is which node the shard-table existence check consulted.
                get_databases_and_schema_from_options(options={'database': 'other', 'schema_name': 'test_sina'})

        mock_shard_table_exists.assert_called_once_with('other')

    def test_without_options(self):
        """
        Case: Call get_database_and_schema_from_options without options.
        Expected: Normal list of databases and no database and schema_name returned.
        """
        databases, schema_name = get_databases_and_schema_from_options(options={})

        self.assertEqual(databases, get_all_databases())
        self.assertIsNone(schema_name)

    @mock.patch('djanquiltdb.management.base.get_all_databases', return_value=[db for db in settings.DATABASES])
    def test_with_database_option(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with a targeted database.
        Expected: Single database and no schema_name returned
        """
        databases, schema_name = get_databases_and_schema_from_options(options={'database': 'other'})

        self.assertTrue(mock_get_all_dbs.called)
        self.assertEqual(databases, ['other'])
        self.assertIsNone(schema_name)

    @mock.patch('djanquiltdb.management.base.get_all_databases', return_value=[db for db in settings.DATABASES])
    def test_with_invalid_db_option(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with a targeted database that does not exist
        Expected: CommandError raised
        """
        with self.assertRaisesMessage(CommandError, 'The database you provided does not exist.'):
            get_databases_and_schema_from_options(options={'database': 'James'})
        self.assertTrue(mock_get_all_dbs.called)

    @mock.patch('djanquiltdb.management.base.get_all_databases', return_value=[db for db in settings.DATABASES])
    def test_get_database_and_schema_from_options_with_schema_name_and_database(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with a targeted database and a schema_name
        Expected: Single database and schema_name returned
        """
        databases, schema_name = get_databases_and_schema_from_options(
            options={'database': 'other', 'schema_name': 'public'}
        )

        self.assertTrue(mock_get_all_dbs.called)
        self.assertEqual(databases, ['other'])
        self.assertEqual(schema_name, 'public')

    @mock.patch('djanquiltdb.management.base.get_all_databases', return_value=[db for db in settings.DATABASES])
    def test_get_database_and_schema_from_options_with_schema_name(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options without a targeted database, but with a schema_name
        Expected: Multiple databases returned and a single schema
        """
        databases, schema_name = get_databases_and_schema_from_options(
            options={'schema_name': 'test_sina', 'check_shard': False}
        )

        self.assertTrue(mock_get_all_dbs.called)
        self.assertCountEqual(databases, ['default', 'other'])
        self.assertEqual(schema_name, 'test_sina')

    @mock.patch(
        'djanquiltdb.management.base.get_all_databases', mock.Mock(return_value=[db for db in settings.DATABASES])
    )
    def test_with_unexisting_shard(self):
        """
        Case: Call get_database_and_schema_from_options with a targeted database and non-existing shard with check_shard
              to True.
        Expected: CommandError raised
        """
        with self.assertRaisesMessage(CommandError, 'Shard other|paul does not exist.'):
            get_databases_and_schema_from_options(
                options={'database': 'other', 'schema_name': 'paul', 'check_shard': True}
            )

    @mock.patch('djanquiltdb.management.base.get_all_databases', return_value=[db for db in settings.DATABASES])
    def test_with_unexisting_shard_not_check_shard(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with a targeted database and non-existing shard with check_shard
              to False.
        Expected: Single database and schema_name returned
        """
        databases, schema_name = get_databases_and_schema_from_options(
            options={'database': 'other', 'schema_name': 'paul', 'check_shard': False}
        )

        self.assertTrue(mock_get_all_dbs.called)
        self.assertEqual(databases, ['other'])
        self.assertEqual(schema_name, 'paul')

    @mock.patch('djanquiltdb.management.base.get_all_databases', return_value=[db for db in settings.DATABASES])
    def test_with_invalid_shard_option(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with invalid shard.
        Expected: CommandError raised
        """
        with self.assertRaisesMessage(CommandError, 'Shard default|george does not exist.'):
            get_databases_and_schema_from_options(
                options={'database': 'default', 'schema_name': 'george', 'check_shard': True}
            )
        self.assertTrue(mock_get_all_dbs.called)

from unittest import mock

from django.db import connection, connections
from django.test import SimpleTestCase, TestCase

from sharding.utils import use_shard, create_schema_on_node, DynamicDbRouter, THREAD_LOCAL, \
    _use_connection, _set_schema
from shardingtest.models import Shard


class UseShardTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.addCleanup(self.clean_up)

    def clean_up(self):
        THREAD_LOCAL.DB_OVERRIDE = None

    @classmethod
    def setUpTestData(cls):  # only runs once for the entire TestCase
        cls.shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='default')
        cls.other_shard = Shard.objects.create(alias='other_shard', schema_name='other_schema', node_name='other')

    @mock.patch("sharding.utils._set_schema")
    def test_use_shard(self, mock_set_schema):
        """
        Case: Call use_shard with a valid shard object
        Expected: Connection and schema to be changed
        """
        with use_shard(self.shard) as env:
            self.assertEqual(connection.settings_dict['NAME'], 'test_sharding')
            mock_set_schema.assert_called_once_with('test_schema', env.connection)

    @mock.patch("sharding.utils._set_schema")
    def test_use_shard_on_other_node(self, mock_set_schema):
        """
        Case: Call use_shard with a valid shard object referring to a non-default node
        Expected: Connection and schema to be changed
        """
        with use_shard(self.other_shard) as env:
            self.assertEqual(THREAD_LOCAL.DB_OVERRIDE, ['other'])
            mock_set_schema.assert_called_once_with('other_schema', env.connection)

    @mock.patch("sharding.utils.connection.set_schema")
    def test_use_shard_with_invalid_argument(self, mock_set_schema):
        """
        Case: Call use_shard with an invalid argument
        Expected: A ValueError to be raised
        """
        with self.assertRaises(ValueError):
            with use_shard('not a Shard object'):
                pass

        self.assertFalse(mock_set_schema.called)
        if hasattr(THREAD_LOCAL, 'DB_OVERRIDE') and THREAD_LOCAL.DB_OVERRIDE is not None:
            self.fail('THREAD_LOCAL.DB_OVERRIDE should be None or not exist.')

    @mock.patch("sharding.utils._set_schema")
    def test_use_shard_inception(self, mock_set_schema):
        """
        Case: Call use_shard within a use_shard enviorment
        Expected: Connection to switch twice and set_schema to be called accordingly
        """
        connection1 = None
        connection2 = None

        with use_shard(self.shard) as env:
            connection1 = env.connection
            mock_set_schema.assert_has_call(mock.call('test_schema', connection1))
            self.assertEqual(THREAD_LOCAL.DB_OVERRIDE, ['default'])

            with use_shard(self.other_shard):
                connection2 = env.connection
                mock_set_schema.assert_has_call(mock.call('other_schema', connection2))
                self.assertEqual(THREAD_LOCAL.DB_OVERRIDE, ['default', 'other'])

        self.assertIsNone(THREAD_LOCAL.DB_OVERRIDE)
        mock_set_schema.assert_has_call(mock.call(None))

    @mock.patch("sharding.utils._set_schema")
    def test_use_shard_invalid_node_name(self, mock_set_schema):
        """
        Case: Call use_shard with a valid shard object referring to a non-default node
        Expected: Connection and schema to be changed
        """
        with self.assertRaises(ValueError):
            with use_shard(node_name='Batman', schema_name='Bat_cave'):
                pass

        self.assertFalse(mock_set_schema.called)
        if hasattr(THREAD_LOCAL, 'DB_OVERRIDE') and THREAD_LOCAL.DB_OVERRIDE is not None:
            self.fail('THREAD_LOCAL.DB_OVERRIDE should be None or not exist.')


class CreateSchemaTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.addCleanup(self.clean_up)

    def clean_up(self):
        # drop schemas if they is created to make the state clean for the next test
        if connections['other'].get_ps_schema('test_schema'):
            connections['other'].cursor().execute("DROP SCHEMA {};".format('test_schema'))

    @mock.patch('sharding.utils.call_command')
    def test_create_schema_no_migration(self, mock_command):
        """
        Case: Call create_schema_on_node with a name and node (no migration)
        Expected: A new PostgreSQL schema is made, no migration called
        """
        _connection = connections['other']
        # first: check if schema does not exist yet.
        self.assertFalse(_connection.get_ps_schema('test_schema'))

        create_schema_on_node('test_schema', 'other', migrate=False)

        # check if it exists now
        self.assertTrue(_connection.get_ps_schema('test_schema'))
        self.assertFalse(mock_command.called)

    @mock.patch('sharding.utils.call_command')
    def test_create_schema_on_nonexisting_node(self, mock_command):
        """
        Case: Call create_schema_on_node with an nonexisting node (no migration)
        Expected: A new PostgreSQL schema is made
        """
        with self.assertRaises(ValueError):
            create_schema_on_node('test_schema', 'phaaap', False)
        self.assertFalse(mock_command.called)

    @mock.patch('sharding.utils.call_command')
    def test_create_schema_migration(self, mock_command):
        """
        Case: Call create_schema_on_node with migration
        Expected: A new PostgreSQL schema is made and the migration is called
        """
        _connection = connections['other']
        self.assertFalse(_connection.get_ps_schema('test_schema'))
        create_schema_on_node('test_schema', 'other', migrate=True)
        self.assertTrue(_connection.get_ps_schema('test_schema'))
        mock_command.assert_called_once_with('migrate', database='other', interactive=False)


class UseConnectionTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.addCleanup(self.clean_up)

    def clean_up(self):
        THREAD_LOCAL.DB_OVERRIDE = None

    def test_use_connection(self):
        self.assertEqual(_use_connection('other'), connections['other'])
        self.assertEqual(THREAD_LOCAL.DB_OVERRIDE, ['other'])

    def test_use_connection_twice(self):
        self.assertEqual(_use_connection('other'), connections['other'])
        self.assertEqual(_use_connection('default'), connections['default'])
        self.assertEqual(THREAD_LOCAL.DB_OVERRIDE, ['other', 'default'])


class SetSchemaTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.addCleanup(self.clean_up)

    def clean_up(self):
        connections['default'].set_schema_to_public()
        connections['other'].set_schema_to_public()

    def test_set_schema_with_connection(self):
        """
        Case: Call utils._set_schema with a schema name and connection
        Excepted: The 'other' connection's search_path set, other connection untouched
        """
        shard = Shard.objects.create(alias='test_shard', schema_name='schema_on_other', node_name='other')
        _connection = connections['other']
        _set_schema(shard.schema_name, _connection)

        self.assertEqual(_connection.schema_name, 'schema_on_other')
        self.assertFalse(_connection.search_path_set)
        self.assertIsNone(connections['default'].schema_name)  # untouched connection

    def test_set_schema_without_connection(self):
        """
        Case: Call utils._set_schema with a node_name that does not occur in the settings
        Excepted: An error raised.
        """
        shard = Shard.objects.create(alias='test_shard', schema_name='schema_on_default', node_name='default')
        _connection = connections['default']
        _set_schema(shard.schema_name)

        self.assertEqual(_connection.schema_name, 'schema_on_default')
        self.assertFalse(_connection.search_path_set)
        self.assertIsNone(connections['other'].schema_name)  # untouched connection


class DynamicDbRouterTestCase(SimpleTestCase):
    def setUp(self):
        super().setUp()
        self.addCleanup(self.clean_up)

        self.router = DynamicDbRouter()

    def clean_up(self):
        THREAD_LOCAL.DB_OVERRIDE = None

    def test_db_for_read_while_not_set(self):
        """
        Case: call db_for_read with no DB_ORVERRIDE set
        Expected: None returned
        """
        self.assertIsNone(self.router.db_for_read(model=mock.MagicMock()))

    def test_db_for_read_while_set_single(self):
        """
        Case: Call db_for_read with a DB_ORVERRIDE set
        Expected: Name of the correct node returned
        """
        THREAD_LOCAL.DB_OVERRIDE = ['test_node']
        self.assertEqual(self.router.db_for_read(model=mock.MagicMock()), 'test_node')

    def test_db_for_read_while_set_multiple(self):
        """
        Case: Call db_for_read with a DB_ORVERRIDE set with more than one entree
        Expected: Name of the correct node returned
        """
        THREAD_LOCAL.DB_OVERRIDE = ['default', 'test_node']
        self.assertEqual(self.router.db_for_read(model=mock.MagicMock()), 'test_node')

    def test_db_for_write_while_not_set(self):
        """
        Case: call db_for_write with no DB_ORVERRIDE set
        Expected: None returned
        """
        self.assertIsNone(self.router.db_for_write(model=mock.MagicMock()))

    def test_db_for_write_while_set_single(self):
        """
        Case: Call db_for_write with a DB_ORVERRIDE set
        Expected: Name of the correct node returned
        """
        THREAD_LOCAL.DB_OVERRIDE = ['test_node']
        self.assertEqual(self.router.db_for_write(model=mock.MagicMock()), 'test_node')

    def test_db_for_write_while_set_multiple(self):
        """
        Case: Call db_for_write with a DB_ORVERRIDE set with more than one entree
        Expected: Name of the correct node returned
        """
        THREAD_LOCAL.DB_OVERRIDE = ['default', 'test_node']
        self.assertEqual(self.router.db_for_write(model=mock.MagicMock()), 'test_node')

    def test_allow_relation(self):
        """
        Case: Call allow_relation
        Expected: Always True
        """
        self.assertTrue(self.router.allow_relation())

    def test_allow_syncdb(self):
        """
        Case: Call allow_syncdb
        Expected: None
        """
        self.assertIsNone(self.router.allow_syncdb())

    def test_allow_migrate(self):
        """
        Case: Call allow_migrate
        Expected: True
        """
        self.assertIsNone(self.router.allow_migrate())

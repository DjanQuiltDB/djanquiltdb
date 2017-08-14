import re
from unittest import mock

from django.core.exceptions import ImproperlyConfigured
from django.db import connection, connections, models
from django.test import SimpleTestCase, TestCase, override_settings

from example.models import Shard, OrganizationShards
from sharding.utils import use_shard, create_schema_on_node, DynamicDbRouter, THREAD_LOCAL, \
    _use_connection, _set_schema, create_template_schema, migrate_schema, get_template_name, _node_exists, \
    StateException, use_shard_for, get_shard_for, for_each_shard
from sharding.decorators import sharded_model, mirrored_model


def test_model():
    """
    A decorator for marking a model to be used for testing and not to be migrated.
    """
    def configure(cls):
        cls.test_model = True
        return cls

    return configure


@sharded_model()
@test_model()
class DummyShardedModel(models.Model):

    class Meta:
        app_label = 'sharding'


@mirrored_model()
@test_model()
class DummyMirroredModel(models.Model):

    class Meta:
        app_label = 'sharding'


@test_model()
class DummyNonShardedModel(models.Model):

    class Meta:
        app_label = 'sharding'


class ShardingTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.addCleanup(self.clean_up)

    def clean_up(self):
        THREAD_LOCAL.DB_OVERRIDE = None

        for con in connections:
            connections[con].clone_function_set = False
            connections[con].set_schema_to_public()

            # remove all schemas made in tests.
            for schema in connections[con].get_all_pg_schemas():
                schema = schema[0]
                if schema.startswith('test') or schema == get_template_name():
                    connections[con].cursor().execute('DROP SCHEMA "{}" CASCADE;'.format(schema))


class GetTemplateName(SimpleTestCase):
    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_get_template_unset(self):
        """
        Case: Call get_template_name when it is not set in the settings.
        Expected: The default template name: 'template'
        """
        self.assertEqual(get_template_name(), 'template')

    @override_settings(SHARDING={'TEMPLATE_NAME': 'new-template', 'SHARD_CLASS': 'example.models.Shard'})
    def test_get_template_set(self):
        """
        Case: Call get_template_name while it is set in the settings
        Expected: Set name: 'new-template'
        """
        self.assertEqual(get_template_name(), 'new-template')


class UseShardTestCase(ShardingTestCase):
    def setUp(self):
        pass  # prevent ShardingTestCase addCleanup

    @classmethod
    def setUpClass(cls):  # only runs once for the entire TestCase
        super().setUpClass()
        create_template_schema('default')
        create_template_schema('other')
        cls.shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='default',
                                         state=Shard.STATE_ACTIVE)
        cls.other_shard = Shard.objects.create(alias='other_shard', schema_name='test_other_schema', node_name='other',
                                               state=Shard.STATE_ACTIVE)
        cls.inactive_shard = Shard.objects.create(alias='inactive_shard', schema_name='test_inactive_schema',
                                                  node_name='other', state=Shard.STATE_MAINTENANCE)

    @classmethod
    def tearDownClass(cls):  # run when TestCase is done
        super().clean_up(cls)  # we only want to clean stuff up at the end of the TestCase

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
            mock_set_schema.assert_called_once_with('test_other_schema', env.connection)

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

    @mock.patch("sharding.utils.connection.set_schema")
    def test_use_shard_with_inactive_shard(self, mock_set_schema):
        """
        Case: Call use_shard with a shard that is not active
        Expected: A StateException to be raised
        """
        with self.assertRaises(StateException) as error:
            with use_shard(self.inactive_shard):
                pass
        self.assertEqual(error.exception.state, Shard.STATE_MAINTENANCE)

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
                mock_set_schema.assert_has_call(mock.call('test_other_schema', connection2))
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


class UseShardForTestCase(TestCase):
    def setUp(self):
        super().setUp()

        with mock.patch('sharding.utils.create_schema_on_node'):
            self.shard1 = Shard.objects.create(alias='test_sharding', schema_name='test_schema', node_name='default',
                                               state=Shard.STATE_ACTIVE)

        self.org_shard1 = OrganizationShards.objects.create(organization_id=1, shard=self.shard1)

    @mock.patch('sharding.utils._set_schema')
    def test_use_shard_for(self, mock_set_schema):
        with use_shard_for(1):
            mock_set_schema.assert_called_once_with(self.shard1.schema_name, connections[self.shard1.node_name])


class GetShardForTestCase(TestCase):
    def setUp(self):
        super().setUp()

        with mock.patch('sharding.utils.create_schema_on_node'):
            self.shard1 = Shard.objects.create(alias='test_sharding', schema_name='test_schema', node_name='default',
                                               state=Shard.STATE_ACTIVE)

        self.org_shard1 = OrganizationShards.objects.create(organization_id=1, shard=self.shard1)

    def test_use_shard_for(self):
        self.assertEqual(get_shard_for(1), self.shard1)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_use_shard_for_without_setting(self):
        with self.assertRaises(ImproperlyConfigured):
            self.assertEqual(get_shard_for(1), self.shard1)


class CreateSchemaOnNodeTestCase(ShardingTestCase):
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.clone_schema')
    def test_create_schema_no_migration(self, mock_clone_schema):
        """
        Case: Call create_schema_on_node with a name and node (no migration)
        Expected: A new PostgreSQL schema is made, no clone_schema called
        """
        _connection = connections['other']
        # first: check if schema does not exist yet.
        self.assertFalse(_connection.get_ps_schema('test_schema'))

        create_schema_on_node('test_schema', 'other', migrate=False)

        # check if it exists now
        self.assertTrue(_connection.get_ps_schema('test_schema'))
        self.assertFalse(mock_clone_schema.called)

    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.clone_schema')
    def test_create_schema_on_nonexisting_node(self, mock_clone_schema):
        """
        Case: Call create_schema_on_node with an nonexisting node (no migration)
        Expected: A new PostgreSQL schema is made
        """
        with self.assertRaises(ValueError):
            create_schema_on_node('test_schema', 'phaaap', False)
        self.assertFalse(mock_clone_schema.called)

    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.clone_schema')
    def test_create_schema_migration(self, mock_clone_schema):
        """
        Case: Call create_schema_on_node with migration
        Expected: A new PostgreSQL schema is made and the clone_schema is called
        """
        _connection = connections['other']
        self.assertFalse(_connection.get_ps_schema('test_schema'))
        create_schema_on_node('test_schema', 'other', migrate=True)
        self.assertTrue(_connection.get_ps_schema('test_schema'))
        mock_clone_schema.assert_called_once_with('template', 'test_schema')

    @override_settings(SHARDING={'TEMPLATE_NAME': 'other-template', 'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.clone_schema')
    def test_create_schema_migration_with_different_template_name(self, mock_clone_schema):
        """
        Case: Call create_schema_on_node with migration, while a custom template name is set
        Expected: A new PostgreSQL schema is made and the clone_schema is called
        """
        _connection = connections['other']
        self.assertFalse(_connection.get_ps_schema('test_schema'))
        create_schema_on_node('test_schema', 'other', migrate=True)
        self.assertTrue(_connection.get_ps_schema('test_schema'))
        mock_clone_schema.assert_called_once_with('other-template', 'test_schema')

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'NEW_SHARD_NODE': 'other'})
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.clone_schema')
    def test_create_schema_without_node_name_with_setting(self, mock_clone_schema):
        """
        Case: Call use_shard without a node name, but with NEW_SHARD_NODE setting set.
        Expected: A new PostgreSQL schema is made and the clone_schema is called
        """
        _connection = connections['other']
        # first: check if schema does not exist yet.
        self.assertFalse(_connection.get_ps_schema('test_schema'))

        create_schema_on_node('test_schema', None, migrate=True)

        # check if it exists now
        self.assertTrue(_connection.get_ps_schema('test_schema'))
        self.assertTrue(mock_clone_schema.called)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.clone_schema')
    def test_create_schema_without_node_name_without_setting(self, mock_clone_schema):
        """
        Case: Call use_shard without a node name, and without NEW_SHARD_NODE setting set.
        Expected: ValueError raised
        """
        _connection = connections['other']
        # first: check if schema does not exist yet.
        self.assertFalse(_connection.get_ps_schema('test_schema'))

        with self.assertRaises(ValueError):
            create_schema_on_node('test_schema', None, migrate=True)

        self.assertFalse(_connection.get_ps_schema('test_schema'))
        self.assertFalse(mock_clone_schema.called)


class NodeExistsTestCase(SimpleTestCase):
    def test_node_exists(self):
        """
        Case: Call _node_exists with an existing connection name.
        Expect: No error raised.
        """
        _node_exists('default')

    def test_node_does_not_exist(self):
        """
        Case: Call _node_exists with a nonexisting connection name.
        Expect: ValueError raised.
        """
        with self.assertRaises(ValueError):
            _node_exists('nope')


class UseConnectionTestCase(ShardingTestCase):
    def test_use_connection(self):
        """
        Case: Call _use_connection once.
        Expect: DB_OVERRIDE set to the given connection.
        """
        self.assertEqual(_use_connection('other'), connections['other'])
        self.assertEqual(THREAD_LOCAL.DB_OVERRIDE, ['other'])

    def test_use_connection_twice(self):
        """
        Case: Call _use_connection twice.
        Expect: DB_OVERRIDE set to the given connection, and then appended with the second.
        """
        self.assertEqual(_use_connection('other'), connections['other'])
        self.assertEqual(_use_connection('default'), connections['default'])
        self.assertEqual(THREAD_LOCAL.DB_OVERRIDE, ['other', 'default'])


class SetSchemaTestCase(ShardingTestCase):
    def setUp(self):
        create_template_schema('default')
        create_template_schema('other')
        super().setUp()

    def test_set_schema_with_connection(self):
        """
        Case: Call utils._set_schema with a schema name and connection
        Excepted: The 'other' connection's search_path set, other connection untouched
        """
        shard = Shard.objects.create(alias='test_shard1', schema_name='test_schema_on_other', node_name='other')
        _connection = connections['other']
        _set_schema(shard.schema_name, _connection)

        self.assertEqual(_connection.schema_name, 'test_schema_on_other')
        self.assertFalse(_connection.search_path_set)
        self.assertIsNone(connections['default'].schema_name)  # untouched connection

    def test_set_schema_without_connection(self):
        """
        Case: Call utils._set_schema with a node_name that does not occur in the settings
        Excepted: An error raised.
        """
        shard = Shard.objects.create(alias='test_shard2', schema_name='schema_on_default', node_name='default')
        _connection = connections['default']
        _set_schema(shard.schema_name)

        self.assertEqual(_connection.schema_name, 'schema_on_default')
        self.assertFalse(_connection.search_path_set)
        self.assertIsNone(connections['other'].schema_name)  # untouched connection


class DynamicDbRouterTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        self.router = DynamicDbRouter()

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

    def test_allow_relation_between_non_sharded_models(self):
        """
        Case: Call allow_relation with two models that are not sharded
        Expected: None, the router does not care about non-sharded models.
        """
        self.assertIsNone(self.router.allow_relation(DummyNonShardedModel(), DummyNonShardedModel()))

    def test_allow_relation_between_sharded_and_non_sharded_models(self):
        """
        Case: Call allow_relation with a sharded and non-sharded model.
        Expected: False, such relationship is not allowed
        """
        self.assertFalse(self.router.allow_relation(DummyShardedModel(), DummyNonShardedModel()))

    def test_allow_relation_between_sharded_and_mirrored_models(self):
        """
        Case: Call allow_relation with a sharded and mirrored model.
        Expected: True, mirrored exists in the public schema.
        """
        self.assertTrue(self.router.allow_relation(DummyShardedModel(), DummyMirroredModel()))

    def test_allow_relation_between_sharded_models(self):
        """
        Case: Call allow_relation with two sharded models
        Expected: True, we don't check if they are on the same shard yet.
        """
        self.assertTrue(self.router.allow_relation(DummyShardedModel(), DummyShardedModel()))

    def test_allow_syncdb(self):
        """
        Case: Call allow_syncdb on a normal model
        Expected: None
        """
        self.assertIsNone(self.router.allow_syncdb())

    def test_allow_syncdb_on_test_model(self):
        """
        Case: Call allow_syncdb on a test model
        Expected: None
        """
        self.assertFalse(self.router.allow_syncdb(model=DummyShardedModel))

    def test_allow_migrate(self):
        """
        Case: Call allow_migrate
        Expected: True
        """
        self.assertIsNone(self.router.allow_migrate())


class CreateTemplateSchemaTestCase(ShardingTestCase):

    def test_create_template_schema(self):
        """
        Case: call utils.migrate_schema() to migrate the sharded models to the template schema
        Expected: The newly made schema to have to correct table headers.
        :return:
        """
        create_template_schema('default')  # this also calls the migration

        cursor = connection.cursor()
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'template';")
        template_tables = [table[1] for table in cursor.fetchall()]
        # Filter test models]
        template_tables = [table for table in template_tables if not re.search(r'_[t|T]est', table)]
        self.assertEqual(template_tables, ['example_organization', 'example_type', 'example_user'])

    def test_create_template_schema_invalid_node(self):
        """
        Case: call utils.migrate_schema() with an nonexisting connection
        Expected: ValueError raised
        """
        with self.assertRaises(ValueError):
            migrate_schema('no_connection', 'test_schema')  # this also calls the migration

    def test_create_template_schema_invalid_schema_name(self):
        """
        Case: call utils.migrate_schema() with an nonexisting schema_name
        Expected: ValueError raised
        """
        with self.assertRaises(ValueError):
            migrate_schema('default', 'test_schema')  # this also calls the migration


class ForEachShardTestCase(TestCase):
    def setUp(self):
        super().setUp()

        with mock.patch('sharding.utils.create_schema_on_node'):
            self.shard1 = Shard.objects.create(alias='test_sharding', schema_name='test_schema', node_name='default',
                                               state=Shard.STATE_ACTIVE)

    def repeatable_function(self, shard, **kwargs):
        self.shards.append((shard, kwargs) if kwargs else shard)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_for_each_shard(self):
        """
        Case: Call self.repeatable_function for every shard
        Expected: Function is called for every shard
        """
        self.shards = []
        for_each_shard(self.repeatable_function)
        self.assertEqual(self.shards, [self.shard1])

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_for_each_shard_with_kwargs(self):
        """
        Case: Call self.repeatable_function for every shard and pass
              keyword arguments to the function.
        Expected: Function is called for every shard and is called with
                  the keyword arguments provided.
        """
        self.shards = []
        for_each_shard(self.repeatable_function, kwargs={'organization_id': 1})
        self.assertEqual(self.shards, [(self.shard1, {'organization_id': 1})])

import inspect
import re
from unittest import mock
import threading

from django.core.exceptions import ImproperlyConfigured
from django.db import connection, connections, models, ProgrammingError, InterfaceError, OperationalError, transaction
from django.test import SimpleTestCase, TestCase, override_settings, TransactionTestCase

from example.models import Shard, OrganizationShards, Type, SuperType
from sharding.utils import use_shard, create_schema_on_node, DynamicDbRouter, THREAD_LOCAL, \
    _use_connection, _set_schema, create_template_schema, migrate_schema, get_template_name, _node_exists, \
    StateException, use_shard_for, get_shard_for, for_each_shard, State, for_each_node, transaction_for_every_node
from sharding.decorators import sharded_model, mirrored_model, atomic_write_to_every_node


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
    available_apps = ['sharding', 'example']

    def setUp(self):
        super().setUp()
        self.addCleanup(self.clean_up)

    def clean_up(self):
        THREAD_LOCAL.DB_OVERRIDE = None

        for con in connections.all():
            con.clone_function_set = False
            con.set_schema_to_public()

            # remove all schemas made in tests.
            for schema in con.get_all_pg_schemas():
                schema = schema[0]
                if schema.startswith('test') or schema == get_template_name():
                    con.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format(schema))


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
                                         state=State.ACTIVE)
        cls.other_shard = Shard.objects.create(alias='other_shard', schema_name='test_other_schema', node_name='other',
                                               state=State.ACTIVE)
        cls.inactive_shard = Shard.objects.create(alias='inactive_shard', schema_name='test_inactive_schema',
                                                  node_name='other', state=State.MAINTENANCE)

    @classmethod
    def tearDownClass(cls):  # run when TestCase is done
        super().clean_up(cls)  # we only want to clean stuff up at the end of the TestCase

    @mock.patch('sharding.utils._set_schema')
    def test_use_shard(self, mock_set_schema):
        """
        Case: Call use_shard with a valid shard object
        Expected: Connection and schema to be changed
        """
        with use_shard(self.shard) as env:
            self.assertEqual(connection.alias, 'default')
            mock_set_schema.assert_called_once_with('test_schema', env.connection)

    @mock.patch('sharding.utils._set_schema')
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
        self.assertEqual(error.exception.state, State.MAINTENANCE)

        self.assertFalse(mock_set_schema.called)
        if hasattr(THREAD_LOCAL, 'DB_OVERRIDE') and THREAD_LOCAL.DB_OVERRIDE is not None:
            self.fail('THREAD_LOCAL.DB_OVERRIDE should be None or not exist.')

    @mock.patch('sharding.utils._set_schema')
    def test_use_shard_with_inactive_shard_with_state_test_disabled(self, mock_set_schema):
        """
        Case: Call use_shard with a shard that is not active, but active_only_schemas is False
        Expected: No StateException to be raised
        """
        with use_shard(self.inactive_shard, active_only_schemas=False):
            pass

        self.assertTrue(mock_set_schema.called)

    @mock.patch('sharding.utils._set_schema')
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

    @mock.patch('sharding.utils._set_schema')
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

    @mock.patch('sharding.utils._set_schema')
    def test_use_shard_with_inactive_mapping_objects(self, mock_set_schema):
        """
        Case: Call use_shard with a valid shard object that contains inactive mapping objects
        Expected: StateException to be raised
        """
        shard = Shard.objects.create(alias='halls_of_justice', schema_name='test_halls_of_justice', node_name='default',
                                     state=State.ACTIVE)
        OrganizationShards.objects.create(organization_id=5, shard=shard, state=State.ACTIVE)
        OrganizationShards.objects.create(organization_id=6, shard=shard, state=State.MAINTENANCE)

        with self.assertRaises(StateException):
            with use_shard(shard):
                pass
        self.assertFalse(mock_set_schema.called)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('sharding.utils._set_schema')
    def test_use_shard_for_inactive_schemas(self, mock_set_schema):
        """
        Case: Call use_shard with a valid shard object while no mapping_model exists.
        Expected: No StateException to be raised.
        """
        shard = Shard.objects.create(alias='mudville', schema_name='test_schema_muddied', node_name='default',
                                     state=State.ACTIVE)
        OrganizationShards.objects.create(organization_id=5, shard=shard, state=State.ACTIVE)
        OrganizationShards.objects.create(organization_id=6, shard=shard, state=State.MAINTENANCE)
        with use_shard(shard):
            pass

        self.assertTrue(mock_set_schema.called)


class UseShardForTestCase(TestCase):
    def setUp(self):
        super().setUp()

        with mock.patch('sharding.utils.create_schema_on_node'):
            self.shard1 = Shard.objects.create(alias='test_sharding', schema_name='test_schema', node_name='default',
                                               state=State.ACTIVE)

        self.org_shard1 = OrganizationShards.objects.create(organization_id=1, shard=self.shard1, state=State.ACTIVE)
        self.org_shard2 = OrganizationShards.objects.create(organization_id=2, shard=self.shard1,
                                                            state=State.MAINTENANCE)

    @mock.patch('sharding.utils._set_schema')
    def test_use_shard_for(self, mock_set_schema):
        """
        Case: use use_shard_for with valid arguments. There are both an active and inactive schemas on the shard.
        Expected: Successful usage of use_shard_for
        """
        with use_shard_for(1):
            mock_set_schema.assert_called_once_with(self.shard1.schema_name, connections[self.shard1.node_name])

    @mock.patch('sharding.utils._set_schema')
    def test_use_shard_for_inactive_object(self, mock_set_schema):
        """
        Case: use use_shard_for with an inactive mapping object
        Expected: StateException raised
        """
        with self.assertRaises(StateException):
            with use_shard_for(2):
                pass
        self.assertFalse(mock_set_schema.called)


class GetShardForTestCase(TestCase):
    def setUp(self):
        super().setUp()

        with mock.patch('sharding.utils.create_schema_on_node'):
            self.shard1 = Shard.objects.create(alias='test_sharding', schema_name='test_schema', node_name='default',
                                               state=State.ACTIVE)

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
        self.assertNotEqual(connections['default'].schema_name, 'test_schema_on_other')  # untouched connection

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
        self.assertNotEqual(connections['other'].schema_name, 'test_schema_on_other')  # untouched connection


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
        Case: Migrate a combination of unsharded, mirrored and
              sharded models: namely example.models
        Expected: unsharded to go to default-public.
                  mirrored to go to default-public and other-public
                  sharded to go to default-template, other-template,
                  default-schema1 and other-schema2
        """
        create_template_schema('default')  # also calls for a migration

        # mirrored, mapping and django default tables
        default_public_tables = ['django_migrations', 'django_content_type', 'auth_group', 'auth_permission',
                                 'auth_group_permissions', 'example_shard', 'django_session', 'example_type',
                                 'example_supertype', 'example_organizationshards']
        other_public_tables = ['django_migrations',  'example_type', 'example_supertype']  # only the mirrored table
        template_tables = ['django_migrations', 'example_organization', 'example_user']  # only sharded tables

        self.assertCountEqual(connections['default'].get_all_table_headers(schema_name='public'),
                              default_public_tables)
        self.assertCountEqual(connections['default'].get_all_table_headers(schema_name='template'), template_tables)
        self.assertCountEqual(connections['other'].get_all_table_headers(schema_name='public'), other_public_tables)

        create_template_schema('other')
        self.assertCountEqual(connections['other'].get_all_table_headers(schema_name='template'), template_tables)

        create_schema_on_node('schema1', node_name='default', migrate=False)
        # schema is created empty (cause we say: 'migrate=False')
        self.assertCountEqual(connections['default'].get_all_table_headers(schema_name='schema1'), [])

        # obviously, after migration shard schema's have the same tables as the template.
        migrate_schema('default', 'schema1')
        self.assertCountEqual(connections['default'].get_all_table_headers(schema_name='schema1'), template_tables)

        create_schema_on_node('schema2', node_name='other', migrate=True)
        self.assertCountEqual(connections['other'].get_all_table_headers(schema_name='schema2'), template_tables)


class CreateTemplateSchemaTestCase(ShardingTestCase):

    def test_create_template_schema(self):
        """
        Case: call utils.migrate_schema() to migrate the sharded models to the template schema
        Expected: The newly made schema to have to correct table headers.
        """
        create_template_schema('default')  # this also calls the migration

        cursor = connection.cursor()
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'template';")
        template_tables = [table[1] for table in cursor.fetchall()]
        # Filter test models
        template_tables = [table for table in template_tables if not re.search(r'_[t|T]est', table)]
        self.assertEqual(sorted(template_tables), ['django_migrations', 'example_organization', 'example_user'])

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
                                               state=State.ACTIVE)

    def repeatable_function(self, shard=None, shard_id=None, **kwargs):
        if shard:
            self.shards.append((shard, kwargs) if kwargs else shard)
        else:
            self.shards.append((shard_id, kwargs) if kwargs else shard_id)

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

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_for_each_shard_as_id(self):
        """
        Case: Call self.repeatable_function for every shard and get
              shards as ids.
        Expected: Function is called for every shard and the shard id
                  is passed as a argument.
        """
        self.shards = []
        for_each_shard(self.repeatable_function, as_id=True)
        self.assertEqual(self.shards, [self.shard1.id])


@mock.patch('sharding.utils.get_all_databases', return_value=['default', 'other'])
class ForEachNodeTestCase(SimpleTestCase):
    def repeatable_function(self, node_name=None, **kwargs):
        return node_name, kwargs

    def test_for_each_node(self, mock_get_all_databases):
        """
        Case: Call self.repeatable_function for every node
        Expected: Function is called for every node
        """
        result = for_each_node(self.repeatable_function)
        self.assertEqual(result, {'default': ('default', {}), 'other': ('other', {})})
        self.assertTrue(mock_get_all_databases.called)

    def test_for_each_node_with_kwargs(self, mock_get_all_databases):
        """
        Case: Call self.repeatable_function for every node and pass keyword arguments to the function.
        Expected: Function is called for every node and is called with the keyword arguments provided.
        """
        result = for_each_node(self.repeatable_function, kwargs={'org_id': 1})
        self.assertEqual(result, {'default': ('default', {'org_id': 1}), 'other': ('other', {'org_id': 1})})

        self.assertTrue(mock_get_all_databases.called)


class WriteToEveryNodeSystemTestCase(TransactionTestCase):
    def cleanup(self):
        for_each_node(self.cleanup_shard)

    def cleanup_shard(self, node_name):
        Type.objects.all().delete()

    def setUp(self):
        self.addCleanup(self.cleanup)

    def _post_teardown(self):
        # No need to revert stuff. In fact, it breaks the connections
        pass

    def test_write_to_all_nodes(self):
        """
        Case: Use the @atomic_write_to_every_node on a simple write function.
        Expected: Every Database has been written to.
        Note: This is system test keeping atomic_write_to_every_node and transaction_for_every_node as a black box.
        """
        with use_shard(node_name='default', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)
        with use_shard(node_name='other', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)

        @atomic_write_to_every_node(schema_name='public')
        def write_func(node_name):
            Type.objects.create(name='test_type')

        write_func()

        with use_shard(node_name='default', schema_name='public'):
            self.assertEqual(Type.objects.count(), 1)
            self.assertEqual(Type.objects.first().name, 'test_type')

        with use_shard(node_name='other', schema_name='public'):
            self.assertEqual(Type.objects.count(), 1)
            self.assertEqual(Type.objects.first().name, 'test_type')

    @mock.patch('sharding.decorators.transaction_for_every_node')
    def test_write_to_all_nodes_locking(self, mock_transaction_for_every_node):
        """
        Case: Use the @atomic_write_to_every_node with locking argument given.
        Expected: transaction_for_every_node to be called with the locking arguments.
        """

        @atomic_write_to_every_node(schema_name='public', lock_models=((Type, 'SHARE'),))
        def dummy_func(node_name):
            pass

        dummy_func()

        mock_transaction_for_every_node.assert_called_once_with(lock_models=((Type, 'SHARE'),))


class TransactionForEveryNodeTestCase(SimpleTestCase):
    @mock.patch('sharding.utils.Atomic.__init__')
    @mock.patch('sharding.utils.Atomic.__enter__', autospec=True)
    @mock.patch('sharding.utils.Atomic.__exit__', autospec=True)
    @mock.patch('sharding.utils.get_all_databases', return_value=['sina', 'rose', 'maria'])
    def test_parent_calls(self, mock_get_all_databases, mock_exit, mock_enter, mock_init):
        """
        Case: Use @transaction_for_every_node.
        Expected: The parent context manager classes to be called with the correct self.using set.
        """
        enter_connections = []
        exit_connections = []

        def fake_enter(self):
            enter_connections.append(self.using)

        def fake_exit(self, exc_type, exc_value, traceback):
            exit_connections.append(self.using)

        mock_enter.side_effect = fake_enter
        mock_exit.side_effect = fake_exit

        with transaction_for_every_node():
            pass

        self.assertFalse(mock_init.called)
        self.assertEqual(mock_enter.call_count, 3)
        self.assertEqual(mock_exit.call_count, 3)
        self.assertCountEqual(enter_connections, ['sina', 'rose', 'maria'])
        self.assertCountEqual(exit_connections, ['sina', 'rose', 'maria'])
        self.assertTrue(mock_get_all_databases.called)


class TransactionForEveryNodeTransactionTestCase(TransactionTestCase):
    def cleanup(self):
        for_each_node(self.cleanup_shard)

    def cleanup_shard(self, node_name):
        Type.objects.all().delete()

    def setUp(self):
        self.addCleanup(self.cleanup)

    def _post_teardown(self):
        # No need to revert stuff. In fact, it breaks the connections
        pass

    def test_with_failure_during_write(self):
        """
        Case: Use @transaction_for_every_node and fail when writing.
        Expected: All transactions to be rolled back.
        """
        with use_shard(node_name='default', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)
        with use_shard(node_name='other', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)

        with self.assertRaises(ProgrammingError):
            with transaction_for_every_node():
                with use_shard(node_name='default', schema_name='public'):
                    Type.objects.create(name='test_type')  # this is to be rolled back
                with use_shard(node_name='other', schema_name='public'):
                    raise ProgrammingError('table "Type" does not exist')

        with use_shard(node_name='default', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)
        with use_shard(node_name='other', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)

    def test_with_closing_connection_during_write(self):
        """
        Case: Use @transaction_for_every_node and close connection when writing.
        Expected: All transactions to be rolled back.
        """
        with use_shard(node_name='default', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)
        with use_shard(node_name='other', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)

        with self.assertRaises(InterfaceError):
            with transaction_for_every_node():
                with use_shard(node_name='default', schema_name='public'):
                    Type.objects.create(name='test_type')  # this is to be rolled back
                with use_shard(node_name='other', schema_name='public') as shard:
                    shard.connection.close()
                    Type.objects.create(name='test_type')  # this gives a 'connection already closed' exception

        with use_shard(node_name='default', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)
        with use_shard(node_name='other', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)

    def test_table_lock_execution(self):
        """
        Case: use @transaction_for_every_node with lock_models given
        Expected: SQL command to lock the models is executed
        """

        # mocking a cursor.execute is a bit of a faff
        with mock.patch('sharding.utils.connections') as mock_connections:
            mock_connection = mock_connections.__getitem__ = mock.Mock()
            mock_cursor = mock_connection.return_value.cursor = mock.Mock()
            mock_execute = mock_cursor.return_value.execute = mock.Mock()

            with transaction_for_every_node(lock_models=((Type, 'ROW SHARE'), (SuperType, 'SHARE'))):
                pass

            self.assertEqual(mock_execute.call_count, 4)  # we test with two databases and 2 tables
            mock_execute.assert_any_call('LOCK TABLE {} IN {} MODE'.format('example_type', 'ROW SHARE'))
            mock_execute.assert_any_call('LOCK TABLE {} IN {} MODE'.format('example_supertype', 'SHARE'))

    def test_with_table_lock(self):
        """
        Case: use @transaction_for_every_node with lock_models given
        Expected: The models to be locked and other writes outside the transaction to be blocked
        """

        def conflicting_transaction():
            """
            Create a new tranassction, independant on those made in `transaction_for_every_node`
            Try to claim a exclusive on the table locked by `transaction_for_every_node`.
            Close connection so not to interfere with the rest of the TestCase.
            """
            with transaction.atomic():
                con = connections['default']
                with self.assertRaises(OperationalError):
                    con.cursor().execute(
                        'LOCK TABLE "example_type" IN ACCESS EXCLUSIVE MODE NOWAIT'
                    )
                    Type.objects.create(name='test_type')
                con.close()

        with transaction_for_every_node(lock_models=((Type, 'ACCESS EXCLUSIVE'),)):
            with use_shard(node_name='default', schema_name='public'):
                # Don't create an object before calling the other thread.
                # A write will lock the table too, and will only be released when the transaction is committed.
                # So this will always make the test succeed,
                # even if transaction_for_every_node does not lock anything.

                t1 = threading.Thread(target=conflicting_transaction)
                t1.start()
                t1.join()

                Type.objects.create(name='test_type')

        with use_shard(node_name='default', schema_name='public'):
            self.assertEqual(Type.objects.count(), 1)


class WriteToEveryNodeTestCase(SimpleTestCase):
    @mock.patch('sharding.decorators.transaction_for_every_node')
    @mock.patch('sharding.decorators.use_shard')
    @mock.patch('sharding.decorators.get_all_databases', return_value=['sina', 'rose', 'maria'])
    def test_write_to_every_node(self, mock_get_all_databases, mock_use_shard, mock_transaction):
        """
        Case: Use the @atomic_write_to_every_node, and call the decorated function with an argument.
        Expected: transaction_for_every_node, use_shard and the decorated function to be called.
        """
        use_schemas = []

        @atomic_write_to_every_node(schema_name='some_schema')
        def test_function(test_argument, node_name):
            use_schemas.append(node_name)
            self.assertEqual(test_argument, 'Sunstone')

        test_function('Sunstone')

        mock_use_shard.assert_any_call(node_name='sina', schema_name='some_schema')
        mock_use_shard.assert_any_call(node_name='rose', schema_name='some_schema')
        mock_use_shard.assert_any_call(node_name='maria', schema_name='some_schema')
        self.assertEqual(mock_transaction.call_count, 1)
        self.assertEqual(mock_get_all_databases.call_count, 1)
        self.assertCountEqual(use_schemas, ['sina', 'rose', 'maria'])

    @mock.patch('sharding.decorators.transaction_for_every_node')
    @mock.patch('sharding.decorators.use_shard')
    @mock.patch('sharding.decorators.get_all_databases', return_value=['sina', 'rose', 'maria'])
    def test_write_to_every_node_return_value(self, mock_get_all_databases, mock_use_shard, mock_transaction):
        """
        Case: Use the @atomic_write_to_every_node, and call the decorated function with an argument.
        Expected: The function gives back a dict with the node_name as keys and the return value as their values
        """
        @atomic_write_to_every_node(schema_name='some_schema')
        def test_function(test_argument, node_name):
            return (test_argument, node_name)

        return_value = test_function('Firestone')

        mock_use_shard.assert_any_call(node_name='sina', schema_name='some_schema')
        mock_use_shard.assert_any_call(node_name='rose', schema_name='some_schema')
        mock_use_shard.assert_any_call(node_name='maria', schema_name='some_schema')
        self.assertEqual(mock_transaction.call_count, 1)
        self.assertEqual(mock_get_all_databases.call_count, 1)
        self.assertEqual({
            'sina': ('Firestone', 'sina'),
            'rose': ('Firestone', 'rose'),
            'maria': ('Firestone', 'maria'),
        }, return_value)

    def test_decorated_with(self):
        """
        Case: Check if the function is decorator with a specific decorator
        Expected: The function is decorated and called with the expected argument
        """
        @atomic_write_to_every_node(schema_name='some_schema')
        def test_function(test_argument, node_name):
            pass

        expected_bound_arguments = inspect.signature(atomic_write_to_every_node).bind('some_schema')

        decorator, bound_arguments = test_function.__decorator__

        self.assertEqual(decorator, atomic_write_to_every_node)
        self.assertEqual(bound_arguments.args, expected_bound_arguments.args)
        self.assertEqual(bound_arguments.kwargs, expected_bound_arguments.kwargs)

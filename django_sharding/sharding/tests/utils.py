import inspect
import re
import threading
from unittest import mock

from django.apps import apps
from django.core.exceptions import ImproperlyConfigured
from django.db import connection, connections, models, ProgrammingError, InterfaceError, OperationalError, transaction
from django.test import SimpleTestCase, TestCase, override_settings, TransactionTestCase

from example.models import Shard, OrganizationShards, Type, SuperType, User, Organization, Statement, Suborganization
from sharding.decorators import sharded_model, mirrored_model, atomic_write_to_every_node
from sharding.utils import use_shard, create_schema_on_node, DynamicDbRouter, THREAD_LOCAL, \
    _use_connection, _set_schema, create_template_schema, migrate_schema, get_template_name, _node_exists, \
    StateException, use_shard_for, get_shard_for, for_each_shard, State, for_each_node, transaction_for_every_node, \
    move_model_to_schema, get_all_databases, ShardingMode, get_sharding_mode, get_model_sharding_mode, \
    get_all_sharded_models, get_shard_class, get_mapping_class, transaction_for_nodes, get_all_mirrored_models


@sharded_model()
class DummyShardedModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'sharding'
        managed = False


@mirrored_model()
class DummyMirroredModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'sharding'
        managed = False


class DummyNonShardedModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'sharding'
        managed = False


class ShardingTestCase(TestCase):
    available_apps = ['sharding', 'example']

    def setUp(self):
        super().setUp()
        self.addCleanup(self.clean_up)

    def clean_up(self):
        THREAD_LOCAL.DB_OVERRIDE = None

        for con in connections.all():
            con.set_schema_to_public()

            # remove all schemas made in tests.
            for schema in con.get_all_pg_schemas():
                schema = schema[0]
                if schema.startswith('test_') or schema == get_template_name():
                    con.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format(schema))


class ShardingTransactionTestCase(TransactionTestCase):
    available_apps = ['sharding', 'example']

    @staticmethod
    def get_all_non_sharded_models():
        models = apps.get_models()
        return [model for model in models if get_model_sharding_mode(model) != ShardingMode.SHARDED
                and not getattr(model, 'test_model', False)]

    @staticmethod
    def get_all_mirrored_models():
        models = apps.get_models()
        return [model for model in models if get_model_sharding_mode(model) == ShardingMode.MIRRORED
                and not getattr(model, 'test_model', False)]

    def setUp(self):
        super().setUp()
        self.addCleanup(self.clean_up)

    def clean_up(self):
        """
        This clean_up looks a bit like the _fixture_teardown from the TransactionTestCase.
        But it does so on each connection and looks at the shards for what to truncate.
        And it removes the test and template schemas like the ShardedTestCase does.
        """
        THREAD_LOCAL.DB_OVERRIDE = None

        for con in connections.all():
            # Remove all schemas made in tests.
            for schema in con.get_all_pg_schemas():
                schema = schema[0]
                if schema.startswith('test_') or schema == get_template_name():
                    con.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format(schema))

            # Truncate all public tables
            if con.get_ps_schema('public'):
                # default|public has more tables's than just the mirrored ones. example_shard for instance.
                models = self.get_all_non_sharded_models() if con.alias == 'default' else self.get_all_mirrored_models()
                con.cursor().execute('TRUNCATE ONLY {} CASCADE;'.format(
                    ', '.join('"{}"'.format(model._meta.db_table) for model in models)
                    ))

            con.set_schema_to_public()

    def _fixture_teardown(self):
        """
        Fixture teardown is no longer necessary. And won't work properly anyway.
        """
        pass


class GetShardClass(SimpleTestCase):
    """
    We don't need to test for the situation where the class is not set in the settings.
    For we will give an error on run in that case.
    """
    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_get_shard_class_set(self):
        """
        Case: Call get_shard_class while it is set in the settings
        Expected: The model class.
        """
        self.assertEqual(get_shard_class(), Shard)


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


class GetMappingClass(SimpleTestCase):
    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_get_mapping_class_unset(self):
        """
        Case: Call get_mapping_class when it is not set in the settings.
        Expected: The default template name: 'template'
        """
        self.assertIsNone(get_mapping_class())

    @override_settings(SHARDING={'MAPPING_MODEL': 'example.models.OrganizationShards',
                                 'SHARD_CLASS': 'example.models.Shard'})
    def test_get_mapping_class_set(self):
        """
        Case: Call get_mapping_class while it is set in the settings
        Expected: The mapping class.
        """
        self.assertEqual(get_mapping_class(), OrganizationShards)


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
        super().tearDownClass()

    @mock.patch('sharding.utils._set_schema')
    def test_use_shard(self, mock_set_schema):
        """
        Case: Call use_shard with a valid shard object
        Expected: Connection and schema to be changed
        """
        with use_shard(self.shard) as env:
            self.assertEqual(connection.alias, 'default')
            mock_set_schema.assert_called_once_with('test_schema', env.connection, include_public=True,
                                                    override_model_use_shard=False, shard_id=self.shard.id,
                                                    mapping_value=None, active_only_schemas=True, lock=True)

    @mock.patch('sharding.utils._set_schema')
    def test_use_shard_on_other_node(self, mock_set_schema):
        """
        Case: Call use_shard with a valid shard object referring to a non-default node
        Expected: Connection and schema to be changed
        """
        with use_shard(self.other_shard) as env:
            self.assertEqual(THREAD_LOCAL.DB_OVERRIDE, ['other'])
            mock_set_schema.assert_called_once_with('test_other_schema', env.connection,  include_public=True,
                                                    override_model_use_shard=False, shard_id=self.other_shard.id,
                                                    mapping_value=None, active_only_schemas=True, lock=True)

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
    def test_use_shard_without_public(self, mock_set_schema):
        """
        Case: Call use_shard within with include_public set to False.
        Expected: Connection to switch twice and set_schema to be called accordingly
        """
        with use_shard(self.shard, include_public=False) as env:
            connection_1 = env.connection

        mock_set_schema.assert_any_call(self.shard.schema_name, connection_1, include_public=False,
                                        override_model_use_shard=False, shard_id=self.shard.id,
                                        mapping_value=None, active_only_schemas=True, lock=True)

    @mock.patch('sharding.utils._set_schema')
    def test_use_shard_inception(self, mock_set_schema):
        """
        Case: Call use_shard within a use_shard environment
        Expected: Connection to switch twice and set_schema to be called accordingly
        """
        with use_shard(self.shard) as env_1:
            connection_1 = env_1.connection
            mock_set_schema.assert_any_call(self.shard.schema_name, connection_1, include_public=True,
                                            override_model_use_shard=False, shard_id=self.shard.id,
                                            mapping_value=None, active_only_schemas=True, lock=True)
            self.assertEqual(THREAD_LOCAL.DB_OVERRIDE, ['default'])

            with use_shard(self.other_shard) as env_2:
                connection_2 = env_2.connection
                mock_set_schema.assert_any_call(self.other_shard.schema_name, connection_2, include_public=True,
                                                override_model_use_shard=False, shard_id=self.other_shard.id,
                                                mapping_value=None, active_only_schemas=True, lock=True)
                self.assertEqual(THREAD_LOCAL.DB_OVERRIDE, ['default', 'other'])

        self.assertIsNone(THREAD_LOCAL.DB_OVERRIDE)

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

    def test_use_shard_set_parameters_on_connection(self):
        """
        Case: Use use_shard
        Expected: Parameters from use_shard are correctly set on the connection for re-use later
        """
        with use_shard(self.shard) as env:
            self.assertEqual(env.connection.schema_name, self.shard.schema_name)
            self.assertEqual(env.connection._shard_id, self.shard.id)
            self.assertEqual(env.connection._mapping_value, None)
            self.assertEqual(env.connection._active_only_schemas, True)
            self.assertEqual(env.connection._override_model_use_shard, False)
            self.assertEqual(env.connection._lock, True)

    @mock.patch('sharding.utils._set_schema', mock.Mock)
    @mock.patch('sharding.utils._use_connection')
    @mock.patch('sharding.utils.use_shard.acquire_lock')
    def test_enable_acquire_advisory_lock(self, mock_acquire_lock, mock_use_connection):
        """
        Case: Use use_shard.enable()
        Expected: acquire_lock to be called.
        """
        mock_use_connection.return_value = connection

        use_shard(self.shard).enable()
        mock_acquire_lock.assert_called_once_with()

    @mock.patch('sharding.utils._set_schema', mock.Mock)
    @mock.patch('sharding.utils.use_shard.release_lock')
    def test_disable_release_advisory_lock(self, mock_release_lock):
        """
        Case: Use use_shard.disable()
        Expected: release_lock to be called.
        """
        env = use_shard(self.shard)
        env.old_schema_name = 'public'
        env.old_override_model_use_shard = False
        env.old_active_only_schemas = False
        env.old_shard_id = None
        env.old_mapping_value = None
        env.old_lock = True
        env.connection = connection

        env.disable()
        mock_release_lock.assert_called_once_with()

    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.acquire_advisory_lock')
    def test_acquire_lock(self, mock_acquire_lock):
        """
        Case: Call use_shard.acquire_lock().
        Expected: Connection's acquire_advisory_lock to be called with the correct arguments.
        """
        env = use_shard(self.shard)
        env.connection = connection
        env.acquire_lock()
        mock_acquire_lock.assert_called_once_with(key='shard_{}'.format(self.shard.id), shared=True)

    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.release_advisory_lock')
    def test_release_lock(self, mock_release_advisory_lock):
        """
        Case: Call use_shard.release_lock().
        Expected: Connection's release_advisory_lock to be called with the correct arguments.
        """
        env = use_shard(self.shard)
        env.connection = connection
        env.release_lock()
        mock_release_advisory_lock.assert_called_once_with(key='shard_{}'.format(self.shard.id), shared=True)


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
            mock_set_schema.assert_called_once_with(self.shard1.schema_name, connections[self.shard1.node_name],
                                                    include_public=True, override_model_use_shard=False,
                                                    shard_id=self.shard1.id, mapping_value=1, active_only_schemas=True,
                                                    lock=True)

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

    @mock.patch('sharding.utils._set_schema')
    def test_use_shard_for_inactive_shard(self, mock_set_schema):
        """
        Case: Use use_shard_for with an active mapping object, but inactive shard
        Expected: StateException raised
        """
        self.shard1.state = State.MAINTENANCE
        self.shard1.save(update_fields=['state'])

        with self.assertRaises(StateException):
            with use_shard_for(1):
                pass
        self.assertFalse(mock_set_schema.called)

    @mock.patch('sharding.utils._set_schema')
    def test_use_shard_for_inactive_object_include_inactive(self, mock_set_schema):
        """
        Case: Use use_shard_for with an inactive mapping object, but have active_only_schemas set to False.
        Expected: Successful usage of use_shard_for
        """
        with use_shard_for(2, active_only_schemas=False):
            mock_set_schema.assert_called_once_with(self.shard1.schema_name, connections[self.shard1.node_name],
                                                    include_public=True, override_model_use_shard=False,
                                                    shard_id=self.shard1.id, mapping_value=2, active_only_schemas=False,
                                                    lock=True)

    def test_use_shard_set_parameters_on_connection(self):
        """
        Case: Use use_shard_for
        Expected: Parameters from use_shard_for are correctly set on the connection for re-use later
        """
        with use_shard_for(1, lock=False) as env:
            self.assertEqual(env.connection.schema_name, self.shard1.schema_name)
            self.assertEqual(env.connection._shard_id, self.shard1.id)
            self.assertEqual(env.connection._mapping_value, 1)
            self.assertEqual(env.connection._active_only_schemas, True)
            self.assertEqual(env.connection._override_model_use_shard, False)
            self.assertEqual(env.connection._lock, False)

    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.acquire_advisory_lock')
    def test_acquire_lock(self, mock_acquire_lock):
        env = use_shard_for(self.org_shard1.organization_id)
        env.connection = connection
        env.acquire_lock()
        mock_acquire_lock.assert_any_call(key='mapping_{}'.format(self.org_shard1.organization_id), shared=True)
        mock_acquire_lock.assert_any_call(key='shard_{}'.format(self.shard1.id), shared=True)

    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.release_advisory_lock')
    def test_release_lock(self, mock_release_advisory_lock):
        env = use_shard_for(self.org_shard1.organization_id)
        env.connection = connection
        env.release_lock()
        mock_release_advisory_lock.assert_any_call(key='mapping_{}'.format(self.org_shard1.organization_id),
                                                   shared=True)
        mock_release_advisory_lock.assert_any_call(key='shard_{}'.format(self.shard1.id), shared=True)


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
        Case: call db_for_read with no DB_OVERRIDE set
        Expected: None returned
        """
        self.assertIsNone(self.router.db_for_read(model=mock.MagicMock()))

    def test_db_for_read_while_set_single(self):
        """
        Case: Call db_for_read with a DB_OVERRIDE set
        Expected: Name of the correct node returned
        """
        THREAD_LOCAL.DB_OVERRIDE = ['test_node']
        self.assertEqual(self.router.db_for_read(model=mock.MagicMock()), 'test_node')

    def test_db_for_read_while_set_multiple(self):
        """
        Case: Call db_for_read with a DB_OVERRIDE set with more than one entry
        Expected: Name of the correct node returned
        """
        THREAD_LOCAL.DB_OVERRIDE = ['default', 'test_node']
        self.assertEqual(self.router.db_for_read(model=mock.MagicMock()), 'test_node')

    def test_db_for_write_while_not_set(self):
        """
        Case: call db_for_write with no DB_OVERRIDE set
        Expected: None returned
        """
        self.assertIsNone(self.router.db_for_write(model=mock.MagicMock()))

    def test_db_for_write_while_set_single(self):
        """
        Case: Call db_for_write with a DB_OVERRIDE set
        Expected: Name of the correct node returned
        """
        THREAD_LOCAL.DB_OVERRIDE = ['test_node']
        self.assertEqual(self.router.db_for_write(model=mock.MagicMock()), 'test_node')

    def test_db_for_write_while_set_multiple(self):
        """
        Case: Call db_for_write with a DB_OVERRIDE set with more than one entry
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

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                 'OVERRIDE_SHARDING_MODE': {
                                     ('sharding', 'dummynonshardedmodel'): ShardingMode.MIRRORED,
                                 }})
    def test_allow_relation_between_sharded_models_settings_override_model(self):
        """
        Case: Call allow_relation with two sharded models, the latter is set through the configuration.
        Expected: True, we don't check if they are on the same shard yet.
        """
        self.assertTrue(self.router.allow_relation(DummyShardedModel(), DummyNonShardedModel()))

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                 'OVERRIDE_SHARDING_MODE': {
                                     ('sharding',): ShardingMode.MIRRORED,
                                 }})
    def test_allow_relation_between_sharded_models_settings_override_app(self):
        """
        Case: Call allow_relation with two sharded models, the latter is set through the configuration.
        Expected: True, we don't check if they are on the same shard yet.
        """
        self.assertTrue(self.router.allow_relation(Organization(), DummyNonShardedModel()))

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
        # The tables present on all non-default public schema's are all the mirrored tables.
        other_public_tables = ['django_migrations',  'example_type', 'example_supertype']
        # The tables present on the template schema's are all the sharded tables.
        template_tables = ['django_migrations', 'example_organization', 'example_suborganization', 'example_user',
                           'example_statement']

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

    def test_not_allow_migrate_sharded_on_public(self):
        """
        Case: Check if sharded model is allowed to migrate on default connection public schema.
        Expected: The sharded model should not be allowed to migrate to the public schema.
        """
        self.assertFalse(self.router.allow_migrate('default', 'example', 'organization'))
        self.assertFalse(self.router.allow_migrate('other', 'example', 'organization'))

    @mock.patch('sharding.utils.logger.warning')
    def test_allow_migrate_on_nonexisting_model(self, mock_logger_warning):
        """
        Case: Call allow_migrate for a model that (no longer) exists.
        Expected: Warning to be logged.
        """
        self.router.allow_migrate('default', 'example', 'outer_space')
        self.assertEqual(mock_logger_warning.call_count, 1)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                 'MAPPING_MODEL': 'example.models.OrganizationShards',
                                 'NEW_SHARD_NODE': 'other',
                                 'OVERRIDE_SHARDING_MODE': {
                                     ('example', 'organization'): ShardingMode.MIRRORED,
                                 }})
    def test_allow_migrate_sharded_settings_override_to_mirrored(self):
        """
        Case: Check if previously sharded model is allowed to migrate onto public schema if it is set
              to ShardingMode.MIRRORED through the configuration.
        Expected: The router should allow the overridden model to be migrated onto the public schema.
        """
        self.assertTrue(self.router.allow_migrate('default', 'example', 'organization'))
        self.assertTrue(self.router.allow_migrate('other', 'example', 'organization'))

    def test_allow_migrate_on_none(self):
        """
        Case: Call allow_migrate without a model_name
        Expected: Programming error to be raised
        """
        with self.assertRaises(ProgrammingError):
            self.router.allow_migrate('default', 'example', model_name=None)

    def test_allow_migrate_with_hints(self):
        """
        Case: run_python in migration with hints given
        Expected: Router to route correctly
        """
        self.assertTrue(self.router.allow_migrate('default', 'example', model_name=None,
                                                  sharding_mode=ShardingMode.MIRRORED))
        self.assertFalse(self.router.allow_migrate('default', 'example', model_name=None,
                                                   sharding_mode=ShardingMode.SHARDED))
        self.assertTrue(self.router.allow_migrate('default', 'example', model_name='organization',
                                                  sharding_mode=ShardingMode.MIRRORED))


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
        self.assertCountEqual(sorted(template_tables), ['django_migrations', 'example_organization',
                                                        'example_suborganization', 'example_user',
                                                        'example_statement'])

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


class GetModelShardingModeTestCase(SimpleTestCase):
    @mock.patch('sharding.utils.get_sharding_mode')
    def test(self, mock_get_sharding_mode):
        """
        Case: Call get_model_sharding_mode.
        Expected: get_sharding_mode to be called with the correct arguments.
        """
        get_model_sharding_mode(User)
        mock_get_sharding_mode.called_once_with(app_label='example', model_name='User')


class GetShardingModeTestCase(SimpleTestCase):
    def test_get_sharding_mode_override_settings(self):
        """
        Case: Set sharding configuration and verify that DynamicDbRouter.get_sharding_mode() it uses the overridden
              settings.
        Expected: The router should return the overriden setting for the model.
        """
        with override_settings(
                SHARDING={'OVERRIDE_SHARDING_MODE': {('example', 'organization'): ShardingMode.MIRRORED, }}):
            self.assertEqual(get_sharding_mode('example', 'organization'), ShardingMode.MIRRORED)

        with override_settings(
                SHARDING={'OVERRIDE_SHARDING_MODE': {('example', ): ShardingMode.MIRRORED, }}):
            self.assertEqual(get_sharding_mode('example', 'user'), ShardingMode.MIRRORED)

    def test_get_sharding_mode_fallback(self):
        """
        Case: Set sharding configuration and verify that DynamicDbRouter.get_sharding_mode() will use the fallback to
              checking model class if the model sharding mode is not overridden in settings.
        Expected: The router should return the class setting for the model.
        """
        with override_settings(
                SHARDING={'OVERRIDE_SHARDING_MODE': {('example', 'user'): ShardingMode.MIRRORED, }}):
            self.assertEqual(get_sharding_mode('example', 'organization'), ShardingMode.SHARDED)

    def test_get_sharding_mode_without_model_name(self):
        """
        Case: Call get_sharding_mode with None as model_name
        Expected: None returned.
        """
        self.assertIsNone(get_sharding_mode('example', None))

    def test_get_sharding_mode_without_model_name_but_with_override(self):
        """
        Case: Call get_sharding_mode with None as model_name, while settings override set.
        Expected: Mode returned as defined by the settings override
        """
        with override_settings(
                SHARDING={'OVERRIDE_SHARDING_MODE': {('example',): ShardingMode.MIRRORED, }}):
            self.assertEqual(get_sharding_mode('example', None), ShardingMode.MIRRORED)


class GetAllShardedModels(TestCase):
    available_apps = ['example']

    def test_with_override(self):
        """
        Case: Call get_all_sharded_models.
        Expected: Only the User and the Statement models to be returned. The rest is mirrored or not sharded.
        Note: System test
        """
        with override_settings(
                SHARDING={'OVERRIDE_SHARDING_MODE':
                          {('example', 'organization'): ShardingMode.MIRRORED, }}):
            self.assertCountEqual(get_all_sharded_models(), [User, Statement, Suborganization])

    @mock.patch('sharding.utils.get_model_sharding_mode')
    def test(self, mock_get_model_sharding_mode):
        """
        Case: Call get_all_sharded_models.
        Expected: get_model_sharding_mode called for each model.
        """
        get_all_sharded_models()
        for model in apps.get_models():
            mock_get_model_sharding_mode.assert_has_call(model=model)


class GetAllMirroredModels(TestCase):
    available_apps = ['example']

    def test_with_override(self):
        """
        Case: Call get_all_mirrored_models.
        Expected: Only SuperType models to be returned. The rest is sharded and/or not mirrored.
        Note: System test
        """
        with override_settings(
                SHARDING={'OVERRIDE_SHARDING_MODE': {('example', 'type'): ShardingMode.SHARDED, }}):
            self.assertCountEqual(get_all_mirrored_models(), [SuperType])

    @mock.patch('sharding.utils.get_model_sharding_mode')
    def test(self, mock_get_model_sharding_mode):
        """
        Case: Call get_all_mirrored_models.
        Expected: get_model_sharding_mode called for each model.
        """
        get_all_mirrored_models()
        for model in apps.get_models():
            mock_get_model_sharding_mode.assert_has_call(model=model)


class GetAllDatabases(SimpleTestCase):
    @override_settings(DATABASES={'default': 'some connection', 'space': 'some otherworldly connection'})
    def test(self):
        """
        Case: Call get_all_databases.
        Expected: Names of the databases defined in the settings.
        """
        self.assertCountEqual(get_all_databases(), ['default', 'space'])


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


class WriteToEveryNodeSystemTestCase(ShardingTransactionTestCase):
    def cleanup(self):
        for_each_node(self.cleanup_shard)

    def cleanup_shard(self, node_name):
        Type.objects.all().delete()

    def setUp(self):
        super().setUp()
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


class TransactionForNodesTestCase(ShardingTransactionTestCase):
    def cleanup(self):
        for_each_node(self.cleanup_shard)

    def cleanup_shard(self, node_name):
        Type.objects.all().delete()

    def setUp(self):
        super().setUp()
        self.addCleanup(self.cleanup)

    def _post_teardown(self):
        # No need to revert stuff. In fact, it breaks the connections
        pass

    @mock.patch('sharding.utils.Atomic.__init__')
    @mock.patch('sharding.utils.Atomic.__enter__', autospec=True)
    @mock.patch('sharding.utils.Atomic.__exit__', autospec=True)
    def test_parent_calls(self, mock_exit, mock_enter, mock_init):
        """
        Case: Use @transaction_for_nodes.
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

        with transaction_for_nodes(nodes=['sina', 'rose', 'maria']):
            pass

        self.assertFalse(mock_init.called)
        self.assertEqual(mock_enter.call_count, 3)
        self.assertEqual(mock_exit.call_count, 3)
        self.assertCountEqual(enter_connections, ['sina', 'rose', 'maria'])
        self.assertCountEqual(exit_connections, ['sina', 'rose', 'maria'])

    def test_with_failure_during_write(self):
        """
        Case: Use @transaction_for_nodes and fail when writing.
        Expected: All transactions to be rolled back.
        """
        with use_shard(node_name='default', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)
        with use_shard(node_name='other', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)

        with self.assertRaises(ProgrammingError):
            with transaction_for_nodes(nodes=['default', 'other']):
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
        Case: Use @transaction_for_nodes and close connection when writing.
        Expected: All transactions to be rolled back.
        """
        with use_shard(node_name='default', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)
        with use_shard(node_name='other', schema_name='public'):
            self.assertEqual(Type.objects.count(), 0)

        with self.assertRaises(InterfaceError):
            with transaction_for_nodes(nodes=['default', 'other']):
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
        Case: use @transaction_for_nodes with lock_models given
        Expected: SQL command to lock the models is executed
        """

        # mocking a cursor.execute is a bit of a faff
        with mock.patch('sharding.utils.connections') as mock_connections:
            mock_connection = mock_connections.__getitem__ = mock.Mock()
            mock_cursor = mock_connection.return_value.cursor = mock.Mock()
            mock_execute = mock_cursor.return_value.execute = mock.Mock()

            with transaction_for_nodes(nodes=['default', 'other'],
                                       lock_models=((Type, 'ROW SHARE'), (SuperType, 'SHARE'))):
                pass

            self.assertEqual(mock_execute.call_count, 4)  # we test with two databases and 2 tables
            mock_execute.assert_any_call('LOCK TABLE "{}" IN {} MODE'.format('example_type', 'ROW SHARE'))
            mock_execute.assert_any_call('LOCK TABLE "{}" IN {} MODE'.format('example_supertype', 'SHARE'))

    def test_with_table_lock(self):
        """
        Case: use @transaction_for_nodes with lock_models given
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

        with transaction_for_nodes(nodes=['default', 'other'],
                                   lock_models=((Type, 'ACCESS EXCLUSIVE'),)):
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


class TransactionForEveryNodeTestCase(SimpleTestCase):
    @mock.patch('sharding.utils.transaction_for_nodes.__init__')
    @mock.patch('sharding.utils.transaction_for_nodes.__enter__', mock.Mock)
    @mock.patch('sharding.utils.transaction_for_nodes.__exit__',  mock.Mock)
    @mock.patch('sharding.utils.get_all_databases', return_value=['default', 'other'])
    def test(self, mock_all_databases, mock_init):
        """
        Case: Use transaction_for_every_node.
        Expected: transaction_for_nodes to be called with the correct value for nodes.
        """
        with transaction_for_every_node():
            pass

        self.assertEqual(mock_all_databases.call_count, 1)
        mock_init.assert_called_once_with(nodes=['default', 'other'])


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
        @atomic_write_to_every_node(schema_name='some_schema', lock_models=())
        def test_function(test_argument, node_name):
            pass

        expected_bound_arguments = inspect.signature(atomic_write_to_every_node).bind('some_schema', ())

        decorator, bound_arguments = test_function.__decorator__

        self.assertEqual(decorator, atomic_write_to_every_node)
        self.assertEqual(bound_arguments.args, expected_bound_arguments.args)
        self.assertEqual(bound_arguments.kwargs, expected_bound_arguments.kwargs)


class MoveModelToSchemaTestCase(ShardingTransactionTestCase):
    available_apps = ['example']

    def clean_up(self):
        """
        Move the table back; else the TestCase might go confused.
        Then perform the normal ShardingTransactionTestCase clean_up
        """
        if Shard.objects.filter(schema_name='test_other_schema').exists():
            with use_shard(node_name='default', schema_name='test_other_schema'):
                move_model_to_schema(model=Type, node_name='default', from_schema_name='test_other_schema',
                                     to_schema_name='public')
        super().clean_up()

    def setUp(self):
        self.addCleanup(self.clean_up)

    def test(self):
        """
        Case: Move the Type model over from public to a shard
        Expected: The Type model to be moved. All data should remain in tact.
        """
        create_template_schema('default')
        other_shard = Shard.objects.create(alias='other', node_name='default', schema_name='test_other_schema',
                                           state=State.ACTIVE)

        # Create a bunch of connected objects.
        supertype = SuperType.objects.create(id=4, name='tesla')  # use a specific id
        type = Type.objects.create(id=3, name='tesla', super=supertype)  # use a specific id
        with use_shard(other_shard):
            organization = Organization.objects.create(name='SpaceX')
            user = User.objects.create(name='Starman', organization=organization, type=type)

        # We don't want to also select the public schema when we select a shard.
        with use_shard(shard=other_shard, include_public=False):
            # Doesn't exist on the shard, but will soon.
            with self.assertRaises(ProgrammingError):
                type.refresh_from_db()

        move_model_to_schema(model=Type, node_name='default', to_schema_name='test_other_schema')

        with use_shard(node_name='default', schema_name='public', include_public=False):
            # Now that we moved the Type table, we should not be able to refresh it.
            with self.assertRaises(ProgrammingError):
                type.refresh_from_db()

            # supertype is not moved, so should remain accessible from the public schema.
            supertype.refresh_from_db()

        with use_shard(shard=other_shard, include_public=False, override_model_use_shard=True):
            # Now that we moved the Type table, we should be able to access it from the shard.
            type.refresh_from_db()

            # supertype is not moved, so should remain inaccessible
            with self.assertRaises(ProgrammingError):
                supertype.refresh_from_db()

            # Organization and User are not moved, so should be on the shard still
            organization.refresh_from_db()
            user.refresh_from_db()

        # Confirm Data integrity, now they are refreshed
        self.assertEqual(user.type_id, 3)
        self.assertEqual(user.type.id, 3)
        self.assertEqual(type.id, 3)
        self.assertEqual(type.name, 'tesla')
        self.assertEqual(type.super_id, 4)
        self.assertEqual(type.super.id, 4)
        self.assertEqual(supertype.id, 4)


class MoveModelToExistingSchemaTestCase(ShardingTransactionTestCase):
    available_apps = ['example']

    def test(self):
        """
        Case: Move a schema to a shard where it already resides
        Expected: move_model_to_schema to raise an error
        """
        create_template_schema('default')
        another_schema = Shard.objects.create(alias='another', node_name='default', schema_name='test_another_schema',
                                              state=State.ACTIVE)

        # The User table is already on the sharded schema.
        with self.assertRaises(ProgrammingError):
            move_model_to_schema(model=User, node_name='default', to_schema_name=another_schema.schema_name)

import inspect
import re
import threading
from unittest import mock

from django.apps import apps
from django.core.exceptions import ImproperlyConfigured
from django.db import connections, ProgrammingError, InterfaceError, OperationalError, transaction
from django.db.utils import ConnectionDoesNotExist
from django.test import SimpleTestCase, override_settings

from example.models import Shard, OrganizationShards, Type, SuperType, User, Organization, Statement, Suborganization, \
    Cake
from sharding.db import connection
from sharding.decorators import atomic_write_to_every_node
from sharding.options import ShardOptions
from sharding.router import set_active_connection, get_active_connection
from sharding.tests import ShardingTestCase, ShardingTransactionTestCase, ResetConnectionTestCaseMixin
from sharding.utils import use_shard, create_schema_on_node, create_template_schema, migrate_schema, \
    get_template_name, _node_exists, StateException, use_shard_for, get_shard_for, for_each_shard, State, \
    for_each_node, transaction_for_every_node, move_model_to_schema, get_all_databases, ShardingMode, \
    get_sharding_mode, get_model_sharding_mode, get_all_sharded_models, get_shard_class, get_mapping_class, \
    transaction_for_nodes, get_all_mirrored_models, delete_schema, schema_exists


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


class SetActiveConnectionTestCase(ResetConnectionTestCaseMixin, SimpleTestCase):
    def test(self):
        """
        Case: Set the active connection to something else
        Expected: Active connection should change
        """
        self.assertEqual(get_active_connection(), 'default')
        set_active_connection('other')
        self.assertEqual(get_active_connection(), 'other')


class UseShardTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema('default')
        create_template_schema('other')
        self.shard = Shard.objects.create(alias='test_shard', schema_name='test_schema', node_name='default',
                                          state=State.ACTIVE)
        self.other_shard = Shard.objects.create(alias='other_shard', schema_name='test_other_schema', node_name='other',
                                                state=State.ACTIVE)
        self.inactive_shard = Shard.objects.create(alias='inactive_shard', schema_name='test_inactive_schema',
                                                   node_name='other', state=State.MAINTENANCE)

    @mock.patch('sharding.router.set_active_connection')
    def test_use_shard(self, mock_set_active_connection):
        """
        Case: Call use_shard with a valid shard object
        Expected: set_active_connection to be called with the correct options
        """
        with use_shard(self.shard):
            pass

        options = ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name,
                               shard_id=self.shard.id, use_shard=True)

        self.assertEqual(mock_set_active_connection.call_count, 2)

        mock_set_active_connection.assert_has_calls([
            # First the shard we want to enter
            mock.call(options),
            # And then to enter the old connection again on exiting the context manager
            mock.call('default'),
        ])

    @mock.patch('sharding.router.set_active_connection')
    def test_use_shard_with_invalid_argument(self, mock_set_active_connection):
        """
        Case: Call use_shard with an invalid argument
        Expected: A ValueError to be raised
        """
        with self.assertRaises(ValueError):
            with use_shard('not a Shard object'):
                pass

        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('sharding.router.set_active_connection')
    def test_use_shard_with_inactive_shard(self, mock_set_active_connection):
        """
        Case: Call use_shard with a shard that is not active
        Expected: A StateException to be raised
        """
        with self.assertRaises(StateException) as error:
            with use_shard(self.inactive_shard):
                pass
        self.assertEqual(error.exception.state, State.MAINTENANCE)

        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('sharding.router.set_active_connection')
    def test_use_shard_with_inactive_shard_with_state_test_disabled(self, mock_set_active_connection):
        """
        Case: Call use_shard with a shard that is not active, but active_only_schemas is False
        Expected: No StateException to be raised
        """
        with use_shard(self.inactive_shard, active_only_schemas=False):
            pass

        self.assertTrue(mock_set_active_connection.called)

    @mock.patch('sharding.router.set_active_connection')
    def test_use_shard_without_public(self, mock_set_active_connection):
        """
        Case: Call use_shard within with include_public set to False.
        Expected: Connection to be switched and include_public False on the connection's options
        """
        with use_shard(self.shard, include_public=False) as env:
            self.assertFalse(env.connection.include_public_schema)

            options = ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name,
                                   shard_id=self.shard.id, use_shard=True, include_public=False)

            mock_set_active_connection.assert_called_once_with(options)

    @mock.patch('sharding.router.set_active_connection')
    def test_use_shard_inception(self, mock_set_active_connection):
        """
        Case: Call use_shard within a use_shard environment
        Expected: Connection to switch twice and set_active_connection to be called accordingly
        """
        with use_shard(self.shard) as env1:
            options1 = ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name,
                                    shard_id=self.shard.id, use_shard=True)
            mock_set_active_connection.assert_called_once_with(options1)

            with use_shard(self.other_shard) as env2:
                options2 = ShardOptions(node_name=self.other_shard.node_name, schema_name=self.other_shard.schema_name,
                                        shard_id=self.other_shard.id, use_shard=True)

                env2._old_connection = env1.options  # Override this, because we mock _set_active_connection

                self.assertEqual(mock_set_active_connection.call_count, 2)
                mock_set_active_connection.assert_has_calls([
                    mock.call(options1),
                    mock.call(options2)
                ])

            self.assertEqual(mock_set_active_connection.call_count, 3)
            mock_set_active_connection.assert_has_calls([
                mock.call(options1),
                mock.call(options2),
                mock.call(options1),
            ])

        self.assertEqual(mock_set_active_connection.call_count, 4)
        mock_set_active_connection.assert_has_calls([
            mock.call(options1),
            mock.call(options2),
            mock.call(options1),
            mock.call('default'),
        ])

    def test_use_shard_inception_integration(self):
        """
        Case: Call use_shard within a use_shard environment
        Expected: Connection to switch twice, and django.db.connection returning the correct connection
        """
        self.assertEqual(get_active_connection(), 'default')
        self.assertEqual(connection.alias, 'default')

        with use_shard(self.shard):
            options1 = ShardOptions(node_name=self.shard.node_name, schema_name=self.shard.schema_name,
                                    shard_id=self.shard.id, use_shard=True)

            self.assertEqual(get_active_connection(), options1)
            self.assertEqual(connection.alias, '{}|{}'.format(self.shard.node_name, self.shard.schema_name))

            with use_shard(self.other_shard):
                options2 = ShardOptions(node_name=self.other_shard.node_name, schema_name=self.other_shard.schema_name,
                                        shard_id=self.other_shard.id, use_shard=True)

                self.assertEqual(get_active_connection(), options2)
                self.assertEqual(connection.alias, '{}|{}'.format(self.other_shard.node_name,
                                                                  self.other_shard.schema_name))
            self.assertEqual(get_active_connection(), options1)
            self.assertEqual(connection.alias, '{}|{}'.format(self.shard.node_name, self.shard.schema_name))

        set_active_connection('default')
        self.assertEqual(connection.alias, 'default')

    @mock.patch('sharding.router.set_active_connection')
    def test_use_shard_invalid_node_name(self, mock_set_active_connection):
        """
        Case: Call use_shard with a valid shard object referring to a non-default node
        Expected: Connection not changed
        """
        with self.assertRaisesMessage(ConnectionDoesNotExist, "The connection Batman doesn't exist"):
            with use_shard(node_name='Batman', schema_name='Bat_cave'):
                pass

        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('sharding.router.set_active_connection')
    def test_use_shard_with_inactive_mapping_objects(self, mock_set_active_connection):
        """
        Case: Call use_shard with a valid shard object that contains inactive mapping objects, with setting
              check_active_mapping_values to True
        Expected: StateException to be raised
        """
        shard = Shard.objects.create(alias='halls_of_justice', schema_name='test_halls_of_justice', node_name='default',
                                     state=State.ACTIVE)
        OrganizationShards.objects.create(organization_id=5, shard=shard, state=State.ACTIVE)
        OrganizationShards.objects.create(organization_id=6, shard=shard, state=State.MAINTENANCE)

        with self.assertRaises(StateException):
            with use_shard(shard, check_active_mapping_values=True):
                pass
        self.assertFalse(mock_set_active_connection.called)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_use_shard_check_active_mapping_values_no_mapping_model(self):
        """
        Case: Set check_active_mapping_values to True on use_shard while not having a mapping model defined
        Expected: ValueError raised
        """
        shard = Shard.objects.create(alias='halls_of_justice', schema_name='test_halls_of_justice', node_name='default',
                                     state=State.ACTIVE)

        with self.assertRaisesMessage(ValueError, "You set 'check_active_mapping_values' to True while you didn't "
                                                  "define the mapping model."):
            with use_shard(shard, check_active_mapping_values=True):
                pass

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('sharding.router.set_active_connection')
    def test_use_shard_for_inactive_schemas(self, mock_set_active_connection):
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

        self.assertTrue(mock_set_active_connection.called)

    def test_use_shard_set_parameters_on_connection(self):
        """
        Case: Use use_shard
        Expected: Parameters from use_shard are correctly set on the connection for re-use later
        """
        with use_shard(self.shard) as env:
            self.assertEqual(env.connection.alias, '{}|{}'.format(self.shard.node_name, self.shard.schema_name))
            self.assertEqual(env.connection.schema_name, self.shard.schema_name)

    @mock.patch('sharding.router.set_active_connection', mock.Mock())
    @mock.patch('sharding.utils.use_shard.acquire_lock')
    def test_enable_acquire_advisory_lock(self, mock_acquire_lock):
        """
        Case: Use use_shard.enable()
        Expected: acquire_lock to be called.
        """
        use_shard(self.shard).enable()
        mock_acquire_lock.assert_called_once_with()

    @mock.patch('sharding.router.set_active_connection', mock.Mock())
    @mock.patch('sharding.utils.use_shard.release_lock')
    def test_disable_release_advisory_lock(self, mock_release_lock):
        """
        Case: Use use_shard.disable()
        Expected: release_lock to be called.
        """
        env = use_shard(self.shard)
        env._old_connection = 'default'
        env.connection = connection
        env._enabled = True

        env.disable()
        mock_release_lock.assert_called_once_with()

    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.acquire_advisory_lock')
    def test_acquire_lock(self, mock_acquire_lock):
        """
        Case: Call use_shard.acquire_lock().
        Expected: Connection's acquire_advisory_lock to be called with the correct arguments.
        """
        env = use_shard(self.shard)
        env.connection = connections[env.options]
        env.acquire_lock()
        mock_acquire_lock.assert_called_once_with('shard_{}'.format(self.shard.id), shared=True)

    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.release_advisory_lock')
    def test_release_lock(self, mock_release_advisory_lock):
        """
        Case: Call use_shard.release_lock().
        Expected: Connection's release_advisory_lock to be called with the correct arguments.
        """
        env = use_shard(self.shard)
        env.connection = connections[env.options]
        env.release_lock()
        mock_release_advisory_lock.assert_called_once_with('shard_{}'.format(self.shard.id), shared=True)

    def test_multiple_disable(self):
        """
        Case: Try to disable a use shard context manager twice
        Expected: It will only be disabled if the use shard context manager is enabled
        """
        env = use_shard(self.shard)
        env.enable()

        with mock.patch('sharding.router.set_active_connection') as mock_set_active_connection:
            env.disable()

        self.assertTrue(mock_set_active_connection.called)

        with mock.patch('sharding.router.set_active_connection') as mock_set_active_connection:
            env.disable()

        self.assertFalse(mock_set_active_connection.called)

    def test_disable_but_no_enable(self):
        """
        Case: Try to disable a use shard context manager without enabling it
        Expected: Disabling is a noop
        """
        env = use_shard(self.shard)

        with mock.patch('sharding.router.set_active_connection') as mock_set_active_connection:
            env.disable()

        self.assertFalse(mock_set_active_connection.called)


class UseShardForTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        with mock.patch('sharding.utils.create_schema_on_node'):
            self.shard1 = Shard.objects.create(alias='test_sharding', schema_name='test_schema', node_name='default',
                                               state=State.ACTIVE)

        self.org_shard1 = OrganizationShards.objects.create(organization_id=1, shard=self.shard1, state=State.ACTIVE)
        self.org_shard2 = OrganizationShards.objects.create(organization_id=2, shard=self.shard1,
                                                            state=State.MAINTENANCE)

    @mock.patch('sharding.router.set_active_connection')
    @mock.patch.object(use_shard_for, 'acquire_lock')
    @mock.patch.object(use_shard_for, 'release_lock')
    def test_use_shard_for(self, mock_release_lock, mock_acquire_lock, mock_set_active_connection):
        """
        Case: Use use_shard_for with valid arguments. There are both an active and inactive schemas on the shard.
        Expected: Successful usage of use_shard_for
        """
        with use_shard_for(1) as env:
            mock_set_active_connection.assert_called_once_with(env.options)

        self.assertEqual(mock_acquire_lock.call_count, 1)
        self.assertEqual(mock_release_lock.call_count, 1)

    @mock.patch('sharding.router.set_active_connection')
    def test_use_shard_for_inactive_object(self, mock_set_active_connection):
        """
        Case: Use use_shard_for with an inactive mapping object
        Expected: StateException raised
        """
        with self.assertRaises(StateException):
            with use_shard_for(2):
                pass
        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('sharding.router.set_active_connection')
    def test_use_shard_for_inactive_shard(self, mock_set_active_connection):
        """
        Case: Use use_shard_for with an active mapping object, but inactive shard
        Expected: StateException raised
        """
        self.shard1.state = State.MAINTENANCE
        self.shard1.save(update_fields=['state'])

        with self.assertRaises(StateException):
            with use_shard_for(1):
                pass
        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('sharding.router.set_active_connection')
    @mock.patch.object(use_shard_for, 'acquire_lock')
    @mock.patch.object(use_shard_for, 'release_lock')
    def test_use_shard_for_inactive_object_include_inactive(self, mock_release_lock, mock_acquire_lock,
                                                            mock_set_active_connection):
        """
        Case: Use use_shard_for with an inactive mapping object, but have active_only_schemas set to False.
        Expected: Successful usage of use_shard_for
        """
        with use_shard_for(2, active_only_schemas=False) as env:
            mock_set_active_connection.assert_called_once_with(env.options)

        self.assertEqual(mock_acquire_lock.call_count, 1)
        self.assertEqual(mock_release_lock.call_count, 1)

    def test_use_shard_set_parameters_on_connection(self):
        """
        Case: Use use_shard_for
        Expected: Parameters from use_shard_for are correctly set on the options
        """
        with use_shard_for(1, lock=False) as env:
            self.assertEqual(env.connection.shard_options, env.options)

            self.assertEqual(env.options.node_name, self.shard1.node_name)
            self.assertEqual(env.options.schema_name, self.shard1.schema_name)
            self.assertEqual(env.options.shard_id, self.shard1.id)
            self.assertEqual(env.options.mapping_value, 1)
            self.assertEqual(env.options.kwargs['lock'], False)

    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.acquire_advisory_lock')
    def test_acquire_lock(self, mock_acquire_lock):
        """
        Case: Call the acquire_lock method on a use_shard_for instance
        Expected: DatabaseWrapper.acquire_advisory_lock called with the correct parameters
        """
        env = use_shard_for(self.org_shard1.organization_id)
        env.connection = connections[env.options]
        env.acquire_lock()

        self.assertEqual(mock_acquire_lock.call_count, 2)

        mock_acquire_lock.assert_has_calls([
            mock.call('shard_{}'.format(self.shard1.id), shared=True),
            mock.call('mapping_{}'.format(self.org_shard1.organization_id), shared=True),
        ])

    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.release_advisory_lock')
    def test_release_lock(self, mock_release_advisory_lock):
        """
        Case: Call the release_lock method on a use_shard_for instance
        Expected: DatabaseWrapper.release_advisory_lock called with the correct parameters
        """
        env = use_shard_for(self.org_shard1.organization_id)
        env.connection = connections[env.options]
        env.release_lock()

        self.assertEqual(mock_release_advisory_lock.call_count, 2)

        mock_release_advisory_lock.assert_has_calls([
            mock.call('shard_{}'.format(self.shard1.id), shared=True),
            mock.call('mapping_{}'.format(self.org_shard1.organization_id), shared=True),
        ])


class GetShardForTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        with mock.patch('sharding.utils.create_schema_on_node'):
            self.shard1 = Shard.objects.create(alias='test_sharding', schema_name='test_schema', node_name='default',
                                               state=State.ACTIVE)

        self.org_shard1 = OrganizationShards.objects.create(organization_id=1, shard=self.shard1, slug='test_slug')

    def test(self):
        """
        Case: Use get_shard_for
        Expected: Correct shard returned
        """
        self.assertEqual(get_shard_for(1), self.shard1)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_get_shard_for_without_setting(self):
        """
        Case: Use get_shard_for while not having MAPPING_MODEL set
        Expected: ImproperlyConfigured raised
        """
        with self.assertRaises(ImproperlyConfigured):
            self.assertEqual(get_shard_for(1), self.shard1)

    def test_active_only_schemas(self):
        """
        Case: Use get_shard_for while having the mapping model in maintenance
        Expected: StateException raised
        """
        self.assertEqual(get_shard_for(1), self.shard1)

        self.org_shard1.state = State.MAINTENANCE
        self.org_shard1.save()

        with self.assertRaises(StateException):
            get_shard_for(1)

    def test_active_only_schemas_false(self):
        """
        Case: Use get_shard_for with active_only set to False, while having the mapping model in maintenance
        Expected: No StateException raised, correct shard returned
        """
        self.org_shard1.state = State.MAINTENANCE
        self.org_shard1.save()

        self.assertEqual(get_shard_for(1, active_only=False), self.shard1)

    def test_field(self):
        """
        Case: Use get_shard_for with a different field provided
        Expected: Shard selected on field 'slug'
        """
        self.assertEqual(get_shard_for('test_slug', field='slug'), self.shard1)


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


class DeleteSchemaTestCase(ShardingTestCase):
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.delete_schema')
    def test(self, mock_delete_schema):
        """
        Case: Call sharding.utils.delete_schema
        Expected: DatabaseWrapper.delete_schema called with the same schema name
        """
        delete_schema(schema_name='test_schema', node_name='default')
        mock_delete_schema.assert_called_once_with('test_schema', is_template=False)

    def test_node_not_exists(self):
        """
        Case: Call sharding.utils.delete_schema with a node name that does not exist
        Expected: ValueError raised
        """
        with self.assertRaises(ValueError):
            delete_schema(schema_name='test_schema', node_name='not_existing_node')

    def test_delete_template_is_template_false(self):
        """
        Case: Delete the template schema without having is_template=True set
        Expected: ValueError raised and schema not deleted
        """
        create_template_schema('default')

        template_name = get_template_name()
        message = "Schema name 'template' cannot be the same as the template name 'template'"
        with self.assertRaisesMessage(ValueError, message):
            delete_schema(schema_name=template_name, node_name='default')

        self.assertTrue(schema_exists('default', template_name))

    def test_delete_template_is_template_true(self):
        """
        Case: Delete the template schema with having is_template=True set
        Expected: Schema deleted
        """
        create_template_schema('default')

        template_name = get_template_name()
        delete_schema(schema_name=template_name, node_name='default', is_template=True)

        self.assertFalse(schema_exists('default', template_name))


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


class CreateTemplateSchemaTestCase(ShardingTestCase):
    def test_create_template_schema(self):
        """
        Case: call utils.migrate_schema() to migrate the sharded models to the template schema
        Expected: The newly made schema to have to correct table headers.
        """
        create_template_schema('default')  # This also calls the migration

        cursor = connection.cursor()
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'template';")
        template_tables = [table[1] for table in cursor.fetchall()]
        # Filter test models
        template_tables = [table for table in template_tables if not re.search(r'_[t|T]est', table)]
        self.assertCountEqual(sorted(template_tables), ['django_migrations', 'example_organization',
                                                        'example_suborganization', 'example_user',
                                                        'example_statement', 'example_cake', 'example_user_cake',
                                                        'example_statement_type'])

    def test_create_template_schema_invalid_node(self):
        """
        Case: call utils.migrate_schema() with an nonexisting connection
        Expected: ValueError raised
        """
        with self.assertRaises(ValueError):
            migrate_schema('no_connection', 'test_schema')  # This also calls the migration

    def test_create_template_schema_invalid_schema_name(self):
        """
        Case: call utils.migrate_schema() with an nonexisting schema_name
        Expected: ValueError raised
        """
        with self.assertRaises(ValueError):
            migrate_schema('default', 'test_schema')  # This also calls the migration

    @mock.patch('sharding.utils.migrate_schema')
    def test_migrate(self, mock_migrate_schema):
        """
        Case: Call create_template_schema with both migrate set to True and False
        Expected: migrate_schema is not called when migrate=False and it is called when migrate=True
        """
        template_name = get_template_name()

        self.assertFalse(schema_exists('default', template_name))
        create_template_schema('default', migrate=False)
        self.assertFalse(mock_migrate_schema.called)

        self.assertFalse(schema_exists('other', template_name))
        create_template_schema('other', migrate=True)
        self.assertTrue(mock_migrate_schema.called)


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

    def test_get_sharding_mode_for_auto_created_model(self):
        """
        Case: Call get_sharding_mode for a model that is created automatically for a many-to-many field.
        Expected: Mode returned as defined by the model which is responsible for the many-to-many relation.
        """
        self.assertEqual(get_sharding_mode('example', 'user_cake'), ShardingMode.SHARDED)


class GetAllShardedModels(ShardingTestCase):
    available_apps = ['example']

    def test_with_override(self):
        """
        Case: Call get_all_sharded_models.
        Expected: Only the User and the Statement models to be returned. The rest is a proxy, mirrored or not sharded.
        Note: System test
        """
        with override_settings(
                SHARDING={'OVERRIDE_SHARDING_MODE':
                          {('example', 'organization'): ShardingMode.MIRRORED, }}):
            self.assertCountEqual(get_all_sharded_models(), [User, Statement, Suborganization, Cake])

    @mock.patch('sharding.utils.get_model_sharding_mode')
    def test(self, mock_get_model_sharding_mode):
        """
        Case: Call get_all_sharded_models.
        Expected: get_model_sharding_mode called for each model that's not a proxy model.
        """
        get_all_sharded_models()
        mock_get_model_sharding_mode.assert_has_calls([
            mock.call(model) for model in apps.get_models(include_auto_created=False) if not model._meta.proxy
        ], any_order=True)

    @mock.patch('sharding.utils.get_model_sharding_mode')
    def test_include_auto_created(self, mock_get_model_sharding_mode):
        """
        Case: Call get_all_sharded_models, with include_auto_created.
        Expected: get_model_sharding_mode called for each model that's not a proxy model.
        """
        get_all_sharded_models(include_auto_created=True)
        mock_get_model_sharding_mode.assert_has_calls([
            mock.call(model) for model in apps.get_models(include_auto_created=True) if not model._meta.proxy
        ], any_order=True)

    def test_include_auto_created_result(self):
        """
        Case: Call get_all_sharded_models, with include_auto_created.
        Expected: All sharded models and auto created fields to be returned.
        """
        # We compare it string based, since we cannot import the auto created fields as classes.
        result = [str(model) for model in get_all_sharded_models(include_auto_created=True)]
        self.assertCountEqual(result,
                              ["<class 'example.models.Organization'>",
                               "<class 'example.models.Suborganization'>",
                               "<class 'example.models.Cake'>",
                               "<class 'example.models.User_cake'>",
                               "<class 'example.models.User'>",
                               "<class 'example.models.Statement_type'>",
                               "<class 'example.models.Statement'>"])


class GetAllMirroredModels(ShardingTestCase):
    available_apps = ['example']

    def test_with_override(self):
        """
        Case: Call get_all_mirrored_models.
        Expected: Only SuperType models to be returned. The rest is sharded and/or not mirrored.
        Note: System test
        """
        with override_settings(
                SHARDING={'OVERRIDE_SHARDING_MODE': {('example', 'type'): ShardingMode.SHARDED,
                                                     ('example', 'mirroreduser'): ShardingMode.SHARDED}}):
            self.assertCountEqual(get_all_mirrored_models(), [SuperType])

    @mock.patch('sharding.utils.get_model_sharding_mode')
    def test(self, mock_get_model_sharding_mode):
        """
        Case: Call get_all_mirrored_models.
        Expected: get_model_sharding_mode called for each model that's not a proxy model.
        """
        get_all_mirrored_models()
        mock_get_model_sharding_mode.assert_has_calls([
            mock.call(model) for model in apps.get_models() if not model._meta.proxy
        ], any_order=True)


class GetAllDatabases(SimpleTestCase):
    @override_settings(DATABASES={'default': 'some connection', 'space': 'some otherworldly connection'})
    def test(self):
        """
        Case: Call get_all_databases.
        Expected: Names of the databases defined in the settings.
        """
        self.assertCountEqual(get_all_databases(), ['default', 'space'])


class ForEachShardTestCase(ShardingTestCase):
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
            Create a new transaction, independent on those made in `transaction_for_every_node`
            Try to claim an exclusive lock on the table locked by `transaction_for_every_node`.
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

    def setUp(self):
        self.addCleanup(self.clean_up)

    @mock.patch('django.core.management.get_commands', mock.Mock(return_value={'migrate_shards': 'sharding'}))
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
        with use_shard(shard=other_shard, include_public=False) as env:
            # Doesn't exist on the shard, but will soon.
            with self.assertRaises(ProgrammingError):
                type.refresh_from_db(using=env.options)
                # type = Type.objects.get(id=type.id)

        move_model_to_schema(model=Type, node_name='default', to_schema_name='test_other_schema')

        # Now that we moved the Type table, we should not be able to refresh it.
        with self.assertRaises(ProgrammingError):
            type.refresh_from_db(using='default')

        # supertype is not moved, so should remain accessible from the public schema.
        supertype.refresh_from_db(using='default')

        with use_shard(shard=other_shard, include_public=False, override_class_method_use_shard=True) as env:
            # Now that we moved the Type table, we should be able to access it from the shard.
            type.refresh_from_db(using=env.options)

            # supertype is not moved, so should remain inaccessible
            with self.assertRaises(ProgrammingError):
                supertype.refresh_from_db(using=env.options)

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

    @mock.patch('django.core.management.get_commands', mock.Mock(return_value={'migrate_shards': 'sharding'}))
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


class SchemaExistsTestCase(ShardingTestCase):
    def test_schema_exists(self):
        """
        Case: Call schema_exists while knowing that schema exists
        Expected: Returns True
        """
        create_template_schema('default')
        self.assertTrue(schema_exists('default', get_template_name()))

    def test_schema_does_not_exists(self):
        """
        Case: Call schema_exists while knowing that schema does not exists
        Expected: Returns False
        """
        self.assertFalse(schema_exists('default', get_template_name()))

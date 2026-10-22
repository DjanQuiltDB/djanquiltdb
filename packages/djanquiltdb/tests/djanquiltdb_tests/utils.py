import inspect
import re
import threading
from unittest import mock

from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import ProgrammingError, connections, models, transaction
from django.db.models.signals import (
    post_delete,
    post_init,
    post_migrate,
    post_save,
    pre_delete,
    pre_init,
    pre_migrate,
    pre_save,
)
from django.db.utils import ConnectionDoesNotExist, IntegrityError, OperationalError
from django.test import SimpleTestCase, override_settings
from django.test.utils import CaptureQueriesContext, isolate_apps
from django.utils.module_loading import import_string

from djanquiltdb import PRIMARY_NODE, single_node_unsharded
from djanquiltdb.db import connection
from djanquiltdb.decorators import (
    atomic_write_to_every_node,
    override_sharding_setting,
    sharded_model,
    single_node_unsharded_model,
)
from djanquiltdb.options import ShardOptions
from djanquiltdb.postgresql_backend.base import DatabaseWrapper, ShardDatabaseWrapper
from djanquiltdb.postgresql_backend.utils import LockCursorWrapperMixin
from djanquiltdb.router import get_active_connection, set_active_connection
from djanquiltdb.utils import (
    OverrideShardingModeWithLowercaseKeys,
    ShardingMode,
    State,
    StateException,
    _node_exists,
    create_schema_on_node,
    create_template_schema,
    delete_schema,
    disable_signals,
    for_each_node,
    for_each_shard,
    get_all_databases,
    get_all_mirrored_models,
    get_all_public_models,
    get_all_public_schema_models,
    get_all_sharded_models,
    get_connection_alias,
    get_declared_placement,
    get_declared_sharding_mode,
    get_mapping_class,
    get_model_definition,
    get_model_sharding_mode,
    get_model_single_node_alias,
    get_placement,
    get_shard_class,
    get_shard_for,
    get_sharding_mode,
    get_template_name,
    migrate_schema,
    move_model_to_schema,
    relation_with_a_model_on_a_single_node_is_allowed,
    resolve_node_alias,
    schema_exists,
    shard_lock_key,
    stored_mapping_value,
    transaction_for_every_node,
    transaction_for_nodes,
    use_shard,
    use_shard_for,
)
from djanquiltdb_tests import (
    OverrideMirroredRoutingMixin,
    ResetConnectionTestCaseMixin,
    ShardingTestCase,
    ShardingTransactionTestCase,
)
from djanquiltdb_tests.app_config import DummyShardedShard
from example.models import (
    Cake,
    CakeType,
    CoatingType,
    DetailedReport,
    Flavour,
    MirroredUser,
    Note,
    Organization,
    OrganizationShard,
    Pudding,
    QuiltSession,
    Report,
    Shard,
    Sprinkle,
    Statement,
    Suborganization,
    SuperType,
    Type,
    Unrelated,
    User,
)


class GetShardClass(SimpleTestCase):
    """
    We don't need to test for the situation where the class is not set in the settings.
    For we will give an error on run in that case.
    """

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    def test_get_shard_class_set(self):
        """
        Case: Call get_shard_class while it is set in the settings.
        Expected: The model class.
        """
        self.assertEqual(get_shard_class(), Shard)

    def test_the_shard_class_is_imported_once_for_two_calls(self):
        """
        Case: Override QUILT_DB with its own value, which empties the cache, and call get_shard_class() twice.
        Expected: The shard class is imported once.
        """
        with override_settings(QUILT_DB=settings.QUILT_DB):
            with mock.patch('djanquiltdb.utils.import_string', wraps=import_string) as wrapped:
                get_shard_class()
                get_shard_class()

        self.assertEqual(wrapped.call_count, 1)

    def test_a_change_to_the_quilt_db_setting_resets_the_cached_shard_class(self):
        """
        Case: Call get_shard_class(), call it again while SHARD_CLASS is set to DummyShardedShard, and call it again
              after that override ends.
        Expected: Shard, then DummyShardedShard, then Shard again.
        """
        self.assertEqual(get_shard_class(), Shard)

        with override_sharding_setting('SHARD_CLASS', 'djanquiltdb_tests.app_config.DummyShardedShard'):
            self.assertEqual(get_shard_class(), DummyShardedShard)

        self.assertEqual(get_shard_class(), Shard)


class GetTemplateName(SimpleTestCase):
    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    def test_get_template_unset(self):
        """
        Case: Call get_template_name when it is not set in the settings.
        Expected: The default template name: 'template'.
        """
        self.assertEqual(get_template_name(), 'template')

    @override_settings(QUILT_DB={'TEMPLATE_NAME': 'new-template', 'SHARD_CLASS': 'example.models.Shard'})
    def test_get_template_set(self):
        """
        Case: Call get_template_name while it is set in the settings.
        Expected: Set name: 'new-template'.
        """
        self.assertEqual(get_template_name(), 'new-template')


class GetMappingClass(SimpleTestCase):
    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    def test_get_mapping_class_unset(self):
        """
        Case: Call get_mapping_class when it is not set in the settings.
        Expected: The default template name: 'template'.
        """
        self.assertIsNone(get_mapping_class())

    @override_settings(
        QUILT_DB={'MAPPING_MODEL': 'example.models.OrganizationShard', 'SHARD_CLASS': 'example.models.Shard'}
    )
    def test_get_mapping_class_set(self):
        """
        Case: Call get_mapping_class while it is set in the settings.
        Expected: The mapping class.
        """
        self.assertEqual(get_mapping_class(), OrganizationShard)

    def test_the_mapping_model_is_imported_once_for_two_calls(self):
        """
        Case: Override QUILT_DB with its own value, which empties the cache, and call get_mapping_class() twice.
        Expected: The mapping model is imported once.
        """
        with override_settings(QUILT_DB=settings.QUILT_DB):
            with mock.patch('djanquiltdb.utils.import_string', wraps=import_string) as wrapped:
                get_mapping_class()
                get_mapping_class()

        self.assertEqual(wrapped.call_count, 1)

    def test_a_change_to_the_quilt_db_setting_resets_the_cached_mapping_model(self):
        """
        Case: get_mapping_class() is called, then called again while the project has no MAPPING_MODEL, and again after
              that override ends.
        Expected: OrganizationShard, then None, then OrganizationShard again.
        """
        self.assertEqual(get_mapping_class(), OrganizationShard)

        with override_sharding_setting('MAPPING_MODEL'):
            self.assertIsNone(get_mapping_class())

        self.assertEqual(get_mapping_class(), OrganizationShard)


class ShardLockKeyTestCase(SimpleTestCase):
    def test_the_name_has_the_shard_id(self):
        """
        Case: Call shard_lock_key() with the shard ids 3 and 0.
        Expected: 'shard_3' and 'shard_0', the names of the advisory locks that use_shard and the move commands take.
        """
        self.assertEqual(shard_lock_key(3), 'shard_3')
        self.assertEqual(shard_lock_key(0), 'shard_0')


class StoredMappingValueTestCase(SimpleTestCase):
    def test_a_value_that_is_not_a_model_instance(self):
        """
        Case: Call stored_mapping_value() with the mapping values 7, 0 and 'north'.
        Expected: Each value as it is.
        """
        for mapping_value in (7, 0, 'north'):
            with self.subTest(mapping_value=mapping_value):
                self.assertEqual(stored_mapping_value(mapping_value), mapping_value)

    def test_a_model_instance_for_a_mapping_field_that_is_not_a_foreign_key(self):
        """
        Case: Call stored_mapping_value() with an Organization whose primary key is 42, while the mapping field
              organization_id of OrganizationShard is not a foreign key.
        Expected: 42, the primary key of the instance.
        """
        self.assertEqual(stored_mapping_value(Organization(pk=42)), 42)

    @isolate_apps('djanquiltdb')
    def test_a_model_instance_for_a_foreign_key_to_another_field(self):
        """
        Case: Call stored_mapping_value() with a MirroredUser, while the mapping field of the mapping model is a foreign
              key to the email address of MirroredUser.
        Expected: The email address of the user, which the mapping field stores, and not its primary key.
        """

        class MirroredUserShard(models.Model):
            shard = models.ForeignKey(Shard, on_delete=models.CASCADE, related_name='+')
            user = models.ForeignKey(MirroredUser, to_field='email', on_delete=models.CASCADE, related_name='+')

            mapping_field = 'user'

            class Meta:
                app_label = 'djanquiltdb'

        with mock.patch('djanquiltdb.utils.get_mapping_class', return_value=MirroredUserShard):
            stored_value = stored_mapping_value(MirroredUser(pk=42, email='luke@layton.l5'))

        self.assertEqual(stored_value, 'luke@layton.l5')


class SetActiveConnectionTestCase(ResetConnectionTestCaseMixin, SimpleTestCase):
    def test(self):
        """
        Case: Set the active connection to something else.
        Expected: Active connection should change.
        """
        self.assertEqual(get_active_connection(), 'default')
        set_active_connection('other')
        self.assertEqual(get_active_connection(), 'other')


class UseShardTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema('default')
        create_template_schema('other')
        self.shard = Shard.objects.create(
            alias='test_shard', schema_name='test_schema', node_name='default', state=State.ACTIVE
        )
        self.other_shard = Shard.objects.create(
            alias='other_shard', schema_name='test_other_schema', node_name='other', state=State.ACTIVE
        )
        self.inactive_shard = Shard.objects.create(
            alias='inactive_shard', schema_name='test_inactive_schema', node_name='other', state=State.MAINTENANCE
        )

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_use_shard(self, mock_set_active_connection):
        """
        Case: Call use_shard with a valid shard object.
        Expected: set_active_connection to be called with the correct options.
        """
        with use_shard(self.shard):
            pass

        options = ShardOptions(
            node_name=self.shard.node_name, schema_name=self.shard.schema_name, shard_id=self.shard.id, use_shard=True
        )

        self.assertEqual(mock_set_active_connection.call_count, 2)

        mock_set_active_connection.assert_has_calls(
            [
                # First the shard we want to enter
                mock.call(options),
                # And then to enter the old connection again on exiting the context manager
                mock.call('default'),
            ]
        )

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_use_shard_with_invalid_argument(self, mock_set_active_connection):
        """
        Case: Call use_shard with an invalid argument.
        Expected: A ValueError to be raised.
        """
        with self.assertRaises(ValueError):
            with use_shard('not a Shard object'):
                pass

        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_use_shard_with_inactive_shard(self, mock_set_active_connection):
        """
        Case: Call use_shard with a shard that is not active.
        Expected: A StateException to be raised.
        """
        with self.assertRaises(StateException) as error:
            with use_shard(self.inactive_shard):
                pass
        self.assertEqual(error.exception.state, State.MAINTENANCE)

        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_use_shard_with_inactive_shard_with_state_test_disabled(self, mock_set_active_connection):
        """
        Case: Call use_shard with a shard that is not active, but active_only_schemas is False.
        Expected: No StateException to be raised.
        """
        with use_shard(self.inactive_shard, active_only_schemas=False):
            pass

        self.assertTrue(mock_set_active_connection.called)

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_use_shard_without_public(self, mock_set_active_connection):
        """
        Case: Call use_shard within with include_public set to False.
        Expected: Connection to be switched and include_public False on the connection's options.
        """
        with use_shard(self.shard, include_public=False) as env:
            self.assertFalse(env.connection.include_public_schema)

            options = ShardOptions(
                node_name=self.shard.node_name,
                schema_name=self.shard.schema_name,
                shard_id=self.shard.id,
                use_shard=True,
                include_public=False,
            )

            mock_set_active_connection.assert_called_once_with(options)

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_use_shard_inception(self, mock_set_active_connection):
        """
        Case: Call use_shard within a use_shard environment.
        Expected: Connection to switch twice and set_active_connection to be called accordingly.
        """
        with use_shard(self.shard) as env1:
            options1 = ShardOptions(
                node_name=self.shard.node_name,
                schema_name=self.shard.schema_name,
                shard_id=self.shard.id,
                use_shard=True,
            )
            mock_set_active_connection.assert_called_once_with(options1)

            with use_shard(self.other_shard) as env2:
                options2 = ShardOptions(
                    node_name=self.other_shard.node_name,
                    schema_name=self.other_shard.schema_name,
                    shard_id=self.other_shard.id,
                    use_shard=True,
                )

                env2._old_connection = env1.options  # Override this, because we mock _set_active_connection

                self.assertEqual(mock_set_active_connection.call_count, 2)
                mock_set_active_connection.assert_has_calls([mock.call(options1), mock.call(options2)])

            self.assertEqual(mock_set_active_connection.call_count, 3)
            mock_set_active_connection.assert_has_calls(
                [
                    mock.call(options1),
                    mock.call(options2),
                    mock.call(options1),
                ]
            )

        self.assertEqual(mock_set_active_connection.call_count, 4)
        mock_set_active_connection.assert_has_calls(
            [
                mock.call(options1),
                mock.call(options2),
                mock.call(options1),
                mock.call('default'),
            ]
        )

    def test_use_shard_inception_integration(self):
        """
        Case: Call use_shard within a use_shard environment.
        Expected: Connection to switch twice, and django.db.connection returning the correct connection.
        """
        self.assertEqual(get_active_connection(), 'default')
        self.assertEqual(connection.alias, 'default')

        with use_shard(self.shard):
            options1 = ShardOptions(
                node_name=self.shard.node_name,
                schema_name=self.shard.schema_name,
                shard_id=self.shard.id,
                use_shard=True,
            )

            self.assertEqual(get_active_connection(), options1)
            self.assertEqual(connection.alias, '{}|{}'.format(self.shard.node_name, self.shard.schema_name))

            with use_shard(self.other_shard):
                options2 = ShardOptions(
                    node_name=self.other_shard.node_name,
                    schema_name=self.other_shard.schema_name,
                    shard_id=self.other_shard.id,
                    use_shard=True,
                )

                self.assertEqual(get_active_connection(), options2)
                self.assertEqual(
                    connection.alias, '{}|{}'.format(self.other_shard.node_name, self.other_shard.schema_name)
                )
            self.assertEqual(get_active_connection(), options1)
            self.assertEqual(connection.alias, '{}|{}'.format(self.shard.node_name, self.shard.schema_name))

        set_active_connection('default')
        self.assertEqual(connection.alias, 'default')

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_use_shard_invalid_node_name(self, mock_set_active_connection):
        """
        Case: Call use_shard with a valid shard object referring to a non-default node.
        Expected: Connection not changed.
        """
        with self.assertRaisesMessage(ConnectionDoesNotExist, "The connection 'Batman' doesn't exist"):
            with use_shard(node_name='Batman', schema_name='Bat_cave'):
                pass

        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_use_shard_with_inactive_mapping_objects(self, mock_set_active_connection):
        """
        Case: Call use_shard with a valid shard object that contains inactive mapping objects, with setting
              check_active_mapping_values to True.
        Expected: StateException to be raised.
        """
        shard = Shard.objects.create(
            alias='halls_of_justice', schema_name='test_halls_of_justice', node_name='default', state=State.ACTIVE
        )
        OrganizationShard.objects.create(organization_id=5, shard=shard, state=State.ACTIVE)
        OrganizationShard.objects.create(organization_id=6, shard=shard, state=State.MAINTENANCE)

        with self.assertRaises(StateException):
            with use_shard(shard, check_active_mapping_values=True):
                pass
        self.assertFalse(mock_set_active_connection.called)

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    def test_use_shard_check_active_mapping_values_no_mapping_model(self):
        """
        Case: Set check_active_mapping_values to True on use_shard while not having a mapping model defined.
        Expected: ValueError raised.
        """
        shard = Shard.objects.create(
            alias='halls_of_justice', schema_name='test_halls_of_justice', node_name='default', state=State.ACTIVE
        )

        with self.assertRaisesMessage(
            ValueError, "You set 'check_active_mapping_values' to True while you didn't define the mapping model."
        ):
            with use_shard(shard, check_active_mapping_values=True):
                pass

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_use_shard_for_inactive_schemas(self, mock_set_active_connection):
        """
        Case: Call use_shard with a valid shard object while no mapping_model exists.
        Expected: No StateException to be raised.
        """
        shard = Shard.objects.create(
            alias='mudville', schema_name='test_schema_muddied', node_name='default', state=State.ACTIVE
        )
        OrganizationShard.objects.create(organization_id=5, shard=shard, state=State.ACTIVE)
        OrganizationShard.objects.create(organization_id=6, shard=shard, state=State.MAINTENANCE)
        with use_shard(shard):
            pass

        self.assertTrue(mock_set_active_connection.called)

    def test_use_shard_set_parameters_on_connection(self):
        """
        Case: Use use_shard.
        Expected: Parameters from use_shard are correctly set on the connection for re-use later.
        """
        with use_shard(self.shard) as env:
            self.assertEqual(env.connection.alias, '{}|{}'.format(self.shard.node_name, self.shard.schema_name))
            self.assertEqual(env.connection.schema_name, self.shard.schema_name)

    @mock.patch('djanquiltdb.router.set_active_connection', mock.Mock())
    @mock.patch('djanquiltdb.utils.use_shard.acquire_lock')
    def test_enable_acquire_advisory_lock(self, mock_acquire_lock):
        """
        Case: Use use_shard.enable().
        Expected: acquire_lock to be called.
        """
        use_shard(self.shard).enable()
        mock_acquire_lock.assert_called_once_with()

    def test_enter_a_schema_without_a_shard_id(self):
        """
        Case: Enter and leave use_shard for a node and a schema, without a shard id or a mapping value, and without
              reading the connection of the use_shard.
        Expected: No shard connection is built and no statements run, because the use_shard has no advisory locks to
                  take.
        """
        with CaptureQueriesContext(connections['default']) as captured:
            with mock.patch('djanquiltdb.apps.ShardDatabaseWrapper', wraps=ShardDatabaseWrapper) as mock_wrapper:
                with use_shard(node_name='default', schema_name=self.shard.schema_name):
                    pass

        self.assertEqual(mock_wrapper.call_count, 0)
        self.assertEqual(captured.captured_queries, [])

    def test_enter_a_shard_inside_a_shard_without_the_public_schema(self):
        """
        Case: Enter use_shard for a shard while a use_shard for another shard on the same node is entered with
              include_public set to False.
        Expected: The shard is entered, because use_shard reads the shard registry again in the public schema of the
                  active node, which the search_path of the outer shard does not have.
        """
        second_shard = Shard.objects.create(
            alias='second_shard', schema_name='test_second_schema', node_name='default', state=State.ACTIVE
        )

        with use_shard(self.shard, include_public=False):
            with use_shard(second_shard) as env:
                self.assertEqual(get_active_connection(), env.options)

    @mock.patch('djanquiltdb.router.set_active_connection')
    @mock.patch('djanquiltdb.utils.use_shard.release_lock')
    def test_enable_shard_put_in_maintenance_while_waiting_for_the_lock(
        self, mock_release_lock, mock_set_active_connection
    ):
        """
        Case: Use use_shard.enable() on an active shard that is put in maintenance while use_shard waits for its lock,
              as move_shard_to_node does while it holds the exclusive lock.
        Expected: A StateException, after which the lock is released and the active connection is not changed.
        """
        env = use_shard(self.shard)

        def put_the_shard_in_maintenance():
            Shard.objects.filter(pk=self.shard.pk).update(state=State.MAINTENANCE)

        with mock.patch.object(env, 'acquire_lock', side_effect=put_the_shard_in_maintenance):
            with self.assertRaises(StateException) as error:
                env.enable()

        self.assertEqual(error.exception.state, State.MAINTENANCE)
        mock_release_lock.assert_called_once_with()
        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('djanquiltdb.router.set_active_connection')
    @mock.patch('djanquiltdb.utils.use_shard.release_lock')
    def test_enable_shard_moved_while_waiting_for_the_lock(self, mock_release_lock, mock_set_active_connection):
        """
        Case: Use use_shard.enable() on an active shard that is moved to another node while use_shard waits for its
              lock, and is active again once use_shard holds it.
        Expected: A StateException, after which the lock is released and the active connection is not changed, because
                  the options point to the node that the shard is on before the move.
        """
        env = use_shard(self.shard)

        def move_the_shard():
            Shard.objects.filter(pk=self.shard.pk).update(node_name='other')

        with mock.patch.object(env, 'acquire_lock', side_effect=move_the_shard):
            with self.assertRaisesMessage(StateException, 'on other|test_schema, which differs from the shard'):
                env.enable()

        mock_release_lock.assert_called_once_with()
        self.assertFalse(mock_set_active_connection.called)

    @override_sharding_setting('MAPPING_MODEL')
    def test_enable_with_a_mapping_value_without_a_mapping_model(self):
        """
        Case: Use use_shard.enable() for a shard and a mapping value in a project without QUILT_DB['MAPPING_MODEL'].
        Expected: ImproperlyConfigured, which get_shard_for raises when use_shard reads the mapping value again, and
                  not an error about the missing mapping model.
        """
        with self.assertRaisesMessage(ImproperlyConfigured, 'MAPPING_MODEL'):
            use_shard(self.shard, mapping_value=7).enable()

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_enable_shard_removed_from_the_registry_while_waiting_for_the_lock(self, mock_set_active_connection):
        """
        Case: Use use_shard.enable() on an active shard whose row is removed from the shard registry while use_shard
              waits for its lock, as flush does.
        Expected: use_shard enters the shard as passed, because the registry has no row to compare the shard with.
        """
        env = use_shard(self.shard)

        def remove_the_shard_from_the_registry():
            Shard.objects.filter(pk=self.shard.pk).delete()

        with mock.patch.object(env, 'acquire_lock', side_effect=remove_the_shard_from_the_registry):
            env.enable()

        mock_set_active_connection.assert_called_once_with(env.options)

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_enable_shard_put_in_maintenance_while_waiting_for_the_lock_active_only_schemas_false(
        self, mock_set_active_connection
    ):
        """
        Case: Use use_shard.enable() with active_only_schemas set to False on a shard that is put in maintenance while
              use_shard waits for its lock.
        Expected: The shard is entered, because use_shard does not check the state of the shard when
                  active_only_schemas is False.
        """
        env = use_shard(self.shard, active_only_schemas=False)

        def put_the_shard_in_maintenance():
            Shard.objects.filter(pk=self.shard.pk).update(state=State.MAINTENANCE)

        with mock.patch.object(env, 'acquire_lock', side_effect=put_the_shard_in_maintenance):
            env.enable()

        mock_set_active_connection.assert_called_once_with(env.options)

    @mock.patch('djanquiltdb.router.set_active_connection')
    @mock.patch('djanquiltdb.utils.use_shard.release_lock')
    def test_enable_shard_moved_while_waiting_for_the_lock_active_only_schemas_false(
        self, mock_release_lock, mock_set_active_connection
    ):
        """
        Case: Use use_shard.enable() with active_only_schemas set to False on a shard that is moved to another node
              while use_shard waits for its lock.
        Expected: A StateException, after which the lock is released and the active connection is not changed, because
                  the options point to the node that the shard is on before the move.
        """
        env = use_shard(self.shard, active_only_schemas=False)

        def move_the_shard():
            Shard.objects.filter(pk=self.shard.pk).update(node_name='other')

        with mock.patch.object(env, 'acquire_lock', side_effect=move_the_shard):
            with self.assertRaisesMessage(StateException, 'on other|test_schema, which differs from the shard'):
                env.enable()

        mock_release_lock.assert_called_once_with()
        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('djanquiltdb.router.set_active_connection', mock.Mock())
    @mock.patch('djanquiltdb.utils.use_shard.release_lock')
    def test_disable_release_advisory_lock(self, mock_release_lock):
        """
        Case: Use use_shard.disable() on a use_shard that is entered and holds the locks of its shard itself.
        Expected: release_lock to be called.
        """
        env = use_shard(self.shard)
        env._old_connection = 'default'
        env.connection = connection
        env._enabled = True
        env._holds_its_own_locks = True

        env.disable()
        mock_release_lock.assert_called_once_with()

    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.acquire_advisory_lock_for_each_key')
    def test_acquire_lock(self, mock_acquire_lock):
        """
        Case: Call use_shard.acquire_lock().
        Expected: Connection's acquire_advisory_lock_for_each_key to be called with the correct arguments.
        """
        env = use_shard(self.shard)
        env.connection = connections[env.options]
        env.acquire_lock()
        mock_acquire_lock.assert_called_once_with(['shard_{}'.format(self.shard.id)], shared=True, xact=True)

    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.release_advisory_lock_for_each_key')
    def test_release_lock(self, mock_release_lock):
        """
        Case: Call use_shard.release_lock().
        Expected: Connection's release_advisory_lock_for_each_key to be called with the correct arguments.
        """
        env = use_shard(self.shard)
        env.connection = connections[env.options]
        env.release_lock()
        mock_release_lock.assert_called_once_with(['shard_{}'.format(self.shard.id)], shared=True)

    def test_multiple_disable(self):
        """
        Case: Try to disable a use shard context manager twice.
        Expected: It will only be disabled if the use shard context manager is enabled.
        """
        env = use_shard(self.shard)
        env.enable()

        with mock.patch('djanquiltdb.router.set_active_connection') as mock_set_active_connection:
            env.disable()

        self.assertTrue(mock_set_active_connection.called)

        with mock.patch('djanquiltdb.router.set_active_connection') as mock_set_active_connection:
            env.disable()

        self.assertFalse(mock_set_active_connection.called)

    def test_disable_but_no_enable(self):
        """
        Case: Try to disable a use shard context manager without enabling it.
        Expected: Disabling is a noop.
        """
        env = use_shard(self.shard)

        with mock.patch('djanquiltdb.router.set_active_connection') as mock_set_active_connection:
            env.disable()

        self.assertFalse(mock_set_active_connection.called)


class UseShardForTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        with mock.patch('djanquiltdb.utils.create_schema_on_node'):
            self.shard1 = Shard.objects.create(
                alias='test_sharding', schema_name='test_schema', node_name='default', state=State.ACTIVE
            )

        self.org_shard1 = OrganizationShard.objects.create(organization_id=1, shard=self.shard1, state=State.ACTIVE)
        self.org_shard2 = OrganizationShard.objects.create(
            organization_id=2, shard=self.shard1, state=State.MAINTENANCE
        )

    @mock.patch('djanquiltdb.router.set_active_connection')
    @mock.patch.object(use_shard_for, 'acquire_lock')
    @mock.patch.object(use_shard_for, 'release_lock')
    def test_use_shard_for(self, mock_release_lock, mock_acquire_lock, mock_set_active_connection):
        """
        Case: Use use_shard_for with valid arguments. There are both an active and inactive schemas on the shard.
        Expected: Successful usage of use_shard_for.
        """
        with use_shard_for(1) as env:
            mock_set_active_connection.assert_called_once_with(env.options)

        self.assertEqual(mock_acquire_lock.call_count, 1)
        self.assertEqual(mock_release_lock.call_count, 1)

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_use_shard_for_inactive_object(self, mock_set_active_connection):
        """
        Case: Use use_shard_for with an inactive mapping object.
        Expected: StateException raised.
        """
        with self.assertRaises(StateException):
            with use_shard_for(2):
                pass
        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_use_shard_for_inactive_shard(self, mock_set_active_connection):
        """
        Case: Use use_shard_for with an active mapping object, but inactive shard.
        Expected: StateException raised.
        """
        self.shard1.state = State.MAINTENANCE
        self.shard1.save(update_fields=['state'])

        with self.assertRaises(StateException):
            with use_shard_for(1):
                pass
        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('djanquiltdb.router.set_active_connection')
    @mock.patch.object(use_shard_for, 'acquire_lock')
    @mock.patch.object(use_shard_for, 'release_lock')
    def test_use_shard_for_inactive_object_include_inactive(
        self, mock_release_lock, mock_acquire_lock, mock_set_active_connection
    ):
        """
        Case: Use use_shard_for with an inactive mapping object, but have active_only_schemas set to False.
        Expected: Successful usage of use_shard_for.
        """
        with use_shard_for(2, active_only_schemas=False) as env:
            mock_set_active_connection.assert_called_once_with(env.options)

        self.assertEqual(mock_acquire_lock.call_count, 1)
        self.assertEqual(mock_release_lock.call_count, 1)

    @mock.patch('djanquiltdb.router.set_active_connection')
    @mock.patch.object(use_shard_for, 'release_lock')
    def test_use_shard_for_mapping_put_in_maintenance_while_waiting_for_the_lock(
        self, mock_release_lock, mock_set_active_connection
    ):
        """
        Case: Use use_shard_for with an active mapping object that is put in maintenance while use_shard_for waits for
              its lock, as move_data_to_shard does while it holds the exclusive lock.
        Expected: A StateException, after which the lock is released and the active connection is not changed.
        """
        env = use_shard_for(1)

        def put_the_mapping_in_maintenance():
            OrganizationShard.objects.filter(pk=self.org_shard1.pk).update(state=State.MAINTENANCE)

        with mock.patch.object(env, 'acquire_lock', side_effect=put_the_mapping_in_maintenance):
            with self.assertRaises(StateException) as error:
                env.enable()

        self.assertEqual(error.exception.state, State.MAINTENANCE)
        mock_release_lock.assert_called_once_with()
        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('djanquiltdb.router.set_active_connection')
    def test_use_shard_for_mapping_put_in_maintenance_while_waiting_for_the_lock_active_only_schemas_false(
        self, mock_set_active_connection
    ):
        """
        Case: Use use_shard_for with active_only_schemas set to False and an active mapping object that is put in
              maintenance while use_shard_for waits for its lock.
        Expected: The shard is entered, because use_shard_for does not check the state of the mapping object when
                  active_only_schemas is False.
        """
        env = use_shard_for(1, active_only_schemas=False)

        def put_the_mapping_in_maintenance():
            OrganizationShard.objects.filter(pk=self.org_shard1.pk).update(state=State.MAINTENANCE)

        with mock.patch.object(env, 'acquire_lock', side_effect=put_the_mapping_in_maintenance):
            env.enable()

        mock_set_active_connection.assert_called_once_with(env.options)

    @mock.patch('djanquiltdb.router.set_active_connection')
    @mock.patch.object(use_shard_for, 'release_lock')
    def test_use_shard_for_shard_moved_while_waiting_for_the_lock_active_only_schemas_false(
        self, mock_release_lock, mock_set_active_connection
    ):
        """
        Case: Use use_shard_for with active_only_schemas set to False and a mapping object in maintenance, whose shard
              is moved to another node while use_shard_for waits for its lock.
        Expected: A StateException, after which the lock is released and the active connection is not changed, because
                  the options point to the node that the shard is on before the move.
        """
        env = use_shard_for(2, active_only_schemas=False)

        def move_the_shard():
            Shard.objects.filter(pk=self.shard1.pk).update(node_name='other')

        with mock.patch.object(env, 'acquire_lock', side_effect=move_the_shard):
            with self.assertRaisesMessage(StateException, 'on other|test_schema, which differs from the shard'):
                env.enable()

        mock_release_lock.assert_called_once_with()
        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('djanquiltdb.router.set_active_connection')
    @mock.patch.object(use_shard, 'release_lock')
    def test_use_shard_with_a_mapping_value_that_moves_to_another_shard_while_waiting_for_the_lock(
        self, mock_release_lock, mock_set_active_connection
    ):
        """
        Case: Use use_shard with a shard and a mapping value of that shard, as the user proxy of the quilt admin does,
              while move_data_to_shard moves the mapping value to another shard during the wait for the lock. The
              mapping object is active again once use_shard holds the lock.
        Expected: A StateException, after which the lock is released and the active connection is not changed, because
                  the mapping value is on another shard than the shard that use_shard was given.
        """
        with mock.patch('djanquiltdb.utils.create_schema_on_node'):
            other_shard = Shard.objects.create(
                alias='other_sharding', schema_name='other_schema', node_name='default', state=State.ACTIVE
            )

        env = use_shard(self.shard1, mapping_value=1)

        def move_the_mapping_value():
            OrganizationShard.objects.filter(pk=self.org_shard1.pk).update(shard=other_shard)

        with mock.patch.object(env, 'acquire_lock', side_effect=move_the_mapping_value):
            with self.assertRaisesMessage(StateException, 'on default|other_schema, which differs from the shard'):
                env.enable()

        mock_release_lock.assert_called_once_with()
        self.assertFalse(mock_set_active_connection.called)

    @mock.patch('djanquiltdb.router.set_active_connection')
    @mock.patch.object(use_shard, 'release_lock')
    def test_use_shard_with_a_mapping_value_put_in_maintenance_while_waiting_for_the_lock(
        self, mock_release_lock, mock_set_active_connection
    ):
        """
        Case: Use use_shard with a shard and an active mapping value of that shard, whose mapping object is put in
              maintenance while use_shard waits for its lock.
        Expected: A StateException, after which the lock is released and the active connection is not changed.
        """
        env = use_shard(self.shard1, mapping_value=1)

        def put_the_mapping_in_maintenance():
            OrganizationShard.objects.filter(pk=self.org_shard1.pk).update(state=State.MAINTENANCE)

        with mock.patch.object(env, 'acquire_lock', side_effect=put_the_mapping_in_maintenance):
            with self.assertRaises(StateException) as error:
                env.enable()

        self.assertEqual(error.exception.state, State.MAINTENANCE)
        mock_release_lock.assert_called_once_with()
        self.assertFalse(mock_set_active_connection.called)

    def test_use_shard_set_parameters_on_connection(self):
        """
        Case: Use use_shard_for.
        Expected: Parameters from use_shard_for are correctly set on the options.
        """
        with use_shard_for(1, lock=False) as env:
            self.assertEqual(env.connection.shard_options, env.options)

            self.assertEqual(env.options.node_name, self.shard1.node_name)
            self.assertEqual(env.options.schema_name, self.shard1.schema_name)
            self.assertEqual(env.options.shard_id, self.shard1.id)
            self.assertEqual(env.options.mapping_value, 1)
            self.assertEqual(env.options.kwargs['lock'], False)

    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.acquire_advisory_lock_for_each_key')
    def test_acquire_lock(self, mock_acquire_lock):
        """
        Case: Call the acquire_lock method on a use_shard_for instance.
        Expected: DatabaseWrapper.acquire_advisory_lock_for_each_key called once, with the lock key of the shard and the
                  lock key of the mapping value.
        """
        env = use_shard_for(self.org_shard1.organization_id)
        env.connection = connections[env.options]
        env.acquire_lock()

        mock_acquire_lock.assert_called_once_with(
            ['shard_{}'.format(self.shard1.id), 'mapping_{}'.format(self.org_shard1.organization_id)],
            shared=True,
            xact=True,
        )

    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.release_advisory_lock_for_each_key')
    def test_release_lock(self, mock_release_lock):
        """
        Case: Call the release_lock method on a use_shard_for instance.
        Expected: DatabaseWrapper.release_advisory_lock_for_each_key called once, with the lock key of the shard and the
                  lock key of the mapping value.
        """
        env = use_shard_for(self.org_shard1.organization_id)
        env.connection = connections[env.options]
        env.release_lock()

        mock_release_lock.assert_called_once_with(
            ['shard_{}'.format(self.shard1.id), 'mapping_{}'.format(self.org_shard1.organization_id)], shared=True
        )


class UseShardInsideEnteredUseShardTestCase(ShardingTransactionTestCase):
    """
    Each test has two active shards on the default node, ``self.shard`` with the mapping values 1 and 2 and
    ``self.other_shard``, and a second database session that checks whether a lock is free. The tests run outside a
    transaction, unless a test starts one.
    """

    def setUp(self):
        super().setUp()

        create_template_schema('default')
        self.shard = Shard.objects.create(
            alias='test_shard', schema_name='test_schema', node_name='default', state=State.ACTIVE
        )
        self.other_shard = Shard.objects.create(
            alias='other_shard', schema_name='test_other_schema', node_name='default', state=State.ACTIVE
        )
        OrganizationShard.objects.create(organization_id=1, shard=self.shard, state=State.ACTIVE)
        OrganizationShard.objects.create(organization_id=2, shard=self.shard, state=State.ACTIVE)

        self.other_session = DatabaseWrapper(connections['default'].settings_dict, 'default')
        self.addCleanup(self.other_session.close)

    def lock_is_free(self, lock_key):
        """
        Returns whether a second database session can take the lock exclusively. The second session releases the lock
        right after it takes the lock.
        """
        key = LockCursorWrapperMixin.get_int_from_key(lock_key)

        with self.other_session.cursor() as cursor:
            cursor.execute('SELECT pg_try_advisory_lock(%s)', [key])
            acquired = cursor.fetchone()[0]
            cursor.execute('SELECT pg_advisory_unlock(%s)', [key])

        return acquired

    def test_inner_use_shard_runs_no_statements(self):
        """
        Case: Enter and leave use_shard for a shard while a use_shard for that shard is entered.
        Expected: No statements run, because the outer use_shard holds the locks of the shard already.
        """
        with use_shard(self.shard):
            with CaptureQueriesContext(connections['default']) as captured:
                with use_shard(self.shard):
                    pass

        self.assertEqual(captured.captured_queries, [])

    def test_inner_use_shard_builds_no_connection(self):
        """
        Case: Enter and leave use_shard for a shard while a use_shard for that shard is entered, without reading the
              connection of the inner use_shard.
        Expected: No shard connection is built, because the inner use_shard takes no locks and no code reads its
                  connection.
        """
        with use_shard(self.shard):
            with mock.patch('djanquiltdb.apps.ShardDatabaseWrapper', wraps=ShardDatabaseWrapper) as mock_wrapper:
                with use_shard(self.shard):
                    pass

        self.assertEqual(mock_wrapper.call_count, 0)

    def test_outer_use_shard_keeps_its_locks_after_inner_use_shard(self):
        """
        Case: Enter use_shard for a shard inside a use_shard for that shard, leave the inner use_shard, then leave the
              outer use_shard.
        Expected: The shard is locked after the inner use_shard is left, and unlocked after the outer use_shard is left.
        """
        with use_shard(self.shard):
            with use_shard(self.shard):
                pass

            self.assertFalse(self.lock_is_free('shard_{}'.format(self.shard.id)))

        self.assertTrue(self.lock_is_free('shard_{}'.format(self.shard.id)))

    def test_reentering_shard_through_another_shard(self):
        """
        Case: Enter a shard, then another shard on the same node, then the first shard again, and leave all three.
        Expected: Entering the first shard again runs no statements, and both shards are unlocked at the end.
        """
        with use_shard(self.shard):
            with use_shard(self.other_shard):
                with CaptureQueriesContext(connections['default']) as captured:
                    with use_shard(self.shard):
                        pass

        self.assertEqual(captured.captured_queries, [])
        self.assertTrue(self.lock_is_free('shard_{}'.format(self.shard.id)))
        self.assertTrue(self.lock_is_free('shard_{}'.format(self.other_shard.id)))

    def test_inner_use_shard_for_another_mapping_value_takes_its_locks(self):
        """
        Case: Enter use_shard_for a mapping value while a use_shard_for another mapping value on the same shard is
              entered.
        Expected: The inner use_shard_for locks its own mapping value, because the outer use_shard_for does not hold
                  that lock. The inner use_shard_for releases that lock when it is left.
        """
        with use_shard_for(1):
            with use_shard_for(2):
                self.assertFalse(self.lock_is_free('mapping_2'))

            self.assertTrue(self.lock_is_free('mapping_2'))

    def assert_takes_its_lock_and_reads_the_shard(self, captured):
        """
        Asserts that the captured statements take the transaction-scoped lock of the shard, and read the shard again.
        """
        key = LockCursorWrapperMixin.get_int_from_key('shard_{}'.format(self.shard.id))
        statements = [query['sql'] for query in captured.captured_queries]

        self.assertEqual(statements[0], 'SELECT pg_advisory_xact_lock_shared({});'.format(key))
        self.assertTrue(any('"example_shard"' in statement for statement in statements[1:]))

    def test_inner_use_shard_runs_no_statements_inside_a_transaction(self):
        """
        Case: Enter and leave use_shard for a shard while a use_shard for that shard is entered inside a transaction.
        Expected: No statements run, because the outer use_shard holds the transaction-scoped lock of the shard until
                  the transaction ends, and checked the shard once it held the lock.
        """
        with transaction.atomic():
            with use_shard(self.shard):
                with CaptureQueriesContext(connections['default']) as captured:
                    with use_shard(self.shard):
                        pass

        self.assertEqual(captured.captured_queries, [])

    def test_a_later_use_shard_in_the_transaction_runs_no_statements(self):
        """
        Case: Inside a transaction, enter and leave use_shard for a shard, then enter and leave use_shard for that shard
              again.
        Expected: The shard is still locked after the first use_shard is left, and the second use_shard runs no
                  statements, because PostgreSQL holds a transaction-scoped lock until the transaction ends.
        """
        with transaction.atomic():
            with use_shard(self.shard):
                pass

            self.assertFalse(self.lock_is_free('shard_{}'.format(self.shard.id)))

            with CaptureQueriesContext(connections['default']) as captured:
                with use_shard(self.shard):
                    pass

        self.assertEqual(captured.captured_queries, [])

    def test_a_rollback_to_a_savepoint_set_before_the_lock_releases_it(self):
        """
        Case: Inside a transaction, set a savepoint, enter and leave use_shard for a shard, roll back to the savepoint,
              and enter use_shard for that shard again.
        Expected: The rollback releases the lock of the shard, and the second use_shard takes the lock again and reads
                  the shard again.
        """
        with transaction.atomic():
            savepoint = transaction.savepoint()

            with use_shard(self.shard):
                pass

            transaction.savepoint_rollback(savepoint)

            self.assertTrue(self.lock_is_free('shard_{}'.format(self.shard.id)))

            with CaptureQueriesContext(connections['default']) as captured:
                with use_shard(self.shard):
                    pass

        self.assert_takes_its_lock_and_reads_the_shard(captured)

    def test_a_rollback_to_a_savepoint_set_after_the_lock_keeps_it(self):
        """
        Case: Inside a transaction, enter and leave use_shard for a shard, set a savepoint, roll back to it, and enter
              use_shard for that shard again.
        Expected: The shard is still locked after the rollback, and the second use_shard runs no statements.
        """
        with transaction.atomic():
            with use_shard(self.shard):
                pass

            savepoint = transaction.savepoint()
            transaction.savepoint_rollback(savepoint)

            self.assertFalse(self.lock_is_free('shard_{}'.format(self.shard.id)))

            with CaptureQueriesContext(connections['default']) as captured:
                with use_shard(self.shard):
                    pass

        self.assertEqual(captured.captured_queries, [])

    def test_a_released_savepoint_hands_its_lock_to_the_transaction(self):
        """
        Case: Inside a transaction, enter and leave use_shard for a shard inside a nested atomic block, which releases
              its savepoint, then roll back to a savepoint set after it, and enter use_shard for that shard again.
        Expected: The shard is still locked, and the second use_shard runs no statements, because the released savepoint
                  handed the lock to the transaction, which a later rollback does not release.
        """
        with transaction.atomic():
            with transaction.atomic():
                with use_shard(self.shard):
                    pass

            savepoint = transaction.savepoint()
            transaction.savepoint_rollback(savepoint)

            self.assertFalse(self.lock_is_free('shard_{}'.format(self.shard.id)))

            with CaptureQueriesContext(connections['default']) as captured:
                with use_shard(self.shard):
                    pass

        self.assertEqual(captured.captured_queries, [])

    def test_the_end_of_the_transaction_forgets_its_locks(self):
        """
        Case: Enter and leave use_shard for a shard inside a transaction, and enter use_shard for that shard after the
              transaction committed.
        Expected: The connection holds no transaction-scoped locks and no savepoints after the commit, and the second
                  use_shard takes the lock of the shard for the session.
        """
        with transaction.atomic():
            with use_shard(self.shard):
                pass

        self.assertEqual(connections['default'].transaction_locks_of_entered_use_shards, {})
        self.assertEqual(connections['default'].open_savepoint_ids, [])

        with use_shard(self.shard):
            session_locks = connections['default'].session_locks_of_entered_use_shards

            self.assertEqual(session_locks['shard_{}'.format(self.shard.id)], 1)

    def test_a_commit_while_a_use_shard_is_entered_releases_its_lock(self):
        """
        Case: Start a transaction, enter use_shard for a shard, commit the transaction while the use_shard is entered,
              and enter use_shard for that shard inside a new transaction.
        Expected: The inner use_shard takes the lock again and reads the shard again, because the commit released the
                  transaction-scoped lock of the outer use_shard.
        """
        outer_transaction = transaction.atomic()
        outer_transaction.__enter__()
        outer_use_shard = use_shard(self.shard)
        outer_use_shard.__enter__()
        outer_transaction.__exit__(None, None, None)

        try:
            with transaction.atomic():
                with CaptureQueriesContext(connections['default']) as captured:
                    with use_shard(self.shard):
                        pass
        finally:
            outer_use_shard.__exit__(None, None, None)

        self.assert_takes_its_lock_and_reads_the_shard(captured)

    def test_closing_the_connection_forgets_the_held_locks(self):
        """
        Case: Close the connection while a use_shard for a shard is entered, then enter use_shard for that shard.
        Expected: The inner use_shard locks the shard again, because closing the connection releases the locks of the
                  session.
        """
        with use_shard(self.shard):
            connections['default'].close()

            with use_shard(self.shard):
                self.assertFalse(self.lock_is_free('shard_{}'.format(self.shard.id)))

    def test_outermost_use_shard_reads_the_shard_once(self):
        """
        Case: Enter and leave use_shard for a shard while no use_shard is entered.
        Expected: Three statements: the lock of the shard, one read of the shard from the shard registry once the
                  lock is held, and the release of the lock.
        """
        key = LockCursorWrapperMixin.get_int_from_key('shard_{}'.format(self.shard.id))

        with CaptureQueriesContext(connections['default']) as captured:
            with use_shard(self.shard):
                pass

        statements = [query['sql'] for query in captured.captured_queries]

        self.assertEqual(len(statements), 3)
        self.assertEqual(statements[0], 'SELECT pg_advisory_lock_shared({});'.format(key))
        self.assertIn('"example_shard"', statements[1])
        self.assertEqual(statements[2], 'SELECT pg_advisory_unlock_shared({});'.format(key))

    def test_outermost_use_shard_for_reads_the_mapping_value_once(self):
        """
        Case: Enter and leave use_shard_for a mapping value while no use_shard is entered. The use_shard_for is
              constructed before the statements are counted, because the constructor looks up the mapping value.
        Expected: Three statements: the locks of the shard and of the mapping value in one statement, one read of the
                  mapping value and its shard once the locks are held, and the releases of the locks in one statement.
        """
        shard_key = LockCursorWrapperMixin.get_int_from_key('shard_{}'.format(self.shard.id))
        mapping_key = LockCursorWrapperMixin.get_int_from_key('mapping_1')
        env = use_shard_for(1)

        with CaptureQueriesContext(connections['default']) as captured:
            with env:
                pass

        statements = [query['sql'] for query in captured.captured_queries]

        self.assertEqual(len(statements), 3)
        self.assertEqual(
            statements[0],
            'SELECT pg_advisory_lock_shared({}), pg_advisory_lock_shared({});'.format(shard_key, mapping_key),
        )
        self.assertIn('"example_organizationshard"', statements[1])
        self.assertIn('"example_shard"', statements[1])
        self.assertEqual(
            statements[2],
            'SELECT pg_advisory_unlock_shared({}), pg_advisory_unlock_shared({});'.format(shard_key, mapping_key),
        )

    def test_recursive_function_decorated_with_use_shard(self):
        """
        Case: Call a function that is decorated with one use_shard object for a shard, and that calls itself once.
        Expected: After the call, the shard is unlocked, the lock counts of the connection are empty, and the active
                  connection is the connection that was active before the call, because each call enters a use_shard
                  of its own.
        """
        active_connection = get_active_connection()
        calls = []

        @use_shard(self.shard)
        def call_once_more(depth):
            calls.append(get_active_connection())
            if depth:
                call_once_more(depth - 1)

        call_once_more(1)

        self.assertEqual(len(calls), 2)
        self.assertTrue(self.lock_is_free('shard_{}'.format(self.shard.id)))
        self.assertEqual(connections['default'].session_locks_of_entered_use_shards, {})
        self.assertEqual(get_active_connection(), active_connection)

    def test_function_decorated_with_use_shard_called_from_two_threads_at_once(self):
        """
        Case: At the same time, two threads call a function that is decorated with one use_shard object for a shard:
              the second thread enters the function while the first thread is inside it.
        Expected: In each thread, the lock counts of the connection are empty after the call, and the active connection
                  is the connection that was active before the call, because each call enters a use_shard of its own.
        """
        first_thread_entered = threading.Event()
        second_thread_left = threading.Event()
        states_after_the_call = {}

        @use_shard(self.shard)
        def wait_inside_the_shard(thread_name):
            if thread_name == 'first':
                first_thread_entered.set()
                second_thread_left.wait(5)

        def call_in_a_thread(thread_name):
            try:
                active_connection = get_active_connection()
                wait_inside_the_shard(thread_name)
                states_after_the_call[thread_name] = (
                    dict(connections['default'].session_locks_of_entered_use_shards),
                    get_active_connection() == active_connection,
                )
            finally:
                if thread_name == 'second':
                    second_thread_left.set()
                connections.close_all()

        first_thread = threading.Thread(target=call_in_a_thread, args=('first',))
        second_thread = threading.Thread(target=call_in_a_thread, args=('second',))

        first_thread.start()
        first_thread_entered.wait(5)
        second_thread.start()
        first_thread.join(10)
        second_thread.join(10)

        self.assertEqual(states_after_the_call, {'first': ({}, True), 'second': ({}, True)})

    def test_entering_an_entered_use_shard_again_raises(self):
        """
        Case: Enter a use_shard object for a shard, then a use_shard object for another shard, and then the first
              use_shard object again.
        Expected: A RuntimeError when the first use_shard object is entered again, because a use_shard object keeps the
                  state of one entry. After both use_shard objects are left, both shards are unlocked and the active
                  connection is the connection that was active before.
        """
        active_connection = get_active_connection()
        first_context = use_shard(self.shard)
        second_context = use_shard(self.other_shard)

        with first_context:
            with second_context:
                with self.assertRaisesMessage(RuntimeError, 'is entered already'):
                    with first_context:
                        pass

        self.assertTrue(self.lock_is_free('shard_{}'.format(self.shard.id)))
        self.assertTrue(self.lock_is_free('shard_{}'.format(self.other_shard.id)))
        self.assertEqual(get_active_connection(), active_connection)

    def test_functions_decorated_with_use_shard_that_call_each_other(self):
        """
        Case: A function decorated with a use_shard object for a shard calls a function decorated with a use_shard
              object for another shard, which calls the first function again.
        Expected: No error, and after the call both shards are unlocked and the active connection is the connection
                  that was active before, because each call enters a use_shard of its own.
        """
        active_connection = get_active_connection()

        @use_shard(self.shard)
        def enter_the_first_shard(depth):
            if depth:
                enter_the_other_shard(depth - 1)

        @use_shard(self.other_shard)
        def enter_the_other_shard(depth):
            enter_the_first_shard(depth)

        enter_the_first_shard(1)

        self.assertTrue(self.lock_is_free('shard_{}'.format(self.shard.id)))
        self.assertTrue(self.lock_is_free('shard_{}'.format(self.other_shard.id)))
        self.assertEqual(get_active_connection(), active_connection)


class GetShardForTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        with mock.patch('djanquiltdb.utils.create_schema_on_node'):
            self.shard1 = Shard.objects.create(
                alias='test_sharding', schema_name='test_schema', node_name='default', state=State.ACTIVE
            )

        self.org_shard1 = OrganizationShard.objects.create(organization_id=1, shard=self.shard1, slug='test_slug')

    def test(self):
        """
        Case: Use get_shard_for.
        Expected: Correct shard returned.
        """
        self.assertEqual(get_shard_for(1), self.shard1)

    def test_inside_a_shard_without_the_public_schema_with_a_mapping_model_that_is_read_on_the_active_node(self):
        """
        Case: Use get_shard_for while a use_shard for a shard is entered with include_public set to False, and the
              mapping model has route_to_primary_db set to False, so that the router sends it to the active connection.
        Expected: Correct shard returned, because get_shard_for reads the mapping table in the public schema of the
                  active node, which the search_path of the shard does not have.
        """
        create_template_schema('default')
        entered_shard = Shard.objects.create(
            alias='entered_shard', schema_name='test_entered_schema', node_name='default', state=State.ACTIVE
        )

        with mock.patch.object(OrganizationShard, 'route_to_primary_db', False):
            with use_shard(entered_shard, include_public=False):
                shard = get_shard_for(1)

        self.assertEqual(shard, self.shard1)

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    def test_get_shard_for_without_setting(self):
        """
        Case: Use get_shard_for while not having MAPPING_MODEL set.
        Expected: ImproperlyConfigured raised.
        """
        with self.assertRaises(ImproperlyConfigured):
            self.assertEqual(get_shard_for(1), self.shard1)

    def test_active_only_schemas(self):
        """
        Case: Use get_shard_for while having the mapping model in maintenance.
        Expected: StateException raised.
        """
        self.assertEqual(get_shard_for(1), self.shard1)

        self.org_shard1.state = State.MAINTENANCE
        self.org_shard1.save()

        with self.assertRaises(StateException):
            get_shard_for(1)

    def test_shard_in_maintenance(self):
        """
        Case: Use get_shard_for while the mapping object is active and its shard is in maintenance.
        Expected: A StateException whose message names the shard and whose state is the state of the shard.
        """
        self.shard1.state = State.MAINTENANCE
        self.shard1.save(update_fields=['state'])

        with self.assertRaisesMessage(StateException, 'Shard test_sharding(default|test_schema) state is ') as error:
            get_shard_for(1)

        self.assertEqual(error.exception.state, State.MAINTENANCE)

    def test_active_only_schemas_false(self):
        """
        Case: Use get_shard_for with active_only set to False, while having the mapping model in maintenance.
        Expected: No StateException raised, correct shard returned.
        """
        self.org_shard1.state = State.MAINTENANCE
        self.org_shard1.save()

        self.assertEqual(get_shard_for(1, active_only=False), self.shard1)

    def test_field(self):
        """
        Case: Use get_shard_for with a different field provided.
        Expected: Shard selected on field 'slug'.
        """
        self.assertEqual(get_shard_for('test_slug', field='slug'), self.shard1)


class CreateSchemaOnNodeTestCase(ShardingTestCase):
    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.clone_schema')
    def test_create_schema_no_migration(self, mock_clone_schema):
        """
        Case: Call create_schema_on_node with a name and node (no migration).
        Expected: A new PostgreSQL schema is made, no clone_schema called.
        """
        _connection = connections['other']
        # first: check if schema does not exist yet.
        self.assertFalse(_connection.get_ps_schema('test_schema'))

        create_schema_on_node('test_schema', 'other', migrate=False)

        # check if it exists now
        self.assertTrue(_connection.get_ps_schema('test_schema'))
        self.assertFalse(mock_clone_schema.called)

    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.clone_schema')
    def test_create_schema_on_nonexisting_node(self, mock_clone_schema):
        """
        Case: Call create_schema_on_node with an nonexisting node (no migration).
        Expected: A new PostgreSQL schema is made.
        """
        with self.assertRaises(ValueError):
            create_schema_on_node('test_schema', 'phaaap', False)
        self.assertFalse(mock_clone_schema.called)

    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.clone_schema')
    def test_create_schema_migration(self, mock_clone_schema):
        """
        Case: Call create_schema_on_node with migration.
        Expected: A new PostgreSQL schema is made and the clone_schema is called.
        """
        _connection = connections['other']
        self.assertFalse(_connection.get_ps_schema('test_schema'))
        create_schema_on_node('test_schema', 'other', migrate=True)
        self.assertTrue(_connection.get_ps_schema('test_schema'))
        mock_clone_schema.assert_called_once_with('template', 'test_schema')

    @override_settings(QUILT_DB={'TEMPLATE_NAME': 'other-template', 'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.clone_schema')
    def test_create_schema_migration_with_different_template_name(self, mock_clone_schema):
        """
        Case: Call create_schema_on_node with migration, while a custom template name is set.
        Expected: A new PostgreSQL schema is made and the clone_schema is called.
        """
        _connection = connections['other']
        self.assertFalse(_connection.get_ps_schema('test_schema'))
        create_schema_on_node('test_schema', 'other', migrate=True)
        self.assertTrue(_connection.get_ps_schema('test_schema'))
        mock_clone_schema.assert_called_once_with('other-template', 'test_schema')

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard', 'NEW_SHARD_NODE': 'other'})
    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.clone_schema')
    def test_create_schema_without_node_name_with_setting(self, mock_clone_schema):
        """
        Case: Call use_shard without a node name, but with NEW_SHARD_NODE setting set.
        Expected: A new PostgreSQL schema is made and the clone_schema is called.
        """
        _connection = connections['other']
        # first: check if schema does not exist yet.
        self.assertFalse(_connection.get_ps_schema('test_schema'))

        create_schema_on_node('test_schema', None, migrate=True)

        # check if it exists now
        self.assertTrue(_connection.get_ps_schema('test_schema'))
        self.assertTrue(mock_clone_schema.called)

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.clone_schema')
    def test_create_schema_without_node_name_without_setting(self, mock_clone_schema):
        """
        Case: Call use_shard without a node name, and without NEW_SHARD_NODE setting set.
        Expected: ValueError raised.
        """
        _connection = connections['other']
        # first: check if schema does not exist yet.
        self.assertFalse(_connection.get_ps_schema('test_schema'))

        with self.assertRaises(ValueError):
            create_schema_on_node('test_schema', None, migrate=True)

        self.assertFalse(_connection.get_ps_schema('test_schema'))
        self.assertFalse(mock_clone_schema.called)


class DeleteSchemaTestCase(ShardingTestCase):
    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.delete_schema')
    def test(self, mock_delete_schema):
        """
        Case: Call sharding.utils.delete_schema.
        Expected: DatabaseWrapper.delete_schema called with the same schema name.
        """
        delete_schema(schema_name='test_schema', node_name='default')
        mock_delete_schema.assert_called_once_with('test_schema', is_template=False)

    def test_node_not_exists(self):
        """
        Case: Call sharding.utils.delete_schema with a node name that does not exist.
        Expected: ValueError raised.
        """
        with self.assertRaises(ValueError):
            delete_schema(schema_name='test_schema', node_name='not_existing_node')

    def test_delete_template_is_template_false(self):
        """
        Case: Delete the template schema without having is_template=True set.
        Expected: ValueError raised and schema not deleted.
        """
        create_template_schema('default')

        template_name = get_template_name()
        message = "Schema name 'template' cannot be the same as the template name 'template'"
        with self.assertRaisesMessage(ValueError, message):
            delete_schema(schema_name=template_name, node_name='default')

        self.assertTrue(schema_exists('default', template_name))

    def test_delete_template_is_template_true(self):
        """
        Case: Delete the template schema with having is_template=True set.
        Expected: Schema deleted.
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
        Case: call utils.migrate_schema() to migrate the sharded models to the template schema.
        Expected: The newly made schema to have to correct table headers.
        """
        create_template_schema('default')  # This also calls the migration

        cursor = connection.cursor()
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'template';")
        template_tables = [table[1] for table in cursor.fetchall()]
        # Filter test models
        template_tables = [table for table in template_tables if not re.search(r'_[t|T]est', table)]
        self.assertCountEqual(
            sorted(template_tables),
            [
                'django_migrations',
                'example_organization',
                'example_note',
                'example_suborganization',
                'example_user',
                'example_statement',
                'example_cake',
                'example_quiltsession',
                'example_report',
                'example_detailedreport',
                'example_pudding',
                'example_user_cake',
                'example_statement_type',
            ],
        )

    def test_create_template_schema_invalid_node(self):
        """
        Case: call utils.migrate_schema() with an nonexisting connection.
        Expected: ValueError raised.
        """
        with self.assertRaises(ValueError):
            migrate_schema('no_connection', 'test_schema')  # This also calls the migration

    def test_create_template_schema_invalid_schema_name(self):
        """
        Case: call utils.migrate_schema() with an nonexisting schema_name.
        Expected: ValueError raised.
        """
        with self.assertRaises(ValueError):
            migrate_schema('default', 'test_schema')  # This also calls the migration

    @mock.patch('djanquiltdb.utils.migrate_schema')
    def test_migrate(self, mock_migrate_schema):
        """
        Case: Call create_template_schema with both migrate set to True and False.
        Expected: migrate_schema is not called when migrate=False and it is called when migrate=True.
        """
        template_name = get_template_name()

        self.assertFalse(schema_exists('default', template_name))
        create_template_schema('default', migrate=False)
        self.assertFalse(mock_migrate_schema.called)

        self.assertFalse(schema_exists('other', template_name))
        create_template_schema('other', migrate=True)
        self.assertTrue(mock_migrate_schema.called)


class GetModelDefinitionTestCase(SimpleTestCase):
    def test_model(self):
        """
        Case: Call get_model_sharding_mode for a normal model class.
        Expected: Model class returned.
        """
        self.assertEqual(get_model_definition(User), User)

    def test_related_model(self):
        """
        Case: Call get_model_sharding_mode for a related model class.
        Expected: Given class' model fiend returned
        """
        # User.cake.rel = <ManyToManyRel: example.user>
        # User.cake.rel.model = <class 'example.models.Cake'>
        self.assertEqual(get_model_definition(User.cake.rel), Cake)


class GetModelShardingModeTestCase(SimpleTestCase):
    @mock.patch('djanquiltdb.utils.get_placement', return_value=(ShardingMode.SHARDED, None))
    @mock.patch('djanquiltdb.utils.get_model_definition', return_value=User)
    def test(self, mock_get_model_definition, mock_get_placement):
        """
        Case: Call get_model_sharding_mode.
        Expected: get_model_definition and get_placement to be called with the correct arguments, and the mode that
                  get_placement returns.
        """
        self.assertEqual(get_model_sharding_mode(User), ShardingMode.SHARDED)
        mock_get_model_definition.assert_called_once_with(User)
        mock_get_placement.assert_called_once_with('example', 'user')


class GetShardingModeTestCase(SimpleTestCase):
    def test_get_sharding_mode_override_settings(self):
        """
        Case: Set sharding configuration and verify that DynamicDbRouter.get_sharding_mode() it uses the overridden
              settings.
        Expected: The router should return the overriden setting for the model.
        """
        with override_settings(
            QUILT_DB={
                'OVERRIDE_SHARDING_MODE': {
                    ('example', 'organization'): ShardingMode.MIRRORED,
                }
            }
        ):
            self.assertEqual(get_sharding_mode('example', 'organization'), ShardingMode.MIRRORED)

        with override_settings(
            QUILT_DB={
                'OVERRIDE_SHARDING_MODE': {
                    ('example',): ShardingMode.MIRRORED,
                }
            }
        ):
            self.assertEqual(get_sharding_mode('example', 'user'), ShardingMode.MIRRORED)

    def test_get_sharding_mode_fallback(self):
        """
        Case: Set sharding configuration and verify that DynamicDbRouter.get_sharding_mode() will use the fallback to
              checking model class if the model sharding mode is not overridden in settings.
        Expected: The router should return the class setting for the model.
        """
        with override_settings(
            QUILT_DB={
                'OVERRIDE_SHARDING_MODE': {
                    ('example', 'user'): ShardingMode.MIRRORED,
                }
            }
        ):
            self.assertEqual(get_sharding_mode('example', 'organization'), ShardingMode.SHARDED)

    def test_get_sharding_mode_without_model_name(self):
        """
        Case: Call get_sharding_mode with None as model_name.
        Expected: None returned.
        """
        self.assertIsNone(get_sharding_mode('example', None))

    def test_get_sharding_mode_without_model_name_but_with_override(self):
        """
        Case: Call get_sharding_mode with None as model_name, while settings override set.
        Expected: Mode returned as defined by the settings override.
        """
        with override_settings(
            QUILT_DB={
                'OVERRIDE_SHARDING_MODE': {
                    ('example',): ShardingMode.PUBLIC,
                }
            }
        ):
            self.assertEqual(get_sharding_mode('example', None), ShardingMode.PUBLIC)

    def test_get_sharding_mode_for_auto_created_model(self):
        """
        Case: Call get_sharding_mode for a model that is created automatically for a many-to-many field.
        Expected: Mode returned as defined by the model which is responsible for the many-to-many relation.
        """
        self.assertEqual(get_sharding_mode('example', 'user_cake'), ShardingMode.SHARDED)


class GetDeclaredShardingModeTestCase(SimpleTestCase):
    def test_the_marker_of_the_mode_decorator_is_read_from_the_class(self):
        """
        Case: Call get_declared_sharding_mode for a model that is marked by a mode decorator.
        Expected: The mode that the decorator set on the class.
        """
        self.assertEqual(get_declared_sharding_mode(User), ShardingMode.SHARDED)

    def test_returns_none_when_nothing_declares_a_mode(self):
        """
        Case: Call get_declared_sharding_mode for a model without a marker or an override entry.
        Expected: None, and not the False that get_sharding_mode returns.
        """
        with isolate_apps('example'):

            class ModelWithoutShardingMode(models.Model):
                class Meta:
                    app_label = 'example'

        self.assertIsNone(get_declared_sharding_mode(ModelWithoutShardingMode))

    def test_override_keys_match_whatever_their_casing(self):
        """
        Case: Call get_declared_sharding_mode while the OVERRIDE_SHARDING_MODE keys have the casing of a settings file,
              as they do before AppConfig.ready() normalizes them.
        Expected: The entries match, and a model-specific entry takes precedence over an app-wide entry.
        """
        with override_settings(
            QUILT_DB={
                'OVERRIDE_SHARDING_MODE': {
                    ('example', 'User'): ShardingMode.MIRRORED,
                    ('Example',): ShardingMode.PUBLIC,
                }
            }
        ):
            self.assertEqual(get_declared_sharding_mode(User), ShardingMode.MIRRORED)
            self.assertEqual(get_declared_sharding_mode(Organization), ShardingMode.PUBLIC)

    def test_the_ready_app_registry_marks_the_override_keys_as_lowercase(self):
        """
        Case: The OVERRIDE_SHARDING_MODE setting is read once AppConfig.ready() has run.
        Expected: It is an OverrideShardingModeWithLowercaseKeys, whose keys are all lowercase.
        """
        override_sharding_mode = settings.QUILT_DB['OVERRIDE_SHARDING_MODE']

        self.assertIsInstance(override_sharding_mode, OverrideShardingModeWithLowercaseKeys)
        self.assertTrue(all(part == part.lower() for key in override_sharding_mode for part in key))

    def test_override_keys_that_are_marked_as_lowercase_are_not_copied(self):
        """
        Case: Call get_declared_sharding_mode while OVERRIDE_SHARDING_MODE is an OverrideShardingModeWithLowercaseKeys,
              as it is once AppConfig.ready() has run.
        Expected: The entry matches, and the entries are not read one by one to build a lowercase copy.
        """
        entries_read = []

        class RecordingOverrideShardingMode(OverrideShardingModeWithLowercaseKeys):
            def items(self):
                entries_read.append(True)
                return super().items()

        with override_settings(
            QUILT_DB={
                'OVERRIDE_SHARDING_MODE': RecordingOverrideShardingMode({('example', 'user'): ShardingMode.PUBLIC})
            }
        ):
            self.assertEqual(get_declared_sharding_mode(User), ShardingMode.PUBLIC)

        self.assertEqual(entries_read, [])

    def test_a_proxy_gets_the_mode_of_its_concrete_model(self):
        """
        Case: Call get_declared_sharding_mode for a proxy of a model whose mode is set only by an override entry.
        Expected: The mode of the concrete model.
        """
        with isolate_apps('example'):

            class ModelWithOverriddenMode(models.Model):
                class Meta:
                    app_label = 'example'

            class ProxyOfModelWithOverriddenMode(ModelWithOverriddenMode):
                class Meta:
                    app_label = 'example'
                    proxy = True

        with override_settings(
            QUILT_DB={'OVERRIDE_SHARDING_MODE': {('example', 'modelwithoverriddenmode'): ShardingMode.MIRRORED}}
        ):
            self.assertEqual(get_declared_sharding_mode(ProxyOfModelWithOverriddenMode), ShardingMode.MIRRORED)

    def test_an_auto_created_model_gets_the_mode_of_the_model_that_caused_its_creation(self):
        """
        Case: Call get_declared_sharding_mode for the auto-created through model of a many-to-many field.
        Expected: The mode of the model that declares the many-to-many field.
        """
        self.assertEqual(get_declared_sharding_mode(User.cake.through), ShardingMode.SHARDED)


class GetDeclaredPlacementTestCase(SimpleTestCase):
    def test_the_decorator_declares_the_mode_and_the_node(self):
        """
        Case: Call get_declared_placement for Unrelated, which is decorated with
              @single_node_unsharded_model(node='default'), and for the sharded User.
        Expected: SINGLE_NODE_UNSHARDED with the node 'default', and SHARDED without a node.
        """
        self.assertEqual(get_declared_placement(Unrelated), (ShardingMode.SINGLE_NODE_UNSHARDED, 'default'))
        self.assertEqual(get_declared_placement(User), (ShardingMode.SHARDED, None))

    def test_an_override_entry_declares_the_mode_and_the_node(self):
        """
        Case: Call get_declared_placement and get_sharding_mode for a model of an app whose OVERRIDE_SHARDING_MODE entry
              is single_node_unsharded(node=PRIMARY_NODE).
        Expected: SINGLE_NODE_UNSHARDED with PRIMARY_NODE as declared, and SINGLE_NODE_UNSHARDED from get_sharding_mode.
        """
        with override_settings(
            QUILT_DB={'OVERRIDE_SHARDING_MODE': {('example',): single_node_unsharded(node=PRIMARY_NODE)}}
        ):
            self.assertEqual(get_declared_placement(User), (ShardingMode.SINGLE_NODE_UNSHARDED, PRIMARY_NODE))
            self.assertEqual(get_sharding_mode('example', 'user'), ShardingMode.SINGLE_NODE_UNSHARDED)

    def test_a_subclass_in_another_mode_has_no_node(self):
        """
        Case: Call get_declared_placement for a sharded subclass of a model on a single node, which inherits the node of
              its parent.
        Expected: SHARDED without a node.
        """
        with isolate_apps('example'):

            @single_node_unsharded_model(node='default')
            class OnDefault(models.Model):
                class Meta:
                    app_label = 'example'

            @sharded_model()
            class ShardedChild(OnDefault):
                class Meta:
                    app_label = 'example'

        self.assertEqual(get_declared_placement(ShardedChild), (ShardingMode.SHARDED, None))


class SingleNodeAliasTestCase(SimpleTestCase):
    def test_primary_node_resolves_to_the_primary_db_alias(self):
        """
        Case: Resolve PRIMARY_NODE and the alias 'default' while PRIMARY_DB_ALIAS is 'other'.
        Expected: 'other' for PRIMARY_NODE, and 'default' for 'default'.
        """
        with override_sharding_setting('PRIMARY_DB_ALIAS', 'other'):
            self.assertEqual(resolve_node_alias(PRIMARY_NODE), 'other')
            self.assertEqual(resolve_node_alias('default'), 'default')

    def test_the_alias_of_a_model(self):
        """
        Case: Ask for the node alias of Unrelated, of the sharded User, and of a model that the app registry does not
              have while an OVERRIDE_SHARDING_MODE entry puts it on PRIMARY_NODE, with PRIMARY_DB_ALIAS set to 'other'.
        Expected: 'default' for Unrelated, None for User, and 'other' for the model without a class.
        """
        self.assertEqual(get_model_single_node_alias(Unrelated), 'default')
        self.assertEqual(get_placement('example', 'unrelated'), (ShardingMode.SINGLE_NODE_UNSHARDED, 'default'))
        self.assertIsNone(get_model_single_node_alias(User))
        self.assertEqual(get_placement('example', 'user'), (ShardingMode.SHARDED, None))

        with override_settings(
            QUILT_DB={
                'PRIMARY_DB_ALIAS': 'other',
                'OVERRIDE_SHARDING_MODE': {('example', 'removed'): single_node_unsharded(node=PRIMARY_NODE)},
            }
        ):
            self.assertEqual(get_placement('example', 'removed'), (ShardingMode.SINGLE_NODE_UNSHARDED, 'other'))

    def test_the_placement_of_an_operation_or_a_model_without_a_mode(self):
        """
        Case: Ask for the placement of an operation without a model name, and of a model that declares no mode.
        Expected: None for the operation and False for the model, each without a node, as get_sharding_mode returns.
        """
        self.assertEqual(get_placement('example', None), (None, None))

        with isolate_apps('example'):

            class ModelWithoutShardingMode(models.Model):
                class Meta:
                    app_label = 'example'

        with mock.patch('djanquiltdb.utils.apps.get_model', return_value=ModelWithoutShardingMode):
            self.assertEqual(get_placement('example', 'modelwithoutshardingmode'), (False, None))

    def test_the_value_of_an_override_entry(self):
        """
        Case: Compare and show two single_node_unsharded() values.
        Expected: Equal when they name the same node, and shown as the call that makes them.
        """
        self.assertEqual(single_node_unsharded(node='default'), single_node_unsharded(node='default'))
        self.assertNotEqual(single_node_unsharded(node='default'), single_node_unsharded(node=PRIMARY_NODE))
        self.assertEqual(repr(single_node_unsharded(node='other')), "single_node_unsharded(node='other')")

    def test_an_override_entry_with_a_node_that_is_not_an_alias(self):
        """
        Case: Call single_node_unsharded() with a list of aliases, which is neither a string nor PRIMARY_NODE.
        Expected: TypeError, as @single_node_unsharded_model() raises for the same node.
        """
        with self.assertRaisesMessage(TypeError, "node must be the alias of a node or PRIMARY_NODE, not ['default']."):
            single_node_unsharded(node=['default'])

    def test_relations_with_a_model_on_a_single_node(self):
        """
        Case: Ask whether a relation is allowed for each pair of the mode and the node of the model that is referred to
              and of the model that refers to it, where at least one of them is on a single node, and for a pair
              without one. False stands for a model without a sharding mode.
        Expected: Only a model on the same node may refer to a model on a single node, and a model on a single node may
                  refer only to a public or mirrored model. None when neither model is on a single node.
        """
        single_node = ShardingMode.SINGLE_NODE_UNSHARDED
        combinations = [
            ('referred to on the same node', (single_node, 'default'), (single_node, 'default'), True),
            ('referred to from another node', (single_node, 'default'), (single_node, 'other'), False),
            ('referred to by a sharded model', (single_node, 'default'), (ShardingMode.SHARDED, None), False),
            ('referred to by a mirrored model', (single_node, 'default'), (ShardingMode.MIRRORED, None), False),
            ('referred to by a model without a mode', (single_node, 'default'), (False, None), False),
            ('referring to a public model', (ShardingMode.PUBLIC, None), (single_node, 'default'), True),
            ('referring to a mirrored model', (ShardingMode.MIRRORED, None), (single_node, 'default'), True),
            ('referring to a sharded model', (ShardingMode.SHARDED, None), (single_node, 'default'), False),
            ('referring to a model without a mode', (False, None), (single_node, 'default'), False),
            ('neither on a single node', (ShardingMode.MIRRORED, None), (ShardingMode.SHARDED, None), None),
        ]

        for name, (referred_mode, referred_node), (referring_mode, referring_node), allowed in combinations:
            with self.subTest(name):
                self.assertIs(
                    relation_with_a_model_on_a_single_node_is_allowed(
                        referred_mode=referred_mode,
                        referred_node=referred_node,
                        referring_mode=referring_mode,
                        referring_node=referring_node,
                    ),
                    allowed,
                )


class GetAllShardedModels(ShardingTestCase):
    available_apps = ['example']

    def test_with_override(self):
        """
        Case: Call get_all_sharded_models.
        Expected: Only the User and the Statement models to be returned. The rest is a proxy, mirrored or not sharded.
        Note: System test
        """
        with override_settings(
            QUILT_DB={
                'OVERRIDE_SHARDING_MODE': {
                    ('example', 'organization'): ShardingMode.MIRRORED,
                }
            }
        ):
            self.assertCountEqual(
                get_all_sharded_models(),
                [User, Statement, Suborganization, Cake, QuiltSession, Report, DetailedReport, Pudding, Note],
            )

    @mock.patch('djanquiltdb.utils.get_model_sharding_mode')
    def test(self, mock_get_model_sharding_mode):
        """
        Case: Call get_all_sharded_models.
        Expected: get_model_sharding_mode called for each model that's not a proxy model.
        """
        get_all_sharded_models()
        mock_get_model_sharding_mode.assert_has_calls(
            [mock.call(model) for model in apps.get_models(include_auto_created=False) if not model._meta.proxy],
            any_order=True,
        )

    @mock.patch('djanquiltdb.utils.get_model_sharding_mode')
    def test_include_auto_created(self, mock_get_model_sharding_mode):
        """
        Case: Call get_all_sharded_models, with include_auto_created.
        Expected: get_model_sharding_mode called for each model that's not a proxy model.
        """
        get_all_sharded_models(include_auto_created=True)
        mock_get_model_sharding_mode.assert_has_calls(
            [mock.call(model) for model in apps.get_models(include_auto_created=True) if not model._meta.proxy],
            any_order=True,
        )

    def test_include_auto_created_result(self):
        """
        Case: Call get_all_sharded_models, with include_auto_created.
        Expected: All sharded models and auto created models to be returned (but not proxy models).
        """
        # We compare it string based, since we cannot import the auto created fields as classes.
        result = [str(model) for model in get_all_sharded_models(include_auto_created=True)]
        self.assertCountEqual(
            result,
            [
                "<class 'example.models.Organization'>",
                "<class 'example.models.Note'>",
                "<class 'example.models.Suborganization'>",
                "<class 'example.models.Cake'>",
                "<class 'example.models.User_cake'>",
                "<class 'example.models.User'>",
                "<class 'example.models.QuiltSession'>",
                "<class 'example.models.Statement_type'>",  # This is a auto-created model
                "<class 'example.models.Statement'>",
                "<class 'example.models.Report'>",
                "<class 'example.models.DetailedReport'>",
                "<class 'example.models.Pudding'>",
            ],
        )

    def test_include_proxy_models(self):
        """
        Case: Call get_all_sharded_models, including proxy models.
        Expected: All sharded proxy and non-proxy models returned (but not auto-created models).
        """
        # We compare it string based, since we cannot import the auto created fields as classes.
        result = [str(model) for model in get_all_sharded_models(include_proxy=True)]
        self.assertCountEqual(
            result,
            [
                "<class 'example.models.Organization'>",
                "<class 'example.models.Note'>",
                "<class 'example.models.Suborganization'>",
                "<class 'example.models.Cake'>",
                "<class 'example.models.ProxyCake'>",  # This is a proxy model
                "<class 'example.models.User'>",
                "<class 'example.models.Statement'>",
                "<class 'example.models.QuiltSession'>",
                "<class 'example.models.Report'>",
                "<class 'example.models.DetailedReport'>",
                "<class 'example.models.Pudding'>",
            ],
        )


class GetAllMirroredModels(ShardingTestCase):
    available_apps = ['example']

    def test_with_override(self):
        """
        Case: Call get_all_mirrored_models, while the settings override models.
        Expected: Only the MirroredUser, OrganizationShard and Shard models returned. Type is overridden,
                  the rest is sharded and/or not mirrored.
        Note: System test
        """
        with override_settings(QUILT_DB={'OVERRIDE_SHARDING_MODE': {('example', 'type'): ShardingMode.SHARDED}}):
            self.assertCountEqual(get_all_mirrored_models(), [MirroredUser, OrganizationShard, Shard])

    def test_with_override_archived_models(self):
        """
        Case: Call get_all_mirrored_models, while the settings override no longer existing models.
        Expected: Only the Type, OrganizationShard and Shard models to be returned. The rest is sharded and/or not
                  mirrored.
        Note: System test
        """
        with override_settings(
            QUILT_DB={
                'OVERRIDE_SHARDING_MODE': {
                    ('non-existing-app', 'long_gone_model'): ShardingMode.MIRRORED,
                    ('example', 'mirroreduser'): ShardingMode.SHARDED,
                }
            }
        ):
            self.assertCountEqual(get_all_mirrored_models(), [Type, OrganizationShard, Shard])

    @mock.patch('djanquiltdb.utils.get_model_sharding_mode')
    def test(self, mock_get_model_sharding_mode):
        """
        Case: Call get_all_mirrored_models.
        Expected: get_model_sharding_mode called for each model that's not a proxy model, nor an auto-created model.
        """
        get_all_mirrored_models()
        mock_get_model_sharding_mode.assert_has_calls(
            [mock.call(model) for model in apps.get_models() if not model._meta.proxy], any_order=True
        )

    def test_include_auto_created_result(self):
        """
        Case: Call get_all_mirrored_models, with include_auto_created.
        Expected: All mirrored models and auto created models to be returned (but not proxy models).
        """
        # We compare it string based, since we cannot import the auto created fields as classes.
        result = [str(model) for model in get_all_mirrored_models(include_auto_created=True)]
        self.assertCountEqual(
            result,
            [
                "<class 'example.models.Shard'>",
                "<class 'example.models.OrganizationShard'>",
                "<class 'example.models.Type'>",
                "<class 'example.models.MirroredUser'>",
                "<class 'example.models.MirroredUser_type'>",
            ],
        )  # This is an auto-created model

    def test_include_proxy_models(self):
        """
        Case: Call get_all_mirrored_models, including proxy models.
        Expected: All mirrored proxy and non-proxy models returned (but not auto-created models).
        """
        # We compare it string based, since we cannot import the auto created fields as classes.
        result = [str(model) for model in get_all_mirrored_models(include_proxy=True)]
        self.assertCountEqual(
            result,
            [
                "<class 'example.models.Shard'>",
                "<class 'example.models.OrganizationShard'>",
                "<class 'example.models.Type'>",
                "<class 'example.models.MirroredUser'>",
                "<class 'example.models.ProxyMirroredUser'>",
            ],
        )  # This is a proxy model


class GetAllPublicModels(ShardingTestCase):
    available_apps = ['example']

    def test_with_override(self):
        """
        Case: Call get_all_public_models, while the settings override models.
        Expected: Only SuperType, MirroredModel and CakeType models to be returned.
                  The rest is sharded and/or not mirrored.
        Note: System test
        """
        with override_settings(QUILT_DB={'OVERRIDE_SHARDING_MODE': {('example', 'mirroreduser'): ShardingMode.PUBLIC}}):
            self.assertCountEqual(
                get_all_public_models(), [SuperType, MirroredUser, CakeType, CoatingType, Flavour, Sprinkle]
            )

    def test_with_override_archived_models(self):
        """
        Case: Call get_all_public_models, while the settings override no longer existing models.
        Expected: Only SuperType and CakeType models to be returned. The rest is sharded and/or not mirrored,
                  or does not exist.
        Note: System test
        """
        with override_settings(
            QUILT_DB={
                'OVERRIDE_SHARDING_MODE': {
                    ('non-existing-app', 'long_gone_model'): ShardingMode.PUBLIC,
                    ('example', 'type'): ShardingMode.SHARDED,
                }
            }
        ):
            self.assertCountEqual(get_all_public_models(), [SuperType, CakeType, CoatingType, Flavour, Sprinkle])

    @mock.patch('djanquiltdb.utils.get_model_sharding_mode')
    def test(self, mock_get_model_sharding_mode):
        """
        Case: Call get_all_public_models.
        Expected: get_model_sharding_mode called for each model that's not a proxy model, nor an auto-created model.
        """
        get_all_public_models()
        mock_get_model_sharding_mode.assert_has_calls(
            [mock.call(model) for model in apps.get_models() if not model._meta.proxy], any_order=True
        )

    def test_include_auto_created_result(self):
        """
        Case: Call get_all_public_models, with include_auto_created.
        Expected: All mirrored models and auto created models to be returned (but not proxy models).
        """
        # We compare it string based, since we cannot import the auto created fields as classes.
        result = [str(model) for model in get_all_public_models(include_auto_created=True)]
        self.assertCountEqual(
            result,
            [
                "<class 'example.models.SuperType'>",
                "<class 'example.models.CakeType'>",
                "<class 'example.models.CoatingType'>",
                "<class 'example.models.CoatingType_super_type'>",
                "<class 'example.models.Flavour'>",
                "<class 'example.models.Sprinkle'>",
            ],
        )  # This is an auto-created model

    def test_include_proxy_models(self):
        """
        Case: Call get_all_public_models, including proxy models.
        Expected: All mirrored proxy and non-proxy models returned (but not auto-created models).
        """
        # We compare it string based, since we cannot import the auto created fields as classes.
        self.maxDiff = None
        result = [str(model) for model in get_all_public_models(include_proxy=True)]
        self.assertCountEqual(
            result,
            [
                "<class 'example.models.SuperType'>",
                "<class 'example.models.CakeType'>",
                "<class 'example.models.ProxyCakeType'>",  # This is a proxy model
                "<class 'example.models.CoatingType'>",
                "<class 'example.models.Flavour'>",
                "<class 'example.models.Sprinkle'>",
            ],
        )


class GetAllPublicSchemaModels(ShardingTestCase):
    available_apps = ['example']

    def test_with_override(self):
        """
        Case: Call get_all_public_schema_models, while the settings override models.
        Expected: Only Shard, OrganizationShard, SuperType, MirroredModel and CakeType models to be returned.
                  The rest is sharded and/or not mirrored.
        Note: System test
        """
        with override_settings(QUILT_DB={'OVERRIDE_SHARDING_MODE': {('example', 'mirroreduser'): ShardingMode.PUBLIC}}):
            self.assertCountEqual(
                get_all_public_schema_models(),
                [Shard, OrganizationShard, SuperType, Type, MirroredUser, CakeType, CoatingType, Flavour, Sprinkle],
            )

    def test_with_override_archived_models(self):
        """
        Case: Call get_all_public_schema_models, while the settings override no longer existing models.
        Expected: Only Shard, OrganizationShard, SuperType, MirroredUser and CakeType models to be returned.
                  The rest is sharded and/or not mirrored, or does not exist.
        Note: System test
        """
        with override_settings(
            QUILT_DB={
                'OVERRIDE_SHARDING_MODE': {
                    ('non-existing-app', 'long_gone_model'): ShardingMode.PUBLIC,
                    ('example', 'type'): ShardingMode.SHARDED,
                }
            }
        ):
            self.assertCountEqual(
                get_all_public_schema_models(),
                [Shard, OrganizationShard, SuperType, MirroredUser, CakeType, CoatingType, Flavour, Sprinkle],
            )

    @mock.patch('djanquiltdb.utils.get_model_sharding_mode')
    def test(self, mock_get_model_sharding_mode):
        """
        Case: Call get_all_public_schema_models.
        Expected: get_model_sharding_mode called for each model that's not a proxy model, nor an auto-created model.
        """
        get_all_public_schema_models()
        mock_get_model_sharding_mode.assert_has_calls(
            [mock.call(model) for model in apps.get_models() if not model._meta.proxy], any_order=True
        )

    def test_include_auto_created_result(self):
        """
        Case: Call get_all_public_schema_models, with include_auto_created.
        Expected: All public and mirrored models and auto created models to be returned (but not proxy models).
        """
        # We compare it string based, since we cannot import the auto created fields as classes.
        result = [str(model) for model in get_all_public_schema_models(include_auto_created=True)]
        self.assertCountEqual(
            result,
            [
                "<class 'example.models.Shard'>",
                "<class 'example.models.OrganizationShard'>",
                "<class 'example.models.Type'>",
                "<class 'example.models.MirroredUser'>",
                "<class 'example.models.MirroredUser_type'>",
                "<class 'example.models.SuperType'>",
                "<class 'example.models.CakeType'>",
                "<class 'example.models.CoatingType'>",
                "<class 'example.models.CoatingType_super_type'>",
                "<class 'example.models.Flavour'>",
                "<class 'example.models.Sprinkle'>",
            ],
        )  # This is an auto-created model

    def test_include_proxy_models(self):
        """
        Case: Call get_all_public_schema_models, including proxy models.
        Expected: All public and mirrored proxy and non-proxy models returned (but not auto-created models).
        """
        # We compare it string based, since we cannot import the auto created fields as classes.
        self.maxDiff = None
        result = [str(model) for model in get_all_public_schema_models(include_proxy=True)]
        self.assertCountEqual(
            result,
            [
                "<class 'example.models.Shard'>",
                "<class 'example.models.OrganizationShard'>",
                "<class 'example.models.Type'>",
                "<class 'example.models.MirroredUser'>",
                "<class 'example.models.ProxyMirroredUser'>",
                "<class 'example.models.SuperType'>",
                "<class 'example.models.CakeType'>",
                "<class 'example.models.ProxyCakeType'>",  # This is a proxy model
                "<class 'example.models.CoatingType'>",
                "<class 'example.models.Flavour'>",
                "<class 'example.models.Sprinkle'>",
            ],
        )


class GetAllDatabases(SimpleTestCase):
    @override_settings(DATABASES={'default': 'some connection', 'space': 'some otherworldly connection'})
    def test(self):
        """
        Case: Call get_all_databases.
        Expected: Names of the databases defined in the settings.
        """
        self.assertCountEqual(get_all_databases(), ['default', 'space'])


class GetConnectionAliasTestCase(ShardingTestCase):
    def test_string(self):
        """
        Case: Call get_connection_alias with a node name as string.
        Expected: The given string returned.
        """
        self.assertEqual(get_connection_alias('Magnetism'), 'Magnetism')

    def test_django_default_connection_proxy(self):
        """
        Case: Call get_connection_alias with a django connection.
        Expected: The connection's db_alias returned.
        """
        self.assertEqual(connection.db_alias, 'default')  # Make sure the value we give is what me expect
        self.assertEqual(get_connection_alias(connection), 'default')

    def test_sharding_default_connection_proxy(self):
        """
        Case: Call get_connection_alias with a sharding connection.
        Expected: The connection's db_alias.node_name returned.
        """
        self.assertEqual(connection.db_alias, 'default')  # Make sure the value we give is what me expect
        self.assertEqual(get_connection_alias(connection), 'default')

    def test_shard_options_connection_proxy(self):
        """
        Case: Call get_connection_alias with a sharding connection.
        Expected: The connection's db_alias.node_name returned.
        """
        with use_shard(node_name='other', schema_name='public'):
            self.assertEqual(connection.db_alias.node_name, 'other')  # Make sure the value we give is what me expect
            self.assertEqual(get_connection_alias(connection), 'other')

    def test_sharding_database_wrapper(self):
        """
        Case: Call get_connection_alias with a sharding database wrapper.
        Expected: The wrappers's alias returned.
        """
        with ShardOptions(node_name='other', schema_name='public').use() as env:
            self.assertEqual(env.connection.alias, 'other')  # Make sure the value we give is what me expect
            self.assertEqual(get_connection_alias(env.connection), 'other')

    def test_un_obtainable_connection_name(self):
        """
        Case: Call get_connection_alias something it cannot retrieve the name of.
        Expected: ValueError raised.
        """
        with self.assertRaises(ValueError):
            get_connection_alias(None)


class ForEachShardTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        with mock.patch('djanquiltdb.utils.create_schema_on_node'):
            self.shard1 = Shard.objects.create(
                alias='test_sharding', schema_name='test_schema', node_name='default', state=State.ACTIVE
            )

    def repeatable_function(self, shard=None, shard_id=None, **kwargs):
        if shard:
            self.shards.append((shard, kwargs) if kwargs else shard)
        else:
            self.shards.append((shard_id, kwargs) if kwargs else shard_id)

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    def test_for_each_shard(self):
        """
        Case: Call self.repeatable_function for every shard.
        Expected: Function is called for every shard.
        """
        self.shards = []
        for_each_shard(self.repeatable_function)
        self.assertEqual(self.shards, [self.shard1])

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    def test_for_each_shard_with_kwargs(self):
        """
        Case: Call self.repeatable_function for every shard and pass keyword arguments to the function.
        Expected: Function is called for every shard and is called with the keyword arguments provided.
        """
        self.shards = []
        for_each_shard(self.repeatable_function, kwargs={'organization_id': 1})
        self.assertEqual(self.shards, [(self.shard1, {'organization_id': 1})])

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    def test_for_each_shard_as_id(self):
        """
        Case: Call self.repeatable_function for every shard and get shards as ids.
        Expected: Function is called for every shard and the shard id is passed as a argument.
        """
        self.shards = []
        for_each_shard(self.repeatable_function, as_id=True)
        self.assertEqual(self.shards, [self.shard1.id])

    def test_for_each_shard_inside_a_shard_without_the_public_schema(self):
        """
        Case: Call self.repeatable_function for every shard, inside a shard that is entered without the public schema.
        Expected: Function is called for every shard in the shard registry.
        """
        create_template_schema('default')
        entered_shard = Shard.objects.create(
            alias='entered_shard', schema_name='test_entered_schema', node_name='default', state=State.ACTIVE
        )

        self.shards = []
        with use_shard(entered_shard, include_public=False):
            for_each_shard(self.repeatable_function)

        self.assertCountEqual(self.shards, [self.shard1, entered_shard])


@mock.patch('djanquiltdb.utils.get_all_databases', return_value=['default', 'other'])
class ForEachNodeTestCase(SimpleTestCase):
    def repeatable_function(self, node_name=None, **kwargs):
        return node_name, kwargs

    def test_for_each_node(self, mock_get_all_databases):
        """
        Case: Call self.repeatable_function for every node.
        Expected: Function is called for every node.
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


class WriteToEveryNodeSystemTestCase(OverrideMirroredRoutingMixin, ShardingTransactionTestCase):
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

    @mock.patch('djanquiltdb.decorators.transaction_for_every_node')
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


class TransactionForNodesTestCase(OverrideMirroredRoutingMixin, ShardingTransactionTestCase):
    @mock.patch('djanquiltdb.utils.Atomic.__init__')
    @mock.patch('djanquiltdb.utils.Atomic.__enter__', autospec=True)
    @mock.patch('djanquiltdb.utils.Atomic.__exit__', autospec=True)
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

    def test_a_commit_failure_still_exits_every_node(self):
        """
        Case: Use transaction_for_nodes across both nodes and violate a deferred constraint on the node whose
              transaction commits first, so its commit itself raises.
        Expected: The error surfaces, but the other node's transaction is closed as well.
        """
        with self.assertRaises(IntegrityError):
            with transaction_for_nodes(nodes=['default', 'other']):
                with use_shard(node_name='default', schema_name='public'):
                    Type.objects.create(name='survivor')
                with use_shard(node_name='other', schema_name='public') as env:
                    env.connection.cursor().execute(
                        'INSERT INTO "{}" (name, super_id) VALUES (%s, %s)'.format(Type._meta.db_table),
                        ['broken', 999999],
                    )

        self.assertFalse(connections['default'].in_atomic_block)
        self.assertFalse(connections['other'].in_atomic_block)

        with use_shard(node_name='default', schema_name='public'):
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

        with self.assertRaises(OperationalError):
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
        Case: use @transaction_for_nodes with lock_models given.
        Expected: SQL command to lock the models is executed.
        """

        # mocking a cursor.execute is a bit of a faff
        with mock.patch('djanquiltdb.utils.connections') as mock_connections:
            mock_connection = mock_connections.__getitem__ = mock.Mock()
            mock_cursor = mock_connection.return_value.cursor = mock.Mock()
            mock_execute = mock_cursor.return_value.execute = mock.Mock()

            with transaction_for_nodes(
                nodes=['default', 'other'], lock_models=((Type, 'ROW SHARE'), (SuperType, 'SHARE'))
            ):
                pass

            self.assertEqual(mock_execute.call_count, 4)  # we test with two databases and 2 tables
            mock_execute.assert_any_call('LOCK TABLE "{}" IN {} MODE'.format('example_type', 'ROW SHARE'))
            mock_execute.assert_any_call('LOCK TABLE "{}" IN {} MODE'.format('example_supertype', 'SHARE'))

    def test_with_table_lock(self):
        """
        Case: use @transaction_for_nodes with lock_models given.
        Expected: The models to be locked and other writes outside the transaction to be blocked.
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
                    con.cursor().execute('LOCK TABLE "example_type" IN ACCESS EXCLUSIVE MODE NOWAIT')
                    Type.objects.create(name='test_type')
                con.close()

        with transaction_for_nodes(nodes=['default', 'other'], lock_models=((Type, 'ACCESS EXCLUSIVE'),)):
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
    @mock.patch('djanquiltdb.utils.transaction_for_nodes.__init__')
    @mock.patch('djanquiltdb.utils.transaction_for_nodes.__enter__', mock.Mock)
    @mock.patch('djanquiltdb.utils.transaction_for_nodes.__exit__', mock.Mock)
    @mock.patch('djanquiltdb.utils.get_all_databases', return_value=['default', 'other'])
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
    @mock.patch('djanquiltdb.decorators.transaction_for_every_node')
    @mock.patch('djanquiltdb.decorators.use_shard')
    @mock.patch('djanquiltdb.decorators.get_all_databases', return_value=['sina', 'rose', 'maria'])
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

    @mock.patch('djanquiltdb.decorators.transaction_for_every_node')
    @mock.patch('djanquiltdb.decorators.use_shard')
    @mock.patch('djanquiltdb.decorators.get_all_databases', return_value=['sina', 'rose', 'maria'])
    def test_write_to_every_node_return_value(self, mock_get_all_databases, mock_use_shard, mock_transaction):
        """
        Case: Use the @atomic_write_to_every_node, and call the decorated function with an argument.
        Expected: The function gives back a dict with the node_name as keys and the return value as their values.
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
        self.assertEqual(
            {
                'sina': ('Firestone', 'sina'),
                'rose': ('Firestone', 'rose'),
                'maria': ('Firestone', 'maria'),
            },
            return_value,
        )

    def test_decorated_with(self):
        """
        Case: Check if the function is decorator with a specific decorator.
        Expected: The function is decorated and called with the expected argument.
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
        Then perform the normal ShardingTransactionTestCase clean_up.
        """
        if Shard.objects.filter(schema_name='test_other_schema').exists():
            with use_shard(node_name='default', schema_name='test_other_schema'):
                move_model_to_schema(
                    model=Type, node_name='default', from_schema_name='test_other_schema', to_schema_name='public'
                )

    def setUp(self):
        super().setUp()

        self.addCleanup(self.clean_up)

    @mock.patch('django.core.management.get_commands', mock.Mock(return_value={'migrate': 'djanquiltdb'}))
    def test(self):
        """
        Case: Move the Type model over from public to a shard
        Expected: The Type model to be moved. All data should remain in tact.
        """
        create_template_schema('default')
        other_shard = Shard.objects.create(
            alias='other', node_name='default', schema_name='test_other_schema', state=State.ACTIVE
        )

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
            # We have to do it with a new query, rather than `refresh_from_db()` since this presents a very unique
            # circumstance:
            # We've created the organization and user objects in a normal `use_shard(other_shard)` context.
            # This context is saved on the model instances retrieved. When using `refresh_from_db` it will reuse those
            # ShardingOptions to fetch the data from the correct place. However, it will query the shard details,
            # which is cannot get since we are in a specific non-public-schema context.
            # Fetching the object in a fresh query will sidestep this specific issue. Hurray for complexity!
            # This behavior is introduced in SHARDING-84.
            Organization.objects.get(id=organization.id)
            User.objects.get(id=user.id)

        # Confirm Data integrity, now they are refreshed
        self.assertEqual(user.type_id, 3)
        self.assertEqual(user.type.id, 3)
        self.assertEqual(type.id, 3)
        self.assertEqual(type.name, 'tesla')
        self.assertEqual(type.super_id, 4)
        self.assertEqual(supertype.id, 4)

        with use_shard(shard=other_shard):
            # Public data is accessible
            type = Type.objects.get(id=type.id)
            self.assertEqual(type.super.id, 4)


class MoveModelToExistingSchemaTestCase(ShardingTransactionTestCase):
    available_apps = ['example']

    @mock.patch('django.core.management.get_commands', mock.Mock(return_value={'migrate': 'djanquiltdb'}))
    def test(self):
        """
        Case: Move a schema to a shard where it already resides.
        Expected: move_model_to_schema to raise an error.
        """
        create_template_schema('default')
        another_schema = Shard.objects.create(
            alias='another', node_name='default', schema_name='test_another_schema', state=State.ACTIVE
        )

        # The User table is already on the sharded schema.
        with self.assertRaises(ProgrammingError):
            move_model_to_schema(model=User, node_name='default', to_schema_name=another_schema.schema_name)


class SchemaExistsTestCase(ShardingTestCase):
    def test_schema_exists(self):
        """
        Case: Call schema_exists while knowing that schema exists.
        Expected: Returns True.
        """
        create_template_schema('default')
        self.assertTrue(schema_exists('default', get_template_name()))

    def test_schema_does_not_exists(self):
        """
        Case: Call schema_exists while knowing that schema does not exists.
        Expected: Returns False.
        """
        self.assertFalse(schema_exists('default', get_template_name()))


class DisableSignalsTestCase(ShardingTestCase):
    def test(self):
        """
        Case: Wrap an instance.save() call in disable_signals.
        Expected: Disconnected signals not called.
        Note: Happy flow system test
        """
        fake_pre_save_signal = mock.Mock()
        fake_post_save_signal = mock.Mock()
        pre_save.connect(fake_pre_save_signal, sender='example.Organization')
        post_save.connect(fake_post_save_signal, sender='example.Organization')

        create_template_schema()
        self.shard = Shard.objects.create(
            alias='light', node_name='default', schema_name='test_light', state=State.ACTIVE
        )

        with use_shard(self.shard):
            organization = Organization(name='EastShade')

            with self.subTest('Signal enabled'):
                organization.save()
                self.assertEqual(fake_pre_save_signal.call_count, 1)
                self.assertEqual(fake_post_save_signal.call_count, 1)

            with self.subTest('Signal disabled'):
                fake_pre_save_signal.reset_mock()
                fake_post_save_signal.reset_mock()

                with disable_signals([post_save]):
                    organization.name = 'WestShade'
                    organization.save()

                organization.refresh_from_db()
                self.assertEqual(organization.name, 'WestShade')

                self.assertEqual(fake_pre_save_signal.call_count, 1)
                self.assertEqual(fake_post_save_signal.call_count, 0)

            with self.subTest('Signal reenabled'):
                fake_pre_save_signal.reset_mock()
                fake_post_save_signal.reset_mock()

                organization.name = 'NorthShade'
                organization.save()
                self.assertEqual(fake_pre_save_signal.call_count, 1)
                self.assertEqual(fake_post_save_signal.call_count, 1)

    def test_init_without_arguments(self):
        """
        Case: Call disable_signals.__init__ without any arguments.
        Expected: Resulting object to have an empty dict as stashed_signals and all common django signals in
                  disabled_signals.
        """
        manager = disable_signals()

        self.assertEqual(manager.stashed_signals, {})
        self.assertEqual(
            manager.disabled_signals,
            [pre_init, post_init, pre_save, post_save, pre_delete, post_delete, pre_migrate, post_migrate],
        )

    def test_init_with_argument(self):
        """
        Case: Call disable_signals.__init__ with a signal as argument.
        Expected: Resulting object to have an empty dict as stashed_signals and only the given signal in
                  disabled_signals.
        """
        manager = disable_signals([pre_save])

        self.assertEqual(manager.stashed_signals, {})
        self.assertEqual(manager.disabled_signals, [pre_save])

    def test_enter(self):
        """
        Case: Call disable_signals.__enter__.
        Expected: Disconnect called for each signal in disabled_signals.
        """
        manager = disable_signals([pre_save])

        with mock.patch.object(manager, 'disconnect') as mock_disconnect:
            manager.__enter__()

        self.assertEqual(mock_disconnect.call_count, 1)

    def test_exit(self):
        """
        Case: Call disable_signals.__exit__.
        Expected: Reconnect called for each signal in stashed_signals.
        """
        manager = disable_signals([pre_save])
        manager.stashed_signals = {pre_save: 'some_receiver'}

        with mock.patch.object(manager, 'reconnect') as mock_disconnect:
            manager.__exit__(None, None, None)

        self.assertEqual(mock_disconnect.call_count, 1)

    def test_disconnect(self):
        """
        Case: Call disable_signals.disconnect with a given signal.
        Expected: Signal's original receivers to be stored in stashed_signals and then emptied, the signal's lock and
                  clear functions called.
        """
        manager = disable_signals()

        fake_signal = mock.Mock()
        fake_signal.lock = mock.Mock()
        fake_signal.lock.__enter__ = mock.Mock()
        fake_signal.lock.__exit__ = mock.Mock()
        fake_signal.receivers = ['some_receiver']
        manager.disconnect(fake_signal)

        self.assertEqual(manager.stashed_signals, {fake_signal: ['some_receiver']})
        self.assertEqual(fake_signal.receivers, [])

        fake_signal.lock.__enter__.assert_called_once_with()
        fake_signal._clear_dead_receivers.assert_called_once_with()
        fake_signal.sender_receivers_cache.clear.assert_called_once_with()

    def test_reconnect(self):
        """
        Case: Call disable_signals.reconnect with a given signal.
        Expected: Only the given signal's original receivers to be restored and removed from stashed_signals.
        """
        manager = disable_signals()

        fake_signal_1 = mock.Mock()
        fake_signal_1.lock = mock.Mock()
        fake_signal_1.lock.__enter__ = mock.Mock()
        fake_signal_1.lock.__exit__ = mock.Mock()
        fake_signal_1.receivers = []

        fake_signal_2 = mock.Mock()
        fake_signal_2.lock = mock.Mock()
        fake_signal_2.lock.__enter__ = mock.Mock()
        fake_signal_2.lock.__exit__ = mock.Mock()
        fake_signal_2.receivers = []

        manager.stashed_signals = {fake_signal_1: ['some_receiver'], fake_signal_2: ['some_other_receiver']}
        manager.reconnect(fake_signal_1)

        # fake_signal_1 locked, cleared and restored
        self.assertEqual(fake_signal_1.receivers, ['some_receiver'])
        fake_signal_1.lock.__enter__.assert_called_once_with()
        fake_signal_1._clear_dead_receivers.assert_called_once_with()
        fake_signal_1.sender_receivers_cache.clear.assert_called_once_with()

        # fake_signal_2 left untouched
        self.assertEqual(manager.stashed_signals, {fake_signal_2: ['some_other_receiver']})
        self.assertEqual(fake_signal_2.receivers, [])
        self.assertFalse(fake_signal_2.lock.__enter__.called)
        self.assertFalse(fake_signal_2._clear_dead_receivers.called)
        self.assertFalse(fake_signal_2.sender_receivers_cache.clear.called)


def fake_transaction_exit(self, exc_type, exc_value, traceback):
    """
    Mock the execute function. After some executions, disconnect the database.
    We use a mutable argument to have keep a state between calls.
    """
    # Content of mocked __exit__ function
    import django.db.transaction

    for database in reversed(self.databases):
        self.using = database

        if database == 'default':
            raise ProgrammingError()

        django.db.transaction.Atomic.__exit__(self, exc_type, exc_value, traceback)


class TransactionUtilTestCase(ShardingTransactionTestCase):
    def test_sharding_transaction(self):
        """
        Case: Create an object on a non-primary node while in a sharding transaction transaction.
              Raise an error so the transaction rolls back.
        Expected: No object committed since the transaction is created for the correct node.
        Note: Vanilla it would _always_ be created on the default node.
        """
        create_template_schema('other')
        other_shard = Shard.objects.create(
            alias='other', node_name='other', schema_name='test_other_schema', state=State.ACTIVE
        )

        with use_shard(other_shard):
            self.assertFalse(Organization.objects.exists())

            with self.assertRaises(ValueError):
                with transaction.atomic():
                    Organization.objects.create(name='Seafarer')
                    raise ValueError()

            # Objects does indeed not exist
            self.assertFalse(Organization.objects.filter(name='Seafarer').exists())

    def test_sharding_transaction_mirrored_model(self):
        """
        Case: Create an object on both the primary and non-primary node while in a sharding transaction transaction.
              Raise an error so the transaction rolls back.
        Expected: No objects committed, since the transaction is created on both the nodes.
        """
        create_template_schema('default')
        create_template_schema('other')
        other_shard = Shard.objects.create(
            alias='other', node_name='other', schema_name='test_other_schema', state=State.ACTIVE
        )

        with use_shard(other_shard):
            self.assertFalse(Organization.objects.exists())

            with self.assertRaises(ValueError):
                with transaction.atomic():  # Cascading transaction
                    # Shard write queries are always routed to the primary node. So it would escape our transaction if
                    # it wasn't cascading with primary as well
                    Shard.objects.create(alias='Windswept Wastes', node_name='default', schema_name='wastes', state='A')
                    Organization.objects.create(name='Seafarer')
                    raise ValueError()

            # Objects does indeed not exist
            self.assertFalse(Organization.objects.filter(name='Seafarer').exists())
            # Shard is never written to the other node, since we do not have replication set up.
            self.assertFalse(Shard.objects.filter(alias='Windswept Wastes').exists())

        with use_shard(node_name='default', schema_name='public'):
            # Shard object also do not exist on the default shard
            self.assertFalse(Shard.objects.filter(alias='Windswept Wastes').exists())

    def test_sharding_transaction_failure_on_outer_return_trip(self):
        """
        Case: Create an object on both the primary and non-primary node while in a sharding transaction transaction.
              No errors raised during transaction, but on upon the closing of the outer transaction.
        Expected: Objects created in the inner transaction to be submitted to db,
                  the ones in the outer transaction are not.
                  Error passed through to main thread.
        Note: This is not behavior we want, but an affirmation on what _is_
        """
        create_template_schema('default')
        create_template_schema('other')
        other_shard = Shard.objects.create(
            alias='other', node_name='other', schema_name='test_other_schema', state=State.ACTIVE
        )

        with use_shard(other_shard):
            self.assertFalse(Organization.objects.exists())

            with self.assertRaises(ProgrammingError):
                with mock.patch(
                    'djanquiltdb.utils.transaction_for_nodes.__exit__', side_effect=fake_transaction_exit, autospec=True
                ):
                    with transaction.atomic():  # Cascading transaction
                        # Shard write queries are always routed to the primary node.
                        # So it will be wrapped in then outer transaction
                        Shard.objects.create(
                            alias='Windswept Wastes', node_name='default', schema_name='wastes', state='A'
                        )
                        Organization.objects.create(name='Seafarer')

            # Cleanup. The connection failed to close properly, since an error occurred.
            with use_shard(node_name='default', schema_name='public') as env:
                env.connection.connection = None

            self.assertTrue(Organization.objects.filter(name='Seafarer').exists())
            # Shard is never written to the other node, since we do not have replication set up.
            self.assertFalse(Shard.objects.filter(alias='Windswept Wastes').exists())

        with use_shard(node_name='default', schema_name='public') as env:
            # Shard object does not exist exist on the default shard, that transaction was not properly comitted.
            self.assertFalse(Shard.objects.filter(alias='Windswept Wastes').exists())

    def test_colliding_inside_transaction(self):
        """
        Case: Within a transaction create an object on the primary node before triggering a collision on the other node.
        Expected: All objects created in that transaction to be reverted.
        """
        create_template_schema('default')
        create_template_schema('other')
        other_shard = Shard.objects.create(
            alias='other', node_name='other', schema_name='test_other_schema', state=State.ACTIVE
        )

        with use_shard(other_shard):
            self.assertFalse(Organization.objects.exists())
            org = Organization.objects.create(name='Seafarer')

            with self.assertRaises(IntegrityError):
                with transaction.atomic():
                    # Create an object on the primary node
                    Shard.objects.create(alias='Windswept Wastes', node_name='default', schema_name='wastes', state='A')

                    # Create object that collides with one that has been created earlier
                    Organization.objects.create(id=org.id, name='Seafarer')

        # Object created on the primary node should be reverted
        self.assertFalse(Shard.objects.filter(alias='Windswept Wastes').exists())

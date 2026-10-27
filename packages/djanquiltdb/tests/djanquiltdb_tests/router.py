from collections import OrderedDict
from threading import Event, Thread
from unittest import mock

from django.apps import apps
from django.db import DEFAULT_DB_ALIAS, ProgrammingError, connections, models
from django.test import SimpleTestCase, override_settings

from djanquiltdb import PRIMARY_NODE, State, single_node_unsharded
from djanquiltdb.decorators import mirrored_model, override_sharding_setting, public_model, sharded_model
from djanquiltdb.options import ShardOptions
from djanquiltdb.router import (
    DynamicDbRouter,
    _active_connection,
    _placement_is_node_scoped,
    get_active_connection,
    node_scoped_placement,
    set_active_connection,
)
from djanquiltdb.utils import (
    OverrideShardingModeWithLowercaseKeys,
    ShardingMode,
    create_schema_on_node,
    create_template_schema,
    migrate_schema,
    use_shard,
)
from djanquiltdb_tests import ShardingTestCase, ShardingTransactionTestCase
from example.models import (
    CakeType,
    DefaultUser,
    MirroredUser,
    Organization,
    OrganizationShard,
    Shard,
    Statement,
    SuperType,
    Type,
    Unrelated,
)


@sharded_model()
class DummyShardedModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'djanquiltdb'
        abstract = True


@mirrored_model()
class DummyMirroredModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'djanquiltdb'
        abstract = True


class DummyPublicModelManager(models.Manager):
    def get_by_natural_key(self, *args, **kwargs):
        return self.get(*args, **kwargs)


@public_model()
class DummyPublicModel(models.Model):
    test_model = True

    objects = DummyPublicModelManager()

    class Meta:
        app_label = 'djanquiltdb'
        abstract = True
        unique_together = [[]]

    def natural_key(self):
        return ()


class DummyNonShardedModel(models.Model):
    test_model = True

    class Meta:
        app_label = 'djanquiltdb'
        abstract = True


class ActiveConnectionTestCase(ShardingTransactionTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        self.shard1 = Shard.objects.create(state=State.ACTIVE, alias='test1', schema_name='test1', node_name='default')
        self.shard2 = Shard.objects.create(state=State.ACTIVE, alias='test2', schema_name='test2', node_name='default')

        self.active_connections = {}

    def test_get_active_connection_none_set(self):
        """
        Case: Have no active connection set
        Expected: get_active_connection() returns the DEFAULT_DB_ALIAS
        """
        delattr(_active_connection, 'connection')
        self.assertEqual(get_active_connection(), DEFAULT_DB_ALIAS)

    def test_get_active_connection(self):
        """
        Case: Get the result of get_active_connection()
        Expected: Returns the value of _active_connection.connection
        """
        sentinel = object()

        _active_connection.connection = sentinel
        self.assertIs(get_active_connection(), sentinel)

    def test_get_active_connection_does_not_read_the_settings_while_a_connection_is_set(self):
        """
        Case: Get the result of get_active_connection() while an active connection is set.
        Expected: Returns the value of _active_connection.connection without reading QUILT_DB. QUILT_DB is read only for
                  the fallback, when no connection is set.
        """
        sentinel = object()

        _active_connection.connection = sentinel
        with mock.patch('djanquiltdb.router.get_primary_db_alias') as mock_get_primary_db_alias:
            self.assertIs(get_active_connection(), sentinel)

        mock_get_primary_db_alias.assert_not_called()

    def test_set_active_connection(self):
        """
        Case: Set the active connection
        Expected: _active_connection.connection is equal to what we set
        """
        sentinel = object()

        set_active_connection(sentinel)
        self.assertIs(get_active_connection(), sentinel)

    def test_multiple_threads(self):
        """
        Case: When having multiple threads, go to a different shard with a context manager in both the threads and
              save the current active connection at that time
        Expected: The active connection should be the one set by that current thread, and should not be shared among
                  threads
        """

        def run(shard, sync_event, wait_for):
            with shard.use() as env:
                sync_event.set()  # Notify that this thread is in a shard context now

                # Do not wait longer than 5 seconds for the other thread to be in the shard context.
                if not wait_for.wait(5):
                    return

                # Save the active connection at this moment for this thread, and save also it's expected value
                self.active_connections[shard] = (get_active_connection(), env.options)

        sync_event1 = Event()
        sync_event2 = Event()
        thread1 = Thread(target=run, kwargs={'shard': self.shard1, 'sync_event': sync_event1, 'wait_for': sync_event2})
        thread2 = Thread(target=run, kwargs={'shard': self.shard2, 'sync_event': sync_event2, 'wait_for': sync_event1})

        thread1.start()
        thread2.start()

        # Wait until the threads are finished
        thread1.join()
        thread2.join()

        self.assertEqual(len(self.active_connections), 2, 'Threads did not start properly')

        # Now assert that the active connection in the thread is the one we expect
        for shard, (active_connection, shard_options) in self.active_connections.items():
            self.assertEqual(
                active_connection,
                shard_options,
                'Active connection for {} is incorrect. Is the active connection thread safe?'.format(shard),
            )


class DynamicDbRouterTestCase(ShardingTestCase):
    def clean_models(self):
        apps.app_configs['djanquiltdb'].models = {}

    def setUp(self):
        super().setUp()

        self.addCleanup(self.clean_models)

        # Trick to register abstract models. We want them to be abstract or they will show up in other tests as well.
        apps.app_configs['djanquiltdb'].models = OrderedDict(
            [
                ('dummyshardedmodel', DummyShardedModel),
                ('dummymirroredmodel', DummyMirroredModel),
                ('dummypublicmodel', DummyPublicModel),
                ('dummynonshardedmodel', DummyNonShardedModel),
            ]
        )

        self.router = DynamicDbRouter()

    def test_db_for_read_while_not_set(self):
        """
        Case: Call db_for_read with the active_connection being None
        Expected: None returned
        """
        set_active_connection(None)
        self.assertIsNone(self.router.db_for_read(model=DummyNonShardedModel))

    def test_db_for_read_while_set(self):
        """
        Case: Call db_for_read with the active_connection being test_node
        Expected: Name of the correct node returned
        """
        set_active_connection('test_node')
        self.assertEqual(self.router.db_for_read(model=DummyNonShardedModel), 'test_node')

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    def test_db_for_read_for_routing_to_primary_db(self):
        """
        Case: Call db_for_read while the given model has routing_to_primary_db of True and False
        Expected: Directly routed to the primary db when set to True, normal flow when set to False.
        """
        set_active_connection('default')

        with self.subTest('route_to_primary_db=True'):
            with mock.patch.object(OrganizationShard, 'route_to_primary_db', True):
                self.assertEqual(self.router.db_for_read(model=OrganizationShard), 'other')

        with self.subTest('route_to_primary_db=False'):
            with mock.patch.object(OrganizationShard, 'route_to_primary_db', False):
                self.assertEqual(self.router.db_for_read(model=OrganizationShard), 'default')

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    @mock.patch('djanquiltdb.router.get_model_sharding_mode')
    @mock.patch('djanquiltdb.router.DynamicDbRouter.db_for_read', return_value='read_answer')
    def test_db_for_write(self, mock_db_for_read, mock_get_model_sharding_mode):
        """
        Case: Call db_for_write
        Expected: Given model's sharding mode checked, db_for_read called when it's not mirrored
        """
        with self.subTest('Mirrored model'):
            mock_get_model_sharding_mode.return_value = ShardingMode.MIRRORED

            self.assertEqual(self.router.db_for_write(model='some_model', hints='shard_options'), 'other')
            mock_get_model_sharding_mode.assert_called_once_with('some_model')
            self.assertEqual(mock_db_for_read.call_count, 1)

            mock_get_model_sharding_mode.reset_mock()
            mock_db_for_read.reset_mock()

        with self.subTest('Non mirrored model'):
            mock_get_model_sharding_mode.return_value = ShardingMode.SHARDED

            self.assertEqual(self.router.db_for_write(model='some_model'), 'read_answer')
            mock_get_model_sharding_mode.assert_called_once_with('some_model')
            mock_db_for_read.assert_called_once_with('some_model')

            mock_get_model_sharding_mode.reset_mock()
            mock_db_for_read.reset_mock()

    @override_sharding_setting('PRIMARY_DB_ALIAS', 'default')
    @mock.patch('djanquiltdb.router.DynamicDbRouter.db_for_read')
    def test_db_for_write_mirrored_model(self, mock_db_for_read):
        """
        Case: Call db_for_write for a mirrored model.
        Expected: db_for_read called and returned if its result is a ShardOptions for the primary connection.
                  Otherwise return the primary connection name.
        """
        with self.subTest('With primary node as ShardingObject in context'):
            mock_db_for_read.reset_mock()

            with use_shard(node_name='default', schema_name='public') as env:
                mock_db_for_read.return_value = env.options
                self.assertEqual(self.router.db_for_write(model=DummyMirroredModel), env.options)

            self.assertEqual(mock_db_for_read.call_count, 2)

        with self.subTest('With primary node as a string in context'):
            mock_db_for_read.reset_mock()

            with use_shard(node_name='default', schema_name='public'):
                mock_db_for_read.return_value = 'default'
                self.assertEqual(self.router.db_for_write(model=DummyMirroredModel), 'default')

            mock_db_for_read.assert_called_once_with(DummyMirroredModel)

        with self.subTest('With non-primary node as ShardingObject in context'):
            mock_db_for_read.reset_mock()

            with use_shard(node_name='other', schema_name='public') as env:
                mock_db_for_read.return_value = env.options
                self.assertEqual(self.router.db_for_write(model=DummyMirroredModel), 'default')

            mock_db_for_read.assert_called_once_with(DummyMirroredModel)

        with self.subTest('With non-primary node as string in context'):
            mock_db_for_read.reset_mock()

            with use_shard(node_name='other', schema_name='public'):
                mock_db_for_read.return_value = 'other'
                self.assertEqual(self.router.db_for_write(model=DummyMirroredModel), 'default')

            mock_db_for_read.assert_called_once_with(DummyMirroredModel)

    def test_db_for_write_while_not_set(self):
        """
        Case: Call db_for_write with the active_connection being None.
        Expected: None returned.
        """
        set_active_connection(None)
        self.assertIsNone(self.router.db_for_write(model=DummyNonShardedModel))

    def test_db_for_write_while_set(self):
        """
        Case: Call db_for_write with the active_connection being test_node
        Expected: Name of the correct node returned
        """
        set_active_connection('test_node')
        self.assertEqual(self.router.db_for_write(model=DummyNonShardedModel), 'test_node')

    def assert_save_table(self, mock_save_table, model, node_name, schema_name, options=True):
        mock_save_table.assert_called_once()
        call_args = mock_save_table.call_args[0]
        self.assertFalse(call_args[0])  # raw SQL
        self.assertEqual(call_args[1], model)  # Model class
        self.assertFalse(call_args[2])  # force_inert
        self.assertFalse(call_args[3])  # force_update
        if options:
            self.assertIsInstance(call_args[4], ShardOptions)  # using
            self.assertEqual(call_args[4].node_name, node_name)
            self.assertEqual(call_args[4].schema_name, schema_name)
        else:
            self.assertEqual(call_args[4], node_name)
        self.assertFalse(call_args[5])  # update_fields

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    @mock.patch('django.db.models.base.Model._save_table')
    def test_db_for_write_system_with_sharding_context(self, mock_save_table):
        """
        Case: Call a write for various types of models from inside a use_shard context.
        Expected: All writes to happen on the connection of the context, except mirrored models: those are routed to
                  the primary connection, and models on a single node, which are routed to their node. The mapping
                  model is mirrored, so it is written to the primary connection whether route_to_primary_db is True or
                  False.
        Note: System test
        """
        create_template_schema('default')
        self.test_shard = Shard.objects.create(
            alias='a_shard', node_name='default', schema_name='test_other_schema', state=State.ACTIVE
        )
        with use_shard(self.test_shard):
            with self.subTest('Model on a single node'):
                mock_save_table.reset_mock()
                obj = DefaultUser()
                obj.save()
                self.assert_save_table(mock_save_table, DefaultUser, 'default', None, options=False)

            with self.subTest('Sharded model'):
                mock_save_table.reset_mock()
                with mock.patch('djanquiltdb_tests.router.Organization._save_table') as mockery:
                    obj = Organization(name='example')
                    obj.save()
                    self.assert_save_table(mockery, Organization, 'default', 'test_other_schema')

            with self.subTest('Public model'):
                mock_save_table.reset_mock()
                obj = CakeType()
                obj.save()
                self.assert_save_table(mock_save_table, CakeType, 'default', 'test_other_schema')

            with self.subTest('Mirrored model'):
                mock_save_table.reset_mock()
                obj = MirroredUser()
                obj.save()
                self.assert_save_table(mock_save_table, MirroredUser, 'other', None, options=False)

            with self.subTest('route_to_primary_db=True'):
                mock_save_table.reset_mock()
                obj = OrganizationShard()
                obj.save()
                self.assert_save_table(mock_save_table, OrganizationShard, 'other', None, options=False)

            with self.subTest('route_to_primary_db=False'):
                mock_save_table.reset_mock()
                OrganizationShard.route_to_primary_db = False

                obj = OrganizationShard()
                obj.save()
                self.assert_save_table(mock_save_table, OrganizationShard, 'other', None, options=False)

                # cleanup
                OrganizationShard.route_to_primary_db = True

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    @mock.patch('django.db.models.base.Model._save_table')
    def test_db_for_write_system_without_sharding_context(self, mock_save_table):
        """
        Case: Call a write for various types of models, outside of a use_shard context.
        Expected: All writes to happen on the default connection -which is what is normally active in
                  test cases,- except mirrored models: those are routed to the primary connection, and models on a
                  single node, which are routed to their node, the default node here. The mapping model is mirrored,
                  so it is written to the primary connection whether route_to_primary_db is True or False.
        Note: System test.
        """
        with self.subTest('Model on a single node'):
            mock_save_table.reset_mock()
            obj = DefaultUser()
            obj.save()
            self.assert_save_table(mock_save_table, DefaultUser, 'default', None, options=False)

        with self.subTest('Sharded model'):
            mock_save_table.reset_mock()
            with mock.patch('djanquiltdb_tests.router.Organization._save_table') as mockery:
                obj = Organization(name='example')
                obj.save()
                self.assert_save_table(mockery, Organization, 'default', None, options=False)
            # Note: This will lead to a DB error if not mocked, since the table would not exist on the public schema

        with self.subTest('Public model'):
            mock_save_table.reset_mock()
            obj = CakeType()
            obj.save()
            self.assert_save_table(mock_save_table, CakeType, 'default', None, options=False)

        with self.subTest('Mirrored model'):
            mock_save_table.reset_mock()
            obj = MirroredUser()
            obj.save()
            self.assert_save_table(mock_save_table, MirroredUser, 'other', None, options=False)

        with self.subTest('route_to_primary_db=True'):
            mock_save_table.reset_mock()
            obj = OrganizationShard()
            obj.save()
            self.assert_save_table(mock_save_table, OrganizationShard, 'other', None, options=False)

        with self.subTest('route_to_primary_db=False'):
            mock_save_table.reset_mock()
            OrganizationShard.route_to_primary_db = False

            obj = OrganizationShard()
            obj.save()
            self.assert_save_table(mock_save_table, OrganizationShard, 'other', None, options=False)

            # cleanup
            OrganizationShard.route_to_primary_db = True

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    def test_db_for_write_end_to_end(self):
        """
        Case: Call a write for various types of models.
        Expected: All writes to happen on the connection of the context, except mirrored models: those are routed to
                  the primary connection.
        Note: System test.
        """
        create_template_schema('default')
        self.test_shard = Shard.objects.create(
            alias='a_shard', node_name='default', schema_name='test_other_schema', state=State.ACTIVE
        )

        with self.subTest('Model on a single node'):
            # In the example app, the Unrelated model is on the default node, and every query of it goes there.
            with use_shard(node_name='other', schema_name='public'):
                Unrelated.objects.create(name='disinterested party')
                self.assertTrue(Unrelated.objects.filter(name='disinterested party').exists())

            with use_shard(self.test_shard):
                self.assertTrue(Unrelated.objects.filter(name='disinterested party').exists())

            self.assertTrue(Unrelated.objects.using('default').filter(name='disinterested party').exists())

        with self.subTest('Sharded model'):
            # Only on the shard, the table does exist on public schemas.
            with use_shard(self.test_shard):
                Organization.objects.create(name='test_cake')

            with use_shard(node_name='default', schema_name='test_other_schema'):
                self.assertTrue(Organization.objects.filter(name='test_cake').exists())

        with self.subTest('Public model'):
            with use_shard(self.test_shard):
                SuperType.objects.create(name='test_cake')

            with use_shard(node_name='default', schema_name='public'):
                self.assertTrue(SuperType.objects.filter(name='test_cake').exists())

            with use_shard(node_name='other', schema_name='public'):
                self.assertFalse(SuperType.objects.filter(name='test_cake').exists())

        with self.subTest('Mirrored model'):
            with use_shard(self.test_shard):
                Type.objects.create(name='test_cake')

            with use_shard(node_name='default', schema_name='public'):
                self.assertFalse(Type.objects.filter(name='test_cake').exists())

            with use_shard(node_name='other', schema_name='public'):
                self.assertTrue(Type.objects.filter(name='test_cake').exists())

        with self.subTest('route_to_primary_db=True'):
            # The mapping model is mirrored, so the row is written to the primary node, which is default here.
            with override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'default'}):
                with use_shard(self.test_shard):
                    organization = Organization.objects.create(name='stay')

                with use_shard(node_name='other', schema_name='public'):
                    OrganizationShard.objects.create(shard=self.test_shard, organization_id=organization.id)

                with use_shard(node_name='default', schema_name='public'):
                    self.assertTrue(OrganizationShard.objects.filter(organization_id=organization.id).exists())

                # cleanup
                OrganizationShard.objects.all().delete()

        with self.subTest('route_to_primary_db=False'):
            # A mirrored model is written to the primary node also when route_to_primary_db is False, which only sends
            # the reads to the active connection.
            with override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'default'}):
                with use_shard(self.test_shard):
                    organization = Organization.objects.create(name='stay')

                with use_shard(node_name='other', schema_name='public'):
                    OrganizationShard.route_to_primary_db = False
                    OrganizationShard.objects.create(shard=self.test_shard, organization_id=organization.id)

                with use_shard(node_name='default', schema_name='public'):
                    self.assertTrue(OrganizationShard.objects.filter(organization_id=organization.id).exists())

                # cleanup
                OrganizationShard.route_to_primary_db = True
                OrganizationShard.objects.all().delete()

    def test_allow_relation(self):
        """
        Case: Call allow_relation for two models with various modes.
        Expected: Correct return value for the given mode combinations.
        """
        combinations = [
            ('PUBLIC -> PUBLIC', CakeType, CakeType, True),
            ('PUBLIC -> MIRRORED', CakeType, MirroredUser, True),
            ('PUBLIC -> SHARDED', CakeType, Statement, False),
            ('PUBLIC -> SINGLE NODE', CakeType, DefaultUser, False),
            ('MIRRORED -> PUBLIC', MirroredUser, CakeType, True),
            ('MIRRORED -> MIRRORED', MirroredUser, MirroredUser, True),
            ('MIRRORED -> SHARDED', MirroredUser, Statement, False),
            ('MIRRORED -> SINGLE NODE', MirroredUser, DefaultUser, False),
            ('SHARDED -> PUBLIC', Statement, CakeType, True),
            ('SHARDED -> MIRRORED', Statement, MirroredUser, True),
            ('SHARDED -> SHARDED', Statement, Statement, True),
            ('SHARDED -> SINGLE NODE', Statement, DefaultUser, False),
            ('SINGLE NODE -> PUBLIC', DefaultUser, CakeType, True),
            ('SINGLE NODE -> MIRRORED', DefaultUser, MirroredUser, True),
            ('SINGLE NODE -> SHARDED', DefaultUser, Statement, False),
            ('SINGLE NODE -> SINGLE NODE on the same node', DefaultUser, Unrelated, True),
        ]
        for name, model_from, model_to, result in combinations:
            with self.subTest(name):
                self.assertEqual(self.router.allow_relation(model_to(), model_from()), result)

    def test_allow_relation_between_models_on_two_nodes_and_without_a_sharding_mode(self):
        """
        Case: Call allow_relation from DefaultUser, which is on the default node, to Unrelated while it is on the other
              node, and between two models of which the router finds no sharding mode.
        Expected: Not allowed between the two nodes, and no opinion without sharding modes.
        """
        with override_sharding_setting(
            'OVERRIDE_SHARDING_MODE',
            OverrideShardingModeWithLowercaseKeys({('example', 'unrelated'): single_node_unsharded(node='other')}),
        ):
            self.assertFalse(self.router.allow_relation(Unrelated(), DefaultUser()))

        with mock.patch('djanquiltdb.router.get_model_placement', return_value=(False, None)):
            self.assertIsNone(self.router.allow_relation(Unrelated(), DefaultUser()))

    @override_settings(
        QUILT_DB={
            'SHARD_CLASS': 'example.models.Shard',
            'OVERRIDE_SHARDING_MODE': {
                ('djanquiltdb', 'simplemodel'): ShardingMode.SHARDED,
            },
        }
    )
    def test_allow_relation_between_sharded_models_settings_override_model(self):
        """
        Case: Call allow_relation with two sharded models, the latter is set through the configuration.
        Expected: True, we don't check if they are on the same shard yet.
        """

        class SimpleModel(models.Model):
            class Meta:
                app_label = 'djanquiltdb'

        self.assertTrue(self.router.allow_relation(Organization(), SimpleModel()))

    @override_settings(
        QUILT_DB={
            'SHARD_CLASS': 'example.models.Shard',
            'OVERRIDE_SHARDING_MODE': {
                ('djanquiltdb',): ShardingMode.SHARDED,
            },
        }
    )
    def test_allow_relation_between_sharded_models_settings_override_app(self):
        """
        Case: Call allow_relation with two sharded models, the latter is set through the configuration.
        Expected: True, we don't check if they are on the same shard yet.
        """

        class ShardedByDefaultModel(models.Model):
            class Meta:
                app_label = 'djanquiltdb'

        self.assertTrue(self.router.allow_relation(Organization(), ShardedByDefaultModel()))

    def test_allow_migrate(self):
        """
        Case: Migrate a combination of unsharded, mirrored and
              sharded models: namely example.models.
        Expected: unsharded to go to default-public.
                  mirrored to go to default-public and other-public
                  sharded to go to default-template, other-template,
                  default-schema1 and other-schema2.
        """
        create_template_schema('default')  # Also calls for a migration

        # Mirrored, mapping and django default tables. Note that this also included tables from apps outside those
        # selected for the testcase. Since the initial migration happens before that, and includes all models knows to
        # django.
        default_public_tables = [
            'django_migrations',
            'django_content_type',
            'auth_group',
            'auth_permission',
            'auth_group_permissions',
            'example_shard',
            'django_session',
            'example_type',
            'example_supertype',
            'example_caketype',
            'example_coatingtype',
            'example_coatingtype_super_type',
            'example_flavour',
            'example_sprinkle',
            'example_organizationshard',
            'example_mirroreduser',
            'example_mirroreduser_type',
            'example_defaultuser',
            'migration_tests_supermirroredmodel',
            'migration_tests_mirroredmodel',
            'example_unrelated',
        ]
        # The tables present on all non-default public schema's are all the mirrored tables.
        other_public_tables = [
            'django_migrations',
            'example_shard',
            'example_type',
            'example_supertype',
            'example_coatingtype',
            'example_coatingtype_super_type',
            'example_caketype',
            'example_flavour',
            'example_sprinkle',
            'django_content_type',
            'auth_group',
            'auth_permission',
            'auth_group_permissions',
            'example_organizationshard',
            'example_mirroreduser',
            'example_mirroreduser_type',
            'migration_tests_supermirroredmodel',
            'migration_tests_mirroredmodel',
        ]
        # The tables present on the template schema's are all the sharded tables.
        template_tables = [
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
        ]

        self.assertCountEqual(connections['default'].get_all_table_headers(schema_name='public'), default_public_tables)
        self.assertCountEqual(connections['default'].get_all_table_headers(schema_name='template'), template_tables)
        self.assertCountEqual(connections['other'].get_all_table_headers(schema_name='public'), other_public_tables)

        create_template_schema('other')
        self.assertCountEqual(connections['other'].get_all_table_headers(schema_name='template'), template_tables)

        create_schema_on_node('schema1', node_name='default', migrate=False)
        # Schema is created empty (cause we say: 'migrate=False')
        self.assertCountEqual(connections['default'].get_all_table_headers(schema_name='schema1'), [])

        # Obviously, after migration shard schema's have the same tables as the template.
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

    @mock.patch('djanquiltdb.router.logger.warning')
    def test_allow_migrate_on_nonexisting_model(self, mock_logger_warning):
        """
        Case: Call allow_migrate for a model that (no longer) exists.
        Expected: Warning to be logged.
        """
        self.router.allow_migrate('default', 'example', 'outer_space')
        self.assertEqual(mock_logger_warning.call_count, 1)

    @override_settings(
        QUILT_DB={
            'SHARD_CLASS': 'example.models.Shard',
            'MAPPING_MODEL': 'example.models.OrganizationShard',
            'NEW_SHARD_NODE': 'other',
            'OVERRIDE_SHARDING_MODE': {
                ('example', 'organization'): ShardingMode.MIRRORED,
            },
        }
    )
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
        Case: Call allow_migrate without a model_name.
        Expected: Programming error to be raised.
        """
        with self.assertRaises(ProgrammingError):
            self.router.allow_migrate('default', 'example', model_name=None)

    def test_allow_migrate_with_hints(self):
        """
        Case: run_python in migration with hints given
        Expected: Router to route correctly
        """
        self.assertTrue(
            self.router.allow_migrate('default', 'example', model_name=None, sharding_mode=ShardingMode.MIRRORED)
        )
        self.assertFalse(
            self.router.allow_migrate('default', 'example', model_name=None, sharding_mode=ShardingMode.SHARDED)
        )
        self.assertTrue(
            self.router.allow_migrate(
                'default', 'example', model_name='organization', sharding_mode=ShardingMode.MIRRORED
            )
        )

    def test_allow_migrate_a_model_on_a_single_node(self):
        """
        Case: Ask whether Unrelated, which is on the default node, may be migrated on the public schemas and the
              template schemas of both nodes, and the same while an OVERRIDE_SHARDING_MODE entry puts it on
              PRIMARY_NODE, with PRIMARY_DB_ALIAS set to the other node.
        Expected: Only on the public schema of its node: the default node, and then the other node.
        """
        aliases = ['default', 'default|template', 'other', 'other|template']

        self.assertEqual(
            [self.router.allow_migrate(alias, 'example', 'unrelated') for alias in aliases], [True, False, False, False]
        )

        with override_settings(
            QUILT_DB={
                'SHARD_CLASS': 'example.models.Shard',
                'PRIMARY_DB_ALIAS': 'other',
                'OVERRIDE_SHARDING_MODE': {('example', 'unrelated'): single_node_unsharded(node=PRIMARY_NODE)},
            }
        ):
            self.assertEqual(
                [self.router.allow_migrate(alias, 'example', 'unrelated') for alias in aliases],
                [False, False, True, False],
            )

    def test_allow_migrate_an_operation_on_a_single_node(self):
        """
        Case: Ask whether an operation without a model, hinted SINGLE_NODE_UNSHARDED with the node 'other', may be
              migrated on the public schemas of both nodes, and ask the same for one without the node hint.
        Expected: Only on the public schema of the other node, and a ProgrammingError without the node hint.
        """
        hints = {'sharding_mode': ShardingMode.SINGLE_NODE_UNSHARDED, 'node': 'other'}

        self.assertFalse(self.router.allow_migrate('default', 'example', model_name=None, **hints))
        self.assertTrue(self.router.allow_migrate('other', 'example', model_name=None, **hints))

        with self.assertRaisesMessage(ProgrammingError, 'Give it a node hint'):
            self.router.allow_migrate(
                'other', 'example', model_name=None, sharding_mode=ShardingMode.SINGLE_NODE_UNSHARDED
            )

    def test_allow_migrate_an_operation_hinted_with_single_node_unsharded(self):
        """
        Case: Ask whether an operation without a model, hinted with single_node_unsharded(node='other'), may be migrated
              on the public schemas and the template schemas of both nodes, and the same for one hinted with
              single_node_unsharded(node=PRIMARY_NODE) while PRIMARY_DB_ALIAS is the other node.
        Expected: Only on the public schema of the other node, in both cases.
        """
        aliases = ['default', 'default|template', 'other', 'other|template']

        self.assertEqual(
            [
                self.router.allow_migrate(alias, 'example', None, sharding_mode=single_node_unsharded(node='other'))
                for alias in aliases
            ],
            [False, False, True, False],
        )

        with override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'}):
            self.assertEqual(
                [
                    self.router.allow_migrate(
                        alias, 'example', None, sharding_mode=single_node_unsharded(node=PRIMARY_NODE)
                    )
                    for alias in aliases
                ],
                [False, False, True, False],
            )

    def test_allow_migrate_an_operation_on_a_single_node_for_a_model_that_the_registry_does_not_have(self):
        """
        Case: Ask whether an operation hinted SINGLE_NODE_UNSHARDED without a node hint, for a model that the app
              registry does not have, may be migrated.
        Expected: False, as for any operation on a model that the registry does not have, and no LookupError.
        """
        self.assertFalse(
            self.router.allow_migrate('default', 'example', 'removed', sharding_mode=ShardingMode.SINGLE_NODE_UNSHARDED)
        )

    def test_db_for_read_and_write_of_a_model_on_a_single_node(self):
        """
        Case: Call db_for_read and db_for_write for Unrelated, which is on the default node, while a shard on the other
              node is active, and while the public schema of the other node is active.
        Expected: The default node in each case.
        """
        for active_connection in (
            ShardOptions(node_name='other', schema_name='some_shard'),
            ShardOptions(node_name='other', schema_name='public'),
        ):
            with self.subTest(active_connection=str(active_connection)):
                set_active_connection(active_connection)
                self.assertEqual(self.router.db_for_read(Unrelated), 'default')
                self.assertEqual(self.router.db_for_write(Unrelated), 'default')


class NodeScopedPlacementTestCase(SimpleTestCase):
    def setUp(self):
        super().setUp()

        self.router = DynamicDbRouter()

    def test_a_bare_node_name_allows_a_sharded_model(self):
        """
        Case: Ask whether the sharded Organization may be migrated on the bare node names 'default' and 'other', outside
              and inside node_scoped_placement().
        Expected: Not allowed outside the block, because a bare node name is the node's public schema there. Allowed
                  inside it, because a bare node name is the whole node there.
        """
        for node_name in ('default', 'other'):
            with self.subTest(node_name=node_name):
                self.assertFalse(self.router.allow_migrate(node_name, 'example', 'organization'))
                with node_scoped_placement():
                    self.assertTrue(self.router.allow_migrate(node_name, 'example', 'organization'))

    def test_shard_options_for_a_bare_node_name_allow_a_sharded_model(self):
        """
        Case: Ask whether the sharded Organization may be migrated on the ShardOptions that ShardOptions.from_alias()
              returns for the bare node name 'default', outside and inside node_scoped_placement().
        Expected: The same answers as for the string 'default': not allowed outside the block, allowed inside it.
        """
        options = ShardOptions.from_alias('default')

        self.assertFalse(self.router.allow_migrate(options, 'example', 'organization'))
        with node_scoped_placement():
            self.assertTrue(self.router.allow_migrate(options, 'example', 'organization'))

    def test_a_name_with_a_schema_keeps_its_answer(self):
        """
        Case: Ask whether the sharded Organization may be migrated on the template schema, on the schema of a shard, on
              a ShardOptions, and on a public schema named with its schema, outside and inside node_scoped_placement().
        Expected: The same answer inside the block as outside it, so the public schema still does not allow it when it
                  is named with its schema.
        """
        aliases = [
            ('default|template', True),
            ('other|some_shard', True),
            (ShardOptions(node_name='default', schema_name='some_shard'), True),
            ('default|public', False),
            (('other', 'public'), False),
            (ShardOptions(node_name='default', schema_name='public'), False),
        ]

        for alias, allowed in aliases:
            with self.subTest(alias=alias):
                self.assertIs(self.router.allow_migrate(alias, 'example', 'organization'), allowed)
                with node_scoped_placement():
                    self.assertIs(self.router.allow_migrate(alias, 'example', 'organization'), allowed)

    def test_other_sharding_modes_keep_their_answer(self):
        """
        Case: Ask whether the mirrored Type, the public CakeType and DefaultUser, which is on the default node, may be
              migrated on the bare node names 'default' and 'other', outside and inside node_scoped_placement().
        Expected: The same answer inside the block as outside it: Type and CakeType on both nodes, DefaultUser on the
                  default node only.
        """
        answers = [
            ('type', 'default', True),
            ('type', 'other', True),
            ('caketype', 'default', True),
            ('caketype', 'other', True),
            ('defaultuser', 'default', True),
            ('defaultuser', 'other', False),
        ]

        for model_name, node_name, allowed in answers:
            with self.subTest(model_name=model_name, node_name=node_name):
                self.assertIs(self.router.allow_migrate(node_name, 'example', model_name), allowed)
                with node_scoped_placement():
                    self.assertIs(self.router.allow_migrate(node_name, 'example', model_name), allowed)

    @mock.patch('djanquiltdb.router.logger.warning')
    def test_the_answers_before_the_sharding_mode_still_win(self, mock_logger_warning):
        """
        Case: Inside node_scoped_placement(), ask about a sharded model that has the test_model hint, about a model that
              does not exist, and about an operation without a model or a sharding mode, each on a bare node name.
        Expected: Not allowed for the test model, not allowed with a warning for the missing model, and a
                  ProgrammingError for the operation without a sharding mode, as outside the block.
        """
        with node_scoped_placement():
            self.assertFalse(self.router.allow_migrate('default', 'example', 'organization', model=DummyShardedModel))
            self.assertFalse(self.router.allow_migrate('default', 'example', 'outer_space'))
            with self.assertRaises(ProgrammingError):
                self.router.allow_migrate('default', 'example', model_name=None)

        self.assertEqual(mock_logger_warning.call_count, 1)

    def test_the_block_ends_with_its_own_answer(self):
        """
        Case: A node_scoped_placement() block inside another one, and a block that an exception leaves.
        Expected: The inner block leaves the outer one in force, and the answer outside a block is back to the public
                  schema's, after the exception too.
        """
        with node_scoped_placement():
            with node_scoped_placement():
                pass
            self.assertTrue(_placement_is_node_scoped.get())

        self.assertFalse(_placement_is_node_scoped.get())

        with self.assertRaises(RuntimeError):
            with node_scoped_placement():
                raise RuntimeError

        self.assertFalse(_placement_is_node_scoped.get())
        self.assertFalse(self.router.allow_migrate('default', 'example', 'organization'))


class DynamicDbRouterSystemTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema('default')
        self.instance = Type.objects.create(name='Wrecker')

    def test_del_mirrored_model_with_relation(self):
        """
        Case: Delete a mirrored model that has a relation.
        Expected: Model gets correctly deleted.
        """
        with use_shard(node_name='default', schema_name='template'):
            Type.objects.filter(id=self.instance.id).delete()
            self.assertFalse(Type.objects.exists())

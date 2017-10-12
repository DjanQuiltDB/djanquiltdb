from unittest import mock

from django.conf import settings
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.recorder import MigrationRecorder
from django.test import override_settings

from example.models import Shard
from migration_tests.tests.migration_base import MigrationTestBase
from sharding.management.commands.migrate_shards import Command as MigrateShards
from sharding.utils import State, use_shard


class ShardedMigrationSystemTestCase(MigrationTestBase):
    available_apps = ['migration_tests']

    def setUp(self):
        super().setUp()
        # this is not added as decorator, for that won't work for the setup.
        self.mock_router = mock.patch('sharding.utils.DynamicDbRouter.allow_migrate').start()
        self.addCleanup(mock.patch.stopall)

        # Unlike other migrations, these test migrations are NOT applied during the creation of the testcase
        # For they are not known to the runner at that point.

        self.databases = MigrateShards().get_all_but_replica_dbs()

        with override_settings(MIGRATION_MODULES={"migration_tests": "migration_tests.test_migrations"}):
            # default|template migrates fully
            # therefore the shards created after this will be fully migrated as well.
            for db in self.databases:
                with use_shard(node_name=db, schema_name='template') as env:
                    executor = MigrationExecutor(env.connection)
                    executor.migrate([('migration_tests', '0003_third')])
                    executor.loader.build_graph()

            # revert 1st shard Sina migrates to the first migration
            self.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default',
                                             state=State.ACTIVE)
            with use_shard(self.sina) as env:
                executor = MigrationExecutor(env.connection)
                executor.migrate([('migration_tests', '0001_initial')])
                executor.loader.build_graph()

            # revert 2nd shard Rose migrates to the first 2 migrations
            self.rose = Shard.objects.create(alias='rose', schema_name='test_rose', node_name='default',
                                             state=State.ACTIVE)
            with use_shard(self.rose) as env:
                executor = MigrationExecutor(env.connection)
                executor.migrate([('migration_tests', '0002_second')])
                executor.loader.build_graph()

            # We keep Maria fully migrated (to 0003)
            self.maria = Shard.objects.create(alias='maria', schema_name='test_maria', node_name='default',
                                              state=State.ACTIVE)

    @override_settings(MIGRATION_MODULES={"migration_tests": "migration_tests.test_migrations"})
    def test_forward_migration_as_a_whole(self):
        """
        Case: Call MultiSchemaMigration.migrate() with shard in several states of migration.
        Expected: All shards to be fully migrated
        Note: This is system test keeping MultiSchemaMigration().migrate() as a black box.
        """
        # Check initial state
        # (This is not necessary, since we defined it in the setup,
        #  but it's nice to clearly see the difference)
        for db in self.databases:
            with use_shard(node_name=db, schema_name='template') as env:
                recorder = MigrationRecorder(env.connection)
                applied_migration_tests = recorder.applied_migrations()
                self.assertTrue(('migration_tests', '0003_third') in applied_migration_tests)
                self.assertTrue(('migration_tests', '0002_second') in applied_migration_tests)
                self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)
        with use_shard(self.sina) as env:
            recorder = MigrationRecorder(env.connection)
            applied_migration_tests = recorder.applied_migrations()
            self.assertFalse(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertFalse(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)
        with use_shard(self.rose) as env:
            recorder = MigrationRecorder(env.connection)
            applied_migration_tests = recorder.applied_migrations()
            self.assertFalse(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)
        with use_shard(self.maria) as env:
            recorder = MigrationRecorder(env.connection)
            applied_migration_tests = recorder.applied_migrations()
            self.assertTrue(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)

        MigrateShards().handle(database='all', verbosity=0)

        # all shards and the template now are fully migrated
        for db in self.databases:
            with use_shard(node_name=db, schema_name='template') as env:
                recorder = MigrationRecorder(env.connection)
                applied_migration_tests = recorder.applied_migrations()
                self.assertTrue(('migration_tests', '0003_third') in applied_migration_tests)
                self.assertTrue(('migration_tests', '0002_second') in applied_migration_tests)
                self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)
        with use_shard(self.sina) as env:
            recorder = MigrationRecorder(env.connection)
            applied_migration_tests = recorder.applied_migrations()
            self.assertTrue(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)
        with use_shard(self.rose) as env:
            recorder = MigrationRecorder(env.connection)
            applied_migration_tests = recorder.applied_migrations()
            self.assertTrue(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)
        with use_shard(self.maria) as env:
            recorder = MigrationRecorder(env.connection)
            applied_migration_tests = recorder.applied_migrations()
            self.assertTrue(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)

        # rollback
        MigrateShards().handle(app_label='migration_tests', migration_name='zero', database='all', verbosity=0)

        # all shards and the template now are back so square 0
        for db in self.databases:
            with use_shard(node_name=db, schema_name='template') as env:
                recorder = MigrationRecorder(env.connection)
                applied_migration_tests = recorder.applied_migrations()
                self.assertFalse(('migration_tests', '0003_third') in applied_migration_tests)
                self.assertFalse(('migration_tests', '0002_second') in applied_migration_tests)
                self.assertFalse(('migration_tests', '0001_initial') in applied_migration_tests)
        with use_shard(self.sina) as env:
            recorder = MigrationRecorder(env.connection)
            applied_migration_tests = recorder.applied_migrations()
            self.assertFalse(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertFalse(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertFalse(('migration_tests', '0001_initial') in applied_migration_tests)
        with use_shard(self.rose) as env:
            recorder = MigrationRecorder(env.connection)
            applied_migration_tests = recorder.applied_migrations()
            self.assertFalse(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertFalse(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertFalse(('migration_tests', '0001_initial') in applied_migration_tests)
        with use_shard(self.maria) as env:
            recorder = MigrationRecorder(env.connection)
            applied_migration_tests = recorder.applied_migrations()
            self.assertFalse(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertFalse(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertFalse(('migration_tests', '0001_initial') in applied_migration_tests)


class ShardedMigrationTestCase(MigrationTestBase):
    available_apps = ['migration_tests', 'sharding']

    def setUp(self):
        super().setUp()
        # prevent ShardingTestCase addCleanup
        self.mock_router = mock.patch('sharding.utils.DynamicDbRouter.allow_migrate').start()
        self.addCleanup(mock.patch.stopall)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default',
                                        state=State.ACTIVE)
        cls.rose = Shard.objects.create(alias='rose', schema_name='test_rose', node_name='default',
                                        state=State.ACTIVE)
        cls.maria = Shard.objects.create(alias='maria', schema_name='test_maria', node_name='default',
                                         state=State.ACTIVE)

    @override_settings(MIGRATION_MODULES={"migration_tests": "migration_tests.test_migrations"})
    @mock.patch('django.core.management.sql.emit_post_migrate_signal')
    @mock.patch('sharding.management.commands.migrate_shards.Command.check_or_migrate_shard')
    @mock.patch('sharding.management.commands.migrate_shards.Command.check_or_migrate_schema')
    @mock.patch('django.core.management.sql.emit_pre_migrate_signal')
    @mock.patch('sharding.management.commands.migrate_shards.Command.get_plan_for_shard')
    @mock.patch('django.db.migrations.graph.MigrationGraph.leaf_nodes')
    @mock.patch('django.db.migrations.loader.MigrationLoader.detect_conflicts')
    @mock.patch('django.db.backends.base.base.BaseDatabaseWrapper.prepare_database')
    @mock.patch('sharding.management.commands.migrate_shards.Command.get_all_but_replica_dbs')
    @mock.patch('sharding.management.commands.migrate_shards.import_module')
    def test_migrate(self,
                     mock_import_module,
                     mock_get_all_dbs,
                     mock_prepare_database,
                     mock_detect_conflicts,
                     mock_leave_nodes,
                     mock_get_plan_for_shard,
                     mock_emit_pre_migrate_signal,
                     mock_check_or_migrate_schema,
                     mock_check_or_migrate_shard,
                     mock_emit_post_migrate_signal):
        """
        Case: Call MultiSchemaMigration.migrate() with shard in several states of migration.
        Expected: A ton of external functions to be called.
        """
        plan = [('migration_tests', '0001_initial'),
                ('migration_tests', '0002_second'),
                ('migration_tests', '0003_third')]
        executor = MigrationExecutor(connection)
        leave_nodes = [('migration_tests', '0003_third')]
        mock_leave_nodes.return_value = leave_nodes
        mock_get_plan_for_shard.return_value = plan
        mock_get_all_dbs.return_value = [db for db in settings.DATABASES]
        mock_detect_conflicts.return_value = False

        MigrateShards().handle(database='all', fake=False, fake_initial=False)

        mock_import_module.assert_called_once_with('.management', 'sharding')
        mock_get_all_dbs.assert_called_once()
        self.assertEqual(mock_prepare_database.call_count, 2)  # we have 2 databases
        mock_detect_conflicts.assert_called_once()
        mock_leave_nodes.assert_called_once()
        mock_emit_pre_migrate_signal.assert_called_once()

        self.assertEqual(mock_get_plan_for_shard.call_count, 7)  # we have 3 shards, 2 templates and 2 publics
        mock_get_plan_for_shard.assert_any_call(self.sina.node_name, self.sina.schema_name, leave_nodes)
        mock_get_plan_for_shard.assert_any_call(self.rose.node_name, self.rose.schema_name, leave_nodes)
        mock_get_plan_for_shard.assert_any_call(self.maria.node_name, self.maria.schema_name, leave_nodes)

        self.assertEqual(mock_check_or_migrate_schema.call_count, 12)  # we have 3 nodes, 2 teamples and 2 publics
        self.assertEqual(mock_check_or_migrate_shard.call_count, 9)  # we have 3 shards and 3 nodes
        plan = executor.migration_plan(leave_nodes)
        for node in plan:
            mock_check_or_migrate_schema.assert_any_call('default', 'public', node, False, False)
            mock_check_or_migrate_schema.assert_any_call('default', 'template', node, False, False)
            mock_check_or_migrate_schema.assert_any_call('other', 'public', node, False, False)
            mock_check_or_migrate_schema.assert_any_call('other', 'template', node, False, False)
            mock_check_or_migrate_shard.assert_any_call(self.sina, node, False, False)
            mock_check_or_migrate_shard.assert_any_call(self.rose, node, False, False)
            mock_check_or_migrate_shard.assert_any_call(self.maria, node, False, False)

        mock_emit_post_migrate_signal.assert_called_once()

    def migration_func(self, targets, plan, fake, fake_initial):
        self.assertEqual(plan, [self.node])
        # test the state, this is the point in execution we can measure it.
        self.assertEqual(self.sina.state, State.MAINTENANCE)

    @override_settings(MIGRATION_MODULES={"migration_tests": "migration_tests.test_migrations"})
    @mock.patch('sharding.management.commands.migrate_shards.MigrationExecutor.migrate')
    def test_check_or_migrate_shard(self, mock_migrate):
        """
        Case: Call MultiSchemaMigration.check_or_migrate_shard()
        Expected: The shard to be put in maintenance mode before migration is called with correct plan_node
        """

        mock_migrate.side_effect = self.migration_func

        self.assertEqual(self.sina.state, State.ACTIVE)

        self.node = (MigrationExecutor(connection).loader.graph.nodes[('migration_tests', '0002_second')], False)
        command = MigrateShards()
        command.verbosity = 0
        command.check_or_migrate_shard(self.sina, plan_node=self.node, fake=False, fake_initial=False)
        self.assertEqual(self.sina.state, State.ACTIVE)

from unittest import mock

from django.conf import settings
from django.core.management import CommandError, call_command
from django.db import connection, DatabaseError
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.recorder import MigrationRecorder
from django.test import override_settings
from django.utils import six

from example.models import Shard
from migration_tests.tests.migration_base import MigrationTestBase
from sharding.management.commands.migrate_shards import Command as MigrateShards
from sharding.utils import State, use_shard, get_template_name


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

        with override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'}):
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
                                              state=State.MAINTENANCE)

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
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
        with use_shard(self.maria, active_only_schemas=False) as env:
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
        with use_shard(self.maria, active_only_schemas=False) as env:
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
        with use_shard(self.maria, active_only_schemas=False) as env:
            recorder = MigrationRecorder(env.connection)
            applied_migration_tests = recorder.applied_migrations()
            self.assertFalse(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertFalse(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertFalse(('migration_tests', '0001_initial') in applied_migration_tests)


class OriginalMigrationTestCase(MigrationTestBase):
    # Taken from the Django source: https://github.com/django/django/blob/stable/1.8.x/tests/migrations/test_commands.py
    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    def test_migrate(self):
        """
        Case: Tests basic usage of the migrate command.
        Expected: Tables only to exist when they should
        """
        # Make sure no tables are created
        self.assertTableNotExists('migration_tests_author')
        self.assertTableNotExists('migration_tests_tribble')
        self.assertTableNotExists('migration_tests_book')
        # Run the migrations to 0001 only
        call_command('migrate_shards', 'migration_tests', '0001', verbosity=0)
        # Make sure the right tables exist
        self.assertTableExists('migration_tests_author')
        self.assertTableExists('migration_tests_tribble')
        self.assertTableNotExists('migration_tests_book')
        # Run migrations all the way
        call_command('migrate_shards', verbosity=0)
        # Make sure the right tables exist
        self.assertTableExists('migration_tests_author')
        self.assertTableNotExists('migration_tests_tribble')
        self.assertTableExists('migration_tests_book')
        # Unmigrate everything
        call_command('migrate_shards', 'migration_tests', 'zero', verbosity=0)
        # Make sure it's all gone
        self.assertTableNotExists('migration_tests_author')
        self.assertTableNotExists('migration_tests_tribble')
        self.assertTableNotExists('migration_tests_book')

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    def test_migrate_fake_initial(self):
        """
        Case: #24184 - Tests that --fake-initial only works if all tables created in
              the initial migration of an app exists
        Expected: Tables only to exist when they should
        """
        # Make sure no tables are created
        self.assertTableNotExists('migration_tests_author')
        self.assertTableNotExists('migration_tests_tribble')
        # Run the migrations to 0001 only
        call_command('migrate_shards', 'migration_tests', '0001', verbosity=0)
        # Make sure the right tables exist
        self.assertTableExists('migration_tests_author')
        self.assertTableExists('migration_tests_tribble')
        # Fake a roll-back
        call_command('migrate_shards', 'migration_tests', 'zero', fake=True, verbosity=0)
        # Make sure the tables still exist
        self.assertTableExists('migration_tests_author')
        self.assertTableExists('migration_tests_tribble')
        # Try to run initial migration
        with self.assertRaises(DatabaseError):
            call_command('migrate_shards', 'migration_tests', '0001', verbosity=0)
        # Run initial migration with an explicit --fake-initial
        out = six.StringIO()
        with mock.patch('django.core.management.color.supports_color', lambda *args: False):
            call_command('migrate_shards', 'migration_tests', '0001', fake_initial=True, stdout=out, verbosity=1)
        self.assertIn(
            'migration_tests.0001_initial... faked',
            out.getvalue().lower()
        )
        # Run migrations all the way
        call_command('migrate_shards', verbosity=0)
        # Make sure the right tables exist
        self.assertTableExists('migration_tests_author')
        self.assertTableNotExists('migration_tests_tribble')
        self.assertTableExists('migration_tests_book')
        # Fake a roll-back
        call_command('migrate_shards', 'migration_tests', 'zero', fake=True, verbosity=0)
        # Make sure the tables still exist
        self.assertTableExists('migration_tests_author')
        self.assertTableNotExists('migration_tests_tribble')
        self.assertTableExists('migration_tests_book')
        # Try to run initial migration
        with self.assertRaises(DatabaseError):
            call_command('migrate_shards', 'migration_tests', verbosity=0)
        # Run initial migration with an explicit --fake-initial
        with self.assertRaises(DatabaseError):
            # Fails because 'migration_tests_tribble' does not exist but needs to in
            # order to make --fake-initial work.
            call_command('migrate_shards', 'migration_tests', fake_initial=True, verbosity=0)
        # Fake a apply
        call_command('migrate_shards', 'migration_tests', fake=True, verbosity=0)
        # Unmigrate everything
        call_command('migrate_shards', 'migration_tests', 'zero', verbosity=0)
        # Make sure it's all gone
        self.assertTableNotExists('migration_tests_author')
        self.assertTableNotExists('migration_tests_tribble')
        self.assertTableNotExists('migration_tests_book')

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_conflict'})
    def test_migrate_conflict_exit(self):
        """
        Case: Call migrate_shards with a conflicting migration set
        Expected: Makes sure that migrate exits if it detects a conflict.
        """
        with self.assertRaisesMessage(CommandError, 'Conflicting migrations detected'):
            call_command('migrate_shards', 'migration_tests')

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_squashed'})
    def test_migrate_record_replaced(self):
        """
        Case: Call migrate_shards with a squashed migration set
        Expected: All original migrations should be marked as run
        """
        recorder = MigrationRecorder(connection)
        out = six.StringIO()
        call_command('migrate_shards', 'migration_tests', verbosity=0)
        call_command('showmigrations', 'migration_tests', stdout=out, no_color=True)
        self.assertEqual(
            'migration_tests\n'
            ' [x] 0001_squashed_0002 (2 squashed migrations)\n',
            out.getvalue().lower()
        )
        applied_migration_tests = recorder.applied_migrations()
        self.assertIn(('migration_tests', '0001_initial'), applied_migration_tests)
        self.assertIn(('migration_tests', '0002_second'), applied_migration_tests)
        self.assertIn(('migration_tests', '0001_squashed_0002'), applied_migration_tests)
        # Rollback changes
        call_command('migrate_shards', 'migration_tests', 'zero', verbosity=0)

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_squashed'})
    def test_migrate_record_squashed(self):
        """
        Case: Call migrate_shards with a squashed migration set, when all original migrations have been run before.
        Expected: Should not migrate anything, all squashed migrations has been run
        """
        recorder = MigrationRecorder(connection)
        recorder.record_applied('migration_tests', '0001_initial')
        recorder.record_applied('migration_tests', '0002_second')
        out = six.StringIO()
        call_command('migrate_shards', 'migration_tests', shard='default|public', verbosity=0)
        call_command('showmigrations', 'migration_tests', stdout=out, no_color=True)
        self.assertEqual(
            'migration_tests\n'
            ' [x] 0001_squashed_0002 (2 squashed migrations)\n',
            out.getvalue().lower()
        )
        self.assertIn(
            ('migration_tests', '0001_squashed_0002'),
            recorder.applied_migrations()
        )
        # No changes were actually applied so there is nothing to rollback


class ShardedMigrationHandleTestCase(MigrationTestBase):
    available_apps = ['migration_tests', 'sharding', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default',
                                        state=State.ACTIVE)
        cls.rose = Shard.objects.create(alias='rose', schema_name='test_rose', node_name='default',
                                        state=State.ACTIVE)
        cls.maria = Shard.objects.create(alias='maria', schema_name='test_maria', node_name='default',
                                         state=State.ACTIVE)

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    @mock.patch('django.core.management.sql.emit_post_migrate_signal')
    @mock.patch('sharding.management.commands.migrate_shards.Command.perform_migration')
    @mock.patch('django.core.management.sql.emit_pre_migrate_signal')
    @mock.patch('sharding.management.commands.migrate_shards.Command.get_plan')
    @mock.patch('sharding.management.commands.migrate_shards.Command.get_targets_from_options')
    @mock.patch('django.db.migrations.loader.MigrationLoader.detect_conflicts')
    @mock.patch('django.db.backends.base.base.BaseDatabaseWrapper.prepare_database')
    @mock.patch('sharding.management.commands.migrate_shards.Command.get_database_and_schema_from_options')
    @mock.patch('sharding.management.commands.migrate_shards.import_module')
    def test_migrate_handle(self,
                            mock_import_module,
                            mock_get_db_from_options,
                            mock_prepare_database,
                            mock_detect_conflicts,
                            mock_get_targets,
                            mock_get_plan,
                            mock_emit_pre_migrate_signal,
                            mock_perform_migration,
                            mock_emit_post_migrate_signal):
        """
        Case: Call MultiSchemaMigration.migrate() with shard in several states of migration.
        Expected: A ton of external functions to be called.
        """
        mock_get_db_from_options.return_value = ([db for db in settings.DATABASES], None)
        mock_detect_conflicts.return_value = False

        MigrateShards().handle(database='all', fake=False, fake_initial=False)

        mock_import_module.assert_called_once_with('.management', 'sharding')
        mock_get_db_from_options.assert_called_once()
        self.assertEqual(mock_prepare_database.call_count, 2)  # we have 2 databases
        mock_detect_conflicts.assert_called_once()
        mock_get_targets.assert_called_once()
        mock_get_plan.assert_called_once()
        mock_emit_pre_migrate_signal.assert_called_once()
        mock_perform_migration.assert_called_once()
        mock_emit_post_migrate_signal.assert_called_once()


class ShardedMigrationGetDatabaseTestCase(MigrationTestBase):
    available_apps = ['migration_tests', 'sharding', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default',
                                        state=State.ACTIVE)

    @mock.patch('sharding.management.commands.migrate_shards.Command.get_all_but_replica_dbs')
    def test_without_special_options(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options without special options.
        Expected: Normal list of databases and no database and schema_name returned.
        """
        databases, schema_name = MigrateShards().get_database_and_schema_from_options(options={})

        self.assertTrue(mock_get_all_dbs.called)
        self.assertEqual(databases, MigrateShards().get_all_but_replica_dbs())
        self.assertIsNone(schema_name)

    @mock.patch('sharding.management.commands.migrate_shards.Command.get_all_but_replica_dbs')
    def test_with_shard_option(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with a targeted shard.
        Expected: single database and schema_name returned
        """
        mock_get_all_dbs.return_value = [db for db in settings.DATABASES]
        databases, schema_name = MigrateShards().get_database_and_schema_from_options(
            options={'shard': 'default|template'})

        self.assertTrue(mock_get_all_dbs.called)
        self.assertEqual(databases, ['default'])
        self.assertEqual(schema_name, 'template')

    @mock.patch('sharding.management.commands.migrate_shards.Command.get_all_but_replica_dbs')
    def test_with_database_option(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with a targeted database.
        Expected: single database and no schema_name returned
        """
        mock_get_all_dbs.return_value = [db for db in settings.DATABASES]
        databases, schema_name = MigrateShards().get_database_and_schema_from_options(options={'database': 'other'})

        self.assertTrue(mock_get_all_dbs.called)
        self.assertEqual(databases, ['other'])
        self.assertIsNone(schema_name)

    @mock.patch('sharding.management.commands.migrate_shards.Command.get_all_but_replica_dbs')
    def test_with_invalid_db_option(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with a targeted database.
        Expected: CommandError raised
        """
        mock_get_all_dbs.return_value = [db for db in settings.DATABASES]

        with self.assertRaises(CommandError) as error:
            MigrateShards().get_database_and_schema_from_options(options={'database': 'James'})
        self.assertEqual(error.exception.args[0], 'You must migrate an existing non-primary DB.')
        self.assertTrue(mock_get_all_dbs.called)

    @mock.patch('sharding.management.commands.migrate_shards.Command.get_all_but_replica_dbs')
    def test_with_database_and_shard_option(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with a targeted database and template_shard.
        Expected: single database and schema_name returned
        """
        mock_get_all_dbs.return_value = [db for db in settings.DATABASES]

        databases, schema_name = MigrateShards().get_database_and_schema_from_options(
             options={'database': 'other', 'shard': 'other|public'})

        self.assertTrue(mock_get_all_dbs.called)
        self.assertEqual(databases, ['other'])
        self.assertEqual(schema_name, 'public')

    @mock.patch('sharding.management.commands.migrate_shards.Command.get_all_but_replica_dbs')
    def test_with_database_and_shard_option2(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with a targeted database and shard.
        Expected: single database and schema_name returned
        """
        mock_get_all_dbs.return_value = [db for db in settings.DATABASES]

        databases, schema_name = MigrateShards().get_database_and_schema_from_options(
             options={'shard': 'default|sina'})

        self.assertTrue(mock_get_all_dbs.called)
        self.assertEqual(databases, ['default'])
        self.assertEqual(schema_name, 'test_sina')

    @mock.patch('sharding.management.commands.migrate_shards.Command.get_all_but_replica_dbs')
    def test_with_unexisting_shard(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with a targeted database and non-existing shard.
        Expected: CommandError raised
        """
        mock_get_all_dbs.return_value = [db for db in settings.DATABASES]

        with self.assertRaises(CommandError) as error:
            MigrateShards().get_database_and_schema_from_options(options={'database': 'other', 'shard': 'other|paul'})
        self.assertEqual(error.exception.args[0], 'Shard paul is not known.')

    @mock.patch('sharding.management.commands.migrate_shards.Command.get_all_but_replica_dbs')
    def test_with_shard_and_wrong_db(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with a shard target and wrong database.
        Expected: CommandError raised
        """
        mock_get_all_dbs.return_value = [db for db in settings.DATABASES]

        with self.assertRaises(CommandError) as error:
            MigrateShards().get_database_and_schema_from_options(options={'database': 'other', 'shard': 'other|sina'})
        self.assertEqual(error.exception.args[0], 'Shard sina does not belong to database other.')

    @mock.patch('sharding.management.commands.migrate_shards.Command.get_all_but_replica_dbs')
    def test_with_invalid_db_and_shard_options(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with invalid targeted database and shard.
        Expected: Value error raised
        """
        mock_get_all_dbs.return_value = [db for db in settings.DATABASES]

        with self.assertRaises(CommandError) as error:
            MigrateShards().get_database_and_schema_from_options(options={'database': 'default',
                                                                          'shard': 'other|public'})
        self.assertEqual(error.exception.args[0], 'You must migrate an existing non-primary DB.')
        self.assertTrue(mock_get_all_dbs.called)

    @mock.patch('sharding.management.commands.migrate_shards.Command.get_all_but_replica_dbs')
    def test_with_invalid_shard_option(self, mock_get_all_dbs):
        """
        Case: Call get_database_and_schema_from_options with invalid shard.
        Expected: Value error raised
        """
        mock_get_all_dbs.return_value = [db for db in settings.DATABASES]

        with self.assertRaises(CommandError) as error:
            MigrateShards().get_database_and_schema_from_options(options={'shard': 'other|george'})
        self.assertEqual(error.exception.args[0], 'Shard george is not known.')
        self.assertTrue(mock_get_all_dbs.called)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ShardedMigrationGetTargetsTestCase(MigrationTestBase):
    available_apps = ['migration_tests', 'sharding', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default',
                                        state=State.ACTIVE)

    @mock.patch('django.db.migrations.graph.MigrationGraph.leaf_nodes')
    def test_without_special_options(self, mock_leave_nodes):
        """
        Case: Call get_targets_from_options without options.
        Expected: All leaf nodes returned as targets.
        """
        leave_nodes = [('migration_tests', '0003_third'), ('sharding', '0002_second')]
        mock_leave_nodes.return_value = leave_nodes

        executor = MigrationExecutor(connection)
        self.assertEqual(MigrateShards().get_targets_from_options(executor, options={}), leave_nodes)

    @mock.patch('django.db.migrations.graph.MigrationGraph.leaf_nodes')
    def test_with_app_label(self, mock_leave_nodes):
        """
        Case: Call get_targets_from_options with app_label as option.
        Expected: Single leaf returned
        """
        leave_nodes = [('migration_tests', '0003_third'), ('example', '0002_auto_20171009_1502')]
        mock_leave_nodes.return_value = leave_nodes

        executor = MigrationExecutor(connection)
        self.assertEqual(MigrateShards().get_targets_from_options(executor, options={'app_label': 'example'}),
                         [('example', '0002_auto_20171009_1502')])

    @mock.patch('django.db.migrations.graph.MigrationGraph.leaf_nodes')
    def test_with_target_migration(self, mock_leave_nodes):
        """
        Case: Call get_targets_from_options with app_label as option.
        Expected: Single leaf returned
        """
        leave_nodes = [('migration_tests', '0003_third'), ('example', '0002_auto_20171009_1502')]
        mock_leave_nodes.return_value = leave_nodes

        executor = MigrationExecutor(connection)
        self.assertEqual(MigrateShards().get_targets_from_options(executor, options={'app_label': 'migration_tests',
                                                                                     'migration_name': '0002_second'}),
                         [('migration_tests', '0002_second')])

    @mock.patch('django.db.migrations.graph.MigrationGraph.leaf_nodes')
    def test_with_zero_target(self, mock_leave_nodes):
        """
        Case: Call get_targets_from_options with zero as option.
        Expected: [(app_label, None)] returned
        """
        leave_nodes = [('migration_tests', '0003_third'), ('example', '0002_auto_20171009_1502')]
        mock_leave_nodes.return_value = leave_nodes

        executor = MigrationExecutor(connection)
        self.assertEqual(MigrateShards().get_targets_from_options(executor, options={'app_label': 'migration_tests',
                                                                                     'migration_name': 'zero'}),
                         [('migration_tests', None)])

    @mock.patch('django.db.migrations.graph.MigrationGraph.leaf_nodes')
    def test_with_unexisting_migration(self, mock_leave_nodes):
        """
        Case: Call get_targets_from_options with nonexisting migration as target
        Expected: CommandError raised
        """
        leave_nodes = [('migration_tests', '0003_third'), ('example', '0002_auto_20171009_1502')]
        mock_leave_nodes.return_value = leave_nodes

        executor = MigrationExecutor(connection)
        with self.assertRaises(CommandError) as error:
            MigrateShards().get_targets_from_options(executor, options={'app_label': 'migration_tests',
                                                                        'migration_name': '9001_over_9k'}),
        self.assertEqual(error.exception.args[0],
                         "Cannot find a migration matching '9001_over_9k' from app 'migration_tests'.")

    @mock.patch('django.db.migrations.graph.MigrationGraph.leaf_nodes')
    def test_with_unexisting_app_label(self, mock_leave_nodes):
        """
        Case: Call get_targets_from_options with nonexisting app_label as target
        Expected: CommandError raised
        """
        leave_nodes = [('migration_tests', '0003_third'), ('example', '0002_auto_20171009_1502')]
        mock_leave_nodes.return_value = leave_nodes

        executor = MigrationExecutor(connection)
        with self.assertRaises(CommandError) as error:
            MigrateShards().get_targets_from_options(executor, options={'app_label': 'Hans'}),
        self.assertEqual(error.exception.args[0],
                         "App 'Hans' does not have migrations (you cannot selectively sync unmigrated apps)")


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ShardedMigrationGetPlanTestCase(MigrationTestBase):
    available_apps = ['migration_tests', 'sharding', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default',
                                        state=State.ACTIVE)

        cls.targets = [('migration_tests', '0003_third')]
        cls.databases = [db for db in settings.DATABASES]

    @mock.patch('sharding.management.commands.migrate_shards.Command.get_plan_for_shard')
    def test_all_shards_called(self, mock_get_plan_for_shard):
        """
        Case: Call get_plan
        Expected: get_plan_for_shard to be called for all schemas
        """
        mock_get_plan_for_shard.return_value = [(('migration_tests', '0001_initial'), False),
                                                (('migration_tests', '0002_second'), False),
                                                (('migration_tests', '0003_third'), False)]
        MigrateShards().get_plan(self.targets, self.databases)
        mock_get_plan_for_shard.assert_any_call('default', 'public', self.targets)
        mock_get_plan_for_shard.assert_any_call('default', 'template', self.targets)
        mock_get_plan_for_shard.assert_any_call('default', 'test_sina', self.targets)
        mock_get_plan_for_shard.assert_any_call('other', 'public', self.targets)
        mock_get_plan_for_shard.assert_any_call('other', 'template', self.targets)

    def test_different_migration_states(self):
        """
        Case: Call get_plan when not all schema's have the same migrationlevel
        Expected: get_plan_for_shard to be called for all schemas
        Note: get_plan_for_shard is not mocked. So it's functionality is taken into account
        """
        # This makes completely unmigrated schemas
        mock_save = mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.clone_schema').start()
        Shard.objects.create(alias='rose', node_name='default', schema_name='test_rose')
        Shard.objects.create(alias='maria', node_name='default', schema_name='test_maria')
        self.assertEqual(mock_save.call_count, 2)
        mock_save.stop()

        # Migrate the public schema's fully
        with use_shard(node_name='default', schema_name='public'):
            call_command('migrate', 'migration_tests', database='default', verbosity=0)
        with use_shard(node_name='other', schema_name='public'):
            call_command('migrate', 'migration_tests', database='other', verbosity=0)

        # Migrate rose a bit
        with use_shard(node_name='default', schema_name='test_rose'):
            call_command('migrate', 'migration_tests', '0001', verbosity=0)

        # Migrate maria a bit further
        with use_shard(node_name='default', schema_name='test_maria'):
            call_command('migrate', 'migration_tests', '0002', verbosity=0)

        # rose is the furthest behind. So we should get her migration path
        self.assertEqual(MigrateShards().get_plan(self.targets, self.databases),
                         MigrateShards().get_plan_for_shard('default', 'test_rose', self.targets))

        # rollback (Cleanup for other tests. Shards are automatically removed.)
        with use_shard(node_name='default', schema_name='public'):
            call_command('migrate', 'migration_tests', 'zero', database='default', verbosity=0)
        with use_shard(node_name='other', schema_name='public'):
            call_command('migrate', 'migration_tests', 'zero', database='other', verbosity=0)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ShardedMigrationGetPlanForShardTestCase(MigrationTestBase):
    available_apps = ['migration_tests', 'sharding', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default',
                                        state=State.ACTIVE)

        cls.targets = [('migration_tests', '0003_third')]
        cls.databases = [db for db in settings.DATABASES]

    @mock.patch('sharding.management.commands.migrate_shards.MigrationExecutor', autospec=True)
    @mock.patch('sharding.utils.use_shard.__exit__', autospec=True)
    @mock.patch('sharding.utils.use_shard.__enter__', autospec=True)
    def test_use_shard_called(self, mock_use_shard_enter, mock_use_shard_exit, mock_executor):
        """
        Case: Call get_plan_for_shard
        Expected: executor.migration_plan and use_sahrd called
        """
        mock_executor.return_value.migration_plan = mock.Mock()

        MigrateShards().get_plan_for_shard(self.sina.node_name, self.sina.schema_name, self.targets)

        self.assertEqual(mock_use_shard_enter.call_count, 1)
        self.assertEqual(mock_use_shard_enter.call_args[0][0].node_name, 'default')
        self.assertEqual(mock_use_shard_enter.call_args[0][0].schema_name, 'test_sina')
        self.assertEqual(mock_executor.call_count, 1)
        mock_executor.return_value.migration_plan.assert_called_once_with(self.targets)
        self.assertEqual(mock_use_shard_exit.call_count, 1)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ShardedMigrationPerformMigrationTestCase(MigrationTestBase):
    available_apps = ['migration_tests', 'sharding', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # This makes completely unmigrated schemas
        mock_save = mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.clone_schema').start()
        cls.rose = Shard.objects.create(alias='rose', node_name='default', schema_name='test_rose',
                                        state=State.ACTIVE)
        cls.maria = Shard.objects.create(alias='maria', node_name='default', schema_name='test_maria',
                                         state=State.ACTIVE)
        mock_save.stop()

        cls.targets = [('migration_tests', '0003_third')]
        cls.databases = [db for db in settings.DATABASES]
        cls.plan = MigrateShards().get_plan_for_shard(cls.rose.node_name, cls.rose.schema_name, cls.targets)

    @mock.patch('sharding.management.commands.migrate_shards.MigrationExecutor', autospec=True)
    @mock.patch('sharding.utils.use_shard.__exit__', autospec=True)
    @mock.patch('sharding.utils.use_shard.__enter__', autospec=True)
    def test_specific_schema(self, mock_use_shard_enter, mock_use_shard_exit, mock_executor):
        """
        Case: Call perform_migration with a specific schema.
        Expected: executor.migrate to be called with the right arguments within a use_shard context manager
        """
        mock_executor.return_value.migrate = mock.Mock()

        MigrateShards().perform_migration(self.plan, ['default'], self.rose.schema_name, False, False)

        self.assertEqual(mock_use_shard_enter.call_count, 1)
        self.assertEqual(mock_use_shard_enter.call_args[0][0].node_name, 'default')
        self.assertEqual(mock_use_shard_enter.call_args[0][0].schema_name, 'test_rose')
        self.assertEqual(mock_executor.call_count, 1)
        mock_executor.return_value.migrate.assert_called_once_with(targets=None, plan=self.plan,
                                                                   fake=False, fake_initial=False)
        self.assertEqual(mock_use_shard_exit.call_count, 1)

    @mock.patch('sharding.management.commands.migrate_shards.Command.check_or_migrate_schema', autospec=True)
    @mock.patch('sharding.management.commands.migrate_shards.Command.check_or_migrate_shard', autospec=True)
    def test_on_all_shards(self, mock_check_or_migrate_shard, mock_check_or_migrate_schema):
        """
        Case: Call perform_migration without a target schema
        Expected: check_or_migrate_schema to be called 12 times (3 migration_nodes, 2 publics, 2 templates)
                  check_or_migrate_shard to be called 6 times (3 migration_nodes, 2 shards)
        """
        template_name = get_template_name()
        migrate_shards = MigrateShards()
        migrate_shards.perform_migration(self.plan, self.databases, None, False, False)

        self.assertEqual(mock_check_or_migrate_schema.call_count, 12)
        self.assertEqual(mock_check_or_migrate_shard.call_count, 6)
        for node in self.plan:
            mock_check_or_migrate_shard.assert_any_call(migrate_shards, self.rose, node, False, False)
            mock_check_or_migrate_shard.assert_any_call(migrate_shards, self.maria, node, False, False)
            mock_check_or_migrate_schema.assert_any_call(migrate_shards, 'default', 'public', node, False, False)
            mock_check_or_migrate_schema.assert_any_call(migrate_shards, 'other', 'public', node, False, False)
            mock_check_or_migrate_schema.assert_any_call(migrate_shards, 'default', template_name, node, False, False)
            mock_check_or_migrate_schema.assert_any_call(migrate_shards, 'other', template_name, node, False, False)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ShardedMigrationCheckOrMigrateSchemaTestCase(MigrationTestBase):
    available_apps = ['migration_tests', 'sharding', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # This makes completely unmigrated schemas
        mock_save = mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.clone_schema').start()
        cls.rose = Shard.objects.create(alias='rose', node_name='default', schema_name='test_rose',
                                        state=State.ACTIVE)
        cls.maria = Shard.objects.create(alias='maria', node_name='default', schema_name='test_maria',
                                         state=State.ACTIVE)
        mock_save.stop()

        cls.targets = [('migration_tests', '0003_third')]
        cls.databases = [db for db in settings.DATABASES]
        cls.plan = MigrateShards().get_plan_for_shard(cls.rose.node_name, cls.rose.schema_name, cls.targets)
        cls.migrateShards = MigrateShards()
        cls.migrateShards.verbosity = 2
        cls.out = six.StringIO()
        # cls.migrateShards.stdout = cls.out

    @mock.patch('sharding.utils.use_shard.__exit__', autospec=True)
    @mock.patch('sharding.utils.use_shard.__enter__', autospec=True)
    def test_use_shard(self, mock_use_shard_enter, mock_use_shard_exit):
        """
        Case: Call check_or_migrate_schema
        Expected: use_shard called
        """
        self.migrateShards.check_or_migrate_schema('other', 'public', self.plan[0], False, False)
        self.assertEqual(mock_use_shard_enter.call_count, 1)
        self.assertEqual(mock_use_shard_enter.call_args[0][0].node_name, 'other')
        self.assertEqual(mock_use_shard_enter.call_args[0][0].schema_name, 'public')
        self.assertEqual(mock_use_shard_exit.call_count, 1)

    @mock.patch('sharding.management.commands.migrate_shards.MigrationExecutor', autospec=True)
    def test_forwards_not_yet_applied(self, mock_executor):
        """
        Case: Call check_or_migrate_schema with a schema that not yet migrated
        Expected: Migrate to be called
        """
        self.migrateShards.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = []
        mock_executor.return_value.migrate = mock.Mock()

        self.migrateShards.check_or_migrate_schema('other', 'public', self.plan[0], False, False)

        self.migrateShards.stdout.write.assert_any_call(
            '    Applying migration_tests.0001_initial to default|public\n')
        mock_executor.return_value.migrate.assert_called_with(targets=None, plan=[self.plan[0]],
                                                              fake=False, fake_initial=False)

    @mock.patch('sharding.management.commands.migrate_shards.MigrationExecutor', autospec=True)
    def test_forwards_already_applied(self, mock_executor):
        """
        Case: Call check_or_migrate_schema with a schema that is already migrated
        Expected: Migrate not to be called
        """
        self.migrateShards.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = [('migration_tests', '0001_initial')]
        mock_executor.return_value.migrate = mock.Mock()

        self.migrateShards.check_or_migrate_schema('other', 'public', self.plan[0], False, False)
        self.migrateShards.stdout.write.assert_any_call(
            '    other|public has migration_tests.0001_initial already applied.\n')
        self.assertFalse(mock_executor.return_value.migrate.called)

    @mock.patch('sharding.management.commands.migrate_shards.MigrationExecutor', autospec=True)
    def test_backwards_already_applied(self, mock_executor):
        """
         Case: Call check_or_migrate_schema with a schema that is migrated; going backwards
        Expected: Migrate to be called
        """
        self.migrateShards.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = [('migration_tests', '0001_initial')]
        mock_executor.return_value.migrate = mock.Mock()

        migration_node = self.plan[0]
        migration_node = (migration_node[0], True)  # set as backwards migration

        self.migrateShards.check_or_migrate_schema('other', 'public', migration_node, False, False)
        self.migrateShards.stdout.write.assert_any_call(
            '    Unapplying migration_tests.0001_initial to default|public\n')
        self.assertTrue(mock_executor.return_value.migrate.called)

    @mock.patch('sharding.management.commands.migrate_shards.MigrationExecutor', autospec=True)
    def test_backwards_unapplied(self, mock_executor):
        """
        Case: Call check_or_migrate_schema with a schema that is not yet applied; going backwards
        Expected: Migrate not to be called
        """
        self.migrateShards.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = []
        mock_executor.return_value.migrate = mock.Mock()

        migration_node = self.plan[0]
        migration_node = (migration_node[0], True)  # set as backwards migration

        self.migrateShards.check_or_migrate_schema('other', 'public', migration_node, False, False)
        self.migrateShards.stdout.write.assert_any_call(
            '    other|public does not have migration_tests.0001_initial applied yet.\n')
        self.assertFalse(mock_executor.return_value.migrate.called)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ShardedMigrationCheckOrMigrateShardTestCase(MigrationTestBase):
    available_apps = ['migration_tests', 'sharding', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # This makes completely unmigrated schemas
        mock_save = mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.clone_schema').start()
        cls.rose = Shard.objects.create(alias='rose', node_name='other', schema_name='test_rose',
                                        state=State.ACTIVE)
        mock_save.stop()

        cls.targets = [('migration_tests', '0003_third')]
        cls.databases = [db for db in settings.DATABASES]
        cls.plan = MigrateShards().get_plan_for_shard(cls.rose.node_name, cls.rose.schema_name, cls.targets)
        cls.migrateShards = MigrateShards()
        cls.migrateShards.verbosity = 2
        cls.out = six.StringIO()

    @mock.patch('sharding.utils.use_shard.__exit__', autospec=True)
    @mock.patch('sharding.utils.use_shard.__enter__', autospec=True)
    def test_use_shard(self, mock_use_shard_enter, mock_use_shard_exit):
        """
        Case: Call check_or_migrate_shard
        Expected: use_shard called
        """
        self.migrateShards.check_or_migrate_shard(self.rose, self.plan[0], False, False)
        self.assertEqual(mock_use_shard_enter.call_count, 1)
        self.assertEqual(mock_use_shard_enter.call_args[0][0].node_name, 'other')
        self.assertEqual(mock_use_shard_enter.call_args[0][0].schema_name, 'test_rose')
        self.assertEqual(mock_use_shard_exit.call_count, 1)

    @mock.patch('sharding.management.commands.migrate_shards.MigrationExecutor', autospec=True)
    def test_forwards_not_yet_applied(self, mock_executor):
        """
        Case: Call check_or_migrate_shard with a schema that not yet migrated
        Expected: Migrate to be called
        """
        self.migrateShards.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = []
        mock_executor.return_value.migrate = mock.Mock()

        self.migrateShards.check_or_migrate_shard(self.rose, self.plan[0], False, False)

        self.migrateShards.stdout.write.assert_any_call(
            '    Applying migration_tests.0001_initial to other|rose\n')
        mock_executor.return_value.migrate.assert_called_with(targets=None, plan=[self.plan[0]],
                                                              fake=False, fake_initial=False)

    @mock.patch('sharding.management.commands.migrate_shards.MigrationExecutor', autospec=True)
    def test_forwards_already_applied(self, mock_executor):
        """
        Case: Call check_or_migrate_shard with a schema that is already migrated
        Expected: Migrate not to be called
        """
        self.migrateShards.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = [('migration_tests', '0001_initial')]
        mock_executor.return_value.migrate = mock.Mock()

        self.migrateShards.check_or_migrate_shard(self.rose, self.plan[0], False, False)
        self.migrateShards.stdout.write.assert_any_call(
            '    other|rose has migration_tests.0001_initial already applied.\n')
        self.assertFalse(mock_executor.return_value.migrate.called)

    @mock.patch('sharding.management.commands.migrate_shards.MigrationExecutor', autospec=True)
    def test_backwards_already_applied(self, mock_executor):
        """
         Case: Call check_or_migrate_shard with a schema that is migrated; going backwards
        Expected: Migrate to be called
        """
        self.migrateShards.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = [('migration_tests', '0001_initial')]
        mock_executor.return_value.migrate = mock.Mock()

        migration_node = self.plan[0]
        migration_node = (migration_node[0], True)  # set as backwards migration

        self.migrateShards.check_or_migrate_shard(self.rose, migration_node, False, False)
        self.migrateShards.stdout.write.assert_any_call(
            '    Unapplying migration_tests.0001_initial to other|rose\n')
        self.assertTrue(mock_executor.return_value.migrate.called)

    @mock.patch('sharding.management.commands.migrate_shards.MigrationExecutor', autospec=True)
    def test_backwards_unapplied(self, mock_executor):
        """
        Case: Call check_or_migrate_shard with a schema that is not yet applied; going backwards
        Expected: Migrate not to be called
        """
        self.migrateShards.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = []
        mock_executor.return_value.migrate = mock.Mock()

        migration_node = self.plan[0]
        migration_node = (migration_node[0], True)  # set as backwards migration

        self.migrateShards.check_or_migrate_shard(self.rose, migration_node, False, False)

        self.migrateShards.stdout.write.assert_any_call(
            '    other|rose does not have migration_tests.0001_initial applied yet.\n')
        self.assertFalse(mock_executor.return_value.migrate.called)

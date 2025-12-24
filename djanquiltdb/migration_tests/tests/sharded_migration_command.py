from io import StringIO
from unittest import mock

from django.conf import settings
from django.core.management import CommandError, call_command, get_commands
from django.db import ProgrammingError, connections
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.migration import Migration
from django.db.migrations.recorder import MigrationRecorder
from django.test import override_settings
from djanquiltdb.db import connection
from djanquiltdb.management.commands.migrate_shards import Command as MigrateShards
from djanquiltdb.tests import ShardingTestCase, disable_db_reconnect
from djanquiltdb.utils import (
    State,
    create_template_schema,
    get_all_databases,
    get_all_sharded_models,
    get_template_name,
    schema_exists,
    use_shard,
)
from example.models import Shard

from djanquiltdb import ShardingMode
from migration_tests.models import MirroredModel, ShardedModel, SuperMirroredModel, SuperShardedModel
from migration_tests.tests.migration_base import MigrationTestCase


class ShardedMigrationCrossSchemaRelationTestCase(ShardingTestCase):
    available_apps = ['djanquiltdb', 'migration_tests', 'example']

    def setUp(self):
        self.databases = get_all_databases()

        super().setUp()

        create_template_schema(migrate=False)
        create_template_schema('other', migrate=False)

        self.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default', state=State.ACTIVE)

    def test_forward_migration(self):
        """
        Case: Run forwards migrations, for migrating involving sharded -> mirrored fKeys
        Expected: No errors, and tables to be created.
        Note: If you get 'ProgrammingError: relation "migration_tests_mirroredmodel" does not exist' that means the
              migration performed on sina shard could not reach the model on the public schema. Making sure that
              works is the point of this test.
        """
        call_command('migrate_shards', verbosity=0)

        super_mirrored = SuperMirroredModel.objects.create(name='super!')
        mirrored = MirroredModel.objects.create(name='less super', super=super_mirrored)

        with use_shard(self.sina):
            super_sharded = SuperShardedModel.objects.create(name='super shard!', to_mirrored=mirrored)
            ShardedModel.objects.create(name='A bit lame', super=super_sharded)


@mock.patch('django.core.management.get_commands', mock.Mock(return_value={'migrate_shards': 'djanquiltdb'}))
class ShardedMigrationSystemTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'example']

    def setUp(self):
        super().setUp()
        # this is not added as decorator, for that won't work for the setup.
        self.mock_router = mock.patch('djanquiltdb.router.DynamicDbRouter.allow_migrate').start()
        self.addCleanup(mock.patch.stopall)

        # Unlike other migrations, these test migrations are NOT applied during the creation of the testcase
        # For they are not known to the runner at that point.

        self.databases = get_all_databases()

        with override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'}):
            for db in self.databases:
                # default|public migrates fully
                with use_shard(node_name=db, schema_name='public') as env:
                    executor = MigrationExecutor(env.connection)
                    executor.migrate([('migration_tests', '0003_third')])
                    executor.loader.build_graph()

                # default|template migrates fully
                # therefore the shards created after this will be fully migrated as well.
                with use_shard(node_name=db, schema_name='template') as env:
                    executor = MigrationExecutor(env.connection)
                    executor.migrate([('migration_tests', '0003_third')])
                    executor.loader.build_graph()

            # revert 1st shard Sina migrates to the first migration
            self.sina = Shard.objects.create(
                alias='sina', schema_name='test_sina', node_name='default', state=State.ACTIVE
            )
            with use_shard(self.sina) as env:
                executor = MigrationExecutor(env.connection)
                executor.migrate([('migration_tests', '0001_initial')])
                executor.loader.build_graph()

            # revert 2nd shard Rose migrates to the first 2 migrations
            self.rose = Shard.objects.create(
                alias='rose', schema_name='test_rose', node_name='default', state=State.ACTIVE
            )
            with use_shard(self.rose) as env:
                executor = MigrationExecutor(env.connection)
                executor.migrate([('migration_tests', '0002_second')])
                executor.loader.build_graph()

            # We keep Maria fully migrated (to 0003)
            self.maria = Shard.objects.create(
                alias='maria', schema_name='test_maria', node_name='default', state=State.MAINTENANCE
            )

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    def test_forward_migration_as_a_whole(self):
        """
        Case: Call migrate_shards with shard in several states of migration.
        Expected: All shards to be fully migrated
        Note: This is system test keeping migrate_shards as a black box.
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

        call_command('migrate_shards', verbosity=0)

        # all shards, templates and publics are now fully migrated
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
        call_command('migrate_shards', app_label='migration_tests', migration_name='zero', verbosity=0)

        # all shards, templates and publics are now back so square 0
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


class OriginalMigrationTestCase(MigrationTestCase):
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
    @disable_db_reconnect()
    def test_migrate_fake_initial(self):
        """
        Case: #24184 - Tests that --fake-initial only works if all tables created in
              the initial migration of an app exists
        Expected: Tables only to exist when they should
        """
        # Make sure no tables are created
        self.assertTableNotExists('migration_tests_author')
        self.assertTableNotExists('migration_tests_tribble')

        with self.subTest('Run the migrations to 0001 only'):
            call_command('migrate_shards', 'migration_tests', '0001', verbosity=0)
            # Make sure the right tables exist
            self.assertTableExists('migration_tests_author')
            self.assertTableExists('migration_tests_tribble')

        with self.subTest('Fake a roll-back'):
            call_command('migrate_shards', 'migration_tests', 'zero', fake=True, verbosity=0)
            # Make sure the tables still exist
            self.assertTableExists('migration_tests_author')
            self.assertTableExists('migration_tests_tribble')

        with self.subTest('Run initial migration'):
            out = StringIO()
            with mock.patch('sys.exit') as mock_exit:
                call_command('migrate_shards', 'migration_tests', '0001', verbosity=0, stderr=out)

            self.assertIn('relation "migration_tests_author" already exists', out.getvalue().lower())
            mock_exit.assert_called_once_with(1)

        with self.subTest('Run initial migration with an explicit --fake-initial'):
            with mock.patch('django.core.management.color.supports_color', lambda *args: False):
                call_command('migrate_shards', 'migration_tests', '0001', fake_initial=True, stdout=out, verbosity=1)
            self.assertIn('migration_tests.0001_initial... faked', out.getvalue().lower())

        with self.subTest('Run all migrations'):
            call_command('migrate_shards', verbosity=0)
            # Make sure the right tables exist
            self.assertTableExists('migration_tests_author')
            self.assertTableNotExists('migration_tests_tribble')
            self.assertTableExists('migration_tests_book')

        with self.subTest('Fake a roll-back'):
            call_command('migrate_shards', 'migration_tests', 'zero', fake=True, verbosity=0)
            # Make sure the tables still exist
            self.assertTableExists('migration_tests_author')
            self.assertTableNotExists('migration_tests_tribble')
            self.assertTableExists('migration_tests_book')

        with self.subTest('Run initial migration'):
            out = StringIO()
            with mock.patch('sys.exit') as mock_exit:
                call_command('migrate_shards', 'migration_tests', stderr=out, verbosity=0)

            self.assertIn('relation "migration_tests_author" already exists', out.getvalue().lower())
            mock_exit.assert_called_once_with(1)

        with self.subTest('Run initial migration with an explicit --fake-initial'):
            # Fails because 'migration_tests_tribble' does not exist but needs to,
            # in order to make --fake-initial work.
            out = StringIO()
            with mock.patch('sys.exit') as mock_exit:
                call_command('migrate_shards', 'migration_tests', fake_initial=True, stderr=out, verbosity=0)

            self.assertIn('relation "migration_tests_author" already exists', out.getvalue().lower())
            mock_exit.assert_called_once_with(1)

        with self.subTest('Fake an apply'):
            call_command('migrate_shards', 'migration_tests', fake=True, verbosity=0)

        with self.subTest('Unmigrate everything'):
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
        out = StringIO()
        call_command('migrate_shards', 'migration_tests', verbosity=0)
        call_command('showmigrations', 'migration_tests', stdout=out, no_color=True)
        self.assertEqual('migration_tests\n [x] 0001_squashed_0002 (2 squashed migrations)\n', out.getvalue().lower())
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
        out = StringIO()
        call_command('migrate_shards', 'migration_tests', schema_name='public', verbosity=0)
        call_command('showmigrations', 'migration_tests', stdout=out, no_color=True)
        self.assertEqual('migration_tests\n [x] 0001_squashed_0002 (2 squashed migrations)\n', out.getvalue().lower())
        self.assertIn(('migration_tests', '0001_squashed_0002'), recorder.applied_migrations())
        # No changes were actually applied so there is nothing to rollback


original_apply_migration = Migration.apply


def fake_apply_migration(self, project_state, schema_editor, collect_sql=False):
    # Raise exception for one specific migration. Apply all the others normally.
    if self.name == '0002_second' and schema_editor.connection.schema_name == 'test_sina':
        raise ProgrammingError('table "migration_test_hometown" does not exist')

    original_apply_migration(self, project_state, schema_editor, collect_sql)


class ShardedMigrationHandleTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.databases = get_all_databases()

    def setUp(self):
        super().setUp()

        self.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default', state=State.ACTIVE)
        self.rose = Shard.objects.create(alias='rose', schema_name='test_rose', node_name='default', state=State.ACTIVE)
        self.maria = Shard.objects.create(
            alias='maria', schema_name='test_maria', node_name='default', state=State.ACTIVE
        )

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    @mock.patch('sys.exit')
    @mock.patch('djanquiltdb.management.commands.migrate_shards.Command.perform_migration')
    @mock.patch('djanquiltdb.management.commands.migrate_shards.Command.get_plan')
    @mock.patch(
        'djanquiltdb.management.commands.migrate_shards.Command.get_targets_from_options',
        return_value=(mock.Mock(), mock.Mock()),
    )
    @mock.patch('djanquiltdb.management.commands.migrate_shards.Command.check_for_app_conflicts')
    @mock.patch('django.db.backends.base.base.BaseDatabaseWrapper.prepare_database')
    @mock.patch('djanquiltdb.management.commands.migrate_shards.get_databases_and_schema_from_options')
    @mock.patch('djanquiltdb.management.commands.migrate_shards.import_module')
    def test_migrate_handle(
        self,
        mock_import_module,
        mock_get_db_from_options,
        mock_prepare_database,
        mock_check_conflicts,
        mock_get_targets,
        mock_get_plan,
        mock_perform_migration,
        mock_exit,
    ):
        """
        Case: Call MigrateShards.handle()
        Expected: A ton of external functions to be called. No specific sys.exit called.
        """
        mock_get_db_from_options.return_value = ([db for db in settings.DATABASES], None)
        mock_check_conflicts.return_value = False

        options = {
            'database': 'all',
            'fake': False,
            'fake_initial': False,
        }
        MigrateShards().handle(**options)

        mock_import_module.assert_called_once_with('.management', 'djanquiltdb')
        mock_get_db_from_options.assert_called_once_with(options)
        self.assertEqual(mock_prepare_database.call_count, 2)  # We have 2 databases
        self.assertEqual(mock_check_conflicts.call_count, 1)
        self.assertEqual(mock_get_targets.call_count, 1)
        self.assertEqual(mock_get_plan.call_count, 1)
        self.assertEqual(mock_perform_migration.call_count, 1)

        mock_exit.assert_called_once_with(1)

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    @mock.patch('sys.exit')
    def test_failure_during_migration(self, mock_exit):
        """
        Case: Call MigrateShards().handle and trigger an error during one of the migrations
        Expected: That migration_node to be completed and then report the error and leave an exit code: 1.
        """

        patcher = mock.patch(
            'django.db.migrations.migration.Migration.apply', side_effect=fake_apply_migration, autospec=True
        )
        mock_apply_migration = patcher.start()

        stderr = StringIO()
        stdout = StringIO()
        migrate_shards = MigrateShards()
        migrate_shards.stderr = stderr
        migrate_shards.stdout = stdout
        migrate_shards.handle(app_label='migration_tests', database='all', fake=False, fake_initial=False, verbosity=0)
        self.assertIn(
            'default|sina: migration_tests.0002_second - programmingerror: table "migration_test_hometown" '
            'does not exist',
            stderr.getvalue().lower(),
        )
        self.assertIn(
            'migration stopped due to errors after completing migration_tests.0002_second.', stdout.getvalue().lower()
        )
        self.assertEqual(mock_apply_migration.call_count, 14)  # 2 migrates for 3 shards, 2 publics and 2 templates.
        patcher.stop()

        # all shards, templates and publics are migrated to 0002 (except sina):
        for db in self.databases:
            with use_shard(node_name=db, schema_name='template') as env:
                recorder = MigrationRecorder(env.connection)
                applied_migration_tests = recorder.applied_migrations()
                self.assertFalse(('migration_tests', '0003_third') in applied_migration_tests)
                self.assertTrue(('migration_tests', '0002_second') in applied_migration_tests)
                self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)
        with use_shard(self.sina) as env:
            recorder = MigrationRecorder(env.connection)
            applied_migration_tests = recorder.applied_migrations()
            self.assertFalse(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertFalse(('migration_tests', '0002_second') in applied_migration_tests)  # this one gave the error
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
            self.assertFalse(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)

        mock_exit.assert_called_once_with(1)

        # rollback
        MigrateShards().handle(app_label='migration_tests', migration_name='zero', database='all', verbosity=0)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ShardedMigrationGetTargetsTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        self.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default', state=State.ACTIVE)

    @mock.patch('django.db.migrations.graph.MigrationGraph.leaf_nodes')
    def test_without_special_options(self, mock_leave_nodes):
        """
        Case: Call get_targets_from_options without options.
        Expected: All leaf nodes returned as targets.
        """
        leave_nodes = [('migration_tests', '0003_third'), ('djanquiltdb', '0002_second')]
        mock_leave_nodes.return_value = leave_nodes

        executor = MigrationExecutor(connection)
        self.assertEqual(MigrateShards().get_targets_from_options(executor, options={}), (True, leave_nodes))

    @mock.patch('django.db.migrations.graph.MigrationGraph.leaf_nodes')
    def test_with_app_label(self, mock_leave_nodes):
        """
        Case: Call get_targets_from_options with app_label as option.
        Expected: Single leaf returned
        """
        leave_nodes = [('migration_tests', '0003_third'), ('example', '0002_auto_20171009_1502')]
        mock_leave_nodes.return_value = leave_nodes

        executor = MigrationExecutor(connection)
        self.assertEqual(
            MigrateShards().get_targets_from_options(executor, options={'app_label': 'example'}),
            (False, [('example', '0002_auto_20171009_1502')]),
        )

    @mock.patch('django.db.migrations.graph.MigrationGraph.leaf_nodes')
    def test_with_target_migration(self, mock_leave_nodes):
        """
        Case: Call get_targets_from_options with app_label as option.
        Expected: Single leaf returned
        """
        leave_nodes = [('migration_tests', '0003_third'), ('example', '0002_auto_20171009_1502')]
        mock_leave_nodes.return_value = leave_nodes

        executor = MigrationExecutor(connection)
        self.assertEqual(
            MigrateShards().get_targets_from_options(
                executor, options={'app_label': 'migration_tests', 'migration_name': '0002_second'}
            ),
            (False, [('migration_tests', '0002_second')]),
        )

    @mock.patch('django.db.migrations.graph.MigrationGraph.leaf_nodes')
    def test_with_zero_target(self, mock_leave_nodes):
        """
        Case: Call get_targets_from_options with zero as option.
        Expected: [(app_label, None)] returned
        """
        leave_nodes = [('migration_tests', '0003_third'), ('example', '0002_auto_20171009_1502')]
        mock_leave_nodes.return_value = leave_nodes

        executor = MigrationExecutor(connection)
        self.assertEqual(
            MigrateShards().get_targets_from_options(
                executor, options={'app_label': 'migration_tests', 'migration_name': 'zero'}
            ),
            (False, [('migration_tests', None)]),
        )

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
            (
                MigrateShards().get_targets_from_options(
                    executor, options={'app_label': 'migration_tests', 'migration_name': '9001_over_9k'}
                ),
            )
        self.assertEqual(
            error.exception.args[0], "Cannot find a migration matching '9001_over_9k' from app 'migration_tests'."
        )

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
            (MigrateShards().get_targets_from_options(executor, options={'app_label': 'Hans'}),)
        self.assertEqual(
            error.exception.args[0], "App 'Hans' does not have migrations (you cannot selectively sync unmigrated apps)"
        )


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ShardedMigrationGetPlanTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.targets = [('migration_tests', '0003_third')]
        cls.databases = [db for db in settings.DATABASES]

    def setUp(self):
        super().setUp()

        self.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default', state=State.ACTIVE)

    @mock.patch('djanquiltdb.management.commands.migrate_shards.Command.get_plan_for_shard')
    def test_all_shards_called(self, mock_get_plan_for_shard):
        """
        Case: Call get_plan
        Expected: get_plan_for_shard to be called for all schemas
        """
        mock_get_plan_for_shard.return_value = [
            (('migration_tests', '0001_initial'), False),
            (('migration_tests', '0002_second'), False),
            (('migration_tests', '0003_third'), False),
        ]

        MigrateShards().get_plan(self.targets, self.databases, None)

        mock_get_plan_for_shard.assert_any_call(self.targets, 'default', 'public')
        mock_get_plan_for_shard.assert_any_call(self.targets, 'default', 'template')
        mock_get_plan_for_shard.assert_any_call(self.targets, 'default', 'test_sina')
        mock_get_plan_for_shard.assert_any_call(self.targets, 'other', 'public')
        mock_get_plan_for_shard.assert_any_call(self.targets, 'other', 'template')

    def test_different_migration_states(self):
        """
        Case: Call get_plan when not all schema's have the same migration level
        Expected: get_plan_for_shard to be called for all schemas
        Note: get_plan_for_shard is not mocked. So it's functionality is taken into account
        """
        # Migrate the public schema's and the template schemas fully
        call_command('migrate_shards', 'migration_tests', database='default', verbosity=0)
        call_command('migrate_shards', 'migration_tests', database='other', verbosity=0)

        # This makes completely unmigrated schemas, because we skip the cloning.
        with mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.clone_schema') as mock_save:
            Shard.objects.create(alias='rose', node_name='default', schema_name='test_rose')
            Shard.objects.create(alias='maria', node_name='default', schema_name='test_maria')
            self.assertEqual(mock_save.call_count, 2)

        # Migrate rose a bit
        call_command(
            'migrate_shards', 'migration_tests', '0001', database='default', schema_name='test_rose', verbosity=0
        )

        # Migrate maria a bit further
        call_command(
            'migrate_shards', 'migration_tests', '0002', database='default', schema_name='test_maria', verbosity=0
        )

        # rose is the furthest behind. So we should get her migration path
        self.assertEqual(
            MigrateShards().get_plan(self.targets, self.databases, None),
            MigrateShards().get_plan_for_shard(self.targets, 'default', 'test_rose'),
        )

        # Rollback: cleanup for other tests. Shards are automatically removed.
        call_command('migrate_shards', 'migration_tests', 'zero', database='default', verbosity=0)
        call_command('migrate_shards', 'migration_tests', 'zero', database='other', verbosity=0)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ShardedMigrationGetPlanForShardTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.targets = [('migration_tests', '0003_third')]
        cls.databases = [db for db in settings.DATABASES]

    def setUp(self):
        super().setUp()

        self.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default', state=State.ACTIVE)

    @mock.patch('djanquiltdb.management.commands.migrate_shards.MigrationExecutor', autospec=True)
    @mock.patch('djanquiltdb.utils.use_shard.__exit__', autospec=True)
    @mock.patch('djanquiltdb.utils.use_shard.__enter__', autospec=True)
    def test_use_shard_called(self, mock_use_shard_enter, mock_use_shard_exit, mock_executor):
        """
        Case: Call get_plan_for_shard
        Expected: executor.migration_plan and use_sahrd called
        """
        mock_executor.return_value.migration_plan = mock.Mock()

        MigrateShards().get_plan_for_shard(self.targets, self.sina.node_name, self.sina.schema_name)

        self.assertEqual(mock_use_shard_enter.call_count, 1)
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.node_name, 'default')
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.schema_name, 'test_sina')
        self.assertEqual(mock_executor.call_count, 1)
        mock_executor.return_value.migration_plan.assert_called_once_with(self.targets)
        self.assertEqual(mock_use_shard_exit.call_count, 1)


class ShardedMigrationCheckForAppConflicts(MigrationTestCase):
    def test_migrate_conflict_exit(self):
        """
        Case: Call check_for_app_conflicts with a conflicting migration set
        Expected: Raise a CommandError
        """
        with self.assertRaisesMessage(CommandError, 'Conflicting migrations detected'):
            mock_executor = mock.Mock()
            mock_executor.loader.detect_conflicts.return_value = {'an app': 'a conflict'}
            MigrateShards().check_for_app_conflicts(executor=mock_executor)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ShardedMigrationPerformMigrationTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        # This makes completely unmigrated schemas
        with mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.clone_schema'):
            self.rose = Shard.objects.create(
                alias='rose', node_name='default', schema_name='test_rose', state=State.ACTIVE
            )
            self.maria = Shard.objects.create(
                alias='maria', node_name='default', schema_name='test_maria', state=State.ACTIVE
            )

        self.targets = [('migration_tests', '0003_third')]
        self.databases = [db for db in settings.DATABASES]
        self.plan = MigrateShards().get_plan_for_shard(self.targets, self.rose.node_name, self.rose.schema_name)

    @mock.patch('djanquiltdb.management.commands.migrate_shards.MigrationExecutor', autospec=True)
    @mock.patch('djanquiltdb.utils.use_shard.__exit__', autospec=True)
    @mock.patch('djanquiltdb.utils.use_shard.__enter__', autospec=True)
    def test_specific_schema(self, mock_use_shard_enter, mock_use_shard_exit, mock_executor):
        """
        Case: Call perform_migration with a specific schema.
        Expected: executor.migrate to be called with the right arguments within a use_shard context manager
        """
        mock_executor.return_value.migrate = mock.Mock()

        MigrateShards().perform_migration(self.plan, ['default'], self.rose.schema_name, False, False)

        self.assertEqual(mock_use_shard_enter.call_count, 1)
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.node_name, 'default')
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.schema_name, 'test_rose')
        self.assertEqual(mock_executor.call_count, 1)
        mock_executor.return_value.migrate.assert_called_once_with(
            targets=None, plan=self.plan, fake=False, fake_initial=False
        )
        self.assertEqual(mock_use_shard_exit.call_count, 1)

    @mock.patch(
        'djanquiltdb.management.commands.migrate_shards.Command.check_or_migrate_schema',
        return_value=False,
        autospec=True,
    )
    @mock.patch(
        'djanquiltdb.management.commands.migrate_shards.Command.check_or_migrate_shard',
        return_value=False,
        autospec=True,
    )
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

    @mock.patch(
        'djanquiltdb.management.commands.migrate_shards.Command.check_or_migrate_schema',
        return_value=True,
        autospec=True,
    )
    @mock.patch(
        'djanquiltdb.management.commands.migrate_shards.Command.check_or_migrate_shard',
        return_value=True,
        autospec=True,
    )
    def test_return_values(self, mock_check_or_migrate_shard, mock_check_or_migrate_schema):
        """
        Case: Call perform_migration while check_or_migrate_schema/shard returns a combination of True and False
        Expected: perform_migration to return True when either check_or_migrate returns True
        """
        for schema_value in [True, False]:
            for shard_value in [True, False]:
                with self.subTest('Return value {}, {}'.format(schema_value, shard_value)):
                    mock_check_or_migrate_shard.reset_mock()
                    mock_check_or_migrate_shard.return_value = shard_value
                    mock_check_or_migrate_schema.reset_mock()
                    mock_check_or_migrate_schema.return_value = schema_value

                    migrate_shards = MigrateShards()
                    return_value = migrate_shards.perform_migration(self.plan, self.databases, None, False, False)

                    self.assertEqual(return_value, schema_value | shard_value)
                    self.assertTrue(mock_check_or_migrate_shard.called)
                    self.assertTrue(mock_check_or_migrate_schema.called)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ShardedMigrationCheckOrMigrateSchemaTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        # This makes completely unmigrated schemas
        with mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.clone_schema'):
            self.rose = Shard.objects.create(
                alias='rose', node_name='default', schema_name='test_rose', state=State.ACTIVE
            )
            self.maria = Shard.objects.create(
                alias='maria', node_name='default', schema_name='test_maria', state=State.ACTIVE
            )

        self.targets = [('migration_tests', '0003_third')]
        self.databases = [db for db in settings.DATABASES]
        self.plan = MigrateShards().get_plan_for_shard(self.targets, self.rose.node_name, self.rose.schema_name)
        self.migrateShards = MigrateShards()
        self.migrateShards.verbosity = 2

    @mock.patch('djanquiltdb.utils.use_shard.__exit__', autospec=True)
    @mock.patch('djanquiltdb.utils.use_shard.__enter__', autospec=True)
    def test_use_shard(self, mock_use_shard_enter, mock_use_shard_exit):
        """
        Case: Call check_or_migrate_schema
        Expected: use_shard called
        """
        self.migrateShards.check_or_migrate_schema('other', 'public', self.plan[0], False, False)
        self.assertEqual(mock_use_shard_enter.call_count, 1)
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.node_name, 'other')
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.schema_name, 'public')
        self.assertEqual(mock_use_shard_exit.call_count, 1)

    @mock.patch('djanquiltdb.management.commands.migrate_shards.MigrationExecutor', autospec=True)
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

        self.migrateShards.stdout.write.assert_any_call('    Applying migration_tests.0001_initial to default|public\n')
        mock_executor.return_value.migrate.assert_called_with(
            targets=None, plan=[self.plan[0]], fake=False, fake_initial=False
        )

    @mock.patch('djanquiltdb.management.commands.migrate_shards.MigrationExecutor', autospec=True)
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
            '    other|public has migration_tests.0001_initial already applied.\n'
        )
        self.assertFalse(mock_executor.return_value.migrate.called)

    @mock.patch('djanquiltdb.management.commands.migrate_shards.MigrationExecutor', autospec=True)
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
            '    Unapplying migration_tests.0001_initial to default|public\n'
        )
        self.assertTrue(mock_executor.return_value.migrate.called)

    @mock.patch('djanquiltdb.management.commands.migrate_shards.MigrationExecutor', autospec=True)
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
            '    other|public does not have migration_tests.0001_initial applied yet.\n'
        )
        self.assertFalse(mock_executor.return_value.migrate.called)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
class ShardedMigrationCheckOrMigrateShardTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        # This makes completely unmigrated schemas
        with mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.clone_schema'):
            self.rose = Shard.objects.create(
                alias='rose', node_name='other', schema_name='test_rose', state=State.ACTIVE
            )

        self.targets = [('migration_tests', '0003_third')]
        self.databases = [db for db in settings.DATABASES]
        self.plan = MigrateShards().get_plan_for_shard(self.targets, self.rose.node_name, self.rose.schema_name)
        self.migrateShards = MigrateShards()
        self.migrateShards.verbosity = 2

    @mock.patch('djanquiltdb.utils.use_shard.__exit__', autospec=True)
    @mock.patch('djanquiltdb.utils.use_shard.__enter__', autospec=True)
    def test_use_shard(self, mock_use_shard_enter, mock_use_shard_exit):
        """
        Case: Call check_or_migrate_shard
        Expected: use_shard called
        """
        self.migrateShards.check_or_migrate_shard(self.rose, self.plan[0], False, False)
        self.assertEqual(mock_use_shard_enter.call_count, 1)
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.node_name, 'other')
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.schema_name, 'test_rose')
        self.assertEqual(mock_use_shard_exit.call_count, 1)

    @mock.patch('djanquiltdb.management.commands.migrate_shards.MigrationExecutor', autospec=True)
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

        self.migrateShards.stdout.write.assert_any_call('    Applying migration_tests.0001_initial to other|rose\n')
        mock_executor.return_value.migrate.assert_called_with(
            targets=None, plan=[self.plan[0]], fake=False, fake_initial=False
        )

    @mock.patch('djanquiltdb.management.commands.migrate_shards.MigrationExecutor', autospec=True)
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
            '    other|rose has migration_tests.0001_initial already applied.\n'
        )
        self.assertFalse(mock_executor.return_value.migrate.called)

    @mock.patch('djanquiltdb.management.commands.migrate_shards.MigrationExecutor', autospec=True)
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
        self.migrateShards.stdout.write.assert_any_call('    Unapplying migration_tests.0001_initial to other|rose\n')
        self.assertTrue(mock_executor.return_value.migrate.called)

    @mock.patch('djanquiltdb.management.commands.migrate_shards.MigrationExecutor', autospec=True)
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
            '    other|rose does not have migration_tests.0001_initial applied yet.\n'
        )
        self.assertFalse(mock_executor.return_value.migrate.called)


class SeparateDatabaseAndStateTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'example']

    def setUp(self):
        # Do not silently mock the router, but do create the template schema

        commands = get_commands()
        commands['migrate_shards'] = 'djanquiltdb'

        with mock.patch('django.core.management.get_commands', return_value=commands):
            create_template_schema()  # The template won't have any migration applied to it initially
            create_template_schema('other')  # The template won't have any migration applied to it initially

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_remove_model'})
    @mock.patch('djanquiltdb.router.DynamicDbRouter.allow_migrate')
    def test(self, mock_allow_migrate):
        """
        Case: Migrate with a SeparateDatabaseAndState operation and a model that does not exist in the apps.
        Expected: allow_migrate to block all database operations, but not the state operations
        """

        self.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default', state=State.ACTIVE)
        with use_shard(self.sina) as env:
            # Setup the shard with the initial migration ran.
            executor = MigrationExecutor(env.connection)
            executor.migrate([('migration_tests', '0001_initial')])
            executor.loader.build_graph()

            recorder = MigrationRecorder(env.connection)
            applied_migration_tests = recorder.applied_migrations()
            self.assertFalse(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertFalse(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)
            # allow_migrate called once for create_model
            self.assertEqual(mock_allow_migrate.call_count, 1)
            mock_allow_migrate.reset_mock()

            executor.migrate([('migration_tests', '0002_second')])
            executor.loader.build_graph()
            applied_migration_tests = recorder.applied_migrations()
            self.assertFalse(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)
            # allow_migrate called twice. For RemoveFIeld and AddField
            self.assertEqual(mock_allow_migrate.call_count, 2)
            mock_allow_migrate.reset_mock()

            # 0003 uses SeparateDatabaseAndState to only perform the state operation
            executor.migrate([('migration_tests', '0003_third')])
            executor.loader.build_graph()
            applied_migration_tests = recorder.applied_migrations()
            self.assertTrue(('migration_tests', '0003_third') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0002_second') in applied_migration_tests)
            self.assertTrue(('migration_tests', '0001_initial') in applied_migration_tests)
            # allow_migrate not called because of the SeparateDatabaseAndState
            self.assertEqual(mock_allow_migrate.call_count, 0)


class RemoveModelMigrationTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        # Do not silently mock the router, but do create the template schema

        commands = get_commands()
        commands['migrate_shards'] = 'djanquiltdb'

        with mock.patch('django.core.management.get_commands', return_value=commands):
            create_template_schema()  # The template won't have any migration applied to it initially
            create_template_schema('other')  # The template won't have any migration applied to it initially

    @override_settings(
        MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_remove_non_existent_model'},
        SHARDING={
            'SHARD_CLASS': 'example.models.Shard',
            'OVERRIDE_SHARDING_MODE': {('migration_tests', 'nonexistingmodel'): ShardingMode.SHARDED},
        },
    )
    def test(self):
        """
        Case: Create and Remove a non existing model in migrations. This model is mentioned in the settings as sharded.
        Expected: The model is created and removed as the migrations dictate. These are not skipped because the model
                  definition is missing as otherwise would be the case.
        """
        self.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default', state=State.ACTIVE)

        call_command('migrate_shards', 'migration_tests', '0001', database='default', verbosity=0)

        with use_shard(self.sina, include_public=False) as env:
            self.assertIn('migration_tests_nonexistingmodel', env.connection.introspection.table_names())

        call_command('migrate_shards', 'migration_tests', '0002', database='default', verbosity=0)

        with use_shard(self.sina, include_public=False) as env:
            self.assertNotIn('migration_tests_nonexistingmodel', env.connection.introspection.table_names())


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_unroutable'})
class UnroutableMigrationTestCase(ShardingTestCase):
    available_apps = ['migration_tests', 'djanquiltdb']

    @mock.patch('sys.exit')
    def test_run_python(self, mock_exit):
        """
        Case: Run a migration with a run_python operation lacking hints.
        Expected: Migration stopped and error printed to stderr, exit code: 1.
        """
        stderr = mock.Mock()
        call_command('migrate_shards', 'migration_tests', '0001_run_python', verbosity=0, stderr=stderr)

        stderr.write.assert_has_calls(
            [
                mock.call(
                    '    default|public: migration_tests.0001_run_python - ProgrammingError: Cannot determine '
                    'sharding mode for this operation (app migration_tests). Are you sure it is bound to an existing '
                    'model or has hints? app_label: migration_tests, model_name: None\n'
                ),
                mock.call(
                    '    other|public: migration_tests.0001_run_python - ProgrammingError: Cannot determine sharding '
                    'mode for this operation (app migration_tests). Are you sure it is bound to an existing model or '
                    'has hints? app_label: migration_tests, model_name: None\n'
                ),
            ],
            any_order=True,
        )

        mock_exit.assert_called_once_with(1)

    def test_run_sql(self):
        """
        Case: Run a migration with a run_sql operation lacking hints.
        Expected: A ProgrammingError to be raised.
        """
        # Mark migration 0001 as migrated, but don't actually perform it. Since it will raise an error.
        executor = MigrationExecutor(connection)
        executor.migrate([('migration_tests', '0001_run_python')], fake=True)
        executor.loader.build_graph()

        with self.assertRaises(ProgrammingError):
            executor.migrate([('migration_tests', '0002_run_sql')])


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_removed_model'})
class UnroutableMigrationTestCase2(ShardingTestCase):
    available_apps = ['migration_tests', 'djanquiltdb']

    @mock.patch('djanquiltdb.router.logger.warning')
    def test(self, mock_logger_warning):
        """
        Case: Run migrations for a model that no longer exists.
        Expected: All operations to trigger a warning.
        """
        call_command('migrate_shards', 'migration_tests', '0001', database='default', verbosity=0)
        call_command('migrate_shards', 'migration_tests', '0002', database='default', verbosity=0)
        call_command('migrate_shards', 'migration_tests', '0003', database='default', verbosity=0)

        self.assertEqual(mock_logger_warning.call_count, 3)
        mock_logger_warning.assert_has_calls(
            [
                mock.call('Migration operation for unknown models are ignored. Are you sure this model still exists?'),
                mock.call('Migration operation for unknown models are ignored. Are you sure this model still exists?'),
                mock.call('Migration operation for unknown models are ignored. Are you sure this model still exists?'),
            ]
        )


class DisableMigrations(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return None


@override_settings(MIGRATION_MODULES=DisableMigrations())
class SyncDbTestCase(MigrationTestCase):
    available_apps = ['djanquiltdb', 'example']

    def test(self):
        """
        Case: Disable the migrations and check whether the template schema is built from the state
        Expected: All tables from the example app are created in the template schema
        """
        call_command('migrate_shards', verbosity=0, interactive=False, run_syncdb=True)

        with use_shard(node_name='default', schema_name=get_template_name()):
            for model in get_all_sharded_models(include_auto_created=True):
                if model._meta.app_label == 'example':
                    self.assertTableExists(model._meta.db_table)

    def test_emit_pre_migrate_signal(self):
        """
        Case: In migrate_shards, run the sync db phase and don't run the sync db phase
        Expected: In both cases, emit_pre_migrate_signal is called
        """
        verbosity = 0
        interactive = False

        with mock.patch('djanquiltdb.management.commands.migrate_shards.emit_pre_migrate_signal') as mock_signal:
            call_command('migrate_shards', verbosity=verbosity, run_syncdb=True, interactive=interactive)
            mock_signal.assert_called_once_with(verbosity, interactive, 'default', plan=mock.ANY)

        with mock.patch('djanquiltdb.management.commands.migrate_shards.emit_pre_migrate_signal') as mock_signal:
            call_command('migrate_shards', verbosity=verbosity, run_syncdb=False, interactive=interactive)
            mock_signal.assert_called_once_with(verbosity, interactive, 'default', plan=mock.ANY)


class StagesMigrationTestCase(ShardingTestCase):
    def test_no_shard_table(self):
        """
        Case: Migrate back to a state where the shard table does not exist on the public schema and then migrate
        Expected: Migrate command should perform migrations on the public schema normally, which will create the shard
                  table again
        """
        self.assertIn(Shard._meta.db_table, connections['default'].introspection.table_names())

        # Let's revert the public schema to an initial state
        call_command('migrate_shards', 'example', 'zero', database='default', verbosity=0)

        self.assertNotIn(Shard._meta.db_table, connections['default'].introspection.table_names())

        # And now do an initial migration, like how we start a project initially
        call_command('migrate_shards', 'example', database='default', verbosity=0)

        self.assertIn(Shard._meta.db_table, connections['default'].introspection.table_names())

    def test_no_template_schema(self):
        """
        Case: Call migrate_shards without having a template schema
        Expected: Migrate command runs without errors
        """
        self.assertFalse(schema_exists('default', get_template_name()))
        call_command('migrate_shards', 'example', database='default', verbosity=0)

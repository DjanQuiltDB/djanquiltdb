from importlib import import_module
from io import StringIO
from unittest import mock

from django.conf import settings
from django.core.management import CommandError, call_command, get_commands
from django.db import ProgrammingError, connections
from django.db.migrations import AddField, Migration
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.loader import MigrationLoader
from django.db.migrations.recorder import MigrationRecorder
from django.test import override_settings

from djanquiltdb import ShardingMode
from djanquiltdb.db import connection
from djanquiltdb.management.commands.migrate import Command as ShardedMigrate
from djanquiltdb.management.executor import (
    SharedFilesMigrationExecutor,
    SharedOperationStates,
    SharedStatesMigrationExecutor,
    compute_states_around_migration_operations,
    enable_shared_migration_states,
)
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import (
    State,
    create_template_schema,
    get_all_databases,
    get_all_sharded_models,
    get_template_name,
    schema_exists,
    use_shard,
)
from djanquiltdb_tests import ShardingTestCase, disable_db_reconnect
from example.models import Shard
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
        call_command('migrate', verbosity=0)

        super_mirrored = SuperMirroredModel.objects.create(name='super!')
        mirrored = MirroredModel.objects.create(name='less super', super=super_mirrored)

        with use_shard(self.sina):
            super_sharded = SuperShardedModel.objects.create(name='super shard!', to_mirrored=mirrored)
            ShardedModel.objects.create(name='A bit lame', super=super_sharded)


@mock.patch('django.core.management.get_commands', mock.Mock(return_value={'migrate': 'djanquiltdb'}))
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
        Case: Call migrate with shard in several states of migration.
        Expected: All shards to be fully migrated
        Note: This is system test keeping migrate as a black box.
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

        call_command('migrate', verbosity=0)

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
        call_command('migrate', app_label='migration_tests', migration_name='zero', verbosity=0)

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
        call_command('migrate', 'migration_tests', '0001', verbosity=0)
        # Make sure the right tables exist
        self.assertTableExists('migration_tests_author')
        self.assertTableExists('migration_tests_tribble')
        self.assertTableNotExists('migration_tests_book')
        # Run migrations all the way
        call_command('migrate', verbosity=0)
        # Make sure the right tables exist
        self.assertTableExists('migration_tests_author')
        self.assertTableNotExists('migration_tests_tribble')
        self.assertTableExists('migration_tests_book')
        # Unmigrate everything
        call_command('migrate', 'migration_tests', 'zero', verbosity=0)
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
            call_command('migrate', 'migration_tests', '0001', verbosity=0)
            # Make sure the right tables exist
            self.assertTableExists('migration_tests_author')
            self.assertTableExists('migration_tests_tribble')

        with self.subTest('Fake a roll-back'):
            call_command('migrate', 'migration_tests', 'zero', fake=True, verbosity=0)
            # Make sure the tables still exist
            self.assertTableExists('migration_tests_author')
            self.assertTableExists('migration_tests_tribble')

        with self.subTest('Run initial migration'):
            out = StringIO()
            with mock.patch('sys.exit') as mock_exit:
                call_command('migrate', 'migration_tests', '0001', verbosity=0, stderr=out)

            self.assertIn('relation "migration_tests_author" already exists', out.getvalue().lower())
            mock_exit.assert_called_once_with(1)

        with self.subTest('Run initial migration with an explicit --fake-initial'):
            with mock.patch('django.core.management.color.supports_color', lambda *args: False):
                call_command('migrate', 'migration_tests', '0001', fake_initial=True, stdout=out, verbosity=1)
            self.assertIn('migration_tests.0001_initial... faked', out.getvalue().lower())

        with self.subTest('Run all migrations'):
            call_command('migrate', verbosity=0)
            # Make sure the right tables exist
            self.assertTableExists('migration_tests_author')
            self.assertTableNotExists('migration_tests_tribble')
            self.assertTableExists('migration_tests_book')

        with self.subTest('Fake a roll-back'):
            call_command('migrate', 'migration_tests', 'zero', fake=True, verbosity=0)
            # Make sure the tables still exist
            self.assertTableExists('migration_tests_author')
            self.assertTableNotExists('migration_tests_tribble')
            self.assertTableExists('migration_tests_book')

        with self.subTest('Run initial migration'):
            out = StringIO()
            with mock.patch('sys.exit') as mock_exit:
                call_command('migrate', 'migration_tests', stderr=out, verbosity=0)

            self.assertIn('relation "migration_tests_author" already exists', out.getvalue().lower())
            mock_exit.assert_called_once_with(1)

        with self.subTest('Run initial migration with an explicit --fake-initial'):
            # Fails because 'migration_tests_tribble' does not exist but needs to,
            # in order to make --fake-initial work.
            out = StringIO()
            with mock.patch('sys.exit') as mock_exit:
                call_command('migrate', 'migration_tests', fake_initial=True, stderr=out, verbosity=0)

            self.assertIn('relation "migration_tests_author" already exists', out.getvalue().lower())
            mock_exit.assert_called_once_with(1)

        with self.subTest('Fake an apply'):
            call_command('migrate', 'migration_tests', fake=True, verbosity=0)

        with self.subTest('Unmigrate everything'):
            call_command('migrate', 'migration_tests', 'zero', verbosity=0)
            # Make sure it's all gone
            self.assertTableNotExists('migration_tests_author')
            self.assertTableNotExists('migration_tests_tribble')
            self.assertTableNotExists('migration_tests_book')

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_conflict'})
    def test_migrate_conflict_exit(self):
        """
        Case: Call migrate with a conflicting migration set
        Expected: Makes sure that migrate exits if it detects a conflict.
        """
        with self.assertRaisesMessage(CommandError, 'Conflicting migrations detected'):
            call_command('migrate', 'migration_tests')

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_squashed'})
    def test_migrate_record_replaced(self):
        """
        Case: Call migrate with a squashed migration set
        Expected: All original migrations should be marked as run
        """
        recorder = MigrationRecorder(connection)
        out = StringIO()
        call_command('migrate', 'migration_tests', verbosity=0)
        call_command('showmigrations', 'migration_tests', stdout=out, no_color=True)
        self.assertEqual('migration_tests\n [x] 0001_squashed_0002 (2 squashed migrations)\n', out.getvalue().lower())
        applied_migration_tests = recorder.applied_migrations()
        self.assertIn(('migration_tests', '0001_initial'), applied_migration_tests)
        self.assertIn(('migration_tests', '0002_second'), applied_migration_tests)
        self.assertIn(('migration_tests', '0001_squashed_0002'), applied_migration_tests)
        # Rollback changes
        call_command('migrate', 'migration_tests', 'zero', verbosity=0)

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_squashed'})
    def test_migrate_record_squashed(self):
        """
        Case: Call migrate with a squashed migration set, when all original migrations have been run before.
        Expected: Should not migrate anything, all squashed migrations has been run
        """
        recorder = MigrationRecorder(connection)
        recorder.record_applied('migration_tests', '0001_initial')
        recorder.record_applied('migration_tests', '0002_second')
        out = StringIO()
        call_command('migrate', 'migration_tests', schema_name='public', verbosity=0)
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
    @mock.patch('djanquiltdb.management.commands.migrate.Command.perform_migration')
    @mock.patch('djanquiltdb.management.commands.migrate.Command.get_plan')
    @mock.patch(
        'djanquiltdb.management.commands.migrate.Command.get_targets_from_options',
        return_value=(mock.Mock(), mock.Mock()),
    )
    @mock.patch('djanquiltdb.management.commands.migrate.Command.check_for_app_conflicts')
    @mock.patch('django.db.backends.base.base.BaseDatabaseWrapper.prepare_database')
    @mock.patch('djanquiltdb.management.commands.migrate.get_databases_and_schema_from_options')
    @mock.patch('djanquiltdb.management.commands.migrate.import_module')
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
        Case: Call ShardedMigrate.handle()
        Expected: A ton of external functions to be called. No specific sys.exit called.
        """
        mock_get_db_from_options.return_value = ([db for db in settings.DATABASES], None)
        mock_check_conflicts.return_value = False

        options = {
            'database': 'all',
            'fake': False,
            'fake_initial': False,
        }
        ShardedMigrate().handle(**options)

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
        Case: Call ShardedMigrate().handle and trigger an error during one of the migrations
        Expected: That migration_node to be completed and then report the error and leave an exit code: 1.
        """

        patcher = mock.patch(
            'django.db.migrations.migration.Migration.apply', side_effect=fake_apply_migration, autospec=True
        )
        mock_apply_migration = patcher.start()

        stderr = StringIO()
        stdout = StringIO()
        sharded_migrate = ShardedMigrate()
        sharded_migrate.stderr = stderr
        sharded_migrate.stdout = stdout
        sharded_migrate.handle(app_label='migration_tests', database='all', fake=False, fake_initial=False, verbosity=0)
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
        ShardedMigrate().handle(app_label='migration_tests', migration_name='zero', database='all', verbosity=0)

    def schemas_to_migrate(self):
        """
        Return the node and schema name of every public, template and shard schema.
        """
        schemas = [(db, 'public') for db in self.databases] + [(db, get_template_name()) for db in self.databases]
        return schemas + [(shard.node_name, shard.schema_name) for shard in (self.sina, self.rose, self.maria)]

    def assertTablesMigrated(self, schemas):
        """
        Assert that each schema holds the tables the test migrations leave behind.
        """
        for node_name, schema_name in schemas:
            with self.subTest(node_name=node_name, schema_name=schema_name):
                with use_shard(
                    node_name=node_name, schema_name=schema_name, include_public=False, active_only_schemas=False
                ) as env:
                    table_names = env.connection.introspection.table_names()
                self.assertIn('migration_tests_author', table_names)
                self.assertIn('migration_tests_book', table_names)
                self.assertIn('migration_tests_hometown', table_names)
                self.assertNotIn('migration_tests_tribble', table_names)

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    @mock.patch.object(SharedStatesMigrationExecutor, 'apply_from_shared_states', autospec=True)
    def test_every_schema_gets_the_sql(self, mock_apply_from_shared_states):
        """
        Case: Migrate every public, template and shard schema.
        Expected: Each schema holds the tables of the migrations. Django's own migrate migrates each schema, without
                  shared project states.
        """
        sharded_migrate = ShardedMigrate()
        sharded_migrate.stdout = StringIO()
        sharded_migrate.handle(app_label='migration_tests', database='all', fake=False, fake_initial=False, verbosity=0)

        self.assertTablesMigrated(self.schemas_to_migrate())
        self.assertFalse(mock_apply_from_shared_states.called)

        # rollback
        ShardedMigrate().handle(app_label='migration_tests', migration_name='zero', database='all', verbosity=0)

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    @mock.patch.object(SharedStatesMigrationExecutor, 'apply_from_shared_states', autospec=True)
    def test_shards_do_not_share_states(self, mock_apply_from_shared_states):
        """
        Case: Migrate every public, template and shard schema, within enable_shared_migration_states.
        Expected: Django's own migrate migrates each schema. Project states are only shared between the public and
                  template schemas of a database without shards.
        """
        sharded_migrate = ShardedMigrate()
        sharded_migrate.stdout = StringIO()
        with enable_shared_migration_states():
            sharded_migrate.handle(
                app_label='migration_tests', database='all', fake=False, fake_initial=False, verbosity=0
            )

        self.assertTablesMigrated(self.schemas_to_migrate())
        self.assertFalse(mock_apply_from_shared_states.called)

        # rollback
        ShardedMigrate().handle(app_label='migration_tests', migration_name='zero', database='all', verbosity=0)

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    def test_each_check_reads_what_the_schema_has_applied(self):
        """
        Case: Migrate every schema. Something else records the migrations in a template schema after the run plans and
              before it migrates that schema.
        Expected: The template schema does not get those migrations again. The run reads the applied migrations of each
                  schema when it migrates that schema, not when it plans.
        """
        get_plan = ShardedMigrate.get_plan

        def plan_then_record(command, *args, **kwargs):
            plan = get_plan(command, *args, **kwargs)
            with use_shard(node_name='other', schema_name=get_template_name()) as env:
                for name in ('0001_initial', '0002_second', '0003_third'):
                    MigrationRecorder(env.connection).record_applied('migration_tests', name)
            return plan

        sharded_migrate = ShardedMigrate()
        sharded_migrate.stdout = StringIO()
        sharded_migrate.stderr = StringIO()
        with mock.patch.object(ShardedMigrate, 'get_plan', autospec=True, side_effect=plan_then_record):
            with mock.patch('sys.exit'):
                sharded_migrate.handle(
                    app_label='migration_tests', database='all', fake=False, fake_initial=False, verbosity=0
                )

        self.assertEqual(sharded_migrate.stderr.getvalue(), '')
        with use_shard(node_name='other', schema_name=get_template_name(), include_public=False) as env:
            self.assertNotIn('migration_tests_author', env.connection.introspection.table_names())
        self.assertTablesMigrated(
            [schema for schema in self.schemas_to_migrate() if schema != ('other', get_template_name())]
        )

        # rollback (the template schema only has the migrations recorded, so clear those first)
        with use_shard(node_name='other', schema_name=get_template_name()) as env:
            MigrationRecorder(env.connection).migration_qs.filter(app='migration_tests').delete()
        ShardedMigrate().handle(app_label='migration_tests', migration_name='zero', database='all', verbosity=0)

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    @mock.patch.object(MigrationLoader, 'load_disk', autospec=True, side_effect=MigrationLoader.load_disk)
    def test_migrations_are_loaded_from_disk_once(self, mock_load_disk):
        """
        Case: Migrate every public, template and shard schema.
        Expected: The run loads the migration modules from disk once, not for every schema and migration.
        """
        sharded_migrate = ShardedMigrate()
        sharded_migrate.stdout = StringIO()
        sharded_migrate.handle(app_label='migration_tests', database='all', fake=False, fake_initial=False, verbosity=0)

        self.assertEqual(mock_load_disk.call_count, 1)

        # rollback
        ShardedMigrate().handle(app_label='migration_tests', migration_name='zero', database='all', verbosity=0)

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_separate_database'})
    def test_separate_database_and_state_runs_on_every_schema(self):
        """
        Case: Migrate every public, template and shard schema with a SeparateDatabaseAndState migration. Its database
              operations add a field and then run a RunPython that queries it.
        Expected: Each schema gets the column. The RunPython runs on each schema and gets a model with the field.
        """
        assertSeparateDatabaseAndStateRan(self, self.schemas_to_migrate())

        # rollback
        ShardedMigrate().handle(app_label='migration_tests', migration_name='zero', database='all', verbosity=0)


def assertSeparateDatabaseAndStateRan(test_case, schemas):
    """
    Migrate the schemas with the test_migrations_separate_database migrations. Assert that each schema got the column
    that SeparateDatabaseAndState adds, and ran its RunPython with a model that has the field. The caller rolls the
    migrations back.
    """
    ran = import_module('migration_tests.test_migrations_separate_database.0002_pages').COUNT_PAGES_RUNS
    ran.clear()

    sharded_migrate = ShardedMigrate()
    sharded_migrate.stdout = StringIO()
    sharded_migrate.stderr = StringIO()
    sharded_migrate.handle(app_label='migration_tests', database='all', fake=False, fake_initial=False, verbosity=0)

    test_case.assertEqual(sharded_migrate.stderr.getvalue(), '')
    for node_name, schema_name in schemas:
        with test_case.subTest(node_name=node_name, schema_name=schema_name):
            with use_shard(
                node_name=node_name, schema_name=schema_name, include_public=False, active_only_schemas=False
            ) as env:
                with env.connection.cursor() as cursor:
                    columns = env.connection.introspection.get_table_description(cursor, 'migration_tests_author')
            test_case.assertIn('pages', [column.name for column in columns])
    # The connection alias of a public schema is just the node name.
    aliases = [
        node_name if schema_name == 'public' else '{}|{}'.format(node_name, schema_name)
        for node_name, schema_name in schemas
    ]
    test_case.assertEqual(sorted(alias for alias, fields in ran), sorted(aliases))
    test_case.assertEqual({tuple(fields) for alias, fields in ran}, {('id', 'name', 'pages')})


class ShardedMigrationSharedStatesTestCase(MigrationTestCase):
    """
    Tests for runs within enable_shared_migration_states over the public and template schemas of databases without
    shards. This is the setup when Django builds a new test database.
    """

    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.databases = get_all_databases()

    def schemas_to_migrate(self):
        return [(db, schema_name) for db in self.databases for schema_name in ('public', get_template_name())]

    def migrate_with_shared_states(self, **options):
        sharded_migrate = ShardedMigrate()
        sharded_migrate.stdout = StringIO()
        sharded_migrate.stderr = StringIO()
        with enable_shared_migration_states():
            sharded_migrate.handle(app_label='migration_tests', **{'database': 'all', 'verbosity': 0, **options})

        self.assertEqual(sharded_migrate.stderr.getvalue(), '')

    def rollback_test_migrations(self):
        ShardedMigrate().handle(app_label='migration_tests', migration_name='zero', database='all', verbosity=0)

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    @mock.patch(
        'djanquiltdb.management.commands.migrate.SharedStatesMigrationExecutor', wraps=SharedStatesMigrationExecutor
    )
    @mock.patch(
        'djanquiltdb.management.executor.compute_states_around_migration_operations',
        wraps=compute_states_around_migration_operations,
    )
    def test_every_schema_gets_the_sql(self, mock_compute_states_around_migration_operations, mock_executor):
        """
        Case: Migrate every public and template schema, sharing the project states of each migration.
        Expected: Each schema holds the tables of the migrations, so the SQL is not shared. The run computes the states
                  of each migration once per starting point, and builds the executor of each schema once.
        """
        self.migrate_with_shared_states(fake=False, fake_initial=False)

        for node_name, schema_name in self.schemas_to_migrate():
            with self.subTest(node_name=node_name, schema_name=schema_name):
                with use_shard(node_name=node_name, schema_name=schema_name, include_public=False) as env:
                    table_names = env.connection.introspection.table_names()
                self.assertIn('migration_tests_author', table_names)
                self.assertIn('migration_tests_book', table_names)
                self.assertIn('migration_tests_hometown', table_names)
                self.assertNotIn('migration_tests_tribble', table_names)

        # Three migrations in the plan, each computed once for the public schemas and once for the templates. They start
        # from different points, because the templates are created without migrations and the public schemas are not.
        self.assertEqual(mock_compute_states_around_migration_operations.call_count, 2 * 3)
        self.assertEqual(mock_executor.call_count, len(self.schemas_to_migrate()))

        self.rollback_test_migrations()

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_separate_database'})
    @mock.patch.object(AddField, 'state_forwards', autospec=True, side_effect=AddField.state_forwards)
    def test_separate_database_and_state_shares_the_states_of_its_database_operations(self, mock_state_forwards):
        """
        Case: Migrate every public and template schema with a SeparateDatabaseAndState migration. Its database
              operations add a field and then run a RunPython that queries it.
        Expected: Each schema gets the column. The RunPython runs on each schema and gets a model with the field. The
                  run computes the states around the database operations once per starting point, together with the
                  migration's own states, not again for every schema.
        """
        with enable_shared_migration_states():
            assertSeparateDatabaseAndStateRan(self, self.schemas_to_migrate())

        # Once for the migration's own states and once around its database operations. Both happen for the public
        # schemas and for the templates.
        pages = [call for call in mock_state_forwards.call_args_list if call.args[0].name == 'pages']
        self.assertEqual(len(pages), 2 * 2)

        self.rollback_test_migrations()

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    def test_shares_states_when_migrating_forwards(self):
        """
        Case: Migrate every public and template schema forwards.
        Expected: Each migration is applied from the shared states.
        """
        apply_from_shared_states = SharedStatesMigrationExecutor.apply_from_shared_states
        with mock.patch.object(
            SharedStatesMigrationExecutor,
            'apply_from_shared_states',
            autospec=True,
            side_effect=apply_from_shared_states,
        ) as mock_apply_from_shared_states:
            self.migrate_with_shared_states()

        self.assertEqual(mock_apply_from_shared_states.call_count, 3 * len(self.schemas_to_migrate()))

        self.rollback_test_migrations()

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    def test_migrating_backwards_does_not_share_states(self):
        """
        Case: Migrate every public and template schema forwards, then back to the first migration.
        Expected: Django's own migrate unapplies the migrations and builds the states for every schema.
        """
        self.migrate_with_shared_states()

        with mock.patch.object(
            SharedStatesMigrationExecutor, 'apply_from_shared_states', autospec=True
        ) as mock_apply_from_shared_states:
            self.migrate_with_shared_states(migration_name='0001_initial')

        self.assertFalse(mock_apply_from_shared_states.called)
        with use_shard(node_name='default', schema_name=get_template_name(), include_public=False) as env:
            self.assertNotIn('migration_tests_book', env.connection.introspection.table_names())

        self.rollback_test_migrations()

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
    @mock.patch.object(SharedStatesMigrationExecutor, 'apply_from_shared_states', autospec=True)
    def test_one_schema_does_not_share_states(self, mock_apply_from_shared_states):
        """
        Case: Migrate one template schema with --schema-name.
        Expected: Django's own migrate applies the migrations.
        """
        self.migrate_with_shared_states(schema_name=get_template_name())

        self.assertFalse(mock_apply_from_shared_states.called)
        with use_shard(node_name='default', schema_name=get_template_name(), include_public=False) as env:
            self.assertIn('migration_tests_book', env.connection.introspection.table_names())

        self.rollback_test_migrations()


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_squashed_complex'})
class ShardedMigrationPartialSquashTestCase(MigrationTestCase):
    """
    Tests for a shard that has applied some, but not all, of the migrations a squash replaces.

    Django leaves the squash out of the migration graph of that shard and keeps the replaced migrations. A schema that
    has applied none of them gets the squash instead.
    """

    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.databases = get_all_databases()

    def setUp(self):
        super().setUp()

        self.sina = Shard.objects.create(alias='sina', schema_name='test_sina', node_name='default', state=State.ACTIVE)

        # A replaced migration as target makes Django plan without the squash. It applies 1_auto, 2_auto and 3_auto.
        with use_shard(self.sina) as env:
            MigrationExecutor(env.connection).migrate([('migration_tests', '3_auto')])

    def applied_fixture_migrations(self, **use_shard_kwargs):
        """
        Return the names of the fixture migrations that a schema records as applied. (The public schemas also hold the
        migrations of the test database.)
        """
        with use_shard(active_only_schemas=False, **use_shard_kwargs) as env:
            applied = MigrationRecorder(env.connection).applied_migrations()

        fixture = {key for key in MigrationLoader(None).disk_migrations if key[0] == 'migration_tests'}
        return {name for app_label, name in applied if (app_label, name) in fixture}

    def test_schema_with_the_replaced_migrations_applied_skips_them(self):
        """
        Case: Migrate every schema. The plan holds a migration that a squash replaces, because a shard applied part of
              the squash. The public schemas have applied all the migrations it replaces.
        Expected: The graph of the public schemas holds the squash instead. They skip the replaced migrations as already
                  applied, and do not fail on them.
        """
        for node_name in self.databases:
            with use_shard(node_name=node_name, schema_name=PUBLIC_SCHEMA_NAME) as env:
                MigrationExecutor(env.connection).migrate([('migration_tests', '5_auto')])
        for node_name in self.databases:
            with use_shard(node_name=node_name, schema_name=get_template_name()) as env:
                MigrationExecutor(env.connection).migrate([('migration_tests', '5_auto')])

        stderr = StringIO()
        call_command('migrate', 'migration_tests', verbosity=0, stderr=stderr)

        self.assertEqual(stderr.getvalue(), '')
        for node_name in self.databases:
            with self.subTest(node_name=node_name):
                self.assertIn(
                    '7_auto', self.applied_fixture_migrations(node_name=node_name, schema_name=PUBLIC_SCHEMA_NAME)
                )
        self.assertIn('7_auto', self.applied_fixture_migrations(shard=self.sina))


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
        leave_nodes = [('migration_tests', '0003_third'), ('example', '0001_initial')]
        mock_leave_nodes.return_value = leave_nodes

        executor = MigrationExecutor(connection)
        self.assertEqual(ShardedMigrate().get_targets_from_options(executor, options={}), (True, leave_nodes))

    @mock.patch('django.db.migrations.graph.MigrationGraph.leaf_nodes')
    def test_with_app_label(self, mock_leave_nodes):
        """
        Case: Call get_targets_from_options with app_label as option.
        Expected: Single leaf returned
        """
        leave_nodes = [('migration_tests', '0003_third'), ('example', '0001_initial')]
        mock_leave_nodes.return_value = leave_nodes

        executor = MigrationExecutor(connection)
        self.assertEqual(
            ShardedMigrate().get_targets_from_options(executor, options={'app_label': 'example'}),
            (False, [('example', '0001_initial')]),
        )

    def test_with_target_migration(self):
        """
        Case: Call get_targets_from_options with both an app_label and a migration_name.
        Expected: That one migration returned, resolved from its prefix.
        """
        executor = MigrationExecutor(connection)
        self.assertEqual(
            ShardedMigrate().get_targets_from_options(
                executor, options={'app_label': 'migration_tests', 'migration_name': '0002_second'}
            ),
            (False, [('migration_tests', '0002_second')]),
        )

    def test_with_zero_target(self):
        """
        Case: Call get_targets_from_options with zero as option.
        Expected: [(app_label, None)] returned
        """
        executor = MigrationExecutor(connection)
        self.assertEqual(
            ShardedMigrate().get_targets_from_options(
                executor, options={'app_label': 'migration_tests', 'migration_name': 'zero'}
            ),
            (False, [('migration_tests', None)]),
        )

    def test_with_unexisting_migration(self):
        """
        Case: Call get_targets_from_options with nonexisting migration as target
        Expected: CommandError raised
        """
        executor = MigrationExecutor(connection)
        with self.assertRaises(CommandError) as error:
            (
                ShardedMigrate().get_targets_from_options(
                    executor, options={'app_label': 'migration_tests', 'migration_name': '9001_over_9k'}
                ),
            )
        self.assertEqual(
            error.exception.args[0], "Cannot find a migration matching '9001_over_9k' from app 'migration_tests'."
        )

    def test_with_unexisting_app_label(self):
        """
        Case: Call get_targets_from_options with nonexisting app_label as target
        Expected: CommandError raised
        """
        executor = MigrationExecutor(connection)
        with self.assertRaises(CommandError) as error:
            (ShardedMigrate().get_targets_from_options(executor, options={'app_label': 'Hans'}),)
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

    @mock.patch('djanquiltdb.management.commands.migrate.Command.get_plan_for_shard')
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

        ShardedMigrate().get_plan(self.targets, self.databases, None)

        mock_get_plan_for_shard.assert_any_call(self.targets, 'default', 'public')
        mock_get_plan_for_shard.assert_any_call(self.targets, 'default', 'template')
        mock_get_plan_for_shard.assert_any_call(self.targets, 'default', 'test_sina')
        mock_get_plan_for_shard.assert_any_call(self.targets, 'other', 'public')
        mock_get_plan_for_shard.assert_any_call(self.targets, 'other', 'template')

    def test_schemas_of_a_run(self):
        """
        Case: List the schemas of a run over both nodes, with a shard on the first.
        Expected: The public and template schema of each node, then the shard. Each entry holds the shard (or None), the
                  node name and the schema name.
        """
        self.assertEqual(
            list(ShardedMigrate().iter_schemas_to_migrate(self.databases)),
            [
                (None, 'default', 'public'),
                (None, 'default', 'template'),
                (None, 'other', 'public'),
                (None, 'other', 'template'),
                (self.sina, 'default', 'test_sina'),
            ],
        )

    @mock.patch('djanquiltdb.management.commands.migrate.get_shards_by_node', autospec=True)
    def test_schemas_of_a_run_read_the_shard_registry_as_the_other_commands_do(self, mock_get_shards_by_node):
        """
        Case: List the schemas of a run over both nodes, where the shard registry returns a shard on each.
        Expected: The shards come from get_shards_by_node for the nodes of the run. (Like flush and sqlflush, it reads
                  the registry from the primary database.) They follow the public and template schemas, by node.
        """
        rose = Shard(alias='rose', schema_name='test_rose', node_name='other')
        mock_get_shards_by_node.return_value = {'other': [rose], 'default': [self.sina]}

        self.assertEqual(
            list(ShardedMigrate().iter_schemas_to_migrate(self.databases)),
            [
                (None, 'default', 'public'),
                (None, 'default', 'template'),
                (None, 'other', 'public'),
                (None, 'other', 'template'),
                (self.sina, 'default', 'test_sina'),
                (rose, 'other', 'test_rose'),
            ],
        )
        mock_get_shards_by_node.assert_called_once_with(self.databases)

    def test_different_migration_states(self):
        """
        Case: Call get_plan when not all schema's have the same migration level
        Expected: get_plan_for_shard to be called for all schemas
        Note: get_plan_for_shard is not mocked. So it's functionality is taken into account
        """
        # Migrate the public schema's and the template schemas fully
        call_command('migrate', 'migration_tests', database='default', verbosity=0)
        call_command('migrate', 'migration_tests', database='other', verbosity=0)

        # This makes completely unmigrated schemas, because we skip the cloning.
        with mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.clone_schema') as mock_save:
            Shard.objects.create(alias='rose', node_name='default', schema_name='test_rose')
            Shard.objects.create(alias='maria', node_name='default', schema_name='test_maria')
            self.assertEqual(mock_save.call_count, 2)

        # Migrate rose a bit
        call_command('migrate', 'migration_tests', '0001', database='default', schema_name='test_rose', verbosity=0)

        # Migrate maria a bit further
        call_command('migrate', 'migration_tests', '0002', database='default', schema_name='test_maria', verbosity=0)

        # rose is the furthest behind. So we should get her migration path
        self.assertEqual(
            ShardedMigrate().get_plan(self.targets, self.databases, None),
            ShardedMigrate().get_plan_for_shard(self.targets, 'default', 'test_rose'),
        )

        # Rollback: cleanup for other tests. Shards are automatically removed.
        call_command('migrate', 'migration_tests', 'zero', database='default', verbosity=0)
        call_command('migrate', 'migration_tests', 'zero', database='other', verbosity=0)


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

    @mock.patch('djanquiltdb.management.commands.migrate.SharedFilesMigrationExecutor', autospec=True)
    @mock.patch('djanquiltdb.utils.use_shard.__exit__', autospec=True)
    @mock.patch('djanquiltdb.utils.use_shard.__enter__', autospec=True)
    def test_use_shard_called(self, mock_use_shard_enter, mock_use_shard_exit, mock_executor):
        """
        Case: Call get_plan_for_shard
        Expected: An executor for the schema makes the plan inside use_shard
        """
        mock_executor.return_value.migration_plan = mock.Mock()

        ShardedMigrate().get_plan_for_shard(self.targets, self.sina.node_name, self.sina.schema_name)

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
            ShardedMigrate().check_for_app_conflicts(executor=mock_executor)


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
        self.plan = ShardedMigrate().get_plan_for_shard(self.targets, self.rose.node_name, self.rose.schema_name)

    @mock.patch('djanquiltdb.management.commands.migrate.SharedFilesMigrationExecutor', autospec=True)
    @mock.patch('djanquiltdb.utils.use_shard.__exit__', autospec=True)
    @mock.patch('djanquiltdb.utils.use_shard.__enter__', autospec=True)
    def test_specific_schema(self, mock_use_shard_enter, mock_use_shard_exit, mock_executor):
        """
        Case: Call perform_migration with a specific schema.
        Expected: executor.migrate to be called with the right arguments within a use_shard context manager
        """
        mock_executor.return_value.migrate = mock.Mock()

        ShardedMigrate().perform_migration(self.plan, ['default'], self.rose.schema_name, False, False)

        self.assertEqual(mock_use_shard_enter.call_count, 1)
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.node_name, 'default')
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.schema_name, 'test_rose')
        self.assertEqual(mock_executor.call_count, 1)
        mock_executor.return_value.migrate.assert_called_once_with(
            targets=None, plan=self.plan, fake=False, fake_initial=False
        )
        self.assertEqual(mock_use_shard_exit.call_count, 1)

    @mock.patch(
        'djanquiltdb.management.commands.migrate.Command.check_or_migrate_schema',
        return_value=False,
        autospec=True,
    )
    @mock.patch(
        'djanquiltdb.management.commands.migrate.Command.check_or_migrate_shard',
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
        sharded_migrate = ShardedMigrate()
        sharded_migrate.perform_migration(self.plan, self.databases, None, False, False)

        self.assertEqual(mock_check_or_migrate_schema.call_count, 12)
        self.assertEqual(mock_check_or_migrate_shard.call_count, 6)
        for node in self.plan:
            mock_check_or_migrate_shard.assert_any_call(sharded_migrate, self.rose, node, False, False)
            mock_check_or_migrate_shard.assert_any_call(sharded_migrate, self.maria, node, False, False)
            mock_check_or_migrate_schema.assert_any_call(sharded_migrate, 'default', 'public', node, False, False)
            mock_check_or_migrate_schema.assert_any_call(sharded_migrate, 'other', 'public', node, False, False)
            mock_check_or_migrate_schema.assert_any_call(sharded_migrate, 'default', template_name, node, False, False)
            mock_check_or_migrate_schema.assert_any_call(sharded_migrate, 'other', template_name, node, False, False)

    @mock.patch(
        'djanquiltdb.management.commands.migrate.Command.check_or_migrate_shard',
        return_value=False,
        autospec=True,
    )
    @mock.patch(
        'djanquiltdb.management.commands.migrate.Command.check_or_migrate_schema',
        autospec=True,
    )
    def test_each_run_starts_with_new_states(self, mock_check_or_migrate_schema, mock_check_or_migrate_shard):
        """
        Case: Run perform_migration twice on the same command.
        Expected: Each run gets its own shared states and starting states. A second run never starts from what the first
                  one computed.
        """
        seen = []

        def check_or_migrate_schema(command, *args):
            seen.append((command.shared_operation_states, command.shared_starting_states))
            return False

        mock_check_or_migrate_schema.side_effect = check_or_migrate_schema
        sharded_migrate = ShardedMigrate()

        sharded_migrate.perform_migration(self.plan[:1], ['default'], None, False, False)
        sharded_migrate.perform_migration(self.plan[:1], ['default'], None, False, False)

        self.assertIsNot(seen[0][0], seen[-1][0])
        self.assertIsNot(seen[0][1], seen[-1][1])

    @mock.patch(
        'djanquiltdb.management.commands.migrate.Command.check_or_migrate_schema',
        return_value=True,
        autospec=True,
    )
    @mock.patch(
        'djanquiltdb.management.commands.migrate.Command.check_or_migrate_shard',
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

                    sharded_migrate = ShardedMigrate()
                    return_value = sharded_migrate.perform_migration(self.plan, self.databases, None, False, False)

                    self.assertEqual(return_value, schema_value | shard_value)
                    self.assertTrue(mock_check_or_migrate_shard.called)
                    self.assertTrue(mock_check_or_migrate_schema.called)


@override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations'})
@mock.patch.object(MigrationRecorder, 'ensure_schema', autospec=True)
class ShardedMigrationGetExecutorTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    def setUp(self):
        super().setUp()

        self.sharded_migrate = ShardedMigrate()

    def get_executor_for_schema(self, **use_shard_kwargs):
        with use_shard(**use_shard_kwargs) as env:
            return self.sharded_migrate.get_executor_for_schema(env)

    def test_each_call_gets_a_new_executor(self, mock_ensure_schema):
        """
        Case: Get the executor for the same schema in two separate use_shard blocks.
        Expected: A new executor each time. Each one reads the applied migrations of the schema from the database. They
                  share the migrations loaded from disk and the starting states of the run.
        """
        first = self.get_executor_for_schema(node_name='default', schema_name=get_template_name())
        second = self.get_executor_for_schema(node_name='default', schema_name=get_template_name())

        self.assertIsNot(first, second)
        self.assertIsInstance(first, SharedFilesMigrationExecutor)
        self.assertNotIsInstance(first, SharedStatesMigrationExecutor)
        self.assertIs(first.loader.disk_migrations, second.loader.disk_migrations)
        self.assertIs(first.starting_states, self.sharded_migrate.shared_starting_states)
        self.assertIs(second.starting_states, self.sharded_migrate.shared_starting_states)

    def test_sharing_states_reuses_the_executor_of_a_schema(self, mock_ensure_schema):
        """
        Case: Get the executor for the same schema in two separate use_shard blocks, in a run that shares project
              states.
        Expected: The same executor both times. The run builds it once, so it loads the migration graph once.
        """
        self.sharded_migrate.use_shared_states = True
        executor = self.get_executor_for_schema(node_name='default', schema_name=get_template_name())

        self.assertIsInstance(executor, SharedStatesMigrationExecutor)
        self.assertIs(self.get_executor_for_schema(node_name='default', schema_name=get_template_name()), executor)

    def test_getting_an_executor_creates_no_migrations_table(self, mock_ensure_schema):
        """
        Case: Get the executor for a schema, with and without sharing project states.
        Expected: The migrations table of the schema is not created. The executor also plans, and planning must not
                  change a schema with nothing to migrate. (Like Django, the run creates the table when there is a
                  migration to apply.)
        """
        for use_shared_states in (False, True):
            with self.subTest(use_shared_states=use_shared_states):
                self.sharded_migrate.use_shared_states = use_shared_states
                self.get_executor_for_schema(node_name='default', schema_name=get_template_name())

                self.assertFalse(mock_ensure_schema.called)

    def test_sharing_states_gives_each_schema_its_own_executor(self, mock_ensure_schema):
        """
        Case: Get the executor for a public and a template schema on two nodes, in a run that shares project states.
        Expected: A separate executor for each schema, connected to its own node and schema.
        """
        self.sharded_migrate.use_shared_states = True
        targets = [
            (node_name, schema_name)
            for node_name in ('default', 'other')
            for schema_name in (PUBLIC_SCHEMA_NAME, get_template_name())
        ]
        executors = [
            self.get_executor_for_schema(node_name=node_name, schema_name=schema_name)
            for node_name, schema_name in targets
        ]

        self.assertEqual(len({id(executor) for executor in executors}), len(targets))
        for (node_name, schema_name), executor in zip(targets, executors):
            self.assertEqual(executor.connection.settings_dict, connections[node_name].settings_dict)
            self.assertEqual(executor.connection.schema_name, schema_name)

    def test_each_run_starts_with_new_executors(self, mock_ensure_schema):
        """
        Case: Get the executor for a schema in a run that shares project states. Then run the command within
              enable_shared_migration_states with nothing to migrate, and get the executor again.
        Expected: A new executor, so a run never starts from what another run loaded.
        """
        self.sharded_migrate.use_shared_states = True
        executor = self.get_executor_for_schema(node_name='default', schema_name=PUBLIC_SCHEMA_NAME)
        with enable_shared_migration_states():
            self.sharded_migrate.handle(
                app_label='migration_tests', migration_name='zero', database='default', verbosity=0
            )

        self.sharded_migrate.use_shared_states = True
        self.assertIsNot(self.get_executor_for_schema(node_name='default', schema_name=PUBLIC_SCHEMA_NAME), executor)


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
        self.plan = ShardedMigrate().get_plan_for_shard(self.targets, self.rose.node_name, self.rose.schema_name)
        self.sharded_migrate = ShardedMigrate()
        self.sharded_migrate.verbosity = 2
        self.shared_operation_states = self.sharded_migrate.shared_operation_states = mock.Mock(
            spec=SharedOperationStates
        )

    @mock.patch('djanquiltdb.utils.use_shard.__exit__', autospec=True)
    @mock.patch('djanquiltdb.utils.use_shard.__enter__', autospec=True)
    def test_use_shard(self, mock_use_shard_enter, mock_use_shard_exit):
        """
        Case: Call check_or_migrate_schema
        Expected: use_shard called
        """
        self.sharded_migrate.check_or_migrate_schema('other', 'public', self.plan[0], False, False)
        self.assertEqual(mock_use_shard_enter.call_count, 1)
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.node_name, 'other')
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.schema_name, 'public')
        self.assertEqual(mock_use_shard_exit.call_count, 1)

    @mock.patch('djanquiltdb.management.commands.migrate.SharedStatesMigrationExecutor', autospec=True)
    def test_forwards_not_yet_applied_sharing_states(self, mock_executor):
        """
        Case: Call check_or_migrate_schema with a schema that is not yet migrated, in a run that shares project states.
        Expected: The migration is applied from the shared states, not by Django's own migrate
        """
        self.sharded_migrate.use_shared_states = True
        self.sharded_migrate.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = []

        self.sharded_migrate.check_or_migrate_schema('other', 'public', self.plan[0], False, False)

        self.sharded_migrate.stdout.write.assert_any_call('    Applying migration_tests.0001_initial to other|public\n')
        mock_executor.return_value.apply_from_shared_states.assert_called_once_with(
            self.plan[0][0], self.shared_operation_states, False, False
        )
        self.assertFalse(mock_executor.return_value.migrate.called)

    @mock.patch('djanquiltdb.management.commands.migrate.SharedStatesMigrationExecutor', autospec=True)
    def test_backwards_sharing_states_unapplies_through_djangos_migrate(self, mock_executor):
        """
        Case: Call check_or_migrate_schema with a migrated schema, going backwards, in a run that shares project states.
              (handle never shares states for a plan that unapplies.)
        Expected: Django's own migrate unapplies that one migration with states it builds itself. The shared states are
                  only for applying migrations.
        """
        self.sharded_migrate.use_shared_states = True
        self.sharded_migrate.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = [('migration_tests', '0001_initial')]
        migration_node = (self.plan[0][0], True)

        self.sharded_migrate.check_or_migrate_schema('other', 'public', migration_node, False, False)

        mock_executor.return_value.migrate.assert_called_once_with(
            targets=None, plan=[migration_node], fake=False, fake_initial=False
        )
        self.assertFalse(mock_executor.return_value.apply_from_shared_states.called)

    @mock.patch('djanquiltdb.management.commands.migrate.SharedFilesMigrationExecutor', autospec=True)
    def test_forwards_not_yet_applied(self, mock_executor):
        """
        Case: Call check_or_migrate_schema with a schema that not yet migrated
        Expected: Django's own migrate to be called for that one migration
        """
        self.sharded_migrate.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.recorder = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = []

        self.sharded_migrate.check_or_migrate_schema('other', 'public', self.plan[0], False, False)

        self.sharded_migrate.stdout.write.assert_any_call('    Applying migration_tests.0001_initial to other|public\n')
        mock_executor.return_value.migrate.assert_called_once_with(
            targets=None, plan=[self.plan[0]], fake=False, fake_initial=False
        )

    @mock.patch('djanquiltdb.management.commands.migrate.SharedFilesMigrationExecutor', autospec=True)
    def test_forwards_already_applied(self, mock_executor):
        """
        Case: Call check_or_migrate_schema with a schema that is already migrated
        Expected: Migrate not to be called
        """
        self.sharded_migrate.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.recorder = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = [('migration_tests', '0001_initial')]

        self.sharded_migrate.check_or_migrate_schema('other', 'public', self.plan[0], False, False)
        self.sharded_migrate.stdout.write.assert_any_call(
            '    other|public has migration_tests.0001_initial already applied.\n'
        )
        self.assertFalse(mock_executor.return_value.migrate.called)

    @mock.patch('djanquiltdb.management.commands.migrate.SharedFilesMigrationExecutor', autospec=True)
    def test_backwards_already_applied(self, mock_executor):
        """
        Case: Call check_or_migrate_schema with a schema that is migrated; going backwards
        Expected: Django's own migrate to be called for that one migration
        """
        self.sharded_migrate.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.recorder = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = [('migration_tests', '0001_initial')]

        migration_node = self.plan[0]
        migration_node = (migration_node[0], True)  # set as backwards migration

        self.sharded_migrate.check_or_migrate_schema('other', 'public', migration_node, False, False)
        self.sharded_migrate.stdout.write.assert_any_call(
            '    Unapplying migration_tests.0001_initial to other|public\n'
        )
        mock_executor.return_value.migrate.assert_called_once_with(
            targets=None, plan=[migration_node], fake=False, fake_initial=False
        )

    @mock.patch('djanquiltdb.management.commands.migrate.SharedFilesMigrationExecutor', autospec=True)
    def test_backwards_unapplied(self, mock_executor):
        """
        Case: Call check_or_migrate_schema with a schema that is not yet applied; going backwards
        Expected: Migrate not to be called
        """
        self.sharded_migrate.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.recorder = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = []

        migration_node = self.plan[0]
        migration_node = (migration_node[0], True)  # set as backwards migration

        self.sharded_migrate.check_or_migrate_schema('other', 'public', migration_node, False, False)
        self.sharded_migrate.stdout.write.assert_any_call(
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
        self.plan = ShardedMigrate().get_plan_for_shard(self.targets, self.rose.node_name, self.rose.schema_name)
        self.sharded_migrate = ShardedMigrate()
        self.sharded_migrate.verbosity = 2
        self.shared_operation_states = self.sharded_migrate.shared_operation_states = mock.Mock(
            spec=SharedOperationStates
        )

    @mock.patch('djanquiltdb.utils.use_shard.__exit__', autospec=True)
    @mock.patch('djanquiltdb.utils.use_shard.__enter__', autospec=True)
    def test_use_shard(self, mock_use_shard_enter, mock_use_shard_exit):
        """
        Case: Call check_or_migrate_shard
        Expected: use_shard called
        """
        self.sharded_migrate.check_or_migrate_shard(self.rose, self.plan[0], False, False)
        self.assertEqual(mock_use_shard_enter.call_count, 1)
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.node_name, 'other')
        self.assertEqual(mock_use_shard_enter.call_args[0][0].options.schema_name, 'test_rose')
        self.assertEqual(mock_use_shard_exit.call_count, 1)

    def test_migrates_through_the_connection_of_the_shard(self):
        """
        Case: Migrate a shard.
        Expected: The executor, its loader and its recorder all use the connection of the shard context. That connection
                  knows the shard.
        """
        seen = []

        def record_executor_connections(executor, *args, **kwargs):
            seen.append(
                [
                    connection_.shard_options.shard_id
                    for connection_ in (executor.connection, executor.loader.connection, executor.recorder.connection)
                ]
            )

        with mock.patch.object(
            SharedFilesMigrationExecutor, 'migrate', autospec=True, side_effect=record_executor_connections
        ):
            self.sharded_migrate.check_or_migrate_shard(self.rose, self.plan[0], False, False)

        self.assertEqual(seen, [[self.rose.id] * 3])

    @mock.patch('djanquiltdb.management.commands.migrate.SharedFilesMigrationExecutor', autospec=True)
    def test_forwards_not_yet_applied(self, mock_executor):
        """
        Case: Call check_or_migrate_shard with a schema that not yet migrated
        Expected: Django's own migrate to be called for that one migration
        """
        self.sharded_migrate.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.recorder = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = []

        self.sharded_migrate.check_or_migrate_shard(self.rose, self.plan[0], False, False)

        self.sharded_migrate.stdout.write.assert_any_call('    Applying migration_tests.0001_initial to other|rose\n')
        mock_executor.return_value.migrate.assert_called_once_with(
            targets=None, plan=[self.plan[0]], fake=False, fake_initial=False
        )

    @mock.patch('djanquiltdb.management.commands.migrate.SharedFilesMigrationExecutor', autospec=True)
    def test_forwards_already_applied(self, mock_executor):
        """
        Case: Call check_or_migrate_shard with a schema that is already migrated
        Expected: Migrate not to be called
        """
        self.sharded_migrate.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.recorder = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = [('migration_tests', '0001_initial')]

        self.sharded_migrate.check_or_migrate_shard(self.rose, self.plan[0], False, False)
        self.sharded_migrate.stdout.write.assert_any_call(
            '    other|rose has migration_tests.0001_initial already applied.\n'
        )
        self.assertFalse(mock_executor.return_value.migrate.called)

    @mock.patch('djanquiltdb.management.commands.migrate.SharedFilesMigrationExecutor', autospec=True)
    def test_backwards_already_applied(self, mock_executor):
        """
        Case: Call check_or_migrate_shard with a schema that is migrated; going backwards
        Expected: Django's own migrate to be called for that one migration
        """
        self.sharded_migrate.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.recorder = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = [('migration_tests', '0001_initial')]

        migration_node = self.plan[0]
        migration_node = (migration_node[0], True)  # set as backwards migration

        self.sharded_migrate.check_or_migrate_shard(self.rose, migration_node, False, False)
        self.sharded_migrate.stdout.write.assert_any_call('    Unapplying migration_tests.0001_initial to other|rose\n')
        mock_executor.return_value.migrate.assert_called_once_with(
            targets=None, plan=[migration_node], fake=False, fake_initial=False
        )

    @mock.patch('djanquiltdb.management.commands.migrate.SharedFilesMigrationExecutor', autospec=True)
    def test_backwards_unapplied(self, mock_executor):
        """
        Case: Call check_or_migrate_shard with a schema that is not yet applied; going backwards
        Expected: Migrate not to be called
        """
        self.sharded_migrate.stdout.write = mock.Mock()
        mock_executor.return_value.loader = mock.Mock()
        mock_executor.return_value.recorder = mock.Mock()
        mock_executor.return_value.loader.applied_migrations = []

        migration_node = self.plan[0]
        migration_node = (migration_node[0], True)  # set as backwards migration

        self.sharded_migrate.check_or_migrate_shard(self.rose, migration_node, False, False)

        self.sharded_migrate.stdout.write.assert_any_call(
            '    other|rose does not have migration_tests.0001_initial applied yet.\n'
        )
        self.assertFalse(mock_executor.return_value.migrate.called)


class SeparateDatabaseAndStateTestCase(MigrationTestCase):
    available_apps = ['migration_tests', 'example']

    def setUp(self):
        # Do not silently mock the router, but do create the template schema

        commands = get_commands()
        commands['migrate'] = 'djanquiltdb'

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
        commands['migrate'] = 'djanquiltdb'

        with mock.patch('django.core.management.get_commands', return_value=commands):
            create_template_schema()  # The template won't have any migration applied to it initially
            create_template_schema('other')  # The template won't have any migration applied to it initially

    @override_settings(
        MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_remove_non_existent_model'},
        QUILT_DB={
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

        call_command('migrate', 'migration_tests', '0001', database='default', verbosity=0)

        with use_shard(self.sina, include_public=False) as env:
            self.assertIn('migration_tests_nonexistingmodel', env.connection.introspection.table_names())

        call_command('migrate', 'migration_tests', '0002', database='default', verbosity=0)

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
        call_command('migrate', 'migration_tests', '0001_run_python', verbosity=0, stderr=stderr)

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
        call_command('migrate', 'migration_tests', '0001', database='default', verbosity=0)
        call_command('migrate', 'migration_tests', '0002', database='default', verbosity=0)
        call_command('migrate', 'migration_tests', '0003', database='default', verbosity=0)

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
        call_command('migrate', verbosity=0, interactive=False, run_syncdb=True)

        with use_shard(node_name='default', schema_name=get_template_name()):
            for model in get_all_sharded_models(include_auto_created=True):
                if model._meta.app_label == 'example':
                    self.assertTableExists(model._meta.db_table)

    def test_emit_pre_migrate_signal(self):
        """
        Case: In migrate, run the sync db phase and don't run the sync db phase
        Expected: In both cases, emit_pre_migrate_signal is called
        """
        verbosity = 0
        interactive = False

        with mock.patch('djanquiltdb.management.commands.migrate.emit_pre_migrate_signal') as mock_signal:
            call_command('migrate', verbosity=verbosity, run_syncdb=True, interactive=interactive)
            mock_signal.assert_called_once_with(verbosity, interactive, 'default', plan=mock.ANY)

        with mock.patch('djanquiltdb.management.commands.migrate.emit_pre_migrate_signal') as mock_signal:
            call_command('migrate', verbosity=verbosity, run_syncdb=False, interactive=interactive)
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
        call_command('migrate', 'example', 'zero', database='default', verbosity=0)

        self.assertNotIn(Shard._meta.db_table, connections['default'].introspection.table_names())

        # And now do an initial migration, like how we start a project initially
        call_command('migrate', 'example', database='default', verbosity=0)

        self.assertIn(Shard._meta.db_table, connections['default'].introspection.table_names())

    def test_no_template_schema(self):
        """
        Case: Call migrate without having a template schema
        Expected: Migrate command runs without errors
        """
        self.assertFalse(schema_exists('default', get_template_name()))
        call_command('migrate', 'example', database='default', verbosity=0)


class ChangeWarningTestCase(ShardingTestCase):
    """
    The notice printed when there is nothing left to apply but the models have moved on.

    Nothing else in the suite reaches it: it needs an empty plan and a verbosity of at least 1 together. It is also
    the only autodetector the command builds, and it builds whichever one its command class carries rather than
    naming a class of its own, which the last case pins.

    The notice names no app and no change, so it cannot say which detected change produced it. Whether the right
    autodetector was used is therefore asserted on the class itself rather than read out of the output.
    """

    available_apps = ['migration_tests', 'djanquiltdb', 'example']

    NOTHING_TO_APPLY = 'No migrations to apply.'
    HAS_CHANGES = 'not yet reflected in a migration'

    def migrate(self):
        """
        Migrate everything with the database already up to date, so the plan is empty and the check runs.
        """
        out = StringIO()
        call_command('migrate', verbosity=1, stdout=out)

        return out.getvalue()

    def test_an_empty_plan_is_reported(self):
        """
        Case: Migrate a database that is already fully migrated, with every app's migrations matching its models.
        Expected: It says there is nothing to apply and stays quiet about unmigrated changes, so the notice below
                  means something when it does appear.
        """
        output = self.migrate()

        self.assertIn(self.NOTHING_TO_APPLY, output)
        self.assertNotIn(self.HAS_CHANGES, output)

    @override_settings(MIGRATION_MODULES={'migration_tests': 'migration_tests.test_migrations_empty'})
    def test_models_without_a_migration_are_reported(self):
        """
        Case: Migrate with an app whose models have no migrations at all.
        Expected: The notice, and the hint naming makemigrations, so someone who has forgotten to write one is told.
        """
        output = self.migrate()

        self.assertIn(self.HAS_CHANGES, output)
        self.assertIn('makemigrations', output)

    def test_the_autodetector_is_inherited_rather_than_declared(self):
        """
        Case: Read the autodetector class the command builds its unmigrated-changes check with.
        Expected: Exactly the one Django's own migrate command carries. The command declares no autodetector of its
                  own, so whatever a library has layered onto the migration commands is used here too; declaring one
                  would silently drop every library that layers onto them, which is what this guards.
        """
        from django.core.management.commands.migrate import Command as DjangoMigrate

        self.assertIs(ShardedMigrate.autodetector, DjangoMigrate.autodetector)

# Taken from the Django source: https://github.com/django/django/blob/stable/1.8.x/tests/migrations/test_commands.py

from unittest import mock

import six
from django.core.management import call_command, CommandError
from django.db import connection, DatabaseError
from django.db.migrations.recorder import MigrationRecorder
from django.test import override_settings

from sharding.utils import create_template_schema
from .migration_base import MigrationTestBase


@mock.patch('sharding.utils.DynamicDbRouter.allow_migrate')
class MigrateTests(MigrationTestBase):
    """
    Tests running the migrate command.
    """

    @classmethod
    def setUpClass(cls):
        create_template_schema()  # the template won't have any migration applied to it initially
        create_template_schema('other')  # the template won't have any migration applied to it initially

    @override_settings(MIGRATION_MODULES={"migration_tests": "migration_tests.test_migrations"})
    def test_migrate(self, mock_router):
        """
        Tests basic usage of the migrate command.
        """
        # Make sure no tables are created
        self.assertTableNotExists("migration_tests_author")
        self.assertTableNotExists("migration_tests_tribble")
        self.assertTableNotExists("migration_tests_book")
        # Run the migrations to 0001 only
        call_command("migrate_shards", "migration_tests", "0001", verbosity=0)
        # Make sure the right tables exist
        self.assertTableExists("migration_tests_author")
        self.assertTableExists("migration_tests_tribble")
        self.assertTableNotExists("migration_tests_book")
        # Run migrations all the way
        call_command("migrate_shards", verbosity=0)
        # Make sure the right tables exist
        self.assertTableExists("migration_tests_author")
        self.assertTableNotExists("migration_tests_tribble")
        self.assertTableExists("migration_tests_book")
        # Unmigrate everything
        call_command("migrate_shards", "migration_tests", "zero", verbosity=0)
        # Make sure it's all gone
        self.assertTableNotExists("migration_tests_author")
        self.assertTableNotExists("migration_tests_tribble")
        self.assertTableNotExists("migration_tests_book")

    @override_settings(MIGRATION_MODULES={"migration_tests": "migration_tests.test_migrations"})
    def test_migrate_fake_initial(self, mock_router):
        """
        #24184 - Tests that --fake-initial only works if all tables created in
        the initial migration of an app exists
        """
        # Make sure no tables are created
        self.assertTableNotExists("migration_tests_author")
        self.assertTableNotExists("migration_tests_tribble")
        # Run the migrations to 0001 only
        call_command("migrate_shards", "migration_tests", "0001", verbosity=0)
        # Make sure the right tables exist
        self.assertTableExists("migration_tests_author")
        self.assertTableExists("migration_tests_tribble")
        # Fake a roll-back
        call_command("migrate_shards", "migration_tests", "zero", fake=True, verbosity=0)
        # Make sure the tables still exist
        self.assertTableExists("migration_tests_author")
        self.assertTableExists("migration_tests_tribble")
        # Try to run initial migration
        with self.assertRaises(DatabaseError):
            call_command("migrate_shards", "migration_tests", "0001", verbosity=0)
        # Run initial migration with an explicit --fake-initial
        out = six.StringIO()
        with mock.patch('django.core.management.color.supports_color', lambda *args: False):
            call_command("migrate_shards", "migration_tests", "0001", fake_initial=True, stdout=out, verbosity=1)
            # call_command("migrate", "migration_tests", "0001", fake_initial=True, stdout=out, verbosity=1)
        self.assertIn(
            "migration_tests.0001_initial... faked",
            out.getvalue().lower()
        )
        # Run migrations all the way
        call_command("migrate_shards", verbosity=0)
        # Make sure the right tables exist
        self.assertTableExists("migration_tests_author")
        self.assertTableNotExists("migration_tests_tribble")
        self.assertTableExists("migration_tests_book")
        # Fake a roll-back
        call_command("migrate_shards", "migration_tests", "zero", fake=True, verbosity=0)
        # Make sure the tables still exist
        self.assertTableExists("migration_tests_author")
        self.assertTableNotExists("migration_tests_tribble")
        self.assertTableExists("migration_tests_book")
        # Try to run initial migration
        with self.assertRaises(DatabaseError):
            call_command("migrate_shards", "migration_tests", verbosity=0)
        # Run initial migration with an explicit --fake-initial
        with self.assertRaises(DatabaseError):
            # Fails because "migration_tests_tribble" does not exist but needs to in
            # order to make --fake-initial work.
            call_command("migrate_shards", "migration_tests", fake_initial=True, verbosity=0)
        # Fake a apply
        call_command("migrate_shards", "migration_tests", fake=True, verbosity=0)
        # Unmigrate everything
        call_command("migrate_shards", "migration_tests", "zero", verbosity=0)
        # Make sure it's all gone
        self.assertTableNotExists("migration_tests_author")
        self.assertTableNotExists("migration_tests_tribble")
        self.assertTableNotExists("migration_tests_book")

        self.assertTrue(mock_router.called)

    @override_settings(MIGRATION_MODULES={"migration_tests": "migration_tests.test_migrations_conflict"})
    def test_migrate_conflict_exit(self, mock_router):
        """
        Makes sure that migrate exits if it detects a conflict.
        """
        with self.assertRaisesMessage(CommandError, "Conflicting migrations detected"):
            call_command("migrate_shards", "migration_tests")

    @override_settings(MIGRATION_MODULES={"migration_tests": "migration_tests.test_migrations_squashed"})
    def test_migrate_record_replaced(self, mock_router):
        """
        Running a single squashed migration should record all of the original
        replaced migration_tests as run.
        """
        recorder = MigrationRecorder(connection)
        out = six.StringIO()
        call_command("migrate_shards", "migration_tests", verbosity=0)
        call_command("showmigrations", "migration_tests", stdout=out, no_color=True)
        self.assertEqual(
            'migration_tests\n'
            ' [x] 0001_squashed_0002 (2 squashed migrations)\n',
            out.getvalue().lower()
        )
        applied_migration_tests = recorder.applied_migrations()
        self.assertIn(("migration_tests", "0001_initial"), applied_migration_tests)
        self.assertIn(("migration_tests", "0002_second"), applied_migration_tests)
        self.assertIn(("migration_tests", "0001_squashed_0002"), applied_migration_tests)
        # Rollback changes
        call_command("migrate_shards", "migration_tests", "zero", verbosity=0)

    @override_settings(MIGRATION_MODULES={"migration_tests": "migration_tests.test_migrations_squashed"})
    def test_migrate_record_squashed(self, mock_router):
        """
        Running migrate for a squashed migration should record as run
        if all of the replaced migration_tests have been run (#25231).
        """
        recorder = MigrationRecorder(connection)
        recorder.record_applied("migration_tests", "0001_initial")
        recorder.record_applied("migration_tests", "0002_second")
        out = six.StringIO()
        call_command("migrate_shards", "migration_tests", verbosity=0)
        call_command("showmigrations", "migration_tests", stdout=out, no_color=True)
        self.assertEqual(
            'migration_tests\n'
            ' [x] 0001_squashed_0002 (2 squashed migrations)\n',
            out.getvalue().lower()
        )
        self.assertIn(
            ("migration_tests", "0001_squashed_0002"),
            recorder.applied_migrations()
        )
        # No changes were actually applied so there is nothing to rollback

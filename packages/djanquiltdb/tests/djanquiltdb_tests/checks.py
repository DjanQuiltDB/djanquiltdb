from io import StringIO
from unittest import mock, skipIf, skipUnless

import django.core.checks
import django.core.checks.registry
from django.apps import apps
from django.core.checks.registry import CheckRegistry, registry
from django.core.management import call_command, get_commands, load_command_class
from django.core.management.base import SystemCheckError
from django.db import models, router
from django.test import SimpleTestCase, override_settings
from django.test.utils import isolate_apps, override_system_checks

from djanquiltdb.apps import _patch_checks
from djanquiltdb.checks import (
    MUTE_SETTING,
    PGTRIGGER_APP_NAME,
    check_pgtrigger_compatibility,
    check_trigger_writes_stay_in_their_sharding_mode,
    cross_mode_trigger_messages,
    written_tables,
)
from djanquiltdb.decorators import override_sharding_setting, public_model
from djanquiltdb.router import _placement_is_node_scoped
from example.models import CakeType, DefaultUser, Organization, Type, User

# Only the dedicated tox environment installs django-pgtrigger and puts it in INSTALLED_APPS. Everywhere else the
# check must stay silent, which is a case worth asserting in its own right.
PGTRIGGER_INSTALLED = apps.is_installed(PGTRIGGER_APP_NAME)


def warning_ids():
    return [warning.id for warning in check_pgtrigger_compatibility(None)]


class PgtriggerCompatibilityCheckTestCase(SimpleTestCase):
    def test_check_is_registered(self):
        """
        Case: The app config has been readied, as it is for every test run
        Expected: The check is part of Django's check registry
        """
        self.assertIn(check_pgtrigger_compatibility, registry.get_checks())

    @skipIf(PGTRIGGER_INSTALLED, 'django-pgtrigger is installed in this environment')
    def test_incompatible_settings_without_pgtrigger(self):
        """
        Case: Both pgtrigger settings hold an incompatible value, but pgtrigger is not installed
        Expected: No warnings, since the settings mean nothing without the app
        """
        with override_settings(PGTRIGGER_MIGRATIONS=False, PGTRIGGER_INSTALL_ON_MIGRATE=True):
            self.assertEqual(warning_ids(), [])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_default_settings(self):
        """
        Case: pgtrigger is installed and both of its settings are left at their defaults
        Expected: No warnings
        """
        self.assertEqual(warning_ids(), [])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_migrations_disabled(self):
        """
        Case: pgtrigger is installed with PGTRIGGER_MIGRATIONS turned off
        Expected: The triggers-outside-migrations warning
        """
        with override_settings(PGTRIGGER_MIGRATIONS=False):
            self.assertEqual(warning_ids(), ['djanquiltdb.W001'])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_install_on_migrate_enabled(self):
        """
        Case: pgtrigger is installed with PGTRIGGER_INSTALL_ON_MIGRATE turned on
        Expected: The install-at-migrate-time warning
        """
        with override_settings(PGTRIGGER_INSTALL_ON_MIGRATE=True):
            self.assertEqual(warning_ids(), ['djanquiltdb.W002'])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_both_settings_incompatible(self):
        """
        Case: pgtrigger is installed and both of its settings hold an incompatible value
        Expected: A warning for each of them
        """
        with override_settings(PGTRIGGER_MIGRATIONS=False, PGTRIGGER_INSTALL_ON_MIGRATE=True):
            self.assertEqual(warning_ids(), ['djanquiltdb.W001', 'djanquiltdb.W002'])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_muted(self):
        """
        Case: Both pgtrigger settings hold an incompatible value, but the check is muted
        Expected: No warnings
        """
        with override_settings(PGTRIGGER_MIGRATIONS=False, PGTRIGGER_INSTALL_ON_MIGRATE=True):
            with override_sharding_setting(MUTE_SETTING, True):
                self.assertEqual(warning_ids(), [])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_muted_with_a_false_value(self):
        """
        Case: Both pgtrigger settings hold an incompatible value and the mute setting is present but False
        Expected: A warning for each of them, since only a true value mutes the check
        """
        with override_settings(PGTRIGGER_MIGRATIONS=False, PGTRIGGER_INSTALL_ON_MIGRATE=True):
            with override_sharding_setting(MUTE_SETTING, False):
                self.assertEqual(warning_ids(), ['djanquiltdb.W001', 'djanquiltdb.W002'])


class FakeTrigger:
    """
    Stands in for a django-pgtrigger trigger, so the check can be tested without django-pgtrigger installed.
    """

    def __init__(self, body, name='writes'):
        self.body = body
        self.name = name

    def render_func(self, model):
        return self.body

    def get_uri(self, model):
        return '{}:{}'.format(model._meta.label, self.name)


def message_ids(model, body):
    return [message.id for message in cross_mode_trigger_messages([(model, FakeTrigger(body))])]


class WrittenTablesTestCase(SimpleTestCase):
    def test_each_kind_of_write_is_found(self):
        """
        Case: A function body with an INSERT, an UPDATE, a DELETE, a MERGE and a TRUNCATE, each on its own table.
        Expected: The five tables, in the order of the statements.
        """
        body = (
            'INSERT INTO "orders" VALUES (1);\n'
            'UPDATE "invoices" SET x = 1;\n'
            'DELETE FROM "payments";\n'
            'MERGE INTO "deliveries" USING incoming ON true WHEN MATCHED THEN DELETE;\n'
            'TRUNCATE TABLE "archive";'
        )

        self.assertEqual(written_tables(body), ['orders', 'invoices', 'payments', 'deliveries', 'archive'])

    def test_only_and_schema_qualified_names(self):
        """
        Case: An UPDATE ONLY, a DELETE FROM ONLY, and an INSERT into a name that is qualified with a schema.
        Expected: The table names without the schema and without ONLY.
        """
        body = 'UPDATE ONLY "orders" SET x = 1; DELETE FROM ONLY invoices; INSERT INTO "public"."payments" VALUES (1);'

        self.assertEqual(written_tables(body), ['orders', 'invoices', 'payments'])

    def test_unquoted_names_are_lowercased_and_quoted_names_are_kept(self):
        """
        Case: An UPDATE of an unquoted name in uppercase, and an UPDATE of a quoted name with uppercase letters and a
              doubled quote in it.
        Expected: The unquoted name in lowercase, as PostgreSQL folds it, and the quoted name as it is written, with
                  the doubled quote undone.
        """
        self.assertEqual(written_tables('UPDATE Example_Cake SET x = 1;'), ['example_cake'])
        self.assertEqual(written_tables('UPDATE "My""Table" SET x = 1;'), ['My"Table'])

    def test_reads_and_assignments_are_not_writes(self):
        """
        Case: A body that selects into a variable, locks rows with FOR UPDATE and FOR NO KEY UPDATE, and does an
              upsert whose conflict action is DO UPDATE.
        Expected: Only the table of the upsert's INSERT, because a lock and SELECT ... INTO write no table.
        """
        body = (
            'SELECT x INTO var FROM "orders" FOR UPDATE;\n'
            'SELECT x FROM "invoices" FOR NO KEY UPDATE;\n'
            'INSERT INTO "payments" VALUES (1) ON CONFLICT (id) DO UPDATE SET x = 1;'
        )

        self.assertEqual(written_tables(body), ['payments'])

    def test_comments_and_string_literals_are_not_searched(self):
        """
        Case: A body whose only write statements are in a line comment, a block comment, a string literal, and a
              dollar-quoted string. The string literal also contains "--", which must not start a comment.
        Expected: No tables, except the table of the statement after the string literal.
        """
        body = (
            '-- UPDATE "orders" SET x = 1\n'
            '/* DELETE FROM "invoices" */\n'
            'IF TG_OP = \'UPDATE -- "payments"\' THEN\n'
            '    EXECUTE $sql$INSERT INTO "deliveries" VALUES (1)$sql$;\n'
            '    UPDATE "archive" SET x = 1;\n'
            'END IF;'
        )

        self.assertEqual(written_tables(body), ['archive'])

    def test_a_table_is_listed_once(self):
        """
        Case: A body that updates the same table twice.
        Expected: The table once.
        """
        self.assertEqual(written_tables('UPDATE "orders" SET x = 1; UPDATE "orders" SET x = 2;'), ['orders'])

    def test_the_update_action_of_a_merge_is_not_a_table(self):
        """
        Case: A MERGE whose action for matched rows is THEN UPDATE SET.
        Expected: Only the table that the MERGE writes, because the UPDATE after THEN names no table.
        """
        body = 'MERGE INTO "deliveries" USING incoming ON true WHEN MATCHED THEN UPDATE SET x = 1;'

        self.assertEqual(written_tables(body), ['deliveries'])

    def test_an_escaped_quote_in_an_escape_string_does_not_end_it(self):
        """
        Case: A body with an escape string E'...' that has a quote escaped with a backslash in it, followed by an UPDATE
              that sets a column to a string literal. A second body has a string literal without E that ends in a
              backslash, followed by an UPDATE.
        Expected: The table of the UPDATE in both bodies, because a backslash escapes a quote in an escape string only.
        """
        escape_string_body = r"RAISE NOTICE E'it\'s'; UPDATE " + '"orders" SET x = \'y\';'
        backslash_body = r"RAISE NOTICE 'C:\'; UPDATE " + '"invoices" SET x = \'y\';'

        self.assertEqual(written_tables(escape_string_body), ['orders'])
        self.assertEqual(written_tables(backslash_body), ['invoices'])


class CrossModeTriggerMessagesTestCase(SimpleTestCase):
    def test_a_public_or_mirrored_trigger_that_writes_a_sharded_table(self):
        """
        Case: A trigger on the mirrored Type, and one on the public CakeType, each updating the table of the sharded
              User.
        Expected: djanquiltdb.E001 for each.
        """
        self.assertEqual(message_ids(Type, 'UPDATE "example_user" SET x = 1;'), ['djanquiltdb.E001'])
        self.assertEqual(message_ids(CakeType, 'UPDATE "example_user" SET x = 1;'), ['djanquiltdb.E001'])

    def test_a_public_or_sharded_trigger_that_writes_a_mirrored_table(self):
        """
        Case: A trigger on the public CakeType, and one on the sharded User, each inserting into the table of the
              mirrored Type.
        Expected: djanquiltdb.E002 for each.
        """
        self.assertEqual(message_ids(CakeType, 'INSERT INTO "example_type" VALUES (1);'), ['djanquiltdb.E002'])
        self.assertEqual(message_ids(User, 'INSERT INTO "example_type" VALUES (1);'), ['djanquiltdb.E002'])

    def test_a_mirrored_trigger_that_writes_a_public_table(self):
        """
        Case: A trigger on the mirrored Type that deletes from the table of the public CakeType.
        Expected: djanquiltdb.W003, a warning.
        """
        messages = cross_mode_trigger_messages([(Type, FakeTrigger('DELETE FROM "example_caketype";'))])

        self.assertEqual([message.id for message in messages], ['djanquiltdb.W003'])
        self.assertFalse(messages[0].is_serious())

    def test_writes_that_are_allowed(self):
        """
        Case: A sharded trigger that writes another sharded table, one that writes a public table, and a mirrored
              trigger that writes another mirrored table.
        Expected: No messages.
        """
        self.assertEqual(message_ids(User, 'UPDATE "example_cake" SET x = 1;'), [])
        self.assertEqual(message_ids(User, 'UPDATE "example_caketype" SET x = 1;'), [])
        self.assertEqual(message_ids(Type, 'UPDATE "example_type" SET x = 1;'), [])

    def test_models_and_tables_without_a_sharding_mode_are_skipped(self):
        """
        Case: A trigger on DefaultUser, which has no sharding mode, that writes the sharded User, a trigger on the
              mirrored Type that writes DefaultUser, and one that writes a table that no model has.
        Expected: No messages, because the check cannot tell where those tables are.
        """
        self.assertEqual(message_ids(DefaultUser, 'UPDATE "example_user" SET x = 1;'), [])
        self.assertEqual(message_ids(Type, 'UPDATE "example_defaultuser" SET x = 1;'), [])
        self.assertEqual(message_ids(Type, 'UPDATE "no_such_table" SET x = 1;'), [])

    def test_the_message_names_the_trigger_and_both_models(self):
        """
        Case: A trigger named cascade on the mirrored Type that updates the table of the sharded User.
        Expected: The message names the trigger's URI, both models and both sharding modes, and points at the model
                  that the trigger is on.
        """
        [message] = cross_mode_trigger_messages([(Type, FakeTrigger('UPDATE "example_user" SET x = 1;', 'cascade'))])

        self.assertEqual(
            message.msg,
            'The trigger example.Type:cascade on the mirrored model example.Type writes the table of the sharded model '
            'example.User.',
        )
        self.assertIs(message.obj, Type)

    def test_one_message_for_each_table(self):
        """
        Case: A trigger on the mirrored Type that updates the tables of the sharded User and Cake.
        Expected: A message for each of the two tables.
        """
        body = 'UPDATE "example_user" SET x = 1; UPDATE "example_cake" SET x = 1;'

        self.assertEqual(message_ids(Type, body), ['djanquiltdb.E001', 'djanquiltdb.E001'])

    def test_the_message_names_the_concrete_model_of_a_proxy(self):
        """
        Case: A trigger on the mirrored Type that updates the table of the sharded Cake. The proxy model ProxyCake,
              which is registered after Cake, has the same table.
        Expected: The message names Cake, the model that the table belongs to.
        """
        [message] = cross_mode_trigger_messages([(Type, FakeTrigger('UPDATE "example_cake" SET x = 1;'))])

        self.assertIn('the sharded model example.Cake.', message.msg)

    @isolate_apps('example')
    def test_an_unmanaged_model_on_the_same_table_is_not_the_target(self):
        """
        Case: A trigger on the mirrored Type that updates the table of the sharded User. An unmanaged public model,
              which is registered after User, has the same table.
        Expected: djanquiltdb.E001 for User, because the managed model decides how the table is placed.
        """

        @public_model()
        class UnmanagedUser(models.Model):
            class Meta:
                app_label = 'example'
                managed = False
                db_table = 'example_user'

        registered_models = apps.get_models(include_auto_created=True) + [UnmanagedUser]

        with mock.patch('djanquiltdb.checks.apps.get_models', return_value=registered_models):
            [message] = cross_mode_trigger_messages([(Type, FakeTrigger('UPDATE "example_user" SET x = 1;'))])

        self.assertEqual(message.id, 'djanquiltdb.E001')
        self.assertIn('the sharded model example.User.', message.msg)

    def test_a_write_to_a_name_qualified_with_the_schema_of_a_shard(self):
        """
        Case: A trigger on the mirrored Type that updates the table of the sharded User, qualified with the schema of a
              shard.
        Expected: djanquiltdb.E001, with a hint that covers a name that is qualified with a schema.
        """
        [message] = cross_mode_trigger_messages([(Type, FakeTrigger('UPDATE tenant_1."example_user" SET x = 1;'))])

        self.assertEqual(message.id, 'djanquiltdb.E001')
        self.assertIn('the shard whose schema the name is qualified with', message.hint)


class TriggerWritesCheckTestCase(SimpleTestCase):
    def test_check_is_registered(self):
        """
        Case: The app config has been readied, as it is for every test run.
        Expected: The check is part of Django's check registry.
        """
        self.assertIn(check_trigger_writes_stay_in_their_sharding_mode, registry.get_checks())

    @skipIf(PGTRIGGER_INSTALLED, 'django-pgtrigger is installed in this environment')
    def test_without_pgtrigger(self):
        """
        Case: The check runs without django-pgtrigger installed.
        Expected: No messages, because only django-pgtrigger triggers are checked.
        """
        self.assertEqual(check_trigger_writes_stay_in_their_sharding_mode(None), [])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_the_registered_triggers_are_checked(self):
        """
        Case: django-pgtrigger has a trigger registered on the mirrored Type that updates the table of the sharded User.
        Expected: djanquiltdb.E001 from the registered check.
        """
        registered = [(Type, FakeTrigger('UPDATE "example_user" SET x = 1;'))]

        with mock.patch('pgtrigger.registered', return_value=registered):
            messages = check_trigger_writes_stay_in_their_sharding_mode(None)

        self.assertEqual([message.id for message in messages], ['djanquiltdb.E001'])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_only_the_triggers_of_the_given_apps_are_checked(self):
        """
        Case: django-pgtrigger has a trigger registered on the mirrored Type, of the example app, that updates the table
              of the sharded User. The check runs for the djanquiltdb app only, and then for the example app only, as
              `manage.py check <app_label>` runs it.
        Expected: No messages for the djanquiltdb app, and djanquiltdb.E001 for the example app.
        """
        registered = [(Type, FakeTrigger('UPDATE "example_user" SET x = 1;'))]

        with mock.patch('pgtrigger.registered', return_value=registered):
            djanquiltdb_messages = check_trigger_writes_stay_in_their_sharding_mode(
                [apps.get_app_config('djanquiltdb')]
            )
            example_messages = check_trigger_writes_stay_in_their_sharding_mode([apps.get_app_config('example')])

        self.assertEqual(djanquiltdb_messages, [])
        self.assertEqual([message.id for message in example_messages], ['djanquiltdb.E001'])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_the_triggers_of_this_suite_pass(self):
        """
        Case: The check runs for the triggers that the suite's own models declare.
        Expected: No messages, because each of those triggers only rejects writes to its own table.
        """
        self.assertEqual(check_trigger_writes_stay_in_their_sharding_mode(None), [])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_the_check_command_stops_at_the_error(self):
        """
        Case: django-pgtrigger has a trigger registered on the mirrored Type that updates the table of the sharded User,
              and `manage.py check` runs.
        Expected: The command fails with djanquiltdb.E001, as every command that runs the checks would.
        """
        registered = [(Type, FakeTrigger('UPDATE "example_user" SET x = 1;'))]

        with mock.patch('pgtrigger.registered', return_value=registered):
            with self.assertRaisesMessage(SystemCheckError, 'djanquiltdb.E001'):
                call_command('check', tags=['models'], stdout=StringIO(), stderr=StringIO())

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_silenced_system_checks_silence_it(self):
        """
        Case: django-pgtrigger has a trigger registered on the mirrored Type that updates the table of the sharded User,
              SILENCED_SYSTEM_CHECKS lists djanquiltdb.E001, and `manage.py check` runs.
        Expected: The command succeeds, because Django's own setting is the only way to silence the error.
        """
        registered = [(Type, FakeTrigger('UPDATE "example_user" SET x = 1;'))]

        with mock.patch('pgtrigger.registered', return_value=registered):
            with override_settings(SILENCED_SYSTEM_CHECKS=['djanquiltdb.E001']):
                call_command('check', tags=['models'], stdout=StringIO(), stderr=StringIO())


def missing_field_constraint():
    return models.UniqueConstraint(fields=['missing'], name='unique_missing')


def missing_field_messages(messages, model):
    return [message for message in messages if message.id == 'models.E012' and message.obj is model]


def raise_an_error(app_configs=None, **kwargs):
    raise RuntimeError


class NodeScopedPlacementInChecksTestCase(SimpleTestCase):
    databases = '__all__'

    def setUp(self):
        super().setUp()

        self.placements = []

        # A plain function, because the check registry sets an attribute on every check that it registers.
        def record_placement(app_configs=None, **kwargs):
            self.placements.append(router.allow_migrate_model('default', Organization))
            return []

        self.record_placement = record_placement

    def test_every_name_of_run_checks_runs_the_checks_node_scoped(self):
        """
        Case: Run the checks through django.core.checks.run_checks, through django.core.checks.registry.run_checks and
              through the registry's own run_checks, with a check that asks the router about the sharded Organization
              on the bare node name 'default'.
        Expected: The check is told each time that Organization is allowed, and outside the checks it is not.
        """
        with override_system_checks([self.record_placement]):
            django.core.checks.run_checks()
            django.core.checks.registry.run_checks()
            registry.run_checks()

        self.assertEqual(self.placements, [True, True, True])
        self.assertFalse(router.allow_migrate_model('default', Organization))

    def test_the_check_command_runs_the_checks_node_scoped(self):
        """
        Case: Run `manage.py check` with a check that asks the router about the sharded Organization on the bare node
              name 'default'.
        Expected: The check is told that Organization is allowed.
        """
        with override_system_checks([self.record_placement]):
            call_command('check', stdout=StringIO(), stderr=StringIO())

        self.assertEqual(self.placements, [True])

    def test_a_check_that_raises_ends_the_node_scoped_placement(self):
        """
        Case: Run the checks with a check that raises.
        Expected: The exception reaches the caller, and the router answers for the public schema again afterwards.
        """
        with override_system_checks([raise_an_error]):
            with self.assertRaises(RuntimeError):
                django.core.checks.run_checks()

        self.assertFalse(_placement_is_node_scoped.get())

    def test_migrate_places_its_operations_after_the_checks_end(self):
        """
        Case: Run `migrate` with its checks, with a check that asks the router about the sharded Organization on the
              bare node name 'default', and record the router's answer once the command starts its work.
        Expected: The check is told that Organization is allowed, and the command is told that it is not, so migrate
                  creates no sharded table in a public schema.
        """
        command_class = type(load_command_class(get_commands()['migrate'], 'migrate'))

        def handle(*args, **options):
            self.placements.append(router.allow_migrate_model('default', Organization))

        with override_system_checks([self.record_placement]):
            with mock.patch.object(command_class, 'handle', side_effect=handle):
                call_command('migrate', skip_checks=False, stdout=StringIO(), stderr=StringIO())

        self.assertEqual(self.placements, [True, False])

    def test_patching_twice_wraps_once(self):
        """
        Case: Patch the check registry again, as a second AppConfig.ready() would, and take the checks from it.
        Expected: get_checks stays the same function, and the check that it returns wraps the registered check
                  directly, so the check is not wrapped twice.
        """
        get_checks = registry.get_checks

        _patch_checks()

        self.assertIs(registry.get_checks, get_checks)
        with override_system_checks([self.record_placement]):
            [check] = registry.get_checks()
        self.assertIs(check.__wrapped__, self.record_placement)

    def test_django_checks_the_constraints_of_a_sharded_model(self):
        """
        Case: The sharded Organization has a unique constraint on a field that does not exist, and the checks run for
              the database 'default'.
        Expected: models.E012 for Organization, which Django only reports for a database that the routers allow the
                  model on.
        """
        with mock.patch.object(Organization._meta, 'constraints', [missing_field_constraint()]):
            messages = django.core.checks.run_checks(tags=['models'], databases=['default'])

        self.assertEqual(len(missing_field_messages(messages, Organization)), 1)

    def test_a_run_checks_taken_before_djanquiltdb_was_ready_checks_a_sharded_model(self):
        """
        Case: The sharded Organization has a unique constraint on a field that does not exist. The checks run for the
              database 'default' through Django's own CheckRegistry.run_checks, bound to the registry. A caller that
              took run_checks before DjanQuiltDB was ready has this method, such as a Celery worker as it starts.
        Expected: models.E012 for Organization, as through django.core.checks.run_checks.
        """
        run_checks = CheckRegistry.run_checks.__get__(registry)

        with mock.patch.object(Organization._meta, 'constraints', [missing_field_constraint()]):
            messages = run_checks(tags=['models'], databases=['default'])

        self.assertEqual(len(missing_field_messages(messages, Organization)), 1)

    def test_django_checks_the_constraints_of_a_public_model(self):
        """
        Case: The public CakeType has a unique constraint on a field that does not exist, and its checks run for the
              database 'default', inside and outside run_checks.
        Expected: models.E012 for CakeType both times, because the public schema of 'default' has its table either
                  way.
        """
        with mock.patch.object(CakeType._meta, 'constraints', [missing_field_constraint()]):
            messages = django.core.checks.run_checks(tags=['models'], databases=['default'])
            direct_messages = CakeType.check(databases=['default'])

        self.assertEqual(len(missing_field_messages(messages, CakeType)), 1)
        self.assertEqual(len(missing_field_messages(direct_messages, CakeType)), 1)

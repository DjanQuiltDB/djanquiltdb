from django.apps import apps
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.loader import MigrationLoader
from django.db.migrations.questioner import NonInteractiveMigrationQuestioner
from django.db.migrations.state import ProjectState
from django.test import SimpleTestCase

from djanquiltdb.router import DynamicDbRouter
from djanquiltdb.utils import get_template_name

APP_LABEL = 'pgtrigger_tests'


def add_trigger_operations():
    """The AddTrigger operations of this app's initial migration, keyed by the model they name."""
    migration = MigrationLoader(None, ignore_no_migrations=True).disk_migrations[APP_LABEL, '0001_initial']

    return {
        operation.model_name: operation
        for operation in migration.operations
        if operation.__class__.__name__ == 'AddTrigger'
    }


class TriggerOperationRoutingTestCase(SimpleTestCase):
    def setUp(self):
        self.router = DynamicDbRouter()
        self.operations = add_trigger_operations()

    def test_the_operations_name_the_model_they_belong_to(self):
        """
        Case: django-pgtrigger has written a trigger of each sharding mode into a migration.
        Expected: Both operations carry the model name, which is what the router reads the sharding mode from.
        """
        self.assertEqual(set(self.operations), {'shardedprotected', 'publicprotected'})

    def test_the_sharded_trigger_is_placed_off_the_public_schema_without_a_hint(self):
        """
        Case: The router is asked where the sharded model's trigger operation belongs, with no sharding_mode hint.
        Expected: Everywhere but a public schema, the same answer the model itself gets.
        """
        model_name = self.operations['shardedprotected'].model_name

        self.assertTrue(self.router.allow_migrate('default|{}'.format(get_template_name()), APP_LABEL, model_name))
        self.assertTrue(self.router.allow_migrate('default|some_shard', APP_LABEL, model_name))
        self.assertFalse(self.router.allow_migrate('default', APP_LABEL, model_name))

    def test_the_public_trigger_is_placed_on_the_public_schema_without_a_hint(self):
        """
        Case: The router is asked where the public model's trigger operation belongs, with no sharding_mode hint.
        Expected: The public schema only, so it reaches neither the template nor a shard.
        """
        model_name = self.operations['publicprotected'].model_name

        self.assertTrue(self.router.allow_migrate('default', APP_LABEL, model_name))
        self.assertFalse(self.router.allow_migrate('default|{}'.format(get_template_name()), APP_LABEL, model_name))
        self.assertFalse(self.router.allow_migrate('default|some_shard', APP_LABEL, model_name))


class TriggerMigrationStateTestCase(SimpleTestCase):
    def test_no_trigger_change_is_left_unmigrated(self):
        """
        Case: The autodetector compares this app's models against its migrations, as `makemigrations --check` would.
        Expected: Nothing pending, so a django-pgtrigger release that renders its triggers differently is caught here
                  rather than by a shard silently diverging from the template.
        """
        loader = MigrationLoader(None, ignore_no_migrations=True)
        autodetector = MigrationAutodetector(
            loader.project_state(),
            ProjectState.from_apps(apps),
            # The autodetector walks every app, and the example app has a field whose migration and model disagree,
            # which makes the questioner report on it. That is not what this test is about, so swallow the message and
            # trim the answer to this app.
            NonInteractiveMigrationQuestioner(specified_apps={APP_LABEL}, dry_run=True, log=lambda message: None),
        )

        self.assertNotIn(APP_LABEL, autodetector.changes(graph=loader.graph, trim_to_apps={APP_LABEL}))

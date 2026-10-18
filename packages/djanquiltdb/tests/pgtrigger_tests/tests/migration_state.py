import pgtrigger
from django.test import SimpleTestCase

from djanquiltdb.testing import pending_migration_changes
from pgtrigger_tests.models import ShardedProtected


class PendingTriggerChangesTestCase(SimpleTestCase):
    def test_no_trigger_is_left_unmigrated(self):
        """
        Case: The autodetector compares the triggers that the models of pgtrigger_tests declare with its migrations.
        Expected: Nothing pending.
        """
        self.assertEqual(pending_migration_changes({'pgtrigger_tests'}), {})

    def test_a_trigger_without_a_migration_is_reported(self):
        """
        Case: A trigger is registered on a model of pgtrigger_tests, and no migration of the app adds it.
        Expected: The trigger is reported as pending, as makemigrations would write it.
        """
        trigger = pgtrigger.Protect(name='protect_updates', operation=pgtrigger.Update)
        trigger.register(ShardedProtected)
        self.addCleanup(trigger.unregister, ShardedProtected)

        operations = pending_migration_changes({'pgtrigger_tests'}).get('pgtrigger_tests', [])

        self.assertEqual(
            [(type(operation).__name__, operation.model_name, operation.trigger.name) for operation in operations],
            [('AddTrigger', 'shardedprotected', 'protect_updates')],
        )

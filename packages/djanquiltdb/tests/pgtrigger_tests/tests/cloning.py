import pgtrigger
from django.db.utils import ProgrammingError

from djanquiltdb import State
from djanquiltdb.utils import create_template_schema, get_template_name, use_shard
from example.models import Shard
from pgtrigger_tests.models import ShardedProtected
from pgtrigger_tests.tests.base import PgtriggerTestCase, trigger_definition, triggers_in_schema


class TriggerCloningTestCase(PgtriggerTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()
        self.template = get_template_name()

        # Created after the template carries the trigger, which is the case under test: this shard is never migrated,
        # everything it has comes from the clone.
        self.shard = Shard.objects.create(
            alias='late', schema_name='late_schema', node_name='default', state=State.ACTIVE
        )

    def test_a_shard_created_after_the_migration_inherits_every_trigger(self):
        """
        Case: A shard is created once the template already carries a django-pgtrigger trigger.
        Expected: The shard holds exactly the triggers the template holds.
        """
        self.assertIn((self.sharded_table, self.sharded_trigger), triggers_in_schema(self.shard.schema_name))
        self.assertEqual(triggers_in_schema(self.shard.schema_name), triggers_in_schema(self.template))

    def test_the_cloned_trigger_binds_to_the_shard_its_own_table_and_function(self):
        """
        Case: The trigger definition is read from the template, where the table and the trigger function both print
              unqualified, and replayed with the new shard first on the search path.
        Expected: The clone's definition names the shard's table, and the template is not named anywhere in it.
        """
        definition = trigger_definition(self.shard.schema_name, self.sharded_table, self.sharded_trigger)

        self.assertIsNotNone(definition)
        self.assertIn('{}.{}'.format(self.shard.schema_name, self.sharded_table), definition)
        self.assertIn('{}.{}()'.format(self.shard.schema_name, self.sharded_trigger), definition)
        self.assertNotIn(self.template, definition)

    def test_the_cloned_trigger_fires_in_the_shard(self):
        """
        Case: A row is written and then deleted on the shard, whose model is protected against deletes.
        Expected: The write goes through and the delete is refused by the cloned trigger.
        """
        with use_shard(self.shard):
            row = ShardedProtected.objects.create(name='protected')

            with self.assertRaises(ProgrammingError) as context:
                row.delete()

        self.assertIn('Cannot delete rows from {}'.format(self.sharded_table), str(context.exception))

    def test_the_cloned_trigger_can_be_ignored_on_the_shard(self):
        """
        Case: The delete is retried on the shard inside `pgtrigger.ignore`, whose helper lives in the public schema
              rather than in the shard's own copy of the objects.
        Expected: The delete goes through, so the shared helper is reachable from the cloned trigger.
        """
        uri = ShardedProtected._meta.triggers[0].get_uri(ShardedProtected)

        with use_shard(self.shard):
            row = ShardedProtected.objects.create(name='ignorable')

            with pgtrigger.ignore(uri):
                row.delete()

            self.assertFalse(ShardedProtected.objects.filter(name='ignorable').exists())

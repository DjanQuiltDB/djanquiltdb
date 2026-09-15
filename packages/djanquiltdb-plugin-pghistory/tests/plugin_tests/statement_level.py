import pghistory
import pgtrigger
from django.test import SimpleTestCase
from djanquiltdb import ShardingMode, State
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard
from pghistory.models import Context

from example.models import Croissant, CroissantEvent, Shard
from plugin_tests.context_placement import run_only_when_the_context_table_is


class StatementLevelTriggerGenerationTestCase(SimpleTestCase):
    def test_the_level_reaches_the_generated_trigger(self):
        """
        Case: Croissant's call site passes level=pghistory.Statement, which the plugin passes on unchanged.
        Expected: The triggers on the tracked model are statement-level. Reading the events cannot show this, because
                  both levels record the same events.
        """
        levels = {trigger.level for trigger in Croissant._meta.triggers}

        self.assertEqual(levels, {pgtrigger.Statement})


class StatementLevelEventsInsideAShardTestCase(ShardingTransactionTestCase):
    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.shard = Shard.objects.create(
            alias='one', schema_name='one_schema', node_name='default', state=State.ACTIVE
        )
        self.other_shard = Shard.objects.create(
            alias='two', schema_name='two_schema', node_name='default', state=State.ACTIVE
        )

    def test_a_statement_level_trigger_records_events_only_inside_its_shard(self):
        """
        Case: A row of Croissant, a sharded model that is tracked with level=pghistory.Statement, is created and renamed
              inside a shard, while a second shard exists.
        Expected: The shard has the insert and update events, so the trigger could call the context function from inside
                  the schema of the shard. The other shard has no events.
        """
        with use_shard(self.shard):
            croissant = Croissant.objects.create(name='plain')
            croissant.name = 'almond'
            croissant.save()

            events = CroissantEvent.objects.filter(pgh_obj_id=croissant.pk).order_by('pgh_id')

            self.assertEqual([event.pgh_label for event in events], ['insert', 'update'])

        with use_shard(self.other_shard):
            self.assertEqual(CroissantEvent.objects.count(), 0)

    def test_one_statement_that_changes_many_rows_records_an_event_for_each_row(self):
        """
        Case: A single UPDATE changes two rows at once.
        Expected: Each changed row gets its own event.
        """
        with use_shard(self.shard):
            Croissant.objects.create(name='plain')
            Croissant.objects.create(name='almond')

            Croissant.objects.update(name='day-old')

            self.assertEqual(CroissantEvent.objects.filter(pgh_label='update').count(), 2)

    def test_statement_level_events_have_the_denormalized_context(self):
        """
        Case: A Croissant is created inside a context, inside a shard.
        Expected: The event has the context that was set.
        """
        with use_shard(self.shard):
            with pghistory.context(baked_by='ann'):
                croissant = Croissant.objects.create(name='plain')

            event = CroissantEvent.objects.get(pgh_obj_id=croissant.pk)

            self.assertEqual(event.pgh_context, {'baked_by': 'ann'})

    @run_only_when_the_context_table_is(ShardingMode.SHARDED)
    def test_a_statement_level_trigger_writes_a_context_row_in_its_shard(self):
        """
        Case: A Croissant is created inside a context, inside a shard, while the Context table is sharded. The
              trigger calls _pgh_attach_context(), which writes the context to the Context table on the search path,
              although the event copies the context to its own row.
        Expected: The shard has a context row with the metadata. The other shard has none.
        """
        with use_shard(self.shard):
            with pghistory.context(baked_by='ann'):
                Croissant.objects.create(name='plain')

            self.assertEqual(Context.objects.get().metadata, {'baked_by': 'ann'})

        with use_shard(self.other_shard):
            self.assertEqual(Context.objects.count(), 0)

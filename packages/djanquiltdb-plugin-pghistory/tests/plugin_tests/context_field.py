import pghistory
from django.db import models
from django.test import SimpleTestCase, override_settings
from djanquiltdb import ShardingMode, State
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard
from pghistory.models import Context

from djanquiltdb_plugin_pghistory.checks import check_pghistory_sharding_configuration
from example.models import Shard, Waffle, WaffleEvent
from plugin_tests.checks import quilt_db_with_app_sharding_mode


class ContextForeignKeyInsideAShardTestCase(ShardingTransactionTestCase):
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

    def test_the_context_row_is_written_only_in_the_shard_of_the_event(self):
        """
        Case: A Waffle is created inside a context, inside one shard, while a second shard exists. The trigger finds
              _pgh_attach_context() through the search path of the shard.
        Expected: The event has a context with the metadata that was set. The context row is written in the shard that
                  recorded the event, and not in the other one.
        """
        with use_shard(self.shard):
            with pghistory.context(baked_by='ann'):
                waffle = Waffle.objects.create(name='belgian')

            event = WaffleEvent.objects.get(pgh_obj_id=waffle.pk)

            self.assertEqual(event.pgh_context.metadata, {'baked_by': 'ann'})
            self.assertEqual(Context.objects.count(), 1)

        with use_shard(self.other_shard):
            self.assertEqual(Context.objects.count(), 0)

    def test_an_event_written_outside_a_context_has_no_context(self):
        """
        Case: A Waffle is created inside a shard with no active context, so _pgh_attach_context() returns NULL and
              inserts no row.
        Expected: The event has no context, and no context row is written.
        """
        with use_shard(self.shard):
            waffle = Waffle.objects.create(name='plain')

            self.assertIsNone(WaffleEvent.objects.get(pgh_obj_id=waffle.pk).pgh_context_id)
            self.assertEqual(Context.objects.count(), 0)

    def test_one_context_spanning_two_shards_is_recorded_in_each(self):
        """
        Case: A single pghistory.context() block writes tracked rows in two different shards.
        Expected: Both shards have a context row with the same id and the same metadata.
        """
        with pghistory.context(baked_by='ann'):
            with use_shard(self.shard):
                Waffle.objects.create(name='belgian')

            with use_shard(self.other_shard):
                Waffle.objects.create(name='liege')

        with use_shard(self.shard):
            context_in_first_shard = Context.objects.get()

        with use_shard(self.other_shard):
            context_in_other_shard = Context.objects.get()

        self.assertEqual(context_in_first_shard.id, context_in_other_shard.id)
        self.assertEqual(context_in_first_shard.metadata, context_in_other_shard.metadata)


class CallSiteContextFieldTestCase(SimpleTestCase):
    def test_the_generated_event_model_has_a_context_foreign_key(self):
        """
        Case: The pgh_context field of WaffleEvent is inspected, while PGHISTORY_CONTEXT_FIELD uses the denormalized
              context for the whole project.
        Expected: The field is a foreign key, because the tracking call of Waffle asks for one. The tests in
                  ContextForeignKeyInsideAShardTestCase depend on this.
        """
        self.assertIsInstance(WaffleEvent._meta.get_field('pgh_context'), models.ForeignKey)

    def test_the_check_sees_a_context_field_that_a_call_site_passes(self):
        """
        Case: PGHISTORY_CONTEXT_FIELD uses the denormalized context, but the call sites of Waffle, Danish and Strudel
              ask for a context foreign key, and the pghistory app is mirrored.
        Expected: The W003 warning, and not the W001 warning for a Context table that nothing reads.
        """
        with override_settings(QUILT_DB=quilt_db_with_app_sharding_mode('pghistory', ShardingMode.MIRRORED)):
            self.assertEqual(
                [message.id for message in check_pghistory_sharding_configuration(None)],
                ['djanquiltdb_plugin_pghistory.W003'],
            )

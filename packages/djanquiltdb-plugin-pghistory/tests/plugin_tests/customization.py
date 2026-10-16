import pghistory
from django.db import models
from django.test import SimpleTestCase
from djanquiltdb import State
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard

from djanquiltdb_plugin_pghistory import create_event_model
from example.models import Cake, CakeEvent, Muffin, MuffinEvent, Shard


class ProjectBaseModelTestCase(SimpleTestCase):
    def test_every_event_model_has_the_field_of_the_project_base_model(self):
        """
        Case: PGHISTORY_BASE_MODEL names a base that declares pgh_from_batch.
        Expected: The generated event models have the field, regardless of whether their call site customizes anything.
        """
        self.assertIsNotNone(MuffinEvent._meta.get_field('pgh_from_batch'))
        self.assertIsNotNone(CakeEvent._meta.get_field('pgh_from_batch'))

    def test_only_call_sites_that_use_the_wrapper_get_its_trigger(self):
        """
        Case: Muffin is tracked through the track() wrapper that adds the batch flag trigger, and Cake through the
              plugin's track() directly.
        Expected: Only Muffin's event model has the trigger.
        """
        muffin_triggers = [trigger.name for trigger in MuffinEvent._meta.triggers]
        cake_triggers = [trigger.name for trigger in getattr(CakeEvent._meta, 'triggers', [])]

        self.assertIn('set_pgh_from_batch', muffin_triggers)
        self.assertNotIn('set_pgh_from_batch', cake_triggers)

    def test_attrs_and_meta_reach_the_generated_model_unchanged(self):
        """
        Case: create_event_model gets attrs and meta, together with the arguments that the plugin sets itself.
        Expected: Both reach the generated model exactly as in pghistory. The track() wrapper in example.tracking
                  depends on this, because it passes its trigger through meta.
        """
        event_model = create_event_model(
            Cake, abstract=True, attrs={'note': models.TextField(null=True)}, meta={'ordering': ['pgh_id']}
        )

        self.assertIsNotNone(event_model._meta.get_field('note'))
        self.assertEqual(event_model._meta.ordering, ['pgh_id'])


class BatchFlagTriggerTestCase(ShardingTransactionTestCase):
    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.shard = Shard.objects.create(
            alias='one', schema_name='one_schema', node_name='default', state=State.ACTIVE
        )

    def test_the_batch_flag_trigger_sets_the_field_from_the_context_of_the_event(self):
        """
        Case: Muffins are written inside a context that has the batch key, inside one without it, and outside any
              context.
        Expected: pgh_from_batch is True, False and None, in that order. The trigger sees the denormalized context
                  exactly as the event row stores it.
        """
        with use_shard(self.shard):
            with pghistory.context(batch=True):
                batched = Muffin.objects.create(name='blueberry')

            with pghistory.context(flavor='bran'):
                interactive = Muffin.objects.create(name='bran')

            plain = Muffin.objects.create(name='plain')

            self.assertIs(MuffinEvent.objects.get(pgh_obj_id=batched.pk).pgh_from_batch, True)
            self.assertIs(MuffinEvent.objects.get(pgh_obj_id=interactive.pk).pgh_from_batch, False)
            self.assertIsNone(MuffinEvent.objects.get(pgh_obj_id=plain.pk).pgh_from_batch)

    def test_the_field_of_the_project_base_model_is_empty_without_the_wrapper(self):
        """
        Case: A Cake is written inside a context that has the batch key, but the tracking of Cake does not use the
              wrapper.
        Expected: Its event has the field from the project base model, and the field is empty, because only the trigger
                  that the wrapper adds fills it in.
        """
        with use_shard(self.shard):
            with pghistory.context(batch=True):
                cake = Cake.objects.create(name='carrot')

            self.assertIsNone(CakeEvent.objects.get(pgh_obj_id=cake.pk).pgh_from_batch)

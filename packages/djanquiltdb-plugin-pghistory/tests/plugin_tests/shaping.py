from django.core.exceptions import ImproperlyConfigured
from django.db import models
from django.test import SimpleTestCase
from django.test.utils import isolate_apps
from djanquiltdb import State
from djanquiltdb.decorators import sharded_model
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard

from djanquiltdb_plugin_pghistory import (
    MirroredEvent,
    ShardedEvent,
    create_event_model,
    track_mirrored,
    track_public,
    track_sharded,
)
from example.models import Scone, SconeIcingEvent, SconePlainEvent, Shard


class SconeEventModelGenerationTestCase(SimpleTestCase):
    def test_the_fields_keyword_keeps_only_the_named_columns(self):
        """
        Case: SconeIcingEvent is generated with fields=['icing'].
        Expected: It has the icing column and not the name column.
        """
        columns = {field.name for field in SconeIcingEvent._meta.fields}

        self.assertIn('icing', columns)
        self.assertNotIn('name', columns)

    def test_the_exclude_keyword_drops_only_the_named_columns(self):
        """
        Case: SconePlainEvent is generated with exclude=['icing'].
        Expected: It has every tracked column except the excluded one.
        """
        columns = {field.name for field in SconePlainEvent._meta.fields}

        self.assertIn('name', columns)
        self.assertNotIn('icing', columns)

    def test_model_name_is_used_as_given(self):
        """
        Case: Both call sites pass model_name.
        Expected: The generated classes have exactly those names, whatever the base is.
        """
        self.assertEqual(SconeIcingEvent.__name__, 'SconeIcingEvent')
        self.assertEqual(SconePlainEvent.__name__, 'SconePlainEvent')

    def test_obj_field_none_leaves_the_event_model_without_a_back_reference(self):
        """
        Case: SconePlainEvent is generated with obj_field=None, and SconeIcingEvent with an obj field.
        Expected: SconePlainEvent has no pgh_obj, and SconeIcingEvent has one.
        """
        self.assertFalse(hasattr(SconePlainEvent, 'pgh_obj'))
        self.assertTrue(hasattr(SconeIcingEvent, 'pgh_obj'))

    def test_a_custom_related_name_names_the_reverse_accessor(self):
        """
        Case: The call site of SconeIcingEvent passes obj_field=ObjForeignKey(related_name='icing_events').
        Expected: Scone has the icing_events accessor, and not the default events accessor of pghistory.
        """
        self.assertTrue(hasattr(Scone, 'icing_events'))
        self.assertFalse(hasattr(Scone, 'events'))

    def test_a_partial_event_model_cannot_revert(self):
        """
        Case: SconeIcingEvent does not track every field of Scone, so it cannot rebuild a row from one of its events.
        Expected: can_revert is False, and revert() raises RuntimeError, as in pghistory.
        """
        event = SconeIcingEvent()

        self.assertFalse(event.can_revert)

        with self.assertRaises(RuntimeError):
            event.revert()


class ExplicitTrackingFunctionBaseCheckTestCase(SimpleTestCase):
    def test_track_sharded_raises_for_a_base_of_another_mode(self):
        """
        Case: track_sharded gets MirroredEvent as its base model.
        Expected: ImproperlyConfigured, which names ShardedEvent, as soon as track_sharded is called and before it
                  decorates anything.
        """
        with self.assertRaisesMessage(ImproperlyConfigured, 'ShardedEvent'):
            track_sharded(base_model=MirroredEvent)

    def test_track_mirrored_raises_for_a_base_of_another_mode(self):
        """
        Case: track_mirrored gets ShardedEvent as its base model.
        Expected: ImproperlyConfigured, which names MirroredEvent, as soon as track_mirrored is called and before it
                  decorates anything.
        """
        with self.assertRaisesMessage(ImproperlyConfigured, 'MirroredEvent'):
            track_mirrored(base_model=ShardedEvent)

    def test_track_public_raises_for_a_base_of_another_mode(self):
        """
        Case: track_public gets MirroredEvent as its base model.
        Expected: ImproperlyConfigured is raised, with a message that names PublicEvent.
        """
        with self.assertRaisesMessage(ImproperlyConfigured, 'PublicEvent'):
            track_public(base_model=MirroredEvent)


class CompositePrimaryKeyTestCase(SimpleTestCase):
    @isolate_apps('example')
    def test_a_composite_primary_key_raises_the_value_error_of_pghistory(self):
        """
        Case: A sharded model with a composite primary key is passed to create_event_model.
        Expected: The ValueError of pghistory is raised unchanged.
        """

        @sharded_model()
        class Crumb(models.Model):
            first = models.IntegerField()
            second = models.IntegerField()
            pk = models.CompositePrimaryKey('first', 'second')

            class Meta:
                app_label = 'example'

        with self.assertRaisesMessage(ValueError, 'composite primary keys'):
            create_event_model(Crumb, abstract=True)


class SconeEventsInsideAShardTestCase(ShardingTransactionTestCase):
    available_apps = ['djanquiltdb', 'example', 'django.contrib.auth', 'django.contrib.contenttypes']

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.shard = Shard.objects.create(
            alias='one', schema_name='one_schema', node_name='default', state=State.ACTIVE
        )

    def test_both_event_models_of_scone_are_written_inside_the_shard(self):
        """
        Case: A sharded row is created while two event models track different parts of it. SconePlainEvent, which is
              generated with obj_field=None, has no pgh_obj to filter on, so it is queried directly.
        Expected: Both record their insert event in the shard, each with its own columns.
        """
        with use_shard(self.shard):
            scone = Scone.objects.create(name='cherry', icing='none')

            icing_event = SconeIcingEvent.objects.get(pgh_obj_id=scone.pk)
            plain_event = SconePlainEvent.objects.get()

            self.assertEqual(icing_event.icing, 'none')
            self.assertEqual(plain_event.name, 'cherry')

    def test_the_reverse_accessor_reaches_the_events_inside_the_shard(self):
        """
        Case: The events of a sharded Scone row are read through icing_events, the accessor that the obj field names,
              and not through a query on the event model.
        Expected: Both events of the row are found.
        """
        with use_shard(self.shard):
            scone = Scone.objects.create(name='cherry', icing='none')
            scone.icing = 'lemon'
            scone.save()

            labels = [event.pgh_label for event in scone.icing_events.order_by('pgh_id')]

            self.assertEqual(labels, ['scone.iced_insert', 'scone.iced_update'])

from django.test import SimpleTestCase
from djanquiltdb import State
from djanquiltdb.testing import ShardingTestCase, ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard

from djanquiltdb_plugin_pghistory import MirroredEvent, PublicEvent, ShardedEvent
from example.models import (
    Bakery,
    BakeryEvent,
    Cake,
    CakeEvent,
    CakeType,
    CakeTypeEvent,
    CustomCakeEvent,
    Pie,
    PieCustomcakeevent,
    Shard,
)


class GeneratedEventModelTestCase(SimpleTestCase):
    def test_event_models_are_named_after_the_tracked_model_only(self):
        """
        Case: Event models are generated from the plugin bases, which are all named Event, and from CustomCakeEvent.
        Expected: The generated names do not contain Sharded, Mirrored or Public. The event model of Pie, whose base is
                  CustomCakeEvent, is named PieCustomcakeevent, as in pghistory.
        """
        self.assertEqual(CakeEvent.__name__, 'CakeEvent')
        self.assertEqual(BakeryEvent.__name__, 'BakeryEvent')
        self.assertEqual(CakeTypeEvent.__name__, 'CakeTypeEvent')
        self.assertEqual(PieCustomcakeevent.__name__, 'PieCustomcakeevent')

    def test_event_models_inherit_the_base_for_the_mode_of_the_tracked_model(self):
        """
        Case: Models of each mode are tracked, and the call site of Pie passes its own base, CustomCakeEvent.
        Expected: Each generated event model inherits the plugin base that matches the mode of its tracked model, and
                  the event model of Pie inherits the custom base, which is built on ShardedEvent.
        """
        self.assertTrue(issubclass(CakeEvent, ShardedEvent))
        self.assertTrue(issubclass(BakeryEvent, MirroredEvent))
        self.assertTrue(issubclass(CakeTypeEvent, PublicEvent))
        self.assertTrue(issubclass(PieCustomcakeevent, CustomCakeEvent))


class ShardedTrackingTestCase(ShardingTransactionTestCase):
    available_apps = ['djanquiltdb', 'example', 'django.contrib.auth', 'django.contrib.contenttypes']

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.shard = Shard.objects.create(
            alias='one', schema_name='one_schema', node_name='default', state=State.ACTIVE
        )
        self.other_shard = Shard.objects.create(
            alias='two', schema_name='two_schema', node_name='default', state=State.ACTIVE
        )

    def test_sharded_events_are_recorded_only_in_the_shard_that_wrote_them(self):
        """
        Case: A Cake is created and renamed inside a shard that is created from the template schema, while a second
              shard exists.
        Expected: The shard has the insert and update events for the row, and the other shard has no events.
        """
        with use_shard(self.shard):
            cake = Cake.objects.create(name='carrot')
            cake.name = 'chocolate'
            cake.save()

            events = CakeEvent.objects.filter(pgh_obj_id=cake.pk).order_by('pgh_id')

            self.assertEqual([event.pgh_label for event in events], ['insert', 'update'])

        with use_shard(self.other_shard):
            self.assertEqual(CakeEvent.objects.count(), 0)

    def test_the_events_of_a_custom_base_are_recorded_with_its_field(self):
        """
        Case: Pie is tracked with base_model=CustomCakeEvent, whose extra field has a database default.
        Expected: Events are recorded in the shard, with the field of the custom base filled in.
        """
        with use_shard(self.shard):
            pie = Pie.objects.create(name='apple')

            event = PieCustomcakeevent.objects.get(pgh_obj_id=pie.pk)

            self.assertEqual(event.pgh_label, 'insert')
            self.assertTrue(event.baked_with_love)


class MirroredTrackingTestCase(ShardingTestCase):
    def test_mirrored_events_are_recorded_on_the_primary(self):
        """
        Case: A Bakery is created and renamed.
        Expected: The insert and update events are recorded on the primary node, and the second node has no events.
        """
        bakery = Bakery.objects.create(name='corner')
        bakery.name = 'main street'
        bakery.save()

        with use_shard(node_name='default'):
            events = BakeryEvent.objects.filter(pgh_obj_id=bakery.pk).order_by('pgh_id')

            self.assertEqual([event.pgh_label for event in events], ['insert', 'update'])

        with use_shard(node_name='other'):
            self.assertEqual(BakeryEvent.objects.count(), 0)


class PublicTrackingTestCase(ShardingTestCase):
    def test_a_public_event_is_recorded_and_found_by_its_natural_key(self):
        """
        Case: A CakeType is created, and the natural key of its event is passed to get_by_natural_key of the event
              manager.
        Expected: The insert event is recorded, and get_by_natural_key returns that event.
        """
        cake_type = CakeType.objects.create(name='layered')

        event = CakeTypeEvent.objects.get(pgh_obj_id=cake_type.pk)

        self.assertEqual(event.pgh_label, 'insert')
        self.assertEqual(CakeTypeEvent.objects.get_by_natural_key(*event.natural_key()), event)

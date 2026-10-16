from django.test import SimpleTestCase
from djanquiltdb.testing import OverrideMirroredRoutingMixin, ShardingTransactionTestCase
from djanquiltdb.utils import use_shard

from example.models import Bakery, BakeryEvent, Cookie, CookieEvent, Scone, SconeIcingEvent, SconePlainEvent


class EventModelDescriptorTestCase(SimpleTestCase):
    def test_a_model_tracked_once_returns_its_event_model_from_pgh_event_model(self):
        """
        Case: pgh_event_model is read on a tracked model that has exactly one event model.
        Expected: It returns CookieEvent.
        """
        self.assertIs(Cookie.pgh_event_model, CookieEvent)

    def test_a_model_tracked_twice_raises_on_pgh_event_model(self):
        """
        Case: pgh_event_model is read on a tracked model that has two event models.
        Expected: A ValueError that says the model has more than one event model, and that points at pgh_event_models.
                  Sharded model initialization must not read this attribute, because the exception would stop the
                  project from starting.
        """
        with self.assertRaisesMessage(ValueError, 'more than one') as raised:
            Scone.pgh_event_model

        self.assertIn('pgh_event_models', str(raised.exception))

    def test_every_event_model_of_scone_can_be_looked_up_by_tracker_label(self):
        """
        Case: The event models of Scone are looked up in pgh_event_models by the label of each tracker.
        Expected: Each label returns the model that was generated for it.
        """
        self.assertIs(Scone.pgh_event_models['scone.iced_insert'], SconeIcingEvent)
        self.assertIs(Scone.pgh_event_models['scone.iced_update'], SconeIcingEvent)
        self.assertIs(Scone.pgh_event_models['scone.plain_insert'], SconePlainEvent)

    def test_the_labels_that_a_call_site_names_are_the_keys_of_pgh_event_models(self):
        """
        Case: pgh_event_models is read on Cookie, whose trackers have labels of their own.
        Expected: The keys of the dictionary are those labels, and not the operations that the trackers are built from.
        """
        self.assertEqual(
            set(Cookie.pgh_event_models),
            {'cookie.baked', 'cookie.reglazed', 'cookie.eaten', 'cookie.inspected'},
        )


class MirroredEventsOnTheSecondNodeTestCase(OverrideMirroredRoutingMixin, ShardingTransactionTestCase):
    """
    OverrideMirroredRoutingMixin stands in for the replication that a real deployment configures: it sends a mirrored
    write to the active node, and not to the primary.
    """

    databases = '__all__'
    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def test_the_events_of_a_mirrored_model_are_recorded_only_on_the_node_that_wrote_them(self):
        """
        Case: A Bakery row is created on the second node, and OverrideMirroredRoutingMixin lets the write go there. The
              event is then looked up on both nodes.
        Expected: The event is recorded on the second node, and not on the primary, because DjanQuiltDB does not keep
                  the contents of mirrored tables in sync.
        """
        with use_shard(node_name='other'):
            bakery = Bakery.objects.create(name='corner')

            self.assertEqual(BakeryEvent.objects.get(pgh_obj_id=bakery.pk).pgh_label, 'insert')

        with use_shard(node_name='default'):
            self.assertEqual(BakeryEvent.objects.count(), 0)

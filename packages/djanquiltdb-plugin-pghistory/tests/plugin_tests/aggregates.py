import pghistory.models
from djanquiltdb import State
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard

from example.models import Bakery, Cake, CakeEvent, CakeType, Shard


class AggregateEventsTestCase(ShardingTransactionTestCase):
    """
    pghistory's Events model queries the event tables through a raw UNION that names no schema, so the search path
    decides which tables it can span.
    """

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

    def test_aggregating_a_sharded_event_model_reads_only_the_active_shard(self):
        """
        Case: A cake is created in a shard, and Events spans CakeEvent while that shard is active, so the table in the
              union is on the search path. Then the same aggregate query runs with the other shard active.
        Expected: Inside the shard of the cake, the insert event of the cake is returned. With the other shard active,
                  no events are returned. The union names the table without a schema, so only the search path decides
                  which shard's rows the query finds.
        """
        with use_shard(self.shard):
            cake = Cake.objects.create(name='carrot')

            events = pghistory.models.Events.objects.across(CakeEvent).tracks(cake)

            self.assertEqual([event.pgh_label for event in events], ['insert'])

        with use_shard(self.other_shard):
            self.assertEqual(pghistory.models.Events.objects.across(CakeEvent).count(), 0)

    def test_a_sharded_event_model_cannot_be_aggregated_outside_a_shard(self):
        """
        Case: Events spans CakeEvent with no shard active, so only the public schema is on the search path, while the
              table is in the schema of a shard.
        Expected: A database error, and not an empty result. pghistory's admin runs the same query without a shard.
        """
        with self.assertRaises(Exception) as raised:
            list(pghistory.models.Events.objects.across(CakeEvent))

        self.assertIn('example_cakeevent', str(raised.exception))

    def test_event_models_in_the_public_schema_can_be_aggregated_without_a_shard(self):
        """
        Case: Events spans only event models that live in the public schema: a mirrored one and a public one.
        Expected: Both are found with no shard active.
        """
        bakery = Bakery.objects.create(name='corner')
        CakeType.objects.create(name='layered')

        events = pghistory.models.Events.objects.across('example.BakeryEvent', 'example.CakeTypeEvent')

        self.assertEqual(events.count(), 2)
        self.assertIn('example.BakeryEvent', [event.pgh_model for event in events])
        self.assertEqual(events.references(bakery).count(), 1)

    def test_aggregating_sharded_and_public_event_models_works_only_inside_the_shard(self):
        """
        Case: One union spans a sharded event model and a public one, from inside the shard and from outside it.
        Expected: Inside the shard, both are found, because the search path of a shard includes the public schema.
                  Outside the shard, the query fails.
        """
        event_models_to_span = ['example.CakeEvent', 'example.CakeTypeEvent']

        CakeType.objects.create(name='layered')

        with use_shard(self.shard):
            Cake.objects.create(name='carrot')

            self.assertEqual(pghistory.models.Events.objects.across(*event_models_to_span).count(), 2)

        with self.assertRaises(Exception):
            list(pghistory.models.Events.objects.across(*event_models_to_span))

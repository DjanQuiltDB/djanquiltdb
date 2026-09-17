import pghistory
from djanquiltdb import State
from djanquiltdb.testing import ShardingTestCase, ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard

from djanquiltdb_plugin_pghistory import EventQuerySet
from example.models import Bakery, BakeryEvent, Cake, CakeEvent, Shard


class ShardedEventQuerySetTestCase(ShardingTransactionTestCase):
    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.shard = Shard.objects.create(
            alias='one', schema_name='one_schema', node_name='default', state=State.ACTIVE
        )

    def test_a_sharded_event_model_is_queryable_inside_a_shard(self):
        """
        Case: A sharded event model is filtered inside a shard, which runs its queryset through the sharded queryset
              wrapper.
        Expected: The queryset is the plugin's EventQuerySet, which accepts the hints that the sharded queryset wrapper
                  passes, and the query counts one event.
        """
        with use_shard(self.shard):
            cake = Cake.objects.create(name='carrot')

            queryset = CakeEvent.objects.filter(pgh_obj_id=cake.pk)

            self.assertIsInstance(queryset, EventQuerySet)
            self.assertEqual(queryset.count(), 1)


class ProxyFieldQueryTestCase(ShardingTestCase):
    def test_a_proxy_field_reads_from_the_context_of_the_event(self):
        """
        Case: The tracking of a mirrored model declares a proxy field for a context key, and a row is written inside a
              context that has that key.
        Expected: The proxy field of the event returns the context value. The migrated column itself stays empty, so any
                  compiler other than the CTE compiler of the plugin would return None here.
        """
        with pghistory.context(baked_by='ann'):
            bakery = Bakery.objects.create(name='corner')

        event = BakeryEvent.objects.get(pgh_obj_id=bakery.pk)

        self.assertEqual(event.baked_by, 'ann')

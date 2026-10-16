import pghistory
from djanquiltdb import State
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard

from example.models import Cake, CakeToppingsEvent, CakeType, Shard, Waffle, WaffleEvent


class ManyToManyThroughTrackingTestCase(ShardingTransactionTestCase):
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

    def test_adding_and_removing_a_topping_is_recorded_only_inside_the_shard(self):
        """
        Case: A topping is added to a sharded cake and then removed, inside a shard, while a second shard exists. Both
              change rows in the through table, and not in the table of either model.
        Expected: The shard has an event for each, in order. The other shard has no events of the through model.
        """
        cake_type = CakeType.objects.create(name='layered')

        with use_shard(self.shard):
            cake = Cake.objects.create(name='carrot')

            cake.toppings.add(cake_type)
            cake.toppings.remove(cake_type)

            events = CakeToppingsEvent.objects.order_by('pgh_id')

            self.assertEqual([event.pgh_label for event in events], ['topping.added', 'topping.removed'])

        with use_shard(self.other_shard):
            self.assertEqual(CakeToppingsEvent.objects.count(), 0)

    def test_the_recorded_event_names_both_sides_of_the_relation(self):
        """
        Case: A topping is added to a cake inside a shard, and the event is read.
        Expected: The event has the foreign keys to the cake and to the topping.
        """
        cake_type = CakeType.objects.create(name='layered')

        with use_shard(self.shard):
            cake = Cake.objects.create(name='carrot')
            cake.toppings.add(cake_type)

            event = CakeToppingsEvent.objects.get(pgh_label='topping.added')

            self.assertEqual(event.cake_id, cake.pk)
            self.assertEqual(event.caketype_id, cake_type.pk)


class ShardedProxyFieldTestCase(ShardingTransactionTestCase):
    """
    Unlike querysets.ProxyFieldQueryTestCase, these proxy-field queries go through the sharded queryset wrapper.
    """

    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.shard = Shard.objects.create(
            alias='one', schema_name='one_schema', node_name='default', state=State.ACTIVE
        )

    def test_a_proxy_field_resolves_on_a_sharded_event_model(self):
        """
        Case: The events of a sharded model declare a proxy field that reads the context metadata through the context
              foreign key, and a row is written inside a shard, in a context that has the baked_by key.
        Expected: The proxy field returns the value. The migrated column stays empty, so any compiler other than the CTE
                  compiler returns None here.
        """
        with use_shard(self.shard):
            with pghistory.context(baked_by='ann'):
                waffle = Waffle.objects.create(name='belgian')

            event = WaffleEvent.objects.get(pgh_obj_id=waffle.pk)

            self.assertEqual(event.baked_by, 'ann')

    def test_a_proxy_field_is_empty_when_the_context_does_not_have_its_key(self):
        """
        Case: A row is written inside a context that has other keys, so the metadata exists, but the key of the proxy
              field does not.
        Expected: None, and not an error, as in pghistory.
        """
        with use_shard(self.shard):
            with pghistory.context(flavor='plain'):
                waffle = Waffle.objects.create(name='liege')

            self.assertIsNone(WaffleEvent.objects.get(pgh_obj_id=waffle.pk).baked_by)

    def test_a_proxy_field_can_be_filtered_on_inside_a_shard(self):
        """
        Case: The proxy field is used in a filter, which is compiled against the CTE.
        Expected: Only the matching event is returned.
        """
        with use_shard(self.shard):
            with pghistory.context(baked_by='ann'):
                Waffle.objects.create(name='belgian')

            with pghistory.context(baked_by='bob'):
                Waffle.objects.create(name='liege')

            names = [event.name for event in WaffleEvent.objects.filter(baked_by='ann')]

            self.assertEqual(names, ['belgian'])

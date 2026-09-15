from djanquiltdb import State
from djanquiltdb.testing import ShardingTestCase, ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard

from example.models import Bakery, BakeryEvent, Cake, CakeEvent, CakeType, CakeTypeEvent, Shard


class ShardedRevertTestCase(ShardingTransactionTestCase):
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

    def test_reverting_a_sharded_event_updates_the_row_only_in_its_own_shard(self):
        """
        Case: A sharded row is created and renamed inside a shard, while a second shard exists. Its insert event, which
              has the original name, is reverted while the same shard is active. The row still exists, so the revert
              goes through update_or_create.
        Expected: The row has its original name again, and the shard has exactly one row, so the revert updated the row
                  and did not insert a second one. The other shard has no rows, because the revert writes only to the
                  shard that it runs in.
        """
        with use_shard(self.shard):
            cake = Cake.objects.create(name='carrot')
            cake.name = 'chocolate'
            cake.save()

            CakeEvent.objects.get(pgh_obj_id=cake.pk, pgh_label='insert').revert()

            self.assertEqual(Cake.objects.get(pk=cake.pk).name, 'carrot')
            self.assertEqual(Cake.objects.count(), 1)

        with use_shard(self.other_shard):
            self.assertEqual(Cake.objects.count(), 0)

    def test_a_revert_is_itself_tracked(self):
        """
        Case: A sharded row is renamed, and its insert event is reverted.
        Expected: The row has three events: insert, update, and a second update that the revert recorded.
        """
        with use_shard(self.shard):
            cake = Cake.objects.create(name='carrot')
            cake.name = 'chocolate'
            cake.save()

            CakeEvent.objects.get(pgh_obj_id=cake.pk, pgh_label='insert').revert()

            events = CakeEvent.objects.filter(pgh_obj_id=cake.pk).order_by('pgh_id')

            self.assertEqual([event.pgh_label for event in events], ['insert', 'update', 'update'])


class MirroredRevertTestCase(ShardingTestCase):
    def test_reverting_a_mirrored_event_restores_the_row(self):
        """
        Case: A mirrored row is renamed, and its insert event is reverted.
        Expected: The row has its original name again.
        """
        bakery = Bakery.objects.create(name='corner')
        bakery.name = 'main street'
        bakery.save()

        BakeryEvent.objects.get(pgh_obj_id=bakery.pk, pgh_label='insert').revert()

        self.assertEqual(Bakery.objects.get(pk=bakery.pk).name, 'corner')


class PublicRevertTestCase(ShardingTestCase):
    def test_reverting_a_public_event_restores_the_row(self):
        """
        Case: A public row is renamed, and its insert event is reverted.
        Expected: The row has its original name again.
        """
        cake_type = CakeType.objects.create(name='layered')
        cake_type.name = 'rolled'
        cake_type.save()

        CakeTypeEvent.objects.get(pgh_obj_id=cake_type.pk, pgh_label='insert').revert()

        self.assertEqual(CakeType.objects.get(pk=cake_type.pk).name, 'layered')

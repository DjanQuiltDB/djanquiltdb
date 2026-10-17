import pghistory
from django.core.management import call_command
from django.test import SimpleTestCase
from djanquiltdb import ShardingMode, State
from djanquiltdb.plugins import plugin_hooks
from djanquiltdb.testing import OverrideMirroredRoutingMixin, ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard
from pghistory.models import Context
from psycopg.errors import UniqueViolation

from djanquiltdb_plugin_pghistory.data_moves import (
    delete_events_of_purged_rows,
    delete_moved_events,
    keep_only_the_events_of_collected_rows,
)
from example.models import (
    Cake,
    CakeEvent,
    CakeToppingsEvent,
    CakeType,
    Cookie,
    CookieEvent,
    Croissant,
    CroissantEvent,
    Shard,
    Slice,
    SliceEvent,
    Waffle,
    WaffleEvent,
)
from plugin_tests.context_placement import run_only_when_the_context_table_is


def event_rows(event_model, leave_out=()):
    """
    Returns every row of an event model in the active shard, with the value of each of its columns except the ones
    named in leave_out.
    """
    columns = [field.attname for field in event_model._meta.concrete_fields if field.attname not in leave_out]

    return list(event_model.objects.order_by('pgh_id').values_list(*columns))


class MoveShardToNodeTestCase(OverrideMirroredRoutingMixin, ShardingTransactionTestCase):
    """
    move_shard_to_node copies every table of the shard, the event tables included, into a new schema on the other node.
    OverrideMirroredRoutingMixin stands in for the replication that keeps the mirrored Shard table the same on both
    nodes.
    """

    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def setUp(self):
        super().setUp()
        create_template_schema('default')
        create_template_schema('other')

        with use_shard(node_name='default', schema_name='public', override_class_method_use_shard=True):
            self.shard = Shard.objects.create(
                alias='mover', node_name='default', schema_name='mover_schema', state=State.ACTIVE
            )
            cake_type = CakeType.objects.create(name='sponge')

        with use_shard(node_name='other', schema_name='public', override_class_method_use_shard=True):
            Shard.objects.create(
                id=self.shard.id, alias='mover', node_name='default', schema_name='mover_schema', state=State.ACTIVE
            )
            # CakeType does not allow copying, so the other node needs the row with the same natural key.
            self.cake_type_on_the_other_node = CakeType.objects.create(id=cake_type.id + 10, name='sponge')

        with use_shard(self.shard), pghistory.context(baked_by='ann'):
            cake = Cake.objects.create(name='layered')
            cake.name = 'tiered'
            cake.save()
            cake.toppings.add(cake_type)

            Croissant.objects.create(name='plain')

        with use_shard(self.shard):
            self.event_rows_by_model = {
                event_model: event_rows(event_model, leave_out=['caketype_id'])
                for event_model in [CakeEvent, CakeToppingsEvent, CroissantEvent]
            }

    def move_the_shard(self):
        call_command(
            'move_shard_to_node',
            '--source-shard-alias',
            self.shard.alias,
            '--target-node-alias',
            'other',
            '--no-input',
            '--quiet',
        )

    def test_the_moved_shard_has_the_same_events_and_no_events_for_the_move(self):
        """
        Case: A shard with the events of Cake, of its tracked through model and of Croissant, a model that is tracked at
              statement level, moves to the other node. The move copies the rows of the tracked models, and retargets
              the through rows to the CakeType of the other node.
        Expected: The moved shard has exactly the events of the source shard, with the same pgh_id and context, and no
                  event for the rows that the move wrote. The snapshot of the CakeType foreign key in the event of the
                  through model is retargeted like the through row itself, to the CakeType of the other node.
        """
        self.move_the_shard()

        with use_shard(node_name='other', schema_name='mover_schema', active_only_schemas=False):
            for event_model, rows in self.event_rows_by_model.items():
                with self.subTest(event_model=event_model.__name__):
                    self.assertEqual(event_rows(event_model, leave_out=['caketype_id']), rows)

            self.assertEqual(CakeToppingsEvent.objects.get().caketype_id, self.cake_type_on_the_other_node.id)

    def test_a_snapshot_of_a_public_row_that_was_deleted_keeps_its_value(self):
        """
        Case: A CakeType is added to the toppings of the Cake and removed again, so the events of the through model have
              a snapshot of the foreign key to it. The CakeType is then deleted from the source node, and the shard
              moves to the other node.
        Expected: The move succeeds. The snapshots of the deleted CakeType keep its id, because neither node has a row to
                  retarget them to, and the snapshot of the CakeType that the other node has is retargeted to it.
        """
        with use_shard(node_name='default', schema_name='public', override_class_method_use_shard=True):
            lemon = CakeType.objects.create(name='lemon')

        with use_shard(self.shard):
            cake = Cake.objects.get()
            cake.toppings.add(lemon)
            cake.toppings.remove(lemon)

        # Django's collector would look for through rows in the public schema, so the row is deleted with SQL.
        with use_shard(node_name='default', schema_name='public', override_class_method_use_shard=True) as env:
            env.connection.cursor().execute(
                f'DELETE FROM "{CakeType._meta.db_table}" WHERE "id" = %s',
                [lemon.pk],
            )

        self.move_the_shard()

        with use_shard(node_name='other', schema_name='mover_schema', active_only_schemas=False):
            self.assertCountEqual(
                CakeToppingsEvent.objects.values_list('pgh_label', 'caketype_id'),
                [
                    ('topping.added', self.cake_type_on_the_other_node.id),
                    ('topping.added', lemon.pk),
                    ('topping.removed', lemon.pk),
                ],
            )

    def test_the_moved_shard_records_new_events(self):
        """
        Case: The shard moves to the other node, and a Cake is then renamed in the moved shard.
        Expected: The rename is recorded as a new event, after the copied ones, so the triggers fire again after the move.
        """
        self.move_the_shard()

        with use_shard(node_name='other', schema_name='mover_schema', active_only_schemas=False):
            cake = Cake.objects.get()
            cake.name = 'stacked'
            cake.save()

            self.assertEqual(
                [event.pgh_label for event in CakeEvent.objects.order_by('pgh_id')], ['insert', 'update', 'update']
            )


class MoveDataToShardTestCase(ShardingTransactionTestCase):
    """
    move_data_to_shard copies the rows of one root object to another shard on the same node, and deletes them from the
    source shard. Its collector finds the events of each row through their pgh_obj foreign key.
    """

    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.source_shard = Shard.objects.create(
            alias='source', schema_name='source_schema', node_name='default', state=State.ACTIVE
        )
        self.target_shard = Shard.objects.create(
            alias='target', schema_name='target_schema', node_name='default', state=State.ACTIVE
        )

    def move_data(self, model_name, root_object, *options):
        call_command(
            'move_data_to_shard',
            '--source-shard-alias',
            self.source_shard.alias,
            '--target-shard-alias',
            self.target_shard.alias,
            '--model-name',
            model_name,
            '--root-object-id',
            str(root_object.pk),
            '--no-input',
            '--quiet',
            *options,
        )

    def events_in(self, shard, event_model):
        with use_shard(shard, active_only_schemas=False, lock=False):
            return event_rows(event_model)

    def test_the_history_of_a_moved_row_moves_along(self):
        """
        Case: A Cookie, whose trackers record inserts, updates and deletes, is baked and reglazed inside a context, and
              then moved to another shard.
        Expected: The target shard has exactly the events of the source shard, with the same pgh_id and context, and no
                  event for the insert of the move. The source shard has no events left, and no delete event for the
                  delete of the move.
        """
        with use_shard(self.source_shard), pghistory.context(baked_by='ann'):
            cookie = Cookie.objects.create(name='shortbread', glaze='none')
            cookie.glaze = 'chocolate'
            cookie.save()

        rows = self.events_in(self.source_shard, CookieEvent)

        self.move_data('example.cookie', cookie)

        self.assertEqual(len(rows), 2)
        self.assertEqual(self.events_in(self.target_shard, CookieEvent), rows)
        self.assertEqual(self.events_in(self.source_shard, CookieEvent), [])

    def test_a_move_that_fails_raises_the_error_of_the_database(self):
        """
        Case: A Cookie is moved to a shard that already has a Cookie with the same id, so the copy fails inside the
              transaction of the move, while the command ignores the triggers.
        Expected: The command raises the unique violation of the copy, and not an error about the aborted transaction
                  from the cleanup of pgtrigger.ignore(). COPY does not pass Django's error wrapping, so it is psycopg's
                  own error.
        """
        with use_shard(self.source_shard):
            cookie = Cookie.objects.create(name='shortbread', glaze='none')

        with use_shard(self.target_shard):
            Cookie.objects.create(id=cookie.pk, name='macaron', glaze='none')

        with self.assertRaises(UniqueViolation):
            self.move_data('example.cookie', cookie)

    def test_the_history_of_a_row_tracked_at_statement_level_moves_along(self):
        """
        Case: A Croissant, which is tracked at statement level, is created and renamed, and then moved to another shard.
        Expected: The target shard has exactly the events of the source shard, and the source shard has none left.
        """
        with use_shard(self.source_shard):
            croissant = Croissant.objects.create(name='plain')
            croissant.name = 'almond'
            croissant.save()

        rows = self.events_in(self.source_shard, CroissantEvent)

        self.move_data('example.croissant', croissant)

        self.assertEqual(len(rows), 2)
        self.assertEqual(self.events_in(self.target_shard, CroissantEvent), rows)
        self.assertEqual(self.events_in(self.source_shard, CroissantEvent), [])

    def test_moving_without_deleting_keeps_the_history_in_the_source_shard(self):
        """
        Case: A Cookie is moved to another shard with --no-delete.
        Expected: Both shards have the Cookie and exactly the events that the source shard had before the move.
        """
        with use_shard(self.source_shard):
            cookie = Cookie.objects.create(name='shortbread', glaze='none')

        rows = self.events_in(self.source_shard, CookieEvent)

        self.move_data('example.cookie', cookie, '--no-delete')

        self.assertEqual(self.events_in(self.target_shard, CookieEvent), rows)
        self.assertEqual(self.events_in(self.source_shard, CookieEvent), rows)

        for shard in (self.source_shard, self.target_shard):
            with use_shard(shard, active_only_schemas=False, lock=False):
                self.assertTrue(Cookie.objects.filter(pk=cookie.pk).exists())

    def test_the_events_of_a_tracked_through_model_move_along(self):
        """
        Case: A Cake gets a topping, so its tracked through model records an event, and the Cake is moved to another
              shard. The event model of the through model has no foreign key to its row, only a snapshot of its foreign
              key to the Cake.
        Expected: The collector finds the event through that snapshot, so the target shard has the event and the source
                  shard has none left.
        """
        cake_type = CakeType.objects.create(name='sponge')

        with use_shard(self.source_shard):
            cake = Cake.objects.create(name='layered')
            cake.toppings.add(cake_type)

        rows = self.events_in(self.source_shard, CakeToppingsEvent)

        self.move_data('example.cake', cake)

        self.assertEqual(len(rows), 1)
        self.assertEqual(self.events_in(self.target_shard, CakeToppingsEvent), rows)
        self.assertEqual(self.events_in(self.source_shard, CakeToppingsEvent), [])

    def test_the_events_of_a_row_that_was_deleted_before_the_move_stay(self):
        """
        Case: A Cake has two slices, and one of them is deleted. The Cake is then moved to another shard. The events of
              both slices have a snapshot of their foreign key to the Cake, through which the collector finds them.
        Expected: The events of the slice that moves move along. The event of the deleted slice stays in the source
                  shard, because its row does not move.
        """
        with use_shard(self.source_shard):
            cake = Cake.objects.create(name='layered')
            kept_slice = Slice.objects.create(cake=cake, flavour='lemon')
            deleted_slice = Slice.objects.create(cake=cake, flavour='cherry')
            deleted_slice_pk = deleted_slice.pk
            deleted_slice.delete()

        self.move_data('example.cake', cake)

        self.assertEqual(self.event_owners_in(self.target_shard), [kept_slice.pk])
        self.assertEqual(self.event_owners_in(self.source_shard), [deleted_slice_pk])

    def test_the_events_of_a_row_that_belongs_to_another_root_object_stay(self):
        """
        Case: A slice is cut from one Cake and then moved to another Cake, so its first event has a snapshot of a
              foreign key to the first Cake. The first Cake is moved to another shard.
        Expected: No event of the slice moves, because the slice stays in the source shard with the second Cake, and its
                  history stays with it.
        """
        with use_shard(self.source_shard):
            moved_cake = Cake.objects.create(name='layered')
            other_cake = Cake.objects.create(name='tiered')
            cake_slice = Slice.objects.create(cake=moved_cake, flavour='lemon')
            cake_slice.cake = other_cake
            cake_slice.save()

        self.move_data('example.cake', moved_cake)

        self.assertEqual(self.event_owners_in(self.target_shard), [])
        self.assertEqual(self.event_owners_in(self.source_shard), [cake_slice.pk, cake_slice.pk])

    def event_owners_in(self, shard):
        """
        Returns the primary key of the slice of each SliceEvent in the shard.
        """
        with use_shard(shard, active_only_schemas=False, lock=False):
            return list(SliceEvent.objects.order_by('pgh_id').values_list('pgh_obj_id', flat=True))

    @run_only_when_the_context_table_is(ShardingMode.PUBLIC, ShardingMode.MIRRORED)
    def test_the_moved_events_keep_their_context_outside_the_shards(self):
        """
        Case: A Waffle, whose events have a foreign key to the Context table, is created inside a context and moved to
              another shard. The Context table is outside the shards, so the move does not touch it.
        Expected: The moved event still points at the context, which has the metadata that was set.
        """
        with use_shard(self.source_shard), pghistory.context(baked_by='ann'):
            waffle = Waffle.objects.create(name='belgian')

        self.move_data('example.waffle', waffle)

        with use_shard(self.target_shard, active_only_schemas=False, lock=False):
            self.assertEqual(WaffleEvent.objects.get(pgh_obj_id=waffle.pk).pgh_context.metadata, {'baked_by': 'ann'})


class PurgeShardDataTestCase(ShardingTransactionTestCase):
    """
    purge_shard_data deletes the rows of one root object from a shard, usually from the source shard of a
    move_data_to_shard with --no-delete.
    """

    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.source_shard = Shard.objects.create(
            alias='source', schema_name='source_schema', node_name='default', state=State.ACTIVE
        )
        self.target_shard = Shard.objects.create(
            alias='target', schema_name='target_schema', node_name='default', state=State.ACTIVE
        )

    def move_without_deleting(self, model_name, root_object):
        call_command(
            'move_data_to_shard',
            '--source-shard-alias',
            self.source_shard.alias,
            '--target-shard-alias',
            self.target_shard.alias,
            '--model-name',
            model_name,
            '--root-object-id',
            str(root_object.pk),
            '--no-input',
            '--quiet',
            '--no-delete',
        )

    def purge(self, model_name, root_object, simple_collector):
        call_command(
            'purge_shard_data',
            shard_alias=self.source_shard.alias,
            model_name=model_name,
            object_value=str(root_object.pk),
            simple_collector=simple_collector,
            interactive=False,
            verbosity=0,
        )

    def events_in(self, shard, event_model):
        with use_shard(shard, active_only_schemas=False, lock=False):
            return event_rows(event_model)

    def test_purging_a_moved_row_deletes_its_events_and_records_none(self):
        """
        Case: A Cookie, whose trackers record inserts, updates and deletes, is moved with --no-delete, and then purged
              from the source shard, once with Django's collector and once with the simple collector.
        Expected: The source shard has no events of the Cookie left, and no delete event for the purge. The target shard
                  keeps the events that moved.
        """
        for simple_collector in (False, True):
            with self.subTest(simple_collector=simple_collector):
                with use_shard(self.source_shard):
                    cookie = Cookie.objects.create(name=f'shortbread {simple_collector}', glaze='none')
                    cookie.glaze = 'chocolate'
                    cookie.save()

                self.move_without_deleting('example.cookie', cookie)
                rows_in_the_target = self.events_in(self.target_shard, CookieEvent)

                self.purge('example.cookie', cookie, simple_collector)

                with use_shard(self.source_shard, active_only_schemas=False, lock=False):
                    self.assertFalse(CookieEvent.objects.filter(pgh_obj_id=cookie.pk).exists())

                self.assertEqual(self.events_in(self.target_shard, CookieEvent), rows_in_the_target)

    def test_purging_a_row_deletes_the_events_of_its_tracked_through_model(self):
        """
        Case: A Cake with a topping is moved with --no-delete, and then purged from the source shard, once with Django's
              collector and once with the simple collector. The event model of the through model has no foreign key to
              its row, only a snapshot of its foreign key to the Cake.
        Expected: The source shard has no event of the through model of the Cake left, and no event for the removal of
                  the topping by the purge.
        """
        cake_type = CakeType.objects.create(name='sponge')

        for simple_collector in (False, True):
            with self.subTest(simple_collector=simple_collector):
                with use_shard(self.source_shard):
                    cake = Cake.objects.create(name=f'layered {simple_collector}')
                    cake.toppings.add(cake_type)

                self.move_without_deleting('example.cake', cake)
                self.purge('example.cake', cake, simple_collector)

                with use_shard(self.source_shard, active_only_schemas=False, lock=False):
                    self.assertFalse(CakeToppingsEvent.objects.filter(cake_id=cake.pk).exists())

    def test_purging_a_row_keeps_the_events_of_a_row_that_was_deleted_before(self):
        """
        Case: A Cake has a slice that is deleted. The Cake is purged from the shard, once with Django's collector and once
              with the simple collector, which finds the event of the slice through its snapshot of the foreign key to
              the Cake.
        Expected: The event of the deleted slice stays, as it does when the Cake is moved.
        """
        for simple_collector in (False, True):
            with self.subTest(simple_collector=simple_collector):
                with use_shard(self.source_shard):
                    cake = Cake.objects.create(name=f'layered {simple_collector}')
                    deleted_slice = Slice.objects.create(cake=cake, flavour='cherry')
                    deleted_slice_pk = deleted_slice.pk
                    deleted_slice.delete()

                self.purge('example.cake', cake, simple_collector)

                with use_shard(self.source_shard, active_only_schemas=False, lock=False):
                    self.assertTrue(SliceEvent.objects.filter(pgh_obj_id=deleted_slice_pk).exists())


class KeepOnlyTheEventsOfCollectedRowsTestCase(SimpleTestCase):
    def test_an_event_model_whose_events_are_all_left_out_stays_in_the_collected_data(self):
        """
        Case: The collected data has an event of a slice that was not collected, and no other event of that model.
        Expected: The event is left out, and the event model stays in the data with no events. The collector orders its
                  delete by the dependencies that it recorded between the collected models, and a model that is missing
                  from the data stops it from ordering.
        """
        data = {Slice: set(), SliceEvent: {SliceEvent(pgh_id=1, pgh_obj_id=5)}}

        keep_only_the_events_of_collected_rows(command=None, data=data)

        self.assertEqual(data, {Slice: set(), SliceEvent: set()})


class DataMoveHooksTestCase(SimpleTestCase):
    def test_the_plugin_serves_its_hooks_in_the_sections_of_the_commands(self):
        """
        Case: DjanQuiltDB looks up the hooks of move_data_to_shard and purge_shard_data.
        Expected: It finds the plugin's own functions.
        """
        self.assertIn(keep_only_the_events_of_collected_rows, plugin_hooks('move_data_to_shard.post_collect'))
        self.assertIn(delete_moved_events, plugin_hooks('move_data_to_shard.post_delete'))
        self.assertIn(keep_only_the_events_of_collected_rows, plugin_hooks('purge_shard_data.post_collect'))
        self.assertIn(delete_events_of_purged_rows, plugin_hooks('purge_shard_data.post_delete'))


@run_only_when_the_context_table_is(ShardingMode.PUBLIC)
class MoveShardToNodeWithAPublicContextTableTestCase(OverrideMirroredRoutingMixin, ShardingTransactionTestCase):
    """
    The events of Waffle have a foreign key to the public Context table, so moving the shard retargets them by the
    natural key of Context, its UUID.
    """

    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def test_the_moved_events_keep_their_context(self):
        """
        Case: A Waffle is created inside a context, in a shard that then moves to the other node, whose public Context
              table does not have that context yet.
        Expected: The context is copied to the other node with the same id, metadata and timestamps, and the moved event
                  still points at it.
        """
        create_template_schema('default')
        create_template_schema('other')

        with use_shard(node_name='default', schema_name='public', override_class_method_use_shard=True):
            shard = Shard.objects.create(
                alias='mover', node_name='default', schema_name='mover_schema', state=State.ACTIVE
            )

        with use_shard(node_name='other', schema_name='public', override_class_method_use_shard=True):
            Shard.objects.create(
                id=shard.id, alias='mover', node_name='default', schema_name='mover_schema', state=State.ACTIVE
            )

        with use_shard(shard), pghistory.context(baked_by='ann'):
            waffle = Waffle.objects.create(name='belgian')

        with use_shard(shard):
            context_id = WaffleEvent.objects.get(pgh_obj_id=waffle.pk).pgh_context_id

        with use_shard(node_name='default', schema_name='public'):
            timestamps = Context.objects.filter(id=context_id).values_list('created_at', 'updated_at').get()

        call_command(
            'move_shard_to_node',
            '--source-shard-alias',
            'mover',
            '--target-node-alias',
            'other',
            '--no-input',
            '--quiet',
        )

        with use_shard(node_name='other', schema_name='mover_schema', active_only_schemas=False):
            event = WaffleEvent.objects.get(pgh_obj_id=waffle.pk)

            self.assertEqual(event.pgh_context_id, context_id)
            self.assertEqual(event.pgh_context.metadata, {'baked_by': 'ann'})

        with use_shard(node_name='other', schema_name='public'):
            self.assertEqual(list(Context.objects.values_list('id', flat=True)), [context_id])
            self.assertEqual(Context.objects.values_list('created_at', 'updated_at').get(), timestamps)

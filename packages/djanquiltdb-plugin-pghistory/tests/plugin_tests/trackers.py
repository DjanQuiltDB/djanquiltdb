import pghistory
from djanquiltdb import State
from djanquiltdb.options import ShardOptions
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard

from example.models import Cookie, CookieEvent, Shard, Waffle, WaffleEvent


class ManualEventWithAContextForeignKeyTestCase(ShardingTransactionTestCase):
    """
    WaffleEvent has a foreign key to the Context table, so pghistory.create_event() writes a manual event of Waffle
    through its own SQL compiler, on the connection that it is given, and not through the ORM.
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

    def test_a_manual_event_is_stored_in_the_active_shard_with_its_context(self):
        """
        Case: Inside a shard and inside a context, pghistory.create_event writes a manual event of a Waffle, without a
              using argument.
        Expected: The event is stored in the active shard, and has the context. The other shard has no manual event.
        """
        with use_shard(self.shard):
            waffle = Waffle.objects.create(name='belgian')

            with pghistory.context(baked_by='ann'):
                pghistory.create_event(waffle, label='waffle.inspected')

            event = WaffleEvent.objects.get(pgh_label='waffle.inspected')

            self.assertEqual(event.pgh_obj_id, waffle.pk)
            self.assertEqual(event.pgh_context.metadata, {'baked_by': 'ann'})

        with use_shard(self.other_shard):
            self.assertFalse(WaffleEvent.objects.filter(pgh_label='waffle.inspected').exists())

    def test_a_manual_event_is_stored_where_using_points(self):
        """
        Case: Inside one shard, pghistory.create_event writes a manual event of a Waffle with a using argument that
              points at the other shard.
        Expected: The event is stored in the other shard, and not in the active one.
        """
        with use_shard(self.shard):
            waffle = Waffle.objects.create(name='belgian')

            pghistory.create_event(
                waffle, label='waffle.inspected', using=ShardOptions.from_shard(shard=self.other_shard)
            )

            self.assertFalse(WaffleEvent.objects.filter(pgh_label='waffle.inspected').exists())

        with use_shard(self.other_shard):
            self.assertTrue(WaffleEvent.objects.filter(pgh_label='waffle.inspected').exists())


class CookieTrackerEventsInsideAShardTestCase(ShardingTransactionTestCase):
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

    def test_an_update_event_with_a_condition_fires_only_when_the_glaze_changes(self):
        """
        Case: The update tracker has a condition on the glaze field. The cookie is renamed, and then gets a new glaze.
        Expected: Only the new glaze records an event, because the rename does not change the glaze field.
        """
        with use_shard(self.shard):
            cookie = Cookie.objects.create(name='ginger', glaze='none')

            cookie.name = 'gingerbread'
            cookie.save()

            self.assertFalse(CookieEvent.objects.filter(pgh_label='cookie.reglazed').exists())

            cookie.glaze = 'lemon'
            cookie.save()

            event = CookieEvent.objects.get(pgh_label='cookie.reglazed')

            self.assertEqual(event.glaze, 'lemon')

    def test_a_delete_event_keeps_the_values_of_the_deleted_row_only_in_its_shard(self):
        """
        Case: A tracked sharded row is deleted in one shard while a second shard exists.
        Expected: The shard has the delete event with the deleted values, although the row itself has been deleted. The
                  other shard has no events.
        """
        with use_shard(self.shard):
            cookie = Cookie.objects.create(name='ginger', glaze='lemon')
            cookie_pk = cookie.pk
            cookie.delete()

            self.assertFalse(Cookie.objects.filter(pk=cookie_pk).exists())

            event = CookieEvent.objects.get(pgh_label='cookie.eaten')

            self.assertEqual(event.name, 'ginger')
            self.assertEqual(event.glaze, 'lemon')
            self.assertEqual(event.pgh_obj_id, cookie_pk)

        with use_shard(self.other_shard):
            self.assertEqual(CookieEvent.objects.count(), 0)

    def test_a_manual_event_is_stored_only_in_the_active_shard(self):
        """
        Case: pghistory.create_event writes a manual event in one shard while a second shard exists. create_event writes
              through the ORM and not through a trigger, so the router decides where the event is stored.
        Expected: The event is stored in the active shard, with the label and the values of the row. The other shard has
                  no manual event.
        """
        with use_shard(self.shard):
            cookie = Cookie.objects.create(name='ginger', glaze='lemon')

            pghistory.create_event(cookie, label='cookie.inspected')

            event = CookieEvent.objects.get(pgh_label='cookie.inspected')

            self.assertEqual(event.name, 'ginger')
            self.assertEqual(event.pgh_obj_id, cookie.pk)

        with use_shard(self.other_shard):
            self.assertEqual(CookieEvent.objects.filter(pgh_label='cookie.inspected').count(), 0)

    def test_create_event_raises_for_a_label_that_no_tracker_registers(self):
        """
        Case: pghistory.create_event gets a label that none of the model's trackers have.
        Expected: ValueError is raised, as in pghistory.
        """
        with use_shard(self.shard):
            cookie = Cookie.objects.create(name='ginger', glaze='lemon')

            with self.assertRaisesMessage(ValueError, 'not a registered tracker'):
                pghistory.create_event(cookie, label='cookie.dunked')

    def test_the_named_trackers_replace_the_default_trackers(self):
        """
        Case: A Cookie, whose call site names its own trackers, is created and gets a new glaze. The labels of the
              trackers contain a dot.
        Expected: The events have only the labels of the named trackers, exactly as given, and no event has the default
                  'insert' or 'update' label, as in pghistory.
        """
        with use_shard(self.shard):
            cookie = Cookie.objects.create(name='ginger', glaze='none')
            cookie.glaze = 'lemon'
            cookie.save()

            labels = {event.pgh_label for event in CookieEvent.objects.all()}

            self.assertEqual(labels, {'cookie.baked', 'cookie.reglazed'})

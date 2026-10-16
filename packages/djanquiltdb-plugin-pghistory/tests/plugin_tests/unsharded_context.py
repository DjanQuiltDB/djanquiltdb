import pghistory
from django.db import connections
from djanquiltdb import ShardingMode, State
from djanquiltdb.testing import OverrideMirroredRoutingMixin, ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard
from pghistory.models import Context

from example.models import Croissant, CroissantEvent, Shard, Waffle, WaffleEvent
from plugin_tests.context_placement import run_only_when_the_context_table_is

# unsharded_context_example is only installed in the runs in which the Context table is public or mirrored, so the tests
# import its models inside each test, and not at the top of the module.
AVAILABLE_APPS = [
    'djanquiltdb',
    'example',
    'unsharded_context_example',
    'pghistory',
    'django.contrib.auth',
    'django.contrib.contenttypes',
]


def schemas_that_have_the_context_table_and_function(node_name):
    with connections[node_name].cursor() as cursor:
        cursor.execute("SELECT table_schema FROM information_schema.tables WHERE table_name = 'pghistory_context'")
        table_schemas = {row[0] for row in cursor.fetchall()}

        cursor.execute(
            'SELECT namespace.nspname FROM pg_proc AS function '
            'JOIN pg_namespace AS namespace ON namespace.oid = function.pronamespace '
            "WHERE function.proname = '_pgh_attach_context'"
        )
        function_schemas = {row[0] for row in cursor.fetchall()}

    return table_schemas, function_schemas


@run_only_when_the_context_table_is(ShardingMode.PUBLIC, ShardingMode.MIRRORED)
class UnshardedContextInsideAShardTestCase(ShardingTransactionTestCase):
    """
    When pghistory's Context table is public or mirrored, the table and _pgh_attach_context() are in the public schema,
    and the triggers in a shard reach both through the search path of the shard, which ends with the public schema.
    """

    available_apps = AVAILABLE_APPS

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.shard = Shard.objects.create(
            alias='one', schema_name='one_schema', node_name='default', state=State.ACTIVE
        )
        self.other_shard = Shard.objects.create(
            alias='two', schema_name='two_schema', node_name='default', state=State.ACTIVE
        )

    def test_the_context_table_and_function_are_only_in_the_public_schema(self):
        """
        Case: The schemas that have the pghistory_context table and the _pgh_attach_context() function are looked up,
              while the template schema and two shards exist.
        Expected: Only the public schema has them.
        """
        self.assertEqual(schemas_that_have_the_context_table_and_function('default'), ({'public'}, {'public'}))

    def test_a_statement_level_trigger_in_a_shard_writes_its_context_row_outside_the_shard(self):
        """
        Case: A Croissant is created inside a context, inside a shard. Its statement-level trigger calls
              _pgh_attach_context() without a schema, which finds the function in the public schema.
        Expected: The event has the denormalized context, and the Context table in the public schema has a row with the
                  metadata.
        """
        with use_shard(self.shard):
            with pghistory.context(baked_by='ann'):
                croissant = Croissant.objects.create(name='plain')

            self.assertEqual(CroissantEvent.objects.get(pgh_obj_id=croissant.pk).pgh_context, {'baked_by': 'ann'})

        self.assertEqual(Context.objects.get().metadata, {'baked_by': 'ann'})

    def test_a_sharded_event_has_a_foreign_key_to_a_context_row_outside_the_shard(self):
        """
        Case: A Waffle, whose events have a foreign key to the Context table, is created inside a context, inside a
              shard.
        Expected: The event has the context with the metadata, and the row of that context is in the Context table in
                  the public schema.
        """
        with use_shard(self.shard):
            with pghistory.context(baked_by='ann'):
                waffle = Waffle.objects.create(name='belgian')

            event = WaffleEvent.objects.get(pgh_obj_id=waffle.pk)

            self.assertEqual(event.pgh_context.metadata, {'baked_by': 'ann'})

        self.assertEqual(Context.objects.get().id, event.pgh_context_id)

    def test_one_context_spanning_two_shards_is_recorded_in_a_single_row(self):
        """
        Case: A single pghistory.context() block writes tracked rows in two different shards.
        Expected: The Context table has one row, and the events in both shards have a foreign key to it.
        """
        with pghistory.context(baked_by='ann'):
            with use_shard(self.shard):
                Waffle.objects.create(name='belgian')

            with use_shard(self.other_shard):
                Waffle.objects.create(name='liege')

        context = Context.objects.get()

        for shard in (self.shard, self.other_shard):
            with use_shard(shard):
                self.assertEqual(WaffleEvent.objects.get().pgh_context_id, context.id)


@run_only_when_the_context_table_is(ShardingMode.PUBLIC, ShardingMode.MIRRORED)
class UnshardedContextOutsideTheShardsTestCase(ShardingTransactionTestCase):
    """
    The triggers of public and mirrored models run outside the shards, so they need a Context table and a
    _pgh_attach_context() function that are public or mirrored.
    """

    available_apps = AVAILABLE_APPS

    def test_a_public_event_has_a_foreign_key_to_the_context_row(self):
        """
        Case: A Recipe, a public model whose events have a foreign key to the Context table, is created inside a
              context.
        Expected: The event has the context with the metadata.
        """
        from unsharded_context_example.models import Recipe, RecipeEvent

        with pghistory.context(baked_by='ann'):
            recipe = Recipe.objects.create(name='sourdough')

        self.assertEqual(RecipeEvent.objects.get(pgh_obj_id=recipe.pk).pgh_context.metadata, {'baked_by': 'ann'})

    def test_a_mirrored_event_has_a_foreign_key_to_the_context_row(self):
        """
        Case: A Kitchen, a mirrored model whose events have a foreign key to the Context table, is created inside a
              context on the primary node.
        Expected: The event has the context with the metadata.
        """
        from unsharded_context_example.models import Kitchen, KitchenEvent

        with pghistory.context(baked_by='ann'):
            kitchen = Kitchen.objects.create(name='main')

        self.assertEqual(KitchenEvent.objects.get(pgh_obj_id=kitchen.pk).pgh_context.metadata, {'baked_by': 'ann'})

    def test_the_statement_level_trigger_of_a_public_model_records_the_context(self):
        """
        Case: A Grain, a public model that is tracked with level=pghistory.Statement and a denormalized context, is
              created and renamed inside a context, with no shard active.
        Expected: The insert and update events have the context, and the trigger's call to _pgh_attach_context() wrote
                  a Context row with the metadata.
        """
        from unsharded_context_example.models import Grain, GrainEvent

        with pghistory.context(baked_by='ann'):
            grain = Grain.objects.create(name='rye')
            grain.name = 'spelt'
            grain.save()

        events = GrainEvent.objects.filter(pgh_obj_id=grain.pk).order_by('pgh_id')

        self.assertEqual(
            [(event.pgh_label, event.pgh_context) for event in events],
            [('insert', {'baked_by': 'ann'}), ('update', {'baked_by': 'ann'})],
        )
        self.assertEqual(Context.objects.get().metadata, {'baked_by': 'ann'})

    def test_the_statement_level_trigger_of_a_public_model_written_from_inside_a_shard_records_the_context(self):
        """
        Case: A Grain is created inside a context, while a shard is active.
        Expected: The event has the context.
        """
        from unsharded_context_example.models import Grain, GrainEvent

        create_template_schema()
        shard = Shard.objects.create(alias='one', schema_name='one_schema', node_name='default', state=State.ACTIVE)

        with pghistory.context(baked_by='ann'):
            with use_shard(shard):
                grain = Grain.objects.create(name='rye')

        self.assertEqual(GrainEvent.objects.get(pgh_obj_id=grain.pk).pgh_context, {'baked_by': 'ann'})

    def test_the_statement_level_trigger_of_a_mirrored_model_records_the_context(self):
        """
        Case: A Proofer, a mirrored model that is tracked with level=pghistory.Statement and a denormalized context, is
              created inside a context on the primary node.
        Expected: The event has the context.
        """
        from unsharded_context_example.models import Proofer, ProoferEvent

        with pghistory.context(baked_by='ann'):
            proofer = Proofer.objects.create(name='cabinet')

        self.assertEqual(ProoferEvent.objects.get(pgh_obj_id=proofer.pk).pgh_context, {'baked_by': 'ann'})


@run_only_when_the_context_table_is(ShardingMode.PUBLIC, ShardingMode.MIRRORED)
class UnshardedContextOnTheSecondNodeTestCase(OverrideMirroredRoutingMixin, ShardingTransactionTestCase):
    """
    OverrideMirroredRoutingMixin stands in for the replication that a real deployment configures: it sends a mirrored
    write to the active node, and not to the primary.
    """

    databases = '__all__'
    available_apps = AVAILABLE_APPS

    def test_the_context_table_and_function_are_only_in_the_public_schema_of_the_second_node(self):
        """
        Case: The schemas of the second node that have the pghistory_context table and the _pgh_attach_context()
              function are looked up, while the template schema exists on that node.
        Expected: Only the public schema has them.
        """
        create_template_schema('other')

        self.assertEqual(schemas_that_have_the_context_table_and_function('other'), ({'public'}, {'public'}))

    def test_a_mirrored_event_on_the_second_node_has_a_context_row_on_that_node(self):
        """
        Case: A Kitchen is created inside a context on the second node, and the Context tables of both nodes are read.
        Expected: The event has the context with the metadata. The context row is in the Context table of the second
                  node, and not on the primary, because each node has a Context table of its own.
        """
        from unsharded_context_example.models import Kitchen, KitchenEvent

        with use_shard(node_name='other'):
            with pghistory.context(baked_by='ann'):
                kitchen = Kitchen.objects.create(name='annex')

            self.assertEqual(KitchenEvent.objects.get(pgh_obj_id=kitchen.pk).pgh_context.metadata, {'baked_by': 'ann'})
            self.assertEqual(Context.objects.count(), 1)

        with use_shard(node_name='default'):
            self.assertEqual(Context.objects.count(), 0)

    def test_the_statement_level_trigger_of_a_mirrored_model_on_the_second_node_records_the_context(self):
        """
        Case: A Proofer is created inside a context on the second node.
        Expected: The event on the second node has the context.
        """
        from unsharded_context_example.models import Proofer, ProoferEvent

        with use_shard(node_name='other'):
            with pghistory.context(baked_by='ann'):
                proofer = Proofer.objects.create(name='annex')

            self.assertEqual(ProoferEvent.objects.get(pgh_obj_id=proofer.pk).pgh_context, {'baked_by': 'ann'})

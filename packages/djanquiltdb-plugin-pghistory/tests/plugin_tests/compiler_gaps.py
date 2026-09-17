import pghistory
from django.core.management import call_command
from django.db.utils import ProgrammingError
from django.test import SimpleTestCase
from djanquiltdb import ShardingMode, State
from djanquiltdb.testing import OverrideMirroredRoutingMixin, ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard

from djanquiltdb_plugin_pghistory.querysets import EventQuery, EventQueryCompiler
from example.models import Danish, DanishEvent, Shard, Strudel, StrudelEvent
from plugin_tests.context_placement import run_only_when_the_context_table_is


class EventQueryConstructionTestCase(SimpleTestCase):
    def test_the_queryset_builds_the_query_and_compiler_of_the_plugin(self):
        """
        Case: The query of DanishEvent.objects.all() is inspected, and its compiler is requested.
        Expected: The type of the query is the plugin's EventQuery, and the compiler is the plugin's EventQueryCompiler.
        """
        query = DanishEvent.objects.all().query

        self.assertIs(type(query), EventQuery)
        self.assertIsInstance(query.get_compiler(using='default'), EventQueryCompiler)


class KnownCompilerGapTestCase(ShardingTransactionTestCase):
    """
    The known gaps that the comments in EventQueryCompiler._get_cte describe, which pghistory 3.9 has too. These tests
    fail when an upstream fix or a regression in the plugin's CTE changes the behavior.
    """

    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def setUp(self):
        super().setUp()
        create_template_schema()

        self.shard = Shard.objects.create(
            alias='one', schema_name='one_schema', node_name='default', state=State.ACTIVE
        )

    def test_a_db_column_different_from_the_attname_breaks_every_read_of_the_event_model(self):
        """
        Case: A tracked model has a field whose db_column is different from its attname, and its events have a proxy
              field. An event is read by the primary key of its tracked row.
        Expected: A database error that names the column. The CTE names its columns after the attname, but the outer
                  query selects the column name, so every read of such an event model fails.
        """
        with use_shard(self.shard):
            with pghistory.context(baked_by='ann'):
                danish = Danish.objects.create(name='apricot')

            with self.assertRaises(ProgrammingError) as raised:
                DanishEvent.objects.get(pgh_obj_id=danish.pk)

            self.assertIn('pastry_name', str(raised.exception))

    def test_a_proxy_field_that_reads_two_keys_deep_returns_the_value_json_encoded(self):
        """
        Case: A Strudel is created in a context whose metadata has a baker key with a name key inside it. The proxy
              field of StrudelEvent reads that inner key.
        Expected: The value is returned JSON-encoded: '"ann"' and not 'ann'. Django compiles a lookup of more than one
                  key to the #> operator, which the replacement of -> in the CTE does not match.
        """
        with use_shard(self.shard):
            with pghistory.context(baker={'name': 'ann'}):
                strudel = Strudel.objects.create(name='apple')

            self.assertEqual(StrudelEvent.objects.get(pgh_obj_id=strudel.pk).baker_name, '"ann"')


@run_only_when_the_context_table_is(ShardingMode.PUBLIC)
class KnownCompilerGapWhenMovingAShardTestCase(OverrideMirroredRoutingMixin, ShardingTransactionTestCase):
    """
    The db_column gap of KnownCompilerGapTestCase only affects queries that select the proxy fields. DanishEvent has a
    foreign key to the public Context table, so move_shard_to_node retargets it, with queries that leave the proxy
    fields out.
    """

    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def test_a_shard_with_an_event_model_that_the_cte_cannot_read_still_moves(self):
        """
        Case: A Danish, whose event model has a proxy field and a field with a db_column, is created inside a context in
              a shard, which then moves to the other node.
        Expected: The shard moves, and its event points at the context, which the move copied to the other node.
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
            Danish.objects.create(name='apricot')

        call_command(
            'move_shard_to_node',
            '--source-shard-alias',
            'mover',
            '--target-node-alias',
            'other',
            '--no-input',
            '--quiet',
        )

        with use_shard(node_name='other', schema_name='mover_schema', active_only_schemas=False) as env:
            with env.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT context.metadata ->> 'baked_by' FROM example_danishevent event "
                    'JOIN public.pghistory_context context ON context.id = event.pgh_context_id'
                )

                self.assertEqual(cursor.fetchall(), [('ann',)])

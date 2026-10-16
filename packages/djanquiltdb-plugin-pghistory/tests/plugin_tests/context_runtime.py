from unittest import mock

import pghistory
import pghistory.runtime
from django.db import connections
from django.test import SimpleTestCase
from djanquiltdb import State
from djanquiltdb.testing import ShardingTransactionTestCase
from djanquiltdb.utils import create_template_schema, use_shard
from pghistory.runtime import _inject_history_context

from djanquiltdb_plugin_pghistory.context_on_every_node import EveryNodeConnection, send_context_to_every_node
from example.models import Cake, CakeEvent, CakeType, CakeTypeEvent, Shard


class ContextAroundShardsTestCase(ShardingTransactionTestCase):
    """
    A pghistory context belongs to the connection, and entering a shard changes the search path of that connection.
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

    def test_a_context_opened_before_a_shard_is_recorded_on_the_events_written_inside_the_shard(self):
        """
        Case: The context is entered first, and the shard is entered inside it. A request has this order when
              pghistory's middleware wraps it and a view selects the shard.
        Expected: The event has the metadata, and entering the shard does not raise an error in pghistory's execute
                  wrapper, which requires the backend to pass its SET search_path as a string.
        """
        with pghistory.context(baked_by='ann'):
            with use_shard(self.shard):
                cake = Cake.objects.create(name='carrot')

                self.assertEqual(CakeEvent.objects.get(pgh_obj_id=cake.pk).pgh_context, {'baked_by': 'ann'})

    def test_one_context_is_recorded_on_the_events_in_every_shard_that_it_spans(self):
        """
        Case: One context block writes tracked rows in two shards, one after the other.
        Expected: The events in both shards have the context. The context belongs to the connection, so leaving one
                  shard and entering another does not end it.
        """
        with pghistory.context(baked_by='ann'):
            with use_shard(self.shard):
                Cake.objects.create(name='carrot')

            with use_shard(self.other_shard):
                Cake.objects.create(name='chocolate')

        for shard in (self.shard, self.other_shard):
            with use_shard(shard):
                self.assertEqual(CakeEvent.objects.get().pgh_context, {'baked_by': 'ann'})

    def test_nested_contexts_are_merged_inside_a_shard(self):
        """
        Case: A context is opened inside another one and adds a key, and a Cake is created in a shard inside both
              contexts.
        Expected: The event has both keys, as in pghistory: the inner block adds to the outer block.
        """
        with pghistory.context(baked_by='ann'):
            with pghistory.context(oven='stone'):
                with use_shard(self.shard):
                    cake = Cake.objects.create(name='carrot')

                    event = CakeEvent.objects.get(pgh_obj_id=cake.pk)

                    self.assertEqual(event.pgh_context, {'baked_by': 'ann', 'oven': 'stone'})

    def test_metadata_added_by_calling_context_as_a_function_is_recorded_inside_a_shard(self):
        """
        Case: pghistory.context() is called as a function, and not entered as a block, inside an open context and inside
              a shard. The middleware does this to add a user once a view has authenticated one.
        Expected: The event has the added key.
        """
        with pghistory.context(baked_by='ann'):
            with use_shard(self.shard):
                pghistory.context(oven='stone')

                cake = Cake.objects.create(name='carrot')

                event = CakeEvent.objects.get(pgh_obj_id=cake.pk)

                self.assertEqual(event.pgh_context, {'baked_by': 'ann', 'oven': 'stone'})

    def test_a_shard_entered_after_the_context_has_ended_records_no_context(self):
        """
        Case: A row is written in a shard after the context block has ended.
        Expected: Its event has no context. This shows that the wrapper was removed from the connection and does not
                  affect the next shard.
        """
        with pghistory.context(baked_by='ann'):
            with use_shard(self.shard):
                Cake.objects.create(name='carrot')

        with use_shard(self.other_shard):
            cake = Cake.objects.create(name='chocolate')

            self.assertIsNone(CakeEvent.objects.get(pgh_obj_id=cake.pk).pgh_context)


class ContextOnTheSecondNodeTestCase(ShardingTransactionTestCase):
    """
    pghistory.context() sets its variables through an execute wrapper, which the plugin installs on the connection of
    every node.
    """

    databases = '__all__'
    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def setUp(self):
        super().setUp()
        create_template_schema('other')

        self.shard = Shard.objects.create(
            alias='other_one', schema_name='one_schema', node_name='other', state=State.ACTIVE
        )

    def test_a_context_is_recorded_on_the_events_in_a_shard_on_the_second_node(self):
        """
        Case: A Cake is created inside a context, inside a shard on the second node.
        Expected: The event has the metadata.
        """
        with use_shard(self.shard):
            with pghistory.context(baked_by='ann'):
                cake = Cake.objects.create(name='carrot')

            self.assertEqual(CakeEvent.objects.get(pgh_obj_id=cake.pk).pgh_context, {'baked_by': 'ann'})

    def test_a_context_opened_before_a_shard_on_the_second_node_is_recorded_inside_the_shard(self):
        """
        Case: The context is entered while no shard is active, and a shard on the second node is entered inside it, as
              when pghistory's middleware wraps a request and the view selects the shard.
        Expected: The event has the metadata.
        """
        with pghistory.context(baked_by='ann'):
            with use_shard(self.shard):
                cake = Cake.objects.create(name='carrot')

                self.assertEqual(CakeEvent.objects.get(pgh_obj_id=cake.pk).pgh_context, {'baked_by': 'ann'})

    def test_the_execute_wrapper_is_on_every_node_only_while_the_context_is_open(self):
        """
        Case: The execute wrappers of the connection of each node are read inside a context and after it.
        Expected: pghistory's wrapper is on both nodes inside the context, and on neither after it.
        """
        with pghistory.context(baked_by='ann'):
            for node_name in ('default', 'other'):
                with self.subTest(node_name=node_name, inside_the_context=True):
                    self.assertIn(_inject_history_context, connections[node_name].execute_wrappers)

        for node_name in ('default', 'other'):
            with self.subTest(node_name=node_name, inside_the_context=False):
                self.assertNotIn(_inject_history_context, connections[node_name].execute_wrappers)


class SendContextToEveryNodeTestCase(SimpleTestCase):
    def test_sending_the_context_to_every_node_again_keeps_the_connection_that_pghistory_uses(self):
        """
        Case: send_context_to_every_node() runs a second time, as it does when AppConfig.ready() runs again.
        Expected: pghistory keeps the same EveryNodeConnection.
        """
        every_node_connection = pghistory.runtime.connection

        send_context_to_every_node()

        self.assertIsInstance(every_node_connection, EveryNodeConnection)
        self.assertIs(pghistory.runtime.connection, every_node_connection)

    def test_the_wrapper_is_installed_on_the_nodes_that_djanquiltdb_lists(self):
        """
        Case: EveryNodeConnection installs an execute wrapper while get_all_databases(), the list of nodes that
              DjanQuiltDB uses everywhere else, returns only the default node.
        Expected: The wrapper is installed on the connection of the default node, and not on the connection of the
                  other one.
        """

        def wrapper(execute, sql, params, many, context):
            return execute(sql, params, many, context)

        with mock.patch(
            'djanquiltdb_plugin_pghistory.context_on_every_node.get_all_databases', return_value=['default']
        ):
            with EveryNodeConnection().execute_wrapper(wrapper):
                self.assertIn(wrapper, connections['default'].execute_wrappers)
                self.assertNotIn(wrapper, connections['other'].execute_wrappers)


class ContextOnPublicEventsTestCase(ShardingTransactionTestCase):
    """
    The denormalized context is copied to the event row, so these hold wherever pghistory's Context table is placed.
    """

    available_apps = ['djanquiltdb', 'example', 'pghistory', 'django.contrib.auth', 'django.contrib.contenttypes']

    def test_the_events_of_a_public_model_have_the_context(self):
        """
        Case: A CakeType, a public model, is created inside a context, with no shard active.
        Expected: Its event has the metadata.
        """
        with pghistory.context(baked_by='ann'):
            cake_type = CakeType.objects.create(name='layered')

        self.assertEqual(CakeTypeEvent.objects.get(pgh_obj_id=cake_type.pk).pgh_context, {'baked_by': 'ann'})

    def test_the_events_of_a_public_model_written_from_inside_a_shard_have_the_context(self):
        """
        Case: A CakeType is created inside a context, while a shard is active. The row and its event are written in the
              public schema, which is on the search path of the shard.
        Expected: Its event has the metadata.
        """
        create_template_schema()
        shard = Shard.objects.create(alias='one', schema_name='one_schema', node_name='default', state=State.ACTIVE)

        with pghistory.context(baked_by='ann'):
            with use_shard(shard):
                cake_type = CakeType.objects.create(name='layered')

        self.assertEqual(CakeTypeEvent.objects.get(pgh_obj_id=cake_type.pk).pgh_context, {'baked_by': 'ann'})

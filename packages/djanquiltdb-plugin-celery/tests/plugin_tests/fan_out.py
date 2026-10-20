from unittest import mock

from django.test import SimpleTestCase, override_settings
from djanquiltdb.utils import use_shard

from config.celery import app
from djanquiltdb_plugin_celery.decorators import task_for_every_node, task_for_every_shard
from djanquiltdb_plugin_celery.targets import TaskShardError
from djanquiltdb_plugin_celery.utils import (
    aggregate_per_shard,
    fan_out,
    fan_out_to_nodes,
    fanout_shards,
    per_node_signatures,
    per_shard_signatures,
)
from example.tasks import (
    collect_counts,
    count_widgets,
    create_widget,
    record_every_node,
    record_every_shard,
    record_every_shard_on_the_fanout_queue,
    record_node,
    record_node_name,
    record_schema_of_every_shard,
    record_shard_id,
    record_shard_id_and_days,
    record_target,
)
from plugin_tests.base import PluginTestCase
from plugin_tests.tasks import node_and_schema_of


def results_of(task_ids):
    return [app.AsyncResult(task_id).get() for task_id in task_ids]


class FanOutTestCase(PluginTestCase):
    """
    The tasks run eagerly, as the test settings configure, and store their results, so the result of each task that a
    fan-out sends can be read by its id.
    """

    def test_task_for_every_shard_runs_once_in_every_active_shard(self):
        """
        Case: record_every_shard, decorated with task_for_every_shard, is sent without a target.
        Expected: It sends one task for first and one for remote, with the shard_id of each, and none for maintained,
                  which is in maintenance. It returns the ids of those tasks. The function, which does not declare
                  shard_id, does not receive it.
        """
        results = results_of(record_every_shard.delay().get())

        self.assertEqual(
            [node_and_schema_of(result) for result in results],
            [('default', 'first_schema'), ('other', 'remote_schema')],
        )
        self.assertEqual(
            [result['header'] for result in results], [{'shard_id': self.first.id}, {'shard_id': self.remote.id}]
        )
        self.assertEqual([result['kwargs'] for result in results], [{}, {}])

    def test_task_for_every_shard_passes_on_its_arguments(self):
        """
        Case: record_every_shard is sent without a target, with the keyword argument colour='blue'.
        Expected: Each task that it sends receives colour='blue'.
        """
        results = results_of(record_every_shard.delay(colour='blue').get())

        self.assertEqual([result['kwargs']['colour'] for result in results], ['blue', 'blue'])

    def test_task_for_every_shard_with_a_target_runs_in_that_shard_only(self):
        """
        Case: record_every_shard is sent with the shard_id of remote.
        Expected: It runs in remote, as a task that is decorated with task_for_shard does, and sends nothing.
        """
        result = record_every_shard.delay(shard_id=self.remote.id).get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))

    def test_task_for_every_shard_does_not_inherit_the_active_shard(self):
        """
        Case: record_every_shard is sent without a target inside use_shard() for first.
        Expected: It runs once in every active shard.
        """
        with use_shard(self.first):
            results = results_of(record_every_shard.delay().get())

        self.assertEqual(len(results), 2)

    def test_task_for_every_node_runs_once_on_every_node(self):
        """
        Case: record_every_node, decorated with task_for_every_node, is sent without a target.
        Expected: It sends one task for each node, with the node_name of each, which runs in the public schema of that
                  node. The function, which does not declare node_name, does not receive it.
        """
        results = results_of(record_every_node.delay().get())

        self.assertEqual(
            [node_and_schema_of(result) for result in results], [('default', 'public'), ('other', 'public')]
        )
        self.assertEqual([result['kwargs'] for result in results], [{}, {}])

    def test_a_fan_out_task_whose_function_takes_no_arguments_runs_in_every_shard(self):
        """
        Case: record_schema_of_every_shard, decorated with task_for_every_shard, whose function takes no arguments, is
              sent without a target.
        Expected: The fan-out sends one task for first and one for remote, which each run in their shard.
        """
        self.assertEqual(results_of(record_schema_of_every_shard.delay().get()), ['first_schema', 'remote_schema'])

    def test_a_fan_out_task_whose_function_takes_shard_id_is_sent_without_it(self):
        """
        Case: record_shard_id, decorated with task_for_every_shard, whose function takes shard_id without a default, is
              sent without a target.
        Expected: The check of the arguments accepts the message, and the fan-out sends one task for first and one for
                  remote, which each receive the shard_id of their shard.
        """
        results = results_of(record_shard_id.delay().get())

        self.assertEqual(
            results,
            [
                {'node_name': 'default', 'schema_name': 'first_schema', 'received': self.first.id},
                {'node_name': 'other', 'schema_name': 'remote_schema', 'received': self.remote.id},
            ],
        )

    def test_a_fan_out_task_whose_function_takes_node_name_is_sent_without_it(self):
        """
        Case: record_node_name, decorated with task_for_every_node, whose function takes node_name without a default and
              **kwargs, is sent without a target.
        Expected: The check of the arguments accepts the message, and the fan-out sends one task for each node, which
                  each receive the name of their node.
        """
        results = results_of(record_node_name.delay().get())

        self.assertEqual(
            results,
            [
                {'node_name': 'default', 'schema_name': 'public', 'received': 'default'},
                {'node_name': 'other', 'schema_name': 'public', 'received': 'other'},
            ],
        )

    def test_the_other_arguments_of_a_fan_out_task_are_still_checked(self):
        """
        Case: record_shard_id, whose function takes only shard_id, is sent without a target and with a positional
              argument.
        Expected: TypeError before anything is sent, because the arguments of a task are checked against its function.
        """
        with self.assertRaises(TypeError):
            record_shard_id.delay('drafts')

    def test_the_parameters_after_shard_id_are_passed_by_keyword(self):
        """
        Case: record_shard_id_and_days, whose function takes shard_id and then days, both without a default, is sent
              without a target and with days=30.
        Expected: The fan-out sends one task for first and one for remote, which each receive their shard_id and
                  days=30.
        """
        results = results_of(record_shard_id_and_days.delay(days=30).get())

        self.assertEqual(results, [{'received': self.first.id, 'days': 30}, {'received': self.remote.id, 'days': 30}])

    def test_task_for_every_node_with_a_node_name_runs_on_that_node_only(self):
        """
        Case: record_every_node is sent with node_name='other'.
        Expected: It runs in the public schema of the other node, as a task that is decorated with task_for_node does,
                  and sends nothing.
        """
        result = record_every_node.delay(node_name='other').get()

        self.assertEqual(node_and_schema_of(result), ('other', 'public'))

    def test_task_for_every_node_with_a_shard_id_still_runs_on_every_node(self):
        """
        Case: record_every_node is sent with the shard_id of first.
        Expected: It runs once on every node, and each task receives the shard_id unchanged, because a shard_id does not
                  identify a node.
        """
        results = results_of(record_every_node.delay(shard_id=self.first.id).get())

        self.assertEqual(
            [node_and_schema_of(result) for result in results], [('default', 'public'), ('other', 'public')]
        )
        self.assertEqual({result['kwargs']['shard_id'] for result in results}, {self.first.id})

    def test_the_shard_helpers_refuse_a_node_task(self):
        """
        Case: fan_out(), per_shard_signatures() and aggregate_per_shard() are called for record_node, which is decorated
              with task_for_node.
        Expected: TypeError for each, whose message points at fan_out_to_nodes(), before anything is sent.
        """
        helpers = {
            'fan_out': lambda: fan_out(record_node),
            'per_shard_signatures': lambda: per_shard_signatures(record_node),
            'aggregate_per_shard': lambda: aggregate_per_shard(record_node, collect_counts),
        }

        for name, call in helpers.items():
            with self.subTest(helper=name):
                with self.assertRaisesMessage(TypeError, 'example.tasks.record_node runs on a node'):
                    call()

    def test_the_node_helpers_refuse_a_shard_task(self):
        """
        Case: fan_out_to_nodes() and per_node_signatures() are called for record_target, which is decorated with
              task_for_shard.
        Expected: TypeError for each, whose message points at fan_out(), before anything is sent.
        """
        for helper in (fan_out_to_nodes, per_node_signatures):
            with self.subTest(helper=helper.__name__):
                with self.assertRaisesMessage(TypeError, 'example.tasks.record_target runs in a shard'):
                    helper(record_target)

    def test_fan_out_sends_the_task_to_the_given_shards(self):
        """
        Case: fan_out() is called for record_target with only remote.
        Expected: One task, which runs in remote.
        """
        results = [result.get() for result in fan_out(record_target, shards=[self.remote])]

        self.assertEqual([node_and_schema_of(result) for result in results], [('other', 'remote_schema')])

    def test_fan_out_to_nodes_sends_the_task_to_the_given_nodes(self):
        """
        Case: fan_out_to_nodes() is called for record_node with only the other node.
        Expected: One task, which runs in the public schema of the other node.
        """
        results = [result.get() for result in fan_out_to_nodes(record_node, nodes=['other'])]

        self.assertEqual([node_and_schema_of(result) for result in results], [('other', 'public')])

    def test_the_fanout_shards_are_the_active_shards(self):
        """
        Case: fanout_shards() is called with and without include_maintenance.
        Expected: first and remote without it, and maintained too with it, in the order of their primary keys.
        """
        self.assertEqual(list(fanout_shards()), [self.first, self.remote])
        self.assertEqual(list(fanout_shards(include_maintenance=True)), [self.first, self.remote, self.maintained])

    def test_fan_out_inside_a_shard_without_the_public_schema(self):
        """
        Case: fan_out() is called for record_target while first is entered with include_public set to False.
        Expected: One task for first and one for remote, because fanout_shards() reads the Shard table in the public
                  schema of the active node, which the search_path of first does not have.
        """
        with use_shard(self.first, include_public=False):
            results = [result.get() for result in fan_out(record_target)]

        self.assertEqual(
            [node_and_schema_of(result) for result in results],
            [('default', 'first_schema'), ('other', 'remote_schema')],
        )

    def test_the_signatures_name_their_shard_or_node(self):
        """
        Case: per_shard_signatures() is called for record_target with the keyword argument colour='blue', and
              per_node_signatures() for record_node.
        Expected: One signature for each active shard, with its shard_id, and one for each node, with its node_name.
        """
        self.assertEqual(
            [signature.kwargs for signature in per_shard_signatures(record_target, kwargs={'colour': 'blue'})],
            [{'colour': 'blue', 'shard_id': self.first.id}, {'colour': 'blue', 'shard_id': self.remote.id}],
        )
        self.assertEqual(
            [signature.kwargs for signature in per_node_signatures(record_node)],
            [{'node_name': 'default'}, {'node_name': 'other'}],
        )


@override_settings(CELERY_TASK_ALWAYS_EAGER=False)
class FanOutOptionsTestCase(PluginTestCase):
    """
    Tasks do not run eagerly, and app.send_task() is replaced by a mock, so each test reads the options that a fan-out
    sends its tasks with.
    """

    def setUp(self):
        super().setUp()
        self.send_task = self.enterContext(mock.patch.object(app, 'send_task'))

    def sent_options(self, name):
        return [call.kwargs.get(name) for call in self.send_task.call_args_list]

    def test_a_queue_name_sends_every_task_to_that_queue(self):
        """
        Case: fan_out() is called for record_target with queue='fanout'.
        Expected: Both tasks are sent to the queue fanout.
        """
        fan_out(record_target, queue='fanout')

        self.assertEqual(self.sent_options('queue'), ['fanout', 'fanout'])

    def test_a_queue_that_is_not_a_name_is_refused_before_anything_is_published(self):
        """
        Case: fan_out() is called for record_target, and fan_out_to_nodes() for record_node, with a queue that is a
              function, and not the name of a queue.
        Expected: ValueError for each, whose message includes the values that a queue accepts, before anything is
                  published. A queue per shard or per node is set with a router function in Celery's task_routes, which
                  receives the shard_id or node_name of each task.
        """
        for helper, task in ((fan_out, record_target), (fan_out_to_nodes, record_node)):
            with self.subTest(helper=helper.__name__):
                with self.assertRaisesMessage(ValueError, 'and it accepts a string or None'):
                    helper(task, queue=lambda shard_or_node: 'fanout')

        self.send_task.assert_not_called()

    def test_a_spread_that_is_not_a_spread_is_refused_before_anything_is_published(self):
        """
        Case: fan_out() is called for record_target with spread=2.5, and fan_out_to_nodes() for record_node with
              spread=(10, 5).
        Expected: ValueError for each, whose message includes the spread and the values that a spread accepts, before
                  anything is published.
        """
        with self.assertRaisesMessage(ValueError, 'is 2.5, and it accepts an integer that is not negative'):
            fan_out(record_target, spread=2.5)

        with self.assertRaisesMessage(ValueError, 'is (10, 5), and it accepts an integer that is not negative'):
            fan_out_to_nodes(record_node, spread=(10, 5))

        self.send_task.assert_not_called()

    def test_the_parameters_after_shard_id_cannot_be_passed_by_position(self):
        """
        Case: record_shard_id_and_days, whose function takes shard_id and then days, is sent without a target and with
              30 as a positional argument.
        Expected: TypeError before anything is published, because each task that the fan-out sends passes shard_id by
                  keyword, so 30 cannot be passed to days by position.
        """
        with self.assertRaises(TypeError):
            record_shard_id_and_days.delay(30)

        self.send_task.assert_not_called()

    @override_settings(QUILT_CELERY={'FANOUT_QUEUE': 'fanout'})
    def test_the_setting_gives_the_default_queue(self):
        """
        Case: fan_out() is called for record_target without a queue, while QUILT_CELERY['FANOUT_QUEUE'] is fanout.
        Expected: Both tasks are sent to the queue fanout.
        """
        fan_out(record_target)

        self.assertEqual(self.sent_options('queue'), ['fanout', 'fanout'])

    def test_without_a_queue_the_task_is_routed_as_usual(self):
        """
        Case: fan_out() is called for record_target without a queue, and QUILT_CELERY sets none.
        Expected: The tasks are sent without a queue, so that the routing of the Celery app decides.
        """
        fan_out(record_target)

        self.assertEqual(self.sent_options('queue'), [None, None])

    def test_a_spread_delays_each_task_by_a_random_countdown_inside_it(self):
        """
        Case: fan_out() is called for record_target with spread=(5, 10), and with spread=3.
        Expected: Each task has a countdown of 5 to 10 seconds, and then of 0 to 3 seconds.
        """
        with mock.patch('random.randint', side_effect=lambda low, high: (low, high)):
            fan_out(record_target, spread=(5, 10))
            fan_out(record_target, spread=3)

        self.assertEqual(self.sent_options('countdown'), [(5, 10), (5, 10), (0, 3), (0, 3)])

    @override_settings(QUILT_CELERY={'FANOUT_SPREAD': 30})
    def test_the_setting_gives_the_default_spread(self):
        """
        Case: fan_out() is called for record_target without a spread, while QUILT_CELERY['FANOUT_SPREAD'] is 30.
        Expected: Each task has a countdown of 0 to 30 seconds.
        """
        with mock.patch('random.randint', side_effect=lambda low, high: (low, high)):
            fan_out(record_target)

        self.assertEqual(self.sent_options('countdown'), [(0, 30), (0, 30)])

    def test_without_a_spread_the_tasks_have_no_countdown(self):
        """
        Case: fan_out() is called for record_target without a spread, and QUILT_CELERY sets none.
        Expected: The tasks are sent without a countdown.
        """
        fan_out(record_target)

        self.assertEqual(self.sent_options('countdown'), [None, None])

    def test_the_decorator_sets_the_queue_the_spread_and_the_shards_in_maintenance(self):
        """
        Case: record_every_shard_on_the_fanout_queue, decorated with queue='fanout', spread=(1, 2) and
              include_maintenance=True, is called directly without a target, so that it sends its tasks.
        Expected: Three tasks, one for each shard, maintained included, on the queue fanout, with a countdown of 1 to 2
                  seconds.
        """
        with mock.patch('random.randint', side_effect=lambda low, high: (low, high)):
            record_every_shard_on_the_fanout_queue()

        # send_task() receives the name, the positional arguments and the keyword arguments of the task positionally.
        self.assertEqual(
            [call.args[2]['shard_id'] for call in self.send_task.call_args_list],
            [self.first.id, self.remote.id, self.maintained.id],
        )
        self.assertEqual(self.sent_options('queue'), ['fanout'] * 3)
        self.assertEqual(self.sent_options('countdown'), [(1, 2)] * 3)

    def test_the_task_that_starts_a_fan_out_is_sent_without_a_target(self):
        """
        Case: record_every_shard is sent without a target.
        Expected: One message, without a target header, which starts the fan-out where a worker runs it.
        """
        record_every_shard.delay()

        self.assertEqual(self.send_task.call_count, 1)
        self.assertIsNone(self.send_task.call_args.kwargs.get('headers'))


class FanOutDecorationTestCase(SimpleTestCase):
    def test_a_decorator_refuses_a_spread_whose_first_bound_is_above_its_second(self):
        """
        Case: A function is decorated with task_for_every_shard, and with task_for_every_node, with spread=(300, 0).
        Expected: ValueError at decoration, whose message includes the function and the spread, so that no fan-out fails
                  later in a worker.
        """
        for decorator in (task_for_every_shard, task_for_every_node):
            with self.subTest(decorator=decorator.__name__):
                with self.assertRaisesMessage(ValueError, 'clean_up_drafts is (300, 0)'):

                    @decorator(spread=(300, 0))
                    def clean_up_drafts(**kwargs):
                        pass  # pragma: no cover

    def test_a_decorator_refuses_a_negative_spread(self):
        """
        Case: A function is decorated with task_for_every_shard with spread=-5.
        Expected: ValueError at decoration, whose message includes the function and the spread.
        """
        with self.assertRaisesMessage(ValueError, 'clean_up_drafts is -5'):

            @task_for_every_shard(spread=-5)
            def clean_up_drafts(**kwargs):
                pass  # pragma: no cover

    def test_a_decorator_refuses_a_queue_that_is_not_a_name(self):
        """
        Case: A function is decorated with task_for_every_shard, and with task_for_every_node, with a queue that is a
              function.
        Expected: ValueError at decoration, whose message includes the function and says that the queue accepts a
                  string or None.
        """
        for decorator in (task_for_every_shard, task_for_every_node):
            with self.subTest(decorator=decorator.__name__):
                with self.assertRaisesMessage(ValueError, 'clean_up_drafts is <function'):

                    @decorator(queue=lambda shard_or_node: 'fanout')
                    def clean_up_drafts(**kwargs):
                        pass  # pragma: no cover


class AggregatePerShardTestCase(PluginTestCase):
    def test_the_callback_receives_the_result_of_each_shard(self):
        """
        Case: first has two widgets and remote one, and aggregate_per_shard() runs count_widgets in each active shard
              with collect_counts as the callback.
        Expected: The callback receives the count of first and of remote, in the order of the shards.
        """
        create_widget.delay('anvil', shard_id=self.first.id)
        create_widget.delay('hammer', shard_id=self.first.id)
        create_widget.delay('tongs', shard_id=self.remote.id)

        result = aggregate_per_shard(count_widgets, collect_counts.s())

        self.assertEqual(result.get(), [2, 1])

    def test_the_callback_can_be_a_task(self):
        """
        Case: aggregate_per_shard() is called with the task collect_counts as the callback, and not with a signature.
        Expected: The callback receives the count of each active shard.
        """
        self.assertEqual(aggregate_per_shard(count_widgets, collect_counts).get(), [0, 0])

    def test_the_results_of_the_shards_are_stored_for_the_callback(self):
        """
        Case: aggregate_per_shard() builds the chord for count_widgets, which is decorated with ignore_result=True.
        Expected: The tasks in the header of the chord store their results, which the callback needs.
        """
        with mock.patch('djanquiltdb_plugin_celery.utils.chord') as chord:
            aggregate_per_shard(count_widgets, collect_counts.s())

        header, body = chord.call_args.args

        self.assertEqual([signature.options['ignore_result'] for signature in header], [False, False])
        self.assertEqual(body, collect_counts.s())

    @override_settings(CELERY_TASK_ALWAYS_EAGER=False)
    def test_a_callback_without_a_target_is_refused_before_anything_is_published(self):
        """
        Case: aggregate_per_shard() is called with record_target as the task of each shard and as the callback, while
              tasks do not run eagerly. The callback has no target.
        Expected: TaskShardError before anything is published, and not in the worker after the tasks of the shards run.
        """
        with mock.patch.object(app, 'send_task') as send_task:
            with self.assertRaisesMessage(TaskShardError, 'example.tasks.record_target has no shard'):
                aggregate_per_shard(record_target, record_target)

        send_task.assert_not_called()

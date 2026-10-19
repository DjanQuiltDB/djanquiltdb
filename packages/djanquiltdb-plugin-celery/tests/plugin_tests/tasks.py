from unittest import mock

from celery import group, signals
from django.test import override_settings
from djanquiltdb.utils import StateException, use_shard

from config.celery import app
from djanquiltdb_plugin_celery.decorators import task_for_shard
from djanquiltdb_plugin_celery.targets import TARGET_HEADER_NAME, TaskNodeError, TaskQuiltError, TaskShardError
from example.models import TenantShard, Widget
from example.tasks import (
    create_widget,
    record_node,
    record_received_node_name,
    record_received_shard_id,
    record_received_tenant_id,
    record_schema_and_shard,
    record_target,
    record_target_with_one_retry,
    record_target_without_lock,
)
from plugin_tests.base import PluginTestCase


def node_and_schema_of(result):
    return result['node_name'], result['schema_name']


class SendingTestCase(PluginTestCase):
    """
    The tasks run eagerly, as the test settings configure, unless a test turns that off.
    """

    def test_a_task_runs_in_the_shard_of_its_shard_id(self):
        """
        Case: record_target, whose function takes **kwargs and does not declare shard_id, is sent with the shard_id of
              remote, on the other node.
        Expected: It runs in the schema of remote on the other node, and its function does not receive shard_id.
        """
        result = record_target.apply_async(kwargs={'shard_id': self.remote.id}).get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))
        self.assertEqual(result['kwargs'], {})

    def test_a_task_runs_in_the_shard_of_its_mapping_value(self):
        """
        Case: record_target is sent with tenant_id=2, which maps onto remote.
        Expected: It runs in the schema of remote, and its function does not receive tenant_id.
        """
        result = record_target.delay(tenant_id=2).get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))
        self.assertEqual(result['kwargs'], {})

    def test_a_node_task_runs_in_the_public_schema_of_its_node_name(self):
        """
        Case: record_node, decorated with task_for_node, is sent with node_name='other'.
        Expected: It runs in the public schema of the other node, without a lock, and the header identifies that schema.
                  Its function does not receive node_name.
        """
        result = record_node.delay(node_name='other').get()

        self.assertEqual(node_and_schema_of(result), ('other', 'public'))
        self.assertFalse(result['lock'])
        self.assertEqual(result['header'], {'node_name': 'other', 'schema_name': 'public'})
        self.assertEqual(result['kwargs'], {})

    def test_a_node_name_is_an_ordinary_keyword_argument_of_a_shard_task(self):
        """
        Case: record_target, decorated with task_for_shard, is sent with the shard_id of remote and node_name='other'.
        Expected: It runs in the schema of remote, and its function receives node_name, because node_name does not
                  identify the shard of a shard task.
        """
        result = record_target.delay(shard_id=self.remote.id, node_name='other').get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))
        self.assertEqual(result['kwargs'], {'node_name': 'other'})

    def test_a_shard_task_with_only_a_node_name_points_at_task_for_node(self):
        """
        Case: record_target, decorated with task_for_shard, is sent with node_name='other' and no shard.
        Expected: TaskShardError, whose message says that a task that runs on a node is decorated with task_for_node.
        """
        with self.assertRaisesMessage(TaskShardError, 'decorated with task_for_node'):
            record_target.delay(node_name='other')

    def test_a_node_task_without_a_node_name_is_not_sent(self):
        """
        Case: record_node, decorated with task_for_node, is sent without a node_name.
        Expected: TaskNodeError, whose message includes the task, and which is a TaskQuiltError.
        """
        with self.assertRaisesMessage(TaskNodeError, 'example.tasks.record_node has no node'):
            record_node.delay()

        self.assertTrue(issubclass(TaskNodeError, TaskQuiltError))

    def test_a_node_task_with_only_a_shard_id_points_at_task_for_shard(self):
        """
        Case: record_node, decorated with task_for_node, is sent with the shard_id of first and no node_name.
        Expected: TaskNodeError, whose message says that a task that runs in a shard is decorated with task_for_shard.
        """
        with self.assertRaisesMessage(TaskNodeError, 'decorated with task_for_shard'):
            record_node.delay(shard_id=self.first.id)

    def test_the_target_travels_in_a_message_header(self):
        """
        Case: record_target is sent with the shard_id of remote.
        Expected: The request of the task has a header with the target.
        """
        result = record_target.delay(shard_id=self.remote.id).get()

        self.assertEqual(result['header'], {'shard_id': self.remote.id})

    @override_settings(CELERY_TASK_ALWAYS_EAGER=False)
    def test_the_header_is_in_the_message_that_is_published(self):
        """
        Case: record_target is sent with the shard_id of remote while tasks do not run eagerly, so the message is
              published.
        Expected: The message is published with the target header.
        """
        with mock.patch.object(app, 'send_task') as send_task:
            record_target.delay(shard_id=self.remote.id)

        self.assertEqual(send_task.call_args.kwargs['headers'], {TARGET_HEADER_NAME: {'shard_id': self.remote.id}})

    @override_settings(CELERY_TASK_ALWAYS_EAGER=False)
    def test_a_task_without_a_target_is_not_published(self):
        """
        Case: record_target is sent without a target while tasks do not run eagerly.
        Expected: TaskShardError, whose message includes the task, before anything is published.
        """
        with mock.patch.object(app, 'send_task') as send_task:
            with self.assertRaisesMessage(TaskShardError, 'example.tasks.record_target has no shard'):
                record_target.delay()

        send_task.assert_not_called()

    def test_a_task_with_two_targets_is_not_sent(self):
        """
        Case: record_target is sent with both a shard_id and a tenant_id.
        Expected: TaskShardError, because the two could identify different shards.
        """
        with self.assertRaises(TaskShardError):
            record_target.delay(shard_id=self.first.id, tenant_id=2)

    def test_a_task_without_a_target_cannot_be_applied_locally(self):
        """
        Case: record_target is run with apply() without a target.
        Expected: TaskShardError.
        """
        with self.assertRaises(TaskShardError):
            record_target.apply()

    def test_a_task_runs_in_the_target_of_its_header(self):
        """
        Case: record_target is sent without keyword arguments, but with a target header that identifies remote, as in a
              message that another producer publishes.
        Expected: It runs in the schema of remote.
        """
        result = record_target.apply_async(headers={TARGET_HEADER_NAME: {'shard_id': self.remote.id}}).get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))
        self.assertEqual(result['kwargs'], {})

    def test_a_task_that_is_called_directly_runs_in_the_shard_of_its_keyword_arguments(self):
        """
        Case: record_target is called as a function, with the shard_id of remote.
        Expected: It runs in the schema of remote, without a message.
        """
        result = record_target(shard_id=self.remote.id)

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))
        self.assertIsNone(result['header'])

    def test_a_task_that_is_called_directly_without_a_target_raises(self):
        """
        Case: record_target is called as a function without a target.
        Expected: TaskShardError.
        """
        with self.assertRaisesMessage(TaskShardError, 'example.tasks.record_target has no shard'):
            record_target()

    def test_a_node_task_that_is_called_directly_runs_in_the_public_schema_of_its_node(self):
        """
        Case: record_node is called as a function, with node_name='other'.
        Expected: It runs in the public schema of the other node, without a message.
        """
        result = record_node(node_name='other')

        self.assertEqual(node_and_schema_of(result), ('other', 'public'))
        self.assertIsNone(result['header'])

    def test_a_node_task_that_is_called_directly_without_a_node_name_raises(self):
        """
        Case: record_node is called as a function without a node_name.
        Expected: TaskNodeError.
        """
        with self.assertRaisesMessage(TaskNodeError, 'example.tasks.record_node has no node'):
            record_node()

    def test_the_task_writes_to_its_shard(self):
        """
        Case: create_widget, which is not bound, is sent with the shard_id of first.
        Expected: The widget is in first and not in remote.
        """
        create_widget.delay('anvil', shard_id=self.first.id)

        with use_shard(self.first):
            self.assertEqual(list(Widget.objects.values_list('name', flat=True)), ['anvil'])

        with use_shard(self.remote):
            self.assertFalse(Widget.objects.exists())

    def test_the_arguments_are_checked_against_the_function(self):
        """
        Case: create_widget, which is not bound and takes one positional argument, is sent with two positional
              arguments.
        Expected: TypeError before anything is sent, because the arguments of a task are checked against its function.
        """
        with self.assertRaises(TypeError):
            create_widget.delay('anvil', 'hammer', shard_id=self.first.id)

    def test_the_task_locks_the_shard_unless_it_is_told_not_to(self):
        """
        Case: record_target and record_target_without_lock, which is decorated with lock=False, are sent for first.
        Expected: The shard is locked while record_target runs, as use_shard() locks it, and not while
                  record_target_without_lock runs.
        """
        self.assertTrue(record_target.delay(shard_id=self.first.id).get()['lock'])
        self.assertFalse(record_target_without_lock.delay(shard_id=self.first.id).get()['lock'])

    def test_signatures_and_groups_carry_the_target(self):
        """
        Case: record_target is sent as a signature, as an immutable signature and as a group of two signatures.
        Expected: Each one runs in the shard of its own shard_id.
        """
        signature_result = record_target.s(shard_id=self.remote.id).apply_async().get()
        immutable_result = record_target.si(shard_id=self.first.id).apply_async().get()
        group_results = group(
            record_target.s(shard_id=self.first.id), record_target.s(shard_id=self.remote.id)
        ).apply_async()

        self.assertEqual(node_and_schema_of(signature_result), ('other', 'remote_schema'))
        self.assertEqual(node_and_schema_of(immutable_result), ('default', 'first_schema'))
        self.assertEqual(
            [node_and_schema_of(result) for result in group_results.get()],
            [('default', 'first_schema'), ('other', 'remote_schema')],
        )


class TargetKeywordArgumentTestCase(PluginTestCase):
    """
    The function of a task receives the keyword argument that identifies its target only when the function declares that
    keyword argument by name.
    """

    def test_a_function_that_declares_shard_id_receives_the_shard_id_that_is_sent(self):
        """
        Case: record_received_shard_id, whose function declares shard_id, is sent with the shard_id of remote.
        Expected: The function receives the shard_id of remote.
        """
        self.assertEqual(record_received_shard_id.delay(shard_id=self.remote.id).get(), self.remote.id)

    def test_a_function_that_declares_shard_id_receives_the_shard_of_a_mapping_value(self):
        """
        Case: record_received_shard_id is sent with tenant_id=2, which maps onto remote, and is called as a function
              with tenant_id=1, which maps onto first.
        Expected: The function receives the shard_id of remote, and then the shard_id of first, and not tenant_id, which
                  it does not declare.
        """
        self.assertEqual(record_received_shard_id.delay(tenant_id=2).get(), self.remote.id)
        self.assertEqual(record_received_shard_id(tenant_id=1), self.first.id)

    def test_a_function_that_declares_shard_id_receives_the_shard_of_a_header(self):
        """
        Case: record_received_shard_id is sent without keyword arguments, with a target header that identifies remote.
        Expected: The function receives the shard_id of remote.
        """
        from_header = record_received_shard_id.apply_async(
            headers={TARGET_HEADER_NAME: {'shard_id': self.remote.id}}
        ).get()

        self.assertEqual(from_header, self.remote.id)

    def test_a_function_that_declares_the_mapping_kwarg_receives_the_value_that_is_sent(self):
        """
        Case: record_received_tenant_id, whose function declares tenant_id with a default of None, is sent with
              tenant_id=2, and then with the shard_id of remote.
        Expected: The function receives 2, and then None, because a mapping value is not filled in from the shard.
        """
        self.assertEqual(record_received_tenant_id.delay(tenant_id=2).get(), 2)
        self.assertIsNone(record_received_tenant_id.delay(shard_id=self.remote.id).get())

    def test_a_function_that_declares_node_name_receives_the_node_name(self):
        """
        Case: record_received_node_name, decorated with task_for_node, whose function declares node_name, is sent with
              node_name='other', and then with for_node('other') and no keyword arguments.
        Expected: The function receives 'other' both times.
        """
        self.assertEqual(record_received_node_name.delay(node_name='other').get(), 'other')
        self.assertEqual(record_received_node_name.for_node('other').delay().get(), 'other')

    def test_a_keyword_argument_that_the_function_does_not_take_is_refused(self):
        """
        Case: record_received_shard_id, whose function takes only shard_id, is sent with the shard_id of first and
              colour='blue'.
        Expected: TypeError before anything is sent, because the function does not take colour.
        """
        with self.assertRaises(TypeError):
            record_received_shard_id.delay(shard_id=self.first.id, colour='blue')

    @override_settings(QUILT_CELERY={'MAPPING_KWARG': 'customer_id'})
    def test_the_check_of_the_arguments_follows_the_mapping_kwarg_setting(self):
        """
        Case: QUILT_CELERY['MAPPING_KWARG'] is customer_id, and record_received_shard_id is sent with customer_id=2, and
              then with tenant_id=2.
        Expected: With customer_id, the task runs in remote and the function receives the shard_id of remote. With
                  tenant_id, TypeError, because tenant_id is then an ordinary keyword argument, which the function does
                  not take.
        """
        self.assertEqual(record_received_shard_id.delay(customer_id=2).get(), self.remote.id)

        with self.assertRaises(TypeError):
            record_received_shard_id.delay(tenant_id=2)

    @override_settings(CELERY_TASK_ALWAYS_EAGER=False)
    def test_a_task_that_is_decorated_with_typing_false_is_sent_without_a_check_of_its_arguments(self):
        """
        Case: A task whose function takes no arguments is decorated with typing=False, and is sent with the shard_id of
              first and a positional argument, while tasks do not run eagerly.
        Expected: The message is published, as Celery publishes a task with typing=False without a check.
        """

        @task_for_shard(name='plugin_tests.tasks.without_typing', typing=False)
        def without_typing():
            pass  # pragma: no cover

        with mock.patch.object(app, 'send_task') as send_task:
            without_typing.delay('anvil', shard_id=self.first.id)

        send_task.assert_called_once()


class SendingToATargetTestCase(PluginTestCase):
    """
    for_shard() and for_node() return an object that sends a task to the target that they are given, and the methods
    whose names end in _for_shard and _for_node send a task in the same way. The tasks run eagerly, as the test settings
    configure, unless a test turns that off.
    """

    def test_for_shard_sends_the_task_to_a_shard_instance(self):
        """
        Case: record_target.for_shard() is called with remote, and the task is sent with delay() and colour='blue'.
        Expected: It runs in the schema of remote, the header identifies remote, and the function receives colour.
        """
        result = record_target.for_shard(self.remote).delay(colour='blue').get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))
        self.assertEqual(result['header'], {'shard_id': self.remote.id})
        self.assertEqual(result['kwargs'], {'colour': 'blue'})

    def test_for_shard_sends_the_task_to_a_primary_key_and_to_a_mapping_value(self):
        """
        Case: record_target.for_shard() is called with the primary key of remote, and with tenant_id=1, which maps onto
              first. Each task is sent with apply_async().
        Expected: The first task runs in remote and the second in first.
        """
        by_primary_key = record_target.for_shard(self.remote.id).apply_async().get()
        by_mapping_value = record_target.for_shard(tenant_id=1).apply_async().get()

        self.assertEqual(node_and_schema_of(by_primary_key), ('other', 'remote_schema'))
        self.assertEqual(node_and_schema_of(by_mapping_value), ('default', 'first_schema'))

    @override_settings(CELERY_TASK_ALWAYS_EAGER=False)
    def test_apply_async_keeps_the_headers_and_the_options_of_the_caller(self):
        """
        Case: record_target.for_shard(remote).apply_async() is called with a header of the caller and countdown=10,
              while tasks do not run eagerly.
        Expected: The message is published with the header of the caller, the target header and the countdown.
        """
        with mock.patch.object(app, 'send_task') as send_task:
            record_target.for_shard(self.remote).apply_async(headers={'trace': 'abc'}, countdown=10)

        self.assertEqual(
            send_task.call_args.kwargs['headers'], {'trace': 'abc', TARGET_HEADER_NAME: {'shard_id': self.remote.id}}
        )
        self.assertEqual(send_task.call_args.kwargs['countdown'], 10)

    @override_settings(CELERY_TASK_ALWAYS_EAGER=False)
    def test_each_alias_sends_the_message_of_for_shard_and_for_node(self):
        """
        Case: Each method whose name ends in _for_shard is called for record_target and remote, and each method whose
              name ends in _for_node for record_node and the other node, with colour='blue', while tasks do not run
              eagerly and no transaction is open. Each one is compared with the method of the same name on the object of
              for_shard() or for_node().
        Expected: Both publish the same message.
        """
        calls = {
            'delay_for_shard': (
                lambda: record_target.delay_for_shard(self.remote, colour='blue'),
                lambda: record_target.for_shard(self.remote).delay(colour='blue'),
            ),
            'apply_async_for_shard': (
                lambda: record_target.apply_async_for_shard(self.remote, kwargs={'colour': 'blue'}, countdown=5),
                lambda: record_target.for_shard(self.remote).apply_async(kwargs={'colour': 'blue'}, countdown=5),
            ),
            'delay_on_commit_for_shard': (
                lambda: record_target.delay_on_commit_for_shard(self.remote, colour='blue'),
                lambda: record_target.for_shard(self.remote).delay_on_commit(colour='blue'),
            ),
            'apply_async_on_commit_for_shard': (
                lambda: record_target.apply_async_on_commit_for_shard(self.remote, kwargs={'colour': 'blue'}),
                lambda: record_target.for_shard(self.remote).apply_async_on_commit(kwargs={'colour': 'blue'}),
            ),
            'delay_for_node': (
                lambda: record_node.delay_for_node('other', colour='blue'),
                lambda: record_node.for_node('other').delay(colour='blue'),
            ),
            'apply_async_for_node': (
                lambda: record_node.apply_async_for_node('other', kwargs={'colour': 'blue'}, countdown=5),
                lambda: record_node.for_node('other').apply_async(kwargs={'colour': 'blue'}, countdown=5),
            ),
            'delay_on_commit_for_node': (
                lambda: record_node.delay_on_commit_for_node('other', colour='blue'),
                lambda: record_node.for_node('other').delay_on_commit(colour='blue'),
            ),
            'apply_async_on_commit_for_node': (
                lambda: record_node.apply_async_on_commit_for_node('other', kwargs={'colour': 'blue'}),
                lambda: record_node.for_node('other').apply_async_on_commit(kwargs={'colour': 'blue'}),
            ),
        }

        for name, (alias, method) in calls.items():
            with self.subTest(alias=name):
                with mock.patch.object(app, 'send_task') as send_task:
                    alias()
                    method()

                alias_call, method_call = send_task.call_args_list
                self.assertEqual(alias_call, method_call)
                self.assertIn(TARGET_HEADER_NAME, alias_call.kwargs['headers'])

    def test_a_task_keyword_argument_named_shard_reaches_the_function(self):
        """
        Case: record_schema_and_shard, whose function takes a keyword argument named shard, is sent with
              delay_for_shard() for remote and shard='blue'.
        Expected: It runs in the schema of remote, and the function receives shard='blue'.
        """
        result = record_schema_and_shard.delay_for_shard(self.remote, shard='blue').get()

        self.assertEqual(result, {'schema_name': 'remote_schema', 'shard': 'blue'})

    def test_signatures_of_for_shard_carry_the_target(self):
        """
        Case: A group of a signature from for_shard(first).s() and an immutable signature from for_shard(remote).si() of
              record_target, and a signature from for_shard(first).signature(), are sent.
        Expected: Each one runs in the shard that for_shard() is given.
        """
        group_results = group(
            record_target.for_shard(self.first).s(), record_target.for_shard(self.remote).si()
        ).apply_async()
        signature_result = record_target.for_shard(self.first).signature(kwargs={'colour': 'blue'}).delay().get()

        self.assertEqual(
            [node_and_schema_of(result) for result in group_results.get()],
            [('default', 'first_schema'), ('other', 'remote_schema')],
        )
        self.assertEqual(node_and_schema_of(signature_result), ('default', 'first_schema'))
        self.assertEqual(signature_result['kwargs'], {'colour': 'blue'})

    def test_for_shard_without_a_shard_or_with_two_shards_raises(self):
        """
        Case: record_target.for_shard() is called without a shard, with None, and with first and tenant_id=2.
        Expected: TaskShardError for each, whose message includes the task.
        """
        calls = {
            'without a shard': lambda: record_target.for_shard(),
            'with None': lambda: record_target.for_shard(None),
            'with two shards': lambda: record_target.for_shard(self.first, tenant_id=2),
        }

        for name, call in calls.items():
            with self.subTest(call=name):
                with self.assertRaisesMessage(TaskShardError, 'example.tasks.record_target'):
                    call()

    def test_for_shard_with_a_shard_and_a_mapping_value_of_none_sends_the_task_to_the_shard(self):
        """
        Case: record_target.for_shard() is called with remote and tenant_id=None, as a caller passes on an optional
              tenant.
        Expected: The task runs in remote, because a keyword argument whose value is None does not identify a shard.
        """
        result = record_target.for_shard(self.remote, tenant_id=None).delay().get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))

    def test_for_shard_refuses_a_keyword_argument_that_does_not_identify_a_shard(self):
        """
        Case: record_target.for_shard() is called with colour='blue'.
        Expected: TypeError, whose message includes colour and the keyword argument for a mapping value.
        """
        with self.assertRaisesMessage(TypeError, 'colour, and it accepts a shard or tenant_id'):
            record_target.for_shard(colour='blue')

    def test_for_shard_refuses_a_model_instance_that_is_not_a_shard(self):
        """
        Case: record_target.for_shard() is called with an instance of TenantShard, the mapping model.
        Expected: TypeError, whose message includes the class of the instance.
        """
        with self.assertRaisesMessage(TypeError, 'TenantShard'):
            record_target.for_shard(TenantShard.objects.get(tenant_id=1))

    def test_for_node_without_a_node_raises(self):
        """
        Case: record_node.for_node() is called with None.
        Expected: TaskNodeError, whose message includes the task.
        """
        with self.assertRaisesMessage(TaskNodeError, 'example.tasks.record_node'):
            record_node.for_node(None)

    def test_a_target_in_the_keyword_arguments_as_well_is_refused(self):
        """
        Case: record_target.for_shard(first) is sent with the shard_id of remote, and with tenant_id=2. A signature of
              it is built with the shard_id of remote. record_node.for_node('other') is sent with node_name='default'.
        Expected: TaskShardError for each shard task and TaskNodeError for the node task, before anything is sent,
                  because each task then has two targets.
        """
        sender = record_target.for_shard(self.first)
        calls = {
            'delay with shard_id': lambda: sender.delay(shard_id=self.remote.id),
            'apply_async with tenant_id': lambda: sender.apply_async(kwargs={'tenant_id': 2}),
            'signature with shard_id': lambda: sender.s(shard_id=self.remote.id),
        }

        for name, call in calls.items():
            with self.subTest(call=name):
                with self.assertRaisesMessage(TaskShardError, 'sent through for_shard()'):
                    call()

        with self.assertRaisesMessage(TaskNodeError, 'sent through for_node()'):
            record_node.for_node('other').delay(node_name='default')

    def test_for_node_and_for_shard_refuse_a_task_of_the_other_kind(self):
        """
        Case: for_shard() is called for record_node, which runs on a node, and for_node() for record_target, which runs
              in a shard.
        Expected: TypeError for each, whose message points at the method for the task.
        """
        with self.assertRaisesMessage(TypeError, 'example.tasks.record_node runs on a node'):
            record_node.for_shard(self.first)

        with self.assertRaisesMessage(TypeError, 'example.tasks.record_target runs in a shard'):
            record_target.for_node('other')


@override_settings(CELERY_TASK_EAGER_PROPAGATES=False)
class StateRetryTestCase(PluginTestCase):
    """
    An eager task retries only when its exceptions do not propagate, because Celery raises the Retry of an eager task
    when they propagate. Eager retries run at once, so each test reads the countdown of each retry from the task_retry
    signal.
    """

    def setUp(self):
        super().setUp()
        self.retries = []
        signals.task_retry.connect(self.record_retry)
        self.addCleanup(signals.task_retry.disconnect, self.record_retry)

    def record_retry(self, sender, request, reason, **kwargs):
        self.retries.append((reason.when, type(reason.exc)))

    def test_a_shard_in_maintenance_is_retried_with_backoff_until_the_retries_run_out(self):
        """
        Case: record_target is sent for maintained, which is in maintenance during every retry.
        Expected: Five retries, after 5, 10, 20, 40 and 60 seconds, each for a StateException. Then the task fails with
                  the StateException.
        """
        result = record_target.delay(shard_id=self.maintained.id)

        with self.assertRaises(StateException):
            result.get()

        self.assertEqual(
            self.retries,
            [
                (5, StateException),
                (10, StateException),
                (20, StateException),
                (40, StateException),
                (60, StateException),
            ],
        )

    def test_a_shard_that_becomes_active_runs_the_task(self):
        """
        Case: record_target is sent for maintained, which becomes active during the second retry.
        Expected: The task runs in maintained after two retries.
        """

        def make_active_on_second_retry(**kwargs):
            if len(self.retries) == 2:
                self.make_shard_active(self.maintained)

        signals.task_retry.connect(make_active_on_second_retry)
        self.addCleanup(signals.task_retry.disconnect, make_active_on_second_retry)

        result = record_target.delay(shard_id=self.maintained.id).get()

        self.assertEqual((node_and_schema_of(result), result['retries']), (('default', 'maintained_schema'), 2))

    def test_the_decorator_sets_the_retries_and_the_backoff(self):
        """
        Case: record_target_with_one_retry, decorated with max_retries=1 and retry_backoff=2, is sent for maintained.
        Expected: One retry, after 2 seconds, and then the StateException.
        """
        result = record_target_with_one_retry.delay(shard_id=self.maintained.id)

        with self.assertRaises(StateException):
            result.get()

        self.assertEqual(self.retries, [(2, StateException)])

    def test_a_task_that_is_called_directly_raises_at_once(self):
        """
        Case: record_target is called as a function for maintained.
        Expected: The StateException is raised without a retry, as Celery raises the exception of a task that is called
                  directly.
        """
        with self.assertRaises(StateException):
            record_target(shard_id=self.maintained.id)

        self.assertEqual(self.retries, [])


class DecoratorOptionsTestCase(PluginTestCase):
    @override_settings(
        QUILT_CELERY={'MAX_RETRIES': 2, 'RETRY_BACKOFF': 3, 'RETRY_BACKOFF_MAX': 30, 'RETRY_JITTER': True}
    )
    def test_the_settings_give_the_defaults_of_the_retries(self):
        """
        Case: A task is decorated while QUILT_CELERY sets every retry setting.
        Expected: The task has the values of the settings.
        """

        @task_for_shard(name='plugin_tests.tasks.retry_settings')
        def retry_settings():
            pass  # pragma: no cover

        self.assertEqual(
            (
                retry_settings.max_retries,
                retry_settings.retry_backoff,
                retry_settings.retry_backoff_max,
                retry_settings.retry_jitter,
            ),
            (2, 3, 30, True),
        )

    def test_the_default_retries(self):
        """
        Case: A task is decorated without retry options, and QUILT_CELERY sets none.
        Expected: Five retries with a backoff of 5 seconds, at most 60 seconds, without jitter.
        """

        @task_for_shard(name='plugin_tests.tasks.retry_defaults')
        def retry_defaults():
            pass  # pragma: no cover

        self.assertEqual(
            (
                retry_defaults.max_retries,
                retry_defaults.retry_backoff,
                retry_defaults.retry_backoff_max,
                retry_defaults.retry_jitter,
            ),
            (5, 5, 60, False),
        )

    @override_settings(QUILT_CELERY=None)
    def test_a_setting_that_is_not_a_dict_gives_the_default_retries(self):
        """
        Case: A task is decorated while QUILT_CELERY is None, before the system check reports the setting, as a tasks
              module can be imported before the system checks run.
        Expected: The task is created, with five retries with a backoff of 5 seconds, at most 60 seconds, without
                  jitter.
        """

        @task_for_shard(name='plugin_tests.tasks.retry_defaults_without_a_dict')
        def retry_defaults_without_a_dict():
            pass  # pragma: no cover

        self.assertEqual(
            (
                retry_defaults_without_a_dict.max_retries,
                retry_defaults_without_a_dict.retry_backoff,
                retry_defaults_without_a_dict.retry_backoff_max,
                retry_defaults_without_a_dict.retry_jitter,
            ),
            (5, 5, 60, False),
        )

    def test_one_decorator_with_options_can_decorate_two_functions(self):
        """
        Case: task_for_shard(bind=True) is called once, and the decorator that it returns decorates two functions.
        Expected: Both functions receive the task, so the first decoration leaves the options of the second unchanged.
        """
        bound_task = task_for_shard(bind=True)

        @bound_task
        def first_bound_function(self, **kwargs):
            return self.name

        @bound_task
        def second_bound_function(self, **kwargs):
            return self.name

        self.assertEqual(first_bound_function(shard_id=self.first.id), first_bound_function.name)
        self.assertEqual(second_bound_function(shard_id=self.first.id), second_bound_function.name)

    def test_the_autoretry_exceptions_of_the_decorator_are_added_to_state_exception(self):
        """
        Case: A task is decorated with autoretry_for=(KeyError,).
        Expected: The task retries StateException and KeyError.
        """

        @task_for_shard(name='plugin_tests.tasks.retry_key_errors', autoretry_for=(KeyError,))
        def retry_key_errors():
            pass  # pragma: no cover

        self.assertEqual(retry_key_errors.autoretry_for, (StateException, KeyError))

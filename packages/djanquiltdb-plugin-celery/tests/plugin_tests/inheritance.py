from unittest import mock

from django.db import transaction
from django.test import override_settings
from djanquiltdb.utils import use_shard, use_shard_for

from config.celery import app
from djanquiltdb_plugin_celery.targets import TaskShardError
from example.models import Widget
from example.tasks import (
    create_widget,
    create_widget_in_active_shard,
    record_inherited_target,
    record_target,
    record_target_that_inherits,
)
from plugin_tests.base import PluginTestCase
from plugin_tests.tasks import node_and_schema_of


class InheritedTargetTestCase(PluginTestCase):
    """
    The tasks run eagerly, as the test settings configure, unless a test turns that off.
    """

    def test_a_task_that_inherits_runs_in_the_active_shard(self):
        """
        Case: record_inherited_target, decorated with task_for_active_shard, is sent without a target inside use_shard()
              for remote.
        Expected: It runs in the schema of remote, and the header identifies remote by its primary key.
        """
        with use_shard(self.remote):
            result = record_inherited_target.delay().get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))
        self.assertEqual(result['header'], {'shard_id': self.remote.id})
        self.assertEqual(result['kwargs'], {})

    def test_a_shard_that_is_entered_for_a_mapping_value_is_inherited_as_that_mapping_value(self):
        """
        Case: record_inherited_target is sent without a target inside use_shard_for() for tenant 2.
        Expected: It runs in the schema of remote, and the header identifies the shard by the mapping value 2.
        """
        with use_shard_for(2):
            result = record_inherited_target.delay().get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))
        self.assertEqual(result['header'], {'mapping_value': 2})

    def test_the_public_schema_of_a_node_is_not_inherited(self):
        """
        Case: record_inherited_target is sent without a target inside use_shard() for the public schema of the other
              node.
        Expected: TaskShardError, because the public schema of a node is not a shard.
        """
        with use_shard(node_name='other', schema_name='public'):
            with self.assertRaisesMessage(TaskShardError, 'example.tasks.record_inherited_target has no shard'):
                record_inherited_target.delay()

    def test_the_keyword_arguments_take_precedence_over_the_active_shard(self):
        """
        Case: record_inherited_target is sent with the shard_id of remote inside use_shard() for first.
        Expected: It runs in the schema of remote.
        """
        with use_shard(self.first):
            result = record_inherited_target.delay(shard_id=self.remote.id).get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))

    @override_settings(CELERY_TASK_ALWAYS_EAGER=False)
    def test_a_task_that_inherits_is_not_published_outside_a_shard(self):
        """
        Case: record_inherited_target is sent without a target while no shard is active, and tasks do not run eagerly.
        Expected: TaskShardError, whose message includes the task, before anything is published.
        """
        with mock.patch.object(app, 'send_task') as send_task:
            with self.assertRaisesMessage(TaskShardError, 'example.tasks.record_inherited_target has no shard'):
                record_inherited_target.delay()

        send_task.assert_not_called()

    def test_task_for_shard_inherits_when_it_is_told_to(self):
        """
        Case: record_target_that_inherits, decorated with task_for_shard(inherit_active_shard=True), is sent without a
              target inside use_shard() for remote.
        Expected: It runs in the schema of remote, as a task that is decorated with task_for_active_shard does.
        """
        with use_shard(self.remote):
            result = record_target_that_inherits.delay().get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))

    def test_task_for_shard_does_not_inherit_by_default(self):
        """
        Case: record_target, decorated with task_for_shard, is sent without a target inside use_shard() for remote.
        Expected: TaskShardError, because the task does not inherit the active shard.
        """
        with use_shard(self.remote):
            with self.assertRaises(TaskShardError):
                record_target.delay()

    def test_a_signature_inherits_the_shard_that_is_active_where_it_is_built(self):
        """
        Case: A signature of record_inherited_target is built inside use_shard() for remote, and sent after use_shard()
              exits, as the worker sends a later task of a chain.
        Expected: It runs in the schema of remote, and the header identifies remote by its primary key.
        """
        with use_shard(self.remote):
            signature = record_inherited_target.s()

        result = signature.delay().get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))
        self.assertEqual(result['header'], {'shard_id': self.remote.id})

    def test_the_keyword_arguments_at_the_send_take_precedence_over_the_shard_that_a_signature_inherited(self):
        """
        Case: A signature of record_inherited_target is built inside use_shard() for remote, and sent with the shard_id
              of first.
        Expected: It runs in the schema of first, because the keyword arguments take precedence over the shard that the
                  signature inherits.
        """
        with use_shard(self.remote):
            signature = record_inherited_target.s()

        result = signature.delay(shard_id=self.first.id).get()

        self.assertEqual(node_and_schema_of(result), ('default', 'first_schema'))

    def test_a_task_that_inherits_and_is_called_directly_runs_in_the_active_shard(self):
        """
        Case: record_inherited_target is called as a function without a target inside use_shard() for remote, and inside
              use_shard_for() for tenant 1.
        Expected: It runs in the schema of remote, and then in the schema of first, without a message.
        """
        with use_shard(self.remote):
            remote_result = record_inherited_target()

        with use_shard_for(1):
            first_result = record_inherited_target()

        self.assertEqual(node_and_schema_of(remote_result), ('other', 'remote_schema'))
        self.assertEqual(node_and_schema_of(first_result), ('default', 'first_schema'))

    def test_a_task_that_inherits_and_is_called_directly_outside_a_shard_raises(self):
        """
        Case: record_inherited_target is called as a function without a target while no shard is active.
        Expected: TaskShardError, whose message includes the task.
        """
        with self.assertRaisesMessage(TaskShardError, 'example.tasks.record_inherited_target has no shard'):
            record_inherited_target()


class SendingOnCommitTestCase(PluginTestCase):
    def test_delay_on_commit_inherits_the_shard_that_is_active_when_it_is_called(self):
        """
        Case: create_widget_in_active_shard.delay_on_commit() is called inside use_shard() for first, inside a
              transaction that commits after use_shard() exits.
        Expected: No widget before the commit. After the commit, the widget is in first, although no shard is active
                  then.
        """
        with transaction.atomic():
            with use_shard(self.first):
                create_widget_in_active_shard.delay_on_commit('anvil')
                self.assertFalse(Widget.objects.exists())

        with use_shard(self.first):
            self.assertEqual(list(Widget.objects.values_list('name', flat=True)), ['anvil'])

    def test_apply_async_on_commit_sends_the_task_after_the_commit(self):
        """
        Case: create_widget.apply_async_on_commit() is called with the shard_id of remote inside a transaction.
        Expected: The widget is in remote after the commit.
        """
        with transaction.atomic():
            create_widget.apply_async_on_commit(('anvil',), {'shard_id': self.remote.id})

        with use_shard(self.remote):
            self.assertEqual(list(Widget.objects.values_list('name', flat=True)), ['anvil'])

    def test_apply_async_on_commit_sends_the_task_at_once_when_no_transaction_is_open(self):
        """
        Case: create_widget.apply_async_on_commit() is called with the shard_id of remote while no transaction is open
              on any node.
        Expected: The widget is in remote right after the call.
        """
        create_widget.apply_async_on_commit(('anvil',), {'shard_id': self.remote.id})

        with use_shard(self.remote):
            self.assertEqual(list(Widget.objects.values_list('name', flat=True)), ['anvil'])

    def test_delay_on_commit_waits_for_a_transaction_on_another_node(self):
        """
        Case: create_widget_in_active_shard.delay_on_commit() is called inside use_shard() for remote, on the other
              node, inside a transaction that is opened on the primary node before use_shard().
        Expected: No widget in remote before the transaction commits, and the widget after the commit, because the task
                  waits for the transactions that are open at the call on every node.
        """
        with transaction.atomic():
            with use_shard(self.remote):
                create_widget_in_active_shard.delay_on_commit('anvil')
                self.assertFalse(Widget.objects.exists())

        with use_shard(self.remote):
            self.assertEqual(list(Widget.objects.values_list('name', flat=True)), ['anvil'])

    def test_delay_on_commit_sends_nothing_when_a_transaction_on_another_node_rolls_back(self):
        """
        Case: create_widget_in_active_shard.delay_on_commit() is called inside use_shard() for remote, on the other
              node, inside a transaction that is opened on the primary node before use_shard() and then rolls back.
        Expected: No widget in remote, because the task is not sent.
        """

        class RollBack(Exception):
            pass

        with self.assertRaises(RollBack):
            with transaction.atomic():
                with use_shard(self.remote):
                    create_widget_in_active_shard.delay_on_commit('anvil')

                raise RollBack

        with use_shard(self.remote):
            self.assertFalse(Widget.objects.exists())

    def test_apply_async_on_commit_sends_nothing_when_its_savepoint_rolls_back_on_one_of_two_nodes(self):
        """
        Case: create_widget.apply_async_on_commit() is called with the shard_id of remote inside a savepoint on the
              primary node, while a transaction is open on the other node and the primary node. The savepoint rolls
              back, the exception is caught, and both transactions commit.
        Expected: No widget in remote, because the call is rolled back with its savepoint.
        """

        class RollBack(Exception):
            pass

        with transaction.atomic(using='default'):
            with transaction.atomic(using='other'):
                with self.assertRaises(RollBack):
                    with transaction.atomic(using='default'):
                        create_widget.apply_async_on_commit(('anvil',), {'shard_id': self.remote.id})
                        raise RollBack

        with use_shard(self.remote):
            self.assertFalse(Widget.objects.exists())

    def test_apply_async_on_commit_sends_nothing_when_one_of_two_nodes_rolls_back_before_the_other_commits(self):
        """
        Case: create_widget.apply_async_on_commit() is called with the shard_id of remote while a transaction is open on
              the other node and the primary node. The transaction on the primary node rolls back, the exception is
              caught, and the transaction on the other node commits.
        Expected: No widget in remote, because one of the transactions that are open at the call rolls back.
        """

        class RollBack(Exception):
            pass

        with transaction.atomic(using='other'):
            with self.assertRaises(RollBack):
                with transaction.atomic(using='default'):
                    create_widget.apply_async_on_commit(('anvil',), {'shard_id': self.remote.id})
                    raise RollBack

        with use_shard(self.remote):
            self.assertFalse(Widget.objects.exists())

    def test_delay_on_commit_waits_for_a_transaction_on_both_nodes(self):
        """
        Case: create_widget_in_active_shard.delay_on_commit() is called inside transaction.atomic(), which is entered
              inside use_shard() for remote, and so opens a transaction on the other node and on the primary node.
        Expected: No widget in remote before the transaction commits, and the widget after it.
        """
        with use_shard(self.remote):
            with transaction.atomic():
                create_widget_in_active_shard.delay_on_commit('anvil')
                self.assertFalse(Widget.objects.exists())

            self.assertEqual(list(Widget.objects.values_list('name', flat=True)), ['anvil'])

    def test_delay_on_commit_for_shard_sends_the_task_to_its_shard_after_the_commit(self):
        """
        Case: create_widget.delay_on_commit_for_shard() is called for remote inside use_shard() for first, inside a
              transaction that commits after use_shard() exits.
        Expected: No widget before the commit. After the commit, the widget is in remote and not in first.
        """
        with transaction.atomic():
            with use_shard(self.first):
                create_widget.delay_on_commit_for_shard(self.remote, 'anvil')

            with use_shard(self.remote):
                self.assertFalse(Widget.objects.exists())

        with use_shard(self.remote):
            self.assertEqual(list(Widget.objects.values_list('name', flat=True)), ['anvil'])

        with use_shard(self.first):
            self.assertFalse(Widget.objects.exists())

    def test_apply_async_on_commit_of_for_shard_sends_nothing_when_the_transaction_rolls_back(self):
        """
        Case: create_widget.for_shard(remote).apply_async_on_commit() is called inside a transaction that rolls back.
        Expected: No widget in remote, because the task is not sent.
        """

        class RollBack(Exception):
            pass

        with self.assertRaises(RollBack):
            with transaction.atomic():
                create_widget.for_shard(self.remote).apply_async_on_commit(('anvil',))
                raise RollBack

        with use_shard(self.remote):
            self.assertFalse(Widget.objects.exists())

    def test_delay_on_commit_without_a_target_raises_when_it_is_called(self):
        """
        Case: create_widget_in_active_shard.delay_on_commit() is called inside a transaction while no shard is active.
        Expected: TaskShardError at the call, and not when the transaction commits.
        """
        with transaction.atomic():
            with self.assertRaises(TaskShardError):
                create_widget_in_active_shard.delay_on_commit('anvil')

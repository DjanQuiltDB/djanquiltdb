from celery import chain
from celery.contrib.testing.worker import start_worker
from django.test import override_settings
from djanquiltdb.router import get_active_connection
from djanquiltdb.utils import use_shard

from config.celery import app
from djanquiltdb_plugin_celery.targets import TaskShardError
from example.tasks import record_inherited_target, record_node, record_target
from plugin_tests.base import PluginTestCase
from plugin_tests.tasks import node_and_schema_of

#: How long a test waits for the result of a task that the worker runs.
RESULT_TIMEOUT = 10


@override_settings(CELERY_TASK_ALWAYS_EAGER=False)
class WorkerTestCase(PluginTestCase):
    """
    A worker runs in a thread of the test process, and receives the messages that the tests publish through the memory
    broker. The worker thread has an active connection of its own, so a task runs in a shard only through the target in
    its message.
    """

    def setUp(self):
        super().setUp()
        # The worker logs the failure of a task at the ERROR level. The log level is CRITICAL, so that a test that
        # expects the failure does not print that log.
        self.enterContext(
            start_worker(app, pool='solo', loglevel='CRITICAL', perform_ping_check=False, shutdown_timeout=20)
        )

    def test_the_worker_runs_a_task_in_the_shard_of_its_shard_id(self):
        """
        Case: record_target is published with the shard_id of remote, on the other node.
        Expected: The worker runs it in the schema of remote on the other node.
        """
        result = record_target.delay(shard_id=self.remote.id).get(timeout=RESULT_TIMEOUT)

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))
        self.assertEqual(result['header'], {'shard_id': self.remote.id})

    def test_the_worker_runs_a_task_in_the_shard_of_for_shard(self):
        """
        Case: record_target is published with for_shard(remote).delay(), and with delay_for_shard() for first.
        Expected: The worker runs the first task in the schema of remote and the second in the schema of first, and the
                  function receives no shard_id.
        """
        remote_result = record_target.for_shard(self.remote).delay().get(timeout=RESULT_TIMEOUT)
        first_result = record_target.delay_for_shard(self.first).get(timeout=RESULT_TIMEOUT)

        self.assertEqual(node_and_schema_of(remote_result), ('other', 'remote_schema'))
        self.assertEqual(node_and_schema_of(first_result), ('default', 'first_schema'))
        self.assertEqual(remote_result['kwargs'], {})

    def test_the_worker_runs_a_node_task_in_the_public_schema_of_its_node(self):
        """
        Case: record_node is published with node_name='other'.
        Expected: The worker runs it in the public schema of the other node.
        """
        result = record_node.delay(node_name='other').get(timeout=RESULT_TIMEOUT)

        self.assertEqual(node_and_schema_of(result), ('other', 'public'))

    def test_the_worker_runs_a_task_that_inherits_in_the_shard_of_the_sender(self):
        """
        Case: record_inherited_target is published without a target inside use_shard() for first.
        Expected: The worker runs it in the schema of first, and first is active in the sending thread when the result
                  arrives.
        """
        with use_shard(self.first):
            result = record_inherited_target.delay().get(timeout=RESULT_TIMEOUT)

            self.assertEqual(get_active_connection().schema_name, 'first_schema')

        self.assertEqual(node_and_schema_of(result), ('default', 'first_schema'))

    def test_the_worker_runs_each_task_of_a_chain_in_its_own_target(self):
        """
        Case: A chain of record_target with the shard_id of first and record_inherited_target without a target is
              published inside use_shard() for remote. The worker sends the second task after the first has run.
        Expected: The first task runs in the schema of first, and the second in the schema of remote, which is active
                  where the chain is built.
        """
        with use_shard(self.remote):
            result = chain(record_target.si(shard_id=self.first.id), record_inherited_target.si()).delay()

        self.assertEqual(node_and_schema_of(result.get(timeout=RESULT_TIMEOUT)), ('other', 'remote_schema'))
        self.assertEqual(node_and_schema_of(result.parent.get(timeout=RESULT_TIMEOUT)), ('default', 'first_schema'))

    def test_the_worker_runs_a_link_callback_in_the_shard_that_it_inherited(self):
        """
        Case: record_target is published with the shard_id of first and record_inherited_target as its link callback,
              both inside use_shard() for remote. The worker sends the callback after record_target has run.
        Expected: The callback runs in the schema of remote, which is active where the callback is built.
        """
        with use_shard(self.remote):
            callback = record_inherited_target.si()
            callback.freeze()
            record_target.apply_async(kwargs={'shard_id': self.first.id}, link=callback)

        self.assertEqual(
            node_and_schema_of(app.AsyncResult(callback.id).get(timeout=RESULT_TIMEOUT)), ('other', 'remote_schema')
        )

    def test_a_message_without_a_target_fails_in_the_worker(self):
        """
        Case: A message for record_target is published by name with send_task(), without keyword arguments or a target
              header.
        Expected: The task fails with TaskShardError in the worker.
        """
        result = app.send_task(record_target.name)

        with self.assertRaises(TaskShardError):
            result.get(timeout=RESULT_TIMEOUT)

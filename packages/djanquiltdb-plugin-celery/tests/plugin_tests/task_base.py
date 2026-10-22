from unittest import mock

from celery import Celery
from celery.contrib.testing.worker import start_worker
from django.test import override_settings
from djanquiltdb.router import get_active_connection

from config.celery import app
from djanquiltdb_plugin_celery.decorators import task_for_shard
from djanquiltdb_plugin_celery.tasks import QuiltTask, quilt_task_based_on
from example.task_bases import RECORDED_CALLS, ProjectQuiltTask, RecordingCallTask
from plugin_tests.base import PluginTestCase
from plugin_tests.worker import RESULT_TIMEOUT


def schema_of_the_active_connection(**kwargs):
    return get_active_connection().schema_name


def task_named(name, **options):
    """
    Returns a task of schema_of_the_active_connection, decorated with task_for_shard under the given name. The task is
    read through its proxy, so that the app of the tests registers it before this function returns: an app registers a
    task with the task class that the app has at that moment.
    """
    task = task_for_shard(name=f'plugin_tests.task_base.{name}', **options)(schema_of_the_active_connection)
    task._get_current_object()
    return task


class TaskBaseTestCase(PluginTestCase):
    """
    Covers the base class of a task whose decorator is not given a base: the task class of the Celery app combined with
    QuiltTask, or the class that QUILT_CELERY['TASK_BASE'] names.
    """

    def setUp(self):
        super().setUp()
        RECORDED_CALLS.clear()

    def test_a_task_of_an_app_without_a_task_class_of_its_own_has_quilt_task_as_its_base(self):
        """
        Case: A task is decorated in the test app, which sets no task_cls, so Celery's Django fixup gives it DjangoTask.
        Expected: The base of the task class is QuiltTask itself, which already inherits from DjangoTask.
        """
        task = task_named('in_a_stock_app')

        self.assertEqual(task.__class__.__bases__, (QuiltTask,))

    def test_a_task_inherits_from_the_task_class_of_the_app_and_runs_its_call(self):
        """
        Case: The task class of the app is RecordingCallTask, a DjangoTask that records each run in its __call__. A task
              is decorated without a base, and sent for first.
        Expected: The task is a QuiltTask and a RecordingCallTask, runs in first, and RecordingCallTask records its run.
        """
        with mock.patch.object(app, 'task_cls', 'example.task_bases:RecordingCallTask'):
            task = task_named('in_a_recording_app')

        result = task.delay(shard_id=self.first.id).get()

        self.assertIsInstance(task, QuiltTask)
        self.assertIsInstance(task, RecordingCallTask)
        self.assertEqual(result, 'first_schema')
        self.assertEqual(RECORDED_CALLS, ['plugin_tests.task_base.in_a_recording_app'])

    def test_the_tasks_of_one_app_share_their_base(self):
        """
        Case: Two tasks are decorated without a base while the task class of the app is RecordingCallTask.
        Expected: Both have the same base, the class that combines QuiltTask with RecordingCallTask, so the class is
                  created once.
        """
        with mock.patch.object(app, 'task_cls', RecordingCallTask):
            first_task = task_named('first_of_two')
            second_task = task_named('second_of_two')

        self.assertEqual(first_task.__class__.__bases__, (quilt_task_based_on(RecordingCallTask),))
        self.assertEqual(second_task.__class__.__bases__, (quilt_task_based_on(RecordingCallTask),))

    def test_a_task_class_of_the_app_that_inherits_from_quilt_task_is_the_base_as_it_is(self):
        """
        Case: The task class of the app is ProjectQuiltTask, which already inherits from QuiltTask.
        Expected: The base of the task class is ProjectQuiltTask itself.
        """
        with mock.patch.object(app, 'task_cls', ProjectQuiltTask):
            task = task_named('in_a_quilt_app')

        self.assertEqual(task.__class__.__bases__, (ProjectQuiltTask,))

    def test_an_app_that_is_created_after_a_task_is_decorated_gives_the_task_its_task_class(self):
        """
        Case: A task is decorated without a base, and then a second Celery app is created, whose task class is
              RecordingCallTask, as for a tasks module that is imported before the Celery app of the project exists.
        Expected: The second app has the task as a QuiltTask and a RecordingCallTask, and the app of the tests has it
                  as a QuiltTask that is not a RecordingCallTask: each app gives the task its own task class.
        """
        task = task_named('before_the_app')
        later_app = Celery('later_app', task_cls=RecordingCallTask, set_as_current=False)
        later_app.finalize()

        task_of_the_later_app = later_app.tasks['plugin_tests.task_base.before_the_app']

        self.assertIsInstance(task_of_the_later_app, QuiltTask)
        self.assertIsInstance(task_of_the_later_app, RecordingCallTask)
        self.assertIsInstance(task, QuiltTask)
        self.assertNotIsInstance(task, RecordingCallTask)

    @override_settings(QUILT_CELERY={'TASK_BASE': 'example.task_bases.ProjectQuiltTask'})
    def test_the_setting_gives_the_base_instead_of_the_task_class_of_the_app(self):
        """
        Case: QUILT_CELERY['TASK_BASE'] names ProjectQuiltTask, and the task class of the app is RecordingCallTask.
        Expected: The base of the task class is ProjectQuiltTask, so the task does not inherit from RecordingCallTask.
        """
        with mock.patch.object(app, 'task_cls', RecordingCallTask):
            task = task_named('with_the_setting')

        self.assertEqual(task.__class__.__bases__, (ProjectQuiltTask,))
        self.assertNotIsInstance(task, RecordingCallTask)

    @override_settings(QUILT_CELERY={'TASK_BASE': 'example.task_bases.ProjectQuiltTask'})
    def test_the_base_of_the_decorator_takes_precedence_over_the_setting(self):
        """
        Case: QUILT_CELERY['TASK_BASE'] names ProjectQuiltTask, and a task is decorated with a base of its own.
        Expected: The base of the task class is the base of the decorator.
        """

        class BaseOfTheDecorator(QuiltTask):
            pass

        task = task_named('with_a_base_of_the_decorator', base=BaseOfTheDecorator)

        self.assertEqual(task.__class__.__bases__, (BaseOfTheDecorator,))

    @override_settings(QUILT_CELERY={'TASK_BASE': 'celery.Task'})
    def test_a_setting_that_names_a_class_that_does_not_inherit_from_quilt_task_is_refused(self):
        """
        Case: QUILT_CELERY['TASK_BASE'] names celery.Task, and a task is decorated without a base.
        Expected: TypeError at the decoration, as for a base of the decorator that does not inherit from QuiltTask.
        """
        with self.assertRaisesMessage(TypeError, 'QuiltTask'):
            task_named('with_a_plain_setting')


@override_settings(CELERY_TASK_ALWAYS_EAGER=False)
class TaskBaseWorkerTestCase(PluginTestCase):
    def setUp(self):
        super().setUp()
        RECORDED_CALLS.clear()

        # The worker learns the tasks of the app when it starts, so the task is created and registered before the worker
        # starts.
        with mock.patch.object(app, 'task_cls', RecordingCallTask):
            self.task = task_named('on_the_worker')

        self.enterContext(
            start_worker(app, pool='solo', loglevel='CRITICAL', perform_ping_check=False, shutdown_timeout=20)
        )

    def test_the_worker_runs_the_call_of_the_task_class_of_the_app_in_the_shard_of_the_task(self):
        """
        Case: The task class of the app is RecordingCallTask. A task is decorated without a base, and published for
              remote.
        Expected: The worker runs the task in remote, and RecordingCallTask records the run, so the header reaches the
                  task through the __call__ of the project's task class.
        """
        result = self.task.delay(shard_id=self.remote.id).get(timeout=RESULT_TIMEOUT)

        self.assertEqual(result, 'remote_schema')
        self.assertEqual(RECORDED_CALLS, ['plugin_tests.task_base.on_the_worker'])

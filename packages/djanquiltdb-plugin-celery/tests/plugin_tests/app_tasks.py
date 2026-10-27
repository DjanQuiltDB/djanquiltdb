from unittest import mock

from celery import Celery
from celery.contrib.django.task import DjangoTask
from celery.local import PromiseProxy
from django.test import SimpleTestCase
from djanquiltdb.base.celery import QuiltCelery
from djanquiltdb.decorators import celery
from djanquiltdb.decorators.celery import shared_task_for_shard
from djanquiltdb.router import get_active_connection
from djanquiltdb.utils import use_shard

from config.celery import app
from djanquiltdb_plugin_celery.targets import TARGET_HEADER_NAME
from djanquiltdb_plugin_celery.tasks import QuiltTask
from example.task_bases import ProjectQuiltTask, RecordingCallTask
from example.tasks import (
    record_every_node_of_the_app,
    record_every_shard_of_the_app,
    record_inherited_target_of_the_app,
    record_node_of_the_app,
    record_target_of_the_app,
)
from plugin_tests.base import PluginTestCase
from plugin_tests.fan_out import results_of
from plugin_tests.tasks import node_and_schema_of

TARGET_NAMES = ['shard', 'node', 'active_shard', 'every_shard', 'every_node']


def schema_of_the_active_connection(**kwargs):
    return get_active_connection().schema_name


def task_name(name):
    return f'plugin_tests.app_tasks.{name}'


def other_quilt_app(name, **options):
    """
    Returns a QuiltCelery app that is not the current app, and that is not finalized yet.
    """
    return QuiltCelery(name, set_as_current=False, **options)


class CeleryWithRecordingTasks(Celery):
    """
    A Celery app class of a project, which sets a task class of its own.
    """

    task_cls = 'example.task_bases:RecordingCallTask'


class QuiltCeleryBeforeTheProjectClass(QuiltCelery, CeleryWithRecordingTasks):
    pass


class QuiltCeleryAfterTheProjectClass(CeleryWithRecordingTasks, QuiltCelery):
    pass


class QuiltCeleryOfTheProject(QuiltCelery):
    pass


class TaskOfOneAppTestCase(PluginTestCase):
    """
    Compares a task that the task_for_* methods of a QuiltCelery app create with a task of the shared_task_for_*
    decorators. The test app is current, is finalized, and runs its tasks eagerly. The other app does not run its tasks
    eagerly, so each test replaces its send_task() by a mock.
    """

    def setUp(self):
        super().setUp()
        app.finalize()
        self.other_app = other_quilt_app('other_app')

    def test_a_task_of_an_app_belongs_to_that_app_whichever_app_is_current(self):
        """
        Case: A task of the other app and a shared task are created while the test app is current. Then the other app is
              made current.
        Expected: The task of the other app belongs to the other app at both moments. The shared task belongs to the app
                  that is current: first the test app, then the other app.
        """
        task_of_the_other_app = self.other_app.task_for_shard(name=task_name('belongs_to_the_other_app'))(
            schema_of_the_active_connection
        )
        shared_task = shared_task_for_shard(name=task_name('belongs_to_the_current_app'))(
            schema_of_the_active_connection
        )

        self.assertIs(task_of_the_other_app.app, self.other_app)
        self.assertIs(shared_task.app, app)

        self.other_app.set_current()
        self.addCleanup(app.set_current)

        self.assertIs(task_of_the_other_app.app, self.other_app)
        self.assertIs(shared_task.app, self.other_app)

    def test_a_task_of_an_app_is_published_through_that_app(self):
        """
        Case: A task of the other app and a shared task are sent with the shard_id of first, while the test app is
              current.
        Expected: The task of the other app is published through the other app, with the target header, and the test
                  app does not run it. The shared task runs in first through the test app, and the other app publishes
                  nothing for it.
        """
        task_of_the_other_app = self.other_app.task_for_shard(name=task_name('published_by_the_other_app'))(
            schema_of_the_active_connection
        )
        shared_task = shared_task_for_shard(name=task_name('run_by_the_current_app'))(schema_of_the_active_connection)

        with mock.patch.object(self.other_app, 'send_task') as send_task:
            task_of_the_other_app.delay(shard_id=self.first.id)
            shared_result = shared_task.delay(shard_id=self.first.id).get()

        send_task.assert_called_once()
        self.assertEqual(send_task.call_args.args[0], task_name('published_by_the_other_app'))
        self.assertEqual(send_task.call_args.kwargs['headers'][TARGET_HEADER_NAME], {'shard_id': self.first.id})
        self.assertEqual(shared_result, 'first_schema')

    def test_a_task_of_an_app_has_the_task_class_of_that_app_whichever_app_is_current(self):
        """
        Case: The task class of the other app is RecordingCallTask, and the test app has its default task class. A task
              of the other app is created while the test app is current. Then a later app finalizes.
        Expected: The task of the other app is a QuiltTask and a RecordingCallTask. The later app registers a copy of
                  the task with its own task class, which is not a RecordingCallTask.
        """
        recording_app = other_quilt_app('recording_app', task_cls=RecordingCallTask)
        task = recording_app.task_for_shard(name=task_name('of_the_recording_app'))(schema_of_the_active_connection)
        later_app = other_quilt_app('later_app')
        later_app.finalize()

        copy_of_the_later_app = later_app.tasks[task_name('of_the_recording_app')]

        self.assertIsInstance(task, QuiltTask)
        self.assertIsInstance(task, RecordingCallTask)
        self.assertIsInstance(copy_of_the_later_app, QuiltTask)
        self.assertNotIsInstance(copy_of_the_later_app, RecordingCallTask)

    def test_a_task_of_an_app_is_not_registered_in_an_app_that_is_finalized_already(self):
        """
        Case: A task of the other app and a shared task are created, while the test app is finalized already.
        Expected: The test app has the shared task, and not the task of the other app.
        """
        self.other_app.task_for_shard(name=task_name('only_in_the_other_app'), lazy=False)(
            schema_of_the_active_connection
        )
        shared_task_for_shard(name=task_name('in_every_app'))(schema_of_the_active_connection)

        self.assertNotIn(task_name('only_in_the_other_app'), app.tasks)
        self.assertIn(task_name('in_every_app'), app.tasks)

    def test_a_task_that_is_not_shared_is_not_registered_in_an_app_that_finalizes_later(self):
        """
        Case: The other app creates one task with shared=False, and one task without the option. Then a later app
              finalizes.
        Expected: The other app has both tasks. The later app has the task without the option only, because shared is
                  True by default, as for app.task().
        """
        self.other_app.task_for_shard(name=task_name('not_shared'), shared=False)(schema_of_the_active_connection)
        self.other_app.task_for_shard(name=task_name('shared_by_default'))(schema_of_the_active_connection)
        later_app = other_quilt_app('later_app')
        later_app.finalize()

        self.assertIn(task_name('not_shared'), self.other_app.tasks)
        self.assertIn(task_name('shared_by_default'), self.other_app.tasks)
        self.assertNotIn(task_name('not_shared'), later_app.tasks)
        self.assertIn(task_name('shared_by_default'), later_app.tasks)

    def test_a_task_of_an_app_that_is_not_finalized_is_created_when_the_app_finalizes(self):
        """
        Case: The other app, which is not finalized, creates one task by default and one task with lazy=False. Then the
              other app finalizes.
        Expected: The first task is a proxy that creates the task only when the app finalizes, as for app.task(). The
                  task with lazy=False is created at once, and the app registers it.
        """
        lazy_task = self.other_app.task_for_shard(name=task_name('created_when_the_app_finalizes'))(
            schema_of_the_active_connection
        )
        task_that_is_not_lazy = self.other_app.task_for_shard(name=task_name('created_at_once'), lazy=False)(
            schema_of_the_active_connection
        )

        self.assertIsInstance(lazy_task, PromiseProxy)
        self.assertFalse(lazy_task.__evaluated__())
        self.assertNotIsInstance(task_that_is_not_lazy, PromiseProxy)
        self.assertIsInstance(task_that_is_not_lazy, QuiltTask)

        self.other_app.finalize()

        self.assertTrue(lazy_task.__evaluated__())
        self.assertIs(
            self.other_app.tasks[task_name('created_when_the_app_finalizes')], lazy_task._get_current_object()
        )
        self.assertIs(self.other_app.tasks[task_name('created_at_once')], task_that_is_not_lazy)

    def test_a_filter_receives_the_task_and_gives_the_result_of_the_method(self):
        """
        Case: The other app creates a task with filter, as for app.task(), and lazy=False.
        Expected: The filter receives the task, and the method returns what the filter returns.
        """
        filtered_tasks = []

        def record_the_task(task):
            filtered_tasks.append(task)
            return 'the result of the filter'

        result = self.other_app.task_for_shard(name=task_name('filtered'), filter=record_the_task, lazy=False)(
            schema_of_the_active_connection
        )

        self.assertEqual(result, 'the result of the filter')
        self.assertEqual(filtered_tasks, [self.other_app.tasks[task_name('filtered')]])

    def test_the_fan_out_of_a_task_of_an_app_is_published_through_that_app(self):
        """
        Case: task_for_every_shard of the other app creates a task, which is applied without a target, so that its
              fan-out runs locally.
        Expected: The other app publishes one task for first and one for remote, each with its target header.
        """
        task = self.other_app.task_for_every_shard(name=task_name('fans_out_through_the_other_app'))(
            schema_of_the_active_connection
        )

        with mock.patch.object(self.other_app, 'send_task') as send_task:
            task.apply()

        self.assertEqual(
            [call.kwargs['headers'][TARGET_HEADER_NAME] for call in send_task.call_args_list],
            [{'shard_id': self.first.id}, {'shard_id': self.remote.id}],
        )

    def test_the_base_of_the_method_is_the_base_of_the_task(self):
        """
        Case: The other app creates a task with base=ProjectQuiltTask.
        Expected: The base of the task class is ProjectQuiltTask.
        """
        task = self.other_app.task_for_shard(name=task_name('with_a_base'), base=ProjectQuiltTask, lazy=False)(
            schema_of_the_active_connection
        )

        self.assertEqual(task.__class__.__bases__, (ProjectQuiltTask,))


class TaskOfTheTestAppTestCase(PluginTestCase):
    """
    The tasks in example.tasks whose names end in _of_the_app are created by the task_for_* methods of the test app,
    which runs them eagerly.
    """

    def test_task_for_shard_runs_the_task_in_its_shard(self):
        """
        Case: record_target_of_the_app is sent with for_shard() for remote, and with the shard_id of first.
        Expected: It runs in remote and in first, and its function does not receive shard_id.
        """
        remote_result = record_target_of_the_app.for_shard(self.remote).delay().get()
        first_result = record_target_of_the_app.delay(shard_id=self.first.id).get()

        self.assertEqual(node_and_schema_of(remote_result), ('other', 'remote_schema'))
        self.assertEqual(node_and_schema_of(first_result), ('default', 'first_schema'))
        self.assertEqual(first_result['kwargs'], {})

    def test_task_for_node_runs_the_task_in_the_public_schema_of_its_node(self):
        """
        Case: record_node_of_the_app is sent with node_name='other'.
        Expected: It runs in the public schema of the other node.
        """
        result = record_node_of_the_app.delay(node_name='other').get()

        self.assertEqual(node_and_schema_of(result), ('other', 'public'))

    def test_task_for_active_shard_runs_the_task_in_the_active_shard(self):
        """
        Case: record_inherited_target_of_the_app is sent without a shard inside use_shard() for remote.
        Expected: It runs in remote.
        """
        with use_shard(self.remote):
            result = record_inherited_target_of_the_app.delay().get()

        self.assertEqual(node_and_schema_of(result), ('other', 'remote_schema'))

    def test_task_for_every_shard_runs_the_task_once_in_every_active_shard(self):
        """
        Case: record_every_shard_of_the_app is sent without a target.
        Expected: It runs once in first and once in remote, and not in maintained, which is in maintenance.
        """
        results = results_of(record_every_shard_of_the_app.delay().get())

        self.assertEqual(
            [node_and_schema_of(result) for result in results],
            [('default', 'first_schema'), ('other', 'remote_schema')],
        )

    def test_task_for_every_node_runs_the_task_once_on_every_node(self):
        """
        Case: record_every_node_of_the_app is sent without a target.
        Expected: It runs once in the public schema of each node.
        """
        results = results_of(record_every_node_of_the_app.delay().get())

        self.assertEqual(
            [node_and_schema_of(result) for result in results], [('default', 'public'), ('other', 'public')]
        )


class TaskClassOfQuiltCeleryTestCase(SimpleTestCase):
    """
    Celery's Django fixup makes DjangoTask the task class of a plain Celery app without a task class of its own. The
    tests run with DJANGO_SETTINGS_MODULE set, so the fixup is installed in each app.
    """

    def test_a_quilt_celery_app_has_django_task_as_its_task_class(self):
        """
        Case: A QuiltCelery app is created without a task class, as is an app of a subclass of QuiltCelery that does not
              set one.
        Expected: The task class of both is DjangoTask, as for a plain Celery app.
        """
        for app_class in [QuiltCelery, QuiltCeleryOfTheProject]:
            with self.subTest(app_class=app_class.__name__):
                quilt_app = app_class('quilt_app', set_as_current=False)

                self.assertEqual(quilt_app.task_cls, 'celery.contrib.django.task:DjangoTask')

    def test_the_task_class_of_a_class_that_is_combined_with_quilt_celery_is_kept(self):
        """
        Case: CeleryWithRecordingTasks sets RecordingCallTask as its task class, and is combined with QuiltCelery, once
              before QuiltCelery and once after it.
        Expected: The task class of both apps is RecordingCallTask.
        """
        for app_class in [QuiltCeleryBeforeTheProjectClass, QuiltCeleryAfterTheProjectClass]:
            with self.subTest(app_class=app_class.__name__):
                quilt_app = app_class('quilt_app', set_as_current=False)

                self.assertEqual(quilt_app.task_cls, 'example.task_bases:RecordingCallTask')

    def test_the_task_class_of_the_arguments_is_kept(self):
        """
        Case: A QuiltCelery app is created with task_cls=RecordingCallTask, and another with task_cls set to Celery's
              own Task.
        Expected: The task class of each is the class of its arguments, as for a plain Celery app.
        """
        for task_cls in [RecordingCallTask, 'celery.app.task:Task']:
            with self.subTest(task_cls=task_cls):
                quilt_app = QuiltCelery('quilt_app', task_cls=task_cls, set_as_current=False)

                self.assertEqual(quilt_app.task_cls, task_cls)

    def test_an_app_without_the_django_fixup_keeps_the_task_class_of_celery(self):
        """
        Case: A QuiltCelery app is created without any fixup, as outside a Django project.
        Expected: Its task class is Celery's Task, as for a plain Celery app without the Django fixup.
        """
        quilt_app = QuiltCelery('quilt_app', fixups=[], set_as_current=False)

        self.assertEqual(quilt_app.task_cls, 'celery.app.task:Task')

    def test_a_task_of_the_task_method_is_a_django_task(self):
        """
        Case: A QuiltCelery app creates a task with its own task() method, as a project does for a task that does not
              run in a shard.
        Expected: The task is a DjangoTask, and has delay_on_commit().
        """
        quilt_app = QuiltCelery('quilt_app', set_as_current=False)
        task = quilt_app.task(name=task_name('plain_task'), lazy=False)(schema_of_the_active_connection)

        self.assertIsInstance(task, DjangoTask)
        self.assertTrue(callable(task.delay_on_commit))


class AppOptionTestCase(SimpleTestCase):
    def test_a_shared_task_decorator_refuses_an_app(self):
        """
        Case: Each shared_task_for_* decorator is called with app.
        Expected: TypeError, whose message points at the task_for_* methods of QuiltCelery.
        """
        for name in TARGET_NAMES:
            with self.subTest(name=name):
                with self.assertRaisesMessage(TypeError, 'task_for_* methods of a QuiltCelery app'):
                    getattr(celery, f'shared_task_for_{name}')(app=app)

    def test_a_method_of_quilt_celery_refuses_an_app(self):
        """
        Case: Each task_for_* method of the test app is called with app.
        Expected: TypeError, because the method creates a task of its own app.
        """
        for name in TARGET_NAMES:
            with self.subTest(name=name):
                with self.assertRaisesMessage(TypeError, 'task_for_* methods of a QuiltCelery app'):
                    getattr(app, f'task_for_{name}')(app=app)

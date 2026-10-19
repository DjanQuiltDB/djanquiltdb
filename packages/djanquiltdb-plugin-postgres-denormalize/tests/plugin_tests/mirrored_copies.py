import pgtrigger
from django.db import ProgrammingError
from djanquiltdb import State
from djanquiltdb.utils import create_template_schema, use_shard
from postgres_denormalize.batch import defer_cascades, ignore_cascades

from antipattern.models import TaskType, TypedTask
from djanquiltdb_plugin_postgres_denormalize import repair_every_shard
from example.models import Shard
from plugin_tests.base import PluginTestCase

MISSING_TABLE = 'relation "antipattern_typedtask" does not exist'


class MirroredCopyTestCase(PluginTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema('default')
        self.first = Shard.objects.create(
            alias='first', node_name='default', schema_name='first_schema', state=State.ACTIVE
        )
        self.second = Shard.objects.create(
            alias='second', node_name='default', schema_name='second_schema', state=State.ACTIVE
        )

        # The fill trigger of TaskType fails outside a shard, so the task type is created inside one.
        with use_shard(self.first):
            self.task_type = TaskType.objects.create(name='Bug')

        for shard in (self.first, self.second):
            with use_shard(shard):
                TypedTask.objects.create(type=self.task_type)

    def assert_copied_type_names(self, first_name, second_name):
        names = []

        for shard in (self.first, self.second):
            with use_shard(shard):
                names.append(TypedTask.objects.get().type_name)

        self.assertEqual(names, [first_name, second_name])

    def assert_task_type_names(self, names):
        self.assertEqual(list(TaskType.objects.order_by('pk').values_list('name', flat=True)), names)

    def test_creating_a_task_type_outside_a_shard_fails(self):
        """
        Case: A task type is created outside a shard.
        Expected: The fill trigger on TaskType raises that the table of TypedTask does not exist, and the insert is
                  rolled back.
        """
        with self.assertRaisesMessage(ProgrammingError, MISSING_TABLE):
            TaskType.objects.create(name='Chore')

        self.assert_task_type_names(['Bug'])

    def test_renaming_a_task_type_inside_a_shard_corrects_that_shard_only(self):
        """
        Case: The task type is renamed inside the shard first.
        Expected: The copy in first has the new name, and the copy in second keeps the old name.
        """
        with use_shard(self.first):
            TaskType.objects.update(name='Defect')

        self.assert_copied_type_names('Defect', 'Bug')

    def test_renaming_a_task_type_outside_a_shard_fails(self):
        """
        Case: The task type is renamed outside a shard.
        Expected: The cascade trigger on TaskType raises that the table of TypedTask does not exist, and the rename is
                  rolled back.
        """
        with self.assertRaisesMessage(ProgrammingError, MISSING_TABLE):
            TaskType.objects.update(name='Defect')

        self.assert_task_type_names(['Bug'])

    def test_a_batch_helper_inside_a_shard_corrects_that_shard_only(self):
        """
        Case: The task type is renamed inside ignore_cascades() and inside defer_cascades(), each inside the shard
              first.
        Expected: After each, the copy in first has the new name, and the copy in second keeps the old name.
        """
        for batch_helper, name in ((ignore_cascades, 'Defect'), (defer_cascades, 'Issue')):
            with self.subTest(batch_helper=batch_helper.__name__):
                with use_shard(self.first):
                    with batch_helper(TaskType):
                        TaskType.objects.update(name=name)

                self.assert_copied_type_names(name, 'Bug')

    def test_a_batch_helper_outside_a_shard_fails(self):
        """
        Case: The task type is renamed inside ignore_cascades() and inside defer_cascades(), each outside a shard.
        Expected: The correction at the end of the block raises that the table of TypedTask does not exist, and the
                  rename is rolled back.
        """
        for batch_helper in (ignore_cascades, defer_cascades):
            with self.subTest(batch_helper=batch_helper.__name__):
                with self.assertRaisesMessage(ProgrammingError, MISSING_TABLE):
                    with batch_helper(TaskType):
                        TaskType.objects.update(name='Defect')

                self.assert_task_type_names(['Bug'])

    def test_the_documented_workaround_writes_outside_a_shard_and_repairs_every_shard(self):
        """
        Case: Outside a shard, the task type is renamed and another one is created with the cascade and fill triggers
              of TaskType ignored, and repair_every_shard() runs afterwards, as the documentation shows.
        Expected: Both writes succeed, and the copies in both shards have the new name after the repair.
        """
        with pgtrigger.ignore(
            'antipattern.TaskType:denormalize_cascade_antipattern_typedtask',
            'antipattern.TaskType:denormalize_fill_antipattern_typedtask',
        ):
            TaskType.objects.filter(pk=self.task_type.pk).update(name='Defect')
            TaskType.objects.create(name='Chore')

        self.assert_copied_type_names('Bug', 'Bug')

        repair_every_shard()

        self.assert_task_type_names(['Defect', 'Chore'])
        self.assert_copied_type_names('Defect', 'Defect')

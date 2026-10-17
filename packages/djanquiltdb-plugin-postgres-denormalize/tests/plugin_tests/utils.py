from unittest import mock

from django.test import SimpleTestCase, override_settings
from djanquiltdb import State
from djanquiltdb.utils import create_template_schema, use_shard
from postgres_denormalize.fields import drift_condition
from postgres_denormalize.operations import backfill_denormalized

from antipattern.models import TypedTask
from djanquiltdb_plugin_postgres_denormalize import repair_every_shard, repair_shard, sharded_leaves, stale_rows
from example.models import Comment, Shard, Task
from plugin_tests.base import PluginTestCase, create_stale_rows


class ShardedLeavesTestCase(SimpleTestCase):
    @override_settings(POSTGRES_DENORMALIZE={'EXCLUDE_MODELS': ['example.Comment']})
    def test_an_excluded_model_is_not_a_sharded_leaf(self):
        """
        Case: Comment is listed in EXCLUDE_MODELS of django-postgres-denormalize, and sharded_leaves() lists the models
              of the project.
        Expected: Task and TypedTask, because django-postgres-denormalize skips an excluded model, so its copies are
                  neither counted nor repaired.
        """
        self.assertEqual(sharded_leaves(), [Task, TypedTask])


class ShardHelpersTestCase(PluginTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema('default')
        create_template_schema('other')
        self.first = Shard.objects.create(
            alias='first', node_name='default', schema_name='first_schema', state=State.ACTIVE
        )
        self.second = Shard.objects.create(
            alias='second', node_name='default', schema_name='second_schema', state=State.ACTIVE
        )
        self.remote = Shard.objects.create(
            alias='remote', node_name='other', schema_name='remote_schema', state=State.ACTIVE
        )

        for shard in (self.first, self.second, self.remote):
            create_stale_rows(shard)

    @staticmethod
    def _make_active(shard):
        shard.state = State.ACTIVE
        shard.save(update_fields=['state'])

    def test_the_sharded_leaves_are_every_sharded_model_with_a_copy(self):
        """
        Case: sharded_leaves() lists the models of the project.
        Expected: Task, Comment and TypedTask, in the order of the app registry. The mirrored TaskType and the other
                  sharded models declare no copy.
        """
        self.assertEqual(sharded_leaves(), [Task, Comment, TypedTask])

    def test_stale_rows_counts_the_rows_of_each_model_in_one_shard(self):
        """
        Case: stale_rows() runs for the shard first.
        Expected: One stale task and one stale comment. TypedTask, which has no rows, is left out.
        """
        self.assertEqual(stale_rows(self.first), {Task: 1, Comment: 1})

    def test_repair_shard_corrects_that_shard_only(self):
        """
        Case: repair_shard() runs for the shard first.
        Expected: The copies in first are correct, and the copies in second are still stale.
        """
        repair_shard(self.first)

        self.assertEqual(stale_rows(self.first), {})
        self.assertEqual(stale_rows(self.second), {Task: 1, Comment: 1})

        with use_shard(self.first):
            self.assertEqual(Task.objects.get().project_name, 'Apollo')

    def test_repair_shard_corrects_a_shard_on_another_node(self):
        """
        Case: repair_shard() runs for the shard remote, on the other node.
        Expected: The copies in remote are correct.
        """
        repair_shard(self.remote)

        self.assertEqual(stale_rows(self.remote), {})

    def test_repair_shard_corrects_only_the_models_that_it_is_given(self):
        """
        Case: repair_shard() runs for the shard first, with only Task.
        Expected: The task is correct, and the comment is still stale.
        """
        repair_shard(self.first, [Task])

        self.assertEqual(stale_rows(self.first), {Comment: 1})

    def test_a_repair_that_fails_changes_nothing(self):
        """
        Case: repair_shard() runs for the shard first. The correction of Task succeeds, and the correction of Comment
              raises.
        Expected: The error is raised, and the task is still stale, because the repair of a shard is one transaction.
        """

        def fail_for_comment(model, *args, **kwargs):
            if model is Comment:
                raise RuntimeError('the correction failed')

            backfill_denormalized(model, *args, **kwargs)

        with mock.patch('postgres_denormalize.operations.backfill_denormalized', side_effect=fail_for_comment):
            with self.assertRaisesMessage(RuntimeError, 'the correction failed'):
                repair_shard(self.first)

        self.assertEqual(stale_rows(self.first), {Task: 1, Comment: 1})

    def test_repair_every_shard_skips_the_shards_that_are_not_active(self):
        """
        Case: The shard second is in maintenance, and repair_every_shard() runs.
        Expected: It returns the shard second. The copies in first and remote are correct, and the copies in second are
                  still stale.
        """
        self.second.state = State.MAINTENANCE
        self.second.save(update_fields=['state'])
        # The flush after the test enters every shard, so the shard has to be active again by then.
        self.addCleanup(self._make_active, self.second)

        self.assertEqual(repair_every_shard(), [self.second])

        self.assertEqual(stale_rows(self.first), {})
        self.assertEqual(stale_rows(self.remote), {})

        with use_shard(self.second, active_only_schemas=False):
            self.assertEqual(Task.objects.filter(drift_condition(Task)).count(), 1)

    def test_the_helpers_are_served_inside_djanquiltdb(self):
        """
        Case: The helpers are imported from djanquiltdb.utils.postgres_denormalize.
        Expected: They are the plugin's own functions.
        """
        from djanquiltdb.utils import postgres_denormalize
        from djanquiltdb.utils.postgres_denormalize import repair_shard as served_repair_shard

        self.assertIs(served_repair_shard, repair_shard)
        self.assertIs(postgres_denormalize.repair_every_shard, repair_every_shard)

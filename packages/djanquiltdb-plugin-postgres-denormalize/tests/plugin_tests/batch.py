from djanquiltdb import State
from djanquiltdb.utils import create_template_schema, use_shard
from postgres_denormalize.batch import defer_cascades, ignore_cascades

from example.models import Comment, Organization, Project, Shard, Task
from plugin_tests.base import PluginTestCase


class BatchHelperTestMixin:
    def setUp(self):
        super().setUp()
        create_template_schema('default')
        create_template_schema('other')
        self.first = Shard.objects.create(
            alias='first', node_name='default', schema_name='first_schema', state=State.ACTIVE
        )
        self.remote = Shard.objects.create(
            alias='remote', node_name='other', schema_name='remote_schema', state=State.ACTIVE
        )

        for shard in (self.first, self.remote):
            with use_shard(shard):
                project = Project.objects.create(organization=Organization.objects.create(name='Acme'), name='Apollo')
                Comment.objects.create(task=Task.objects.create(project=project, title='Launch'))

    def copied_project_names(self):
        return Task.objects.get().project_name, Comment.objects.get().project_name

    def rename_the_projects(self, shard):
        """
        Returns the copies as they are inside the block.
        """
        with use_shard(shard):
            with self.batch_helper(Project):
                Project.objects.update(name='Gemini')
                return self.copied_project_names()

    def assert_copied_project_names(self, shard, name):
        with use_shard(shard):
            self.assertEqual(self.copied_project_names(), (name, name))

    def test_a_batch_in_a_shard_on_the_primary_node_corrects_that_shard(self):
        """
        Case: The projects are renamed inside the batch helper, inside the shard first.
        Expected: Inside the block, the copies still have the old name, because the cascade triggers were skipped. After
                  it, the copies in first have the new name, and the copies in remote keep the name of the project in
                  remote.
        """
        self.assertEqual(self.rename_the_projects(self.first), ('Apollo', 'Apollo'))

        self.assert_copied_project_names(self.first, 'Gemini')
        self.assert_copied_project_names(self.remote, 'Apollo')

    def test_a_batch_in_a_shard_on_another_node_corrects_that_shard(self):
        """
        Case: The projects are renamed inside the batch helper, inside the shard remote on the other node.
        Expected: The cascade triggers are skipped inside the block, and the copies in remote have the new name after
                  it, because the router chooses the shard's database for the batch and for its correction.
        """
        self.assertEqual(self.rename_the_projects(self.remote), ('Apollo', 'Apollo'))

        self.assert_copied_project_names(self.remote, 'Gemini')
        self.assert_copied_project_names(self.first, 'Apollo')


class IgnoreCascadesTestCase(BatchHelperTestMixin, PluginTestCase):
    batch_helper = staticmethod(ignore_cascades)


class DeferCascadesTestCase(BatchHelperTestMixin, PluginTestCase):
    batch_helper = staticmethod(defer_cascades)

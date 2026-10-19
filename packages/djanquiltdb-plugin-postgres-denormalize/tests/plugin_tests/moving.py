from django.core.management import call_command
from djanquiltdb import State
from djanquiltdb.testing import OverrideMirroredRoutingMixin
from djanquiltdb.utils import create_template_schema, use_shard

from djanquiltdb_plugin_postgres_denormalize import repair_shard, stale_rows
from example.models import Comment, Organization, Shard, Task
from plugin_tests.base import PluginTestCase, create_stale_rows


class MoveDataToShardTestCase(PluginTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()
        self.source = Shard.objects.create(
            alias='source', node_name='default', schema_name='source_schema', state=State.ACTIVE
        )
        self.target = Shard.objects.create(
            alias='target', node_name='default', schema_name='target_schema', state=State.ACTIVE
        )
        create_stale_rows(self.source)

        with use_shard(self.source):
            self.organization = Organization.objects.get()

    def test_a_stale_copy_moves_as_it_is(self):
        """
        Case: move_data_to_shard moves the organization, with its project, task and comment, to the shard target.
        Expected: The move succeeds, and the copies in target are as stale as they were in source, because the move
                  fires no trigger. stale_rows() finds them there, and repair_shard() corrects them.
        """
        call_command(
            'move_data_to_shard',
            '--source-shard-alias',
            self.source.alias,
            '--target-shard-alias',
            self.target.alias,
            '--model-name',
            'example.organization',
            '--root-object-id',
            str(self.organization.id),
            '--no-input',
            '--quiet',
        )

        with use_shard(self.target):
            self.assertEqual(Task.objects.get().project_name, 'stale')

        self.assertEqual(stale_rows(self.target), {Task: 1, Comment: 1})

        repair_shard(self.target)

        self.assertEqual(stale_rows(self.target), {})


class MoveShardToNodeTestCase(OverrideMirroredRoutingMixin, PluginTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema('default')
        create_template_schema('other')

        with use_shard(node_name='default', schema_name='public', override_class_method_use_shard=True):
            self.shard = Shard.objects.create(
                alias='mover', node_name='default', schema_name='mover_schema', state=State.ACTIVE
            )

        # move_shard_to_node needs the shard registered on both nodes.
        with use_shard(node_name='other', schema_name='public', override_class_method_use_shard=True):
            Shard.objects.create(
                id=self.shard.id, alias='mover', node_name='default', schema_name='mover_schema', state=State.ACTIVE
            )

        create_stale_rows(self.shard)

    def test_a_stale_copy_moves_as_it_is(self):
        """
        Case: move_shard_to_node moves the shard mover to the other node.
        Expected: The copies in the moved shard are as stale as they were, because the move fires no trigger.
                  stale_rows() finds them there, and repair_shard() corrects them.
        """
        call_command(
            'move_shard_to_node',
            '--source-shard-alias',
            self.shard.alias,
            '--target-node-alias',
            'other',
            '--no-input',
            '--quiet',
        )
        self.shard.refresh_from_db()

        self.assertEqual(self.shard.node_name, 'other')
        self.assertEqual(stale_rows(self.shard), {Task: 1, Comment: 1})

        repair_shard(self.shard)

        self.assertEqual(stale_rows(self.shard), {})

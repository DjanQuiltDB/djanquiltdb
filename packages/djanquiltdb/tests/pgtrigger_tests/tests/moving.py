import pgtrigger
from django.core.management import call_command
from django.db import ProgrammingError

from djanquiltdb import State
from djanquiltdb.testing import OverrideMirroredRoutingMixin
from djanquiltdb.utils import create_template_schema, use_shard
from example.models import Shard
from pgtrigger_tests.models import PublicProtected, ShardedWriteProtected
from pgtrigger_tests.tests.base import PgtriggerTestCase


class MoveShardToNodeTriggerTestCase(OverrideMirroredRoutingMixin, PgtriggerTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema('default')
        create_template_schema('other')

        with use_shard(node_name='default', schema_name='public', override_class_method_use_shard=True):
            self.shard = Shard.objects.create(
                alias='mover', node_name='default', schema_name='mover_schema', state=State.ACTIVE
            )
            self.public_protected = PublicProtected.objects.create(name='kept')

        with use_shard(node_name='other', schema_name='public', override_class_method_use_shard=True):
            Shard.objects.create(
                id=self.shard.id, alias='mover', node_name='default', schema_name='mover_schema', state=State.ACTIVE
            )
            # The same natural key under another id, so the move has to retarget the foreign key.
            self.public_protected_on_the_other_node = PublicProtected.objects.create(
                id=self.public_protected.id + 10, name='kept'
            )

        with use_shard(self.shard), pgtrigger.ignore():
            self.row = ShardedWriteProtected.objects.create(name='written', public_protected=self.public_protected)

    def test_moving_a_shard_fires_no_trigger_of_the_target_schema(self):
        """
        Case: A shard with a row of ShardedWriteProtected, whose trigger rejects every insert and update, moves to
              another node. The move copies the row into the target schema, and then retargets its foreign key to
              PublicProtected, whose row has another id on that node.
        Expected: The move succeeds. The row is in the target schema, and its foreign key points at the row of the other
                  node.
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

        with use_shard(node_name='other', schema_name='mover_schema', active_only_schemas=False):
            row = ShardedWriteProtected.objects.get(id=self.row.id)

        self.assertEqual(row.name, 'written')
        self.assertEqual(row.public_protected_id, self.public_protected_on_the_other_node.id)

    def test_the_triggers_of_the_target_schema_fire_again_after_the_move(self):
        """
        Case: The shard moves to another node, and a row of ShardedWriteProtected is then written in the moved shard.
        Expected: The trigger rejects the write, so the move enabled the triggers again.
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

        with use_shard(node_name='other', schema_name='mover_schema', active_only_schemas=False):
            with self.assertRaisesMessage(ProgrammingError, 'Cannot insert or update rows'):
                ShardedWriteProtected.objects.create(
                    name='rejected', public_protected=self.public_protected_on_the_other_node
                )

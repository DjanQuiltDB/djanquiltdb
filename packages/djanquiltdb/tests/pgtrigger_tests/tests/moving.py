import pgtrigger
from django.core.management import call_command
from django.db import ProgrammingError, transaction

from djanquiltdb import State
from djanquiltdb.testing import OverrideMirroredRoutingMixin
from djanquiltdb.utils import create_template_schema, pgtrigger_triggers_ignored, use_shard
from example.models import OrganizationShard, Shard
from pgtrigger_tests.models import PublicProtected, ShardedProtected, ShardedWriteProtected
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


class DataMoveTriggerTestCase(PgtriggerTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()
        self.source_shard = Shard.objects.create(
            alias='source', node_name='default', schema_name='source_schema', state=State.ACTIVE
        )
        self.target_shard = Shard.objects.create(
            alias='target', node_name='default', schema_name='target_schema', state=State.ACTIVE
        )
        public_protected = PublicProtected.objects.create(name='kept')

        with use_shard(self.source_shard), pgtrigger.ignore():
            self.write_protected = ShardedWriteProtected.objects.create(
                name='written', public_protected=public_protected
            )
            self.delete_protected = ShardedProtected.objects.create(name='kept')
            # The mapping row lets move_data_to_shard move the ShardedWriteProtected row as a root object.
            OrganizationShard.objects.create(
                shard=self.source_shard, organization_id=self.write_protected.id, state=State.ACTIVE
            )

    def test_moving_data_fires_no_trigger_for_the_copy(self):
        """
        Case: move_data_to_shard moves the ShardedWriteProtected row to the target shard. The move writes the row there
              with COPY, which the row's trigger would reject.
        Expected: The move succeeds. The row is in the target shard, and no longer in the source shard.
        """
        call_command(
            'move_data_to_shard',
            '--source-shard-alias',
            self.source_shard.alias,
            '--target-shard-alias',
            self.target_shard.alias,
            '--model-name',
            'pgtrigger_tests.shardedwriteprotected',
            '--root-object-id',
            str(self.write_protected.id),
            '--no-input',
            '--quiet',
        )

        with use_shard(self.target_shard):
            self.assertTrue(ShardedWriteProtected.objects.filter(id=self.write_protected.id).exists())

        with use_shard(self.source_shard):
            self.assertFalse(ShardedWriteProtected.objects.filter(id=self.write_protected.id).exists())

    def test_purging_data_fires_no_trigger_for_the_delete(self):
        """
        Case: purge_shard_data deletes the ShardedProtected row from the source shard. The row's trigger would reject
              the delete.
        Expected: The purge succeeds, and the row is gone.
        """
        call_command(
            'purge_shard_data',
            '--shard-alias',
            self.source_shard.alias,
            '--model-name',
            'pgtrigger_tests.shardedprotected',
            '--object-value',
            str(self.delete_protected.id),
            '--no-input',
            verbosity=0,
        )

        with use_shard(self.source_shard):
            self.assertFalse(ShardedProtected.objects.filter(id=self.delete_protected.id).exists())

    def test_the_triggers_fire_again_after_the_move(self):
        """
        Case: move_data_to_shard moves the ShardedWriteProtected row, and a row of ShardedWriteProtected is then
              written in the target shard.
        Expected: The trigger rejects the write, because the triggers are only ignored while the move runs.
        """
        call_command(
            'move_data_to_shard',
            '--source-shard-alias',
            self.source_shard.alias,
            '--target-shard-alias',
            self.target_shard.alias,
            '--model-name',
            'pgtrigger_tests.shardedwriteprotected',
            '--root-object-id',
            str(self.write_protected.id),
            '--no-input',
            '--quiet',
        )

        with use_shard(self.target_shard):
            with self.assertRaisesMessage(ProgrammingError, 'Cannot insert or update rows'):
                ShardedWriteProtected.objects.create(
                    name='rejected', public_protected=self.write_protected.public_protected
                )

    def test_a_copy_that_is_the_first_statement_in_the_block_fires_no_trigger(self):
        """
        Case: Inside a transaction, a ShardedWriteProtected row is written with COPY, as the first statement after
              pgtrigger_triggers_ignored() is entered. COPY does not pass Django's execute wrappers, through which
              pgtrigger.ignore() sends its setting to the database.
        Expected: The trigger does not reject the row, so the setting is already in the transaction when the context
                  manager yields.
        """
        with use_shard(self.target_shard, active_only_schemas=False, lock=False) as env:
            cursor = env.connection.cursor()

            with transaction.atomic(using='default'), pgtrigger_triggers_ignored(self.target_shard):
                with cursor.copy(
                    'COPY pgtrigger_tests_shardedwriteprotected (id, name, public_protected_id) FROM STDIN'
                ) as copy:
                    copy.write_row((100, 'copied', self.write_protected.public_protected_id))

            self.assertTrue(ShardedWriteProtected.objects.filter(id=100).exists())

    def test_an_error_in_the_block_is_raised_as_it_is(self):
        """
        Case: Inside a transaction, a statement in the block of pgtrigger_triggers_ignored() fails, so the transaction
              only accepts a rollback.
        Expected: The error of the statement is raised, and not the error of the statement that resets the setting of
                  pgtrigger.ignore().
        """
        with use_shard(self.target_shard, active_only_schemas=False, lock=False) as env:
            cursor = env.connection.cursor()

            with self.assertRaisesMessage(ProgrammingError, 'no_such_table'):
                with transaction.atomic(using='default'), pgtrigger_triggers_ignored(self.target_shard):
                    cursor.execute('SELECT * FROM no_such_table')

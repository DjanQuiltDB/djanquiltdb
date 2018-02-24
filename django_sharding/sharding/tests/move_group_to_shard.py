from unittest import mock

from django.core.management import call_command, CommandError
from django.db import DatabaseError, IntegrityError
from django.test import override_settings

from example.models import Type, User, SuperType, Organization, Shard, Statement, OrganizationShards, Cake
from sharding.tests.utils import ShardingTestCase
from sharding.utils import use_shard, create_template_schema, State
from sharding.management.commands.move_data_to_shard import Command as MoveCommand


class MoveDataToShard(ShardingTestCase):
    def fake_allow_migrate(self, *args, **hints):
        model = hints.pop('model', False)
        if getattr(model, 'test_model', False):
            return False

    def setUp(self):
        super().setUp()

        self.command = MoveCommand()
        self.command.quiet = True

        create_template_schema()
        self.source_shard = Shard.objects.create(alias='court', node_name='default', schema_name='test_source',
                                                 state=State.ACTIVE)
        self.target_shard = Shard.objects.create(alias='Curious Village', node_name='default',
                                                 schema_name='test_target',  state=State.ACTIVE)

        with use_shard(self.source_shard):
            self.super = SuperType.objects.create(name='Character')
            self.type_1 = Type.objects.create(name='Attorney', super=self.super)
            self.organization_1 = Organization.objects.create(name='Ace')
            self.user_1 = User.objects.create(name='Phoenix Wright', email='p@wright.cap',
                                              organization=self.organization_1, type=self.type_1)
            self.statement_1 = Statement.objects.create(content='Objection!', user=self.user_1)
            self.statement_2 = Statement.objects.create(content='discrepancy', user=self.user_1)
            self.cake_1 = Cake.objects.create(name='Carrot Cake', user=self.user_1)
            self.organization_shard = OrganizationShards.objects.create(shard=self.source_shard,
                                                                        organization_id=self.organization_1.id,
                                                                        state=State.ACTIVE)

            self.type_2 = Type.objects.create(name='Professor', super=self.super)
            self.organization_2 = Organization.objects.create(name='Layton inc.',)
            self.user_2 = User.objects.create(name='Layton', email='professor@layton.l5',
                                              organization=self.organization_2, type=self.type_2)
            self.statement_3 = Statement.objects.create(content='Luke!', user=self.user_2)
            self.statement_4 = Statement.objects.create(content='Try to solve this puzzle', user=self.user_2)
            self.cake_2 = Cake.objects.create(name='Apple Pie', user=self.user_2)

        self.data = {Organization: {self.organization_1},
                     User: {self.user_1},
                     Statement: {self.statement_1, self.statement_2},
                     Cake: {self.cake_1}}

        self.options = {'database': 'default',
                        'source_shard_alias': self.source_shard.alias,
                        'target_shard_alias': self.target_shard.alias,
                        'root_object_id': self.organization_1.pk,
                        'model_name': 'example.organization',
                        'no_input': True,
                        'quiet': True}

    def test(self):
        """
        Case: Move an organization to another shard using the move_data_to_shard command.
        Expected: Only that organization and all associated data to be moved over.
        Note: System test
        """
        call_command('move_data_to_shard', **self.options)

        with use_shard(self.source_shard):
            # Fails at the moment: The data is not removed from the source yet.
            self.assertCountEqual(Organization.objects.all(), [self.organization_2])
            self.assertCountEqual(User.objects.all(), [self.user_2])
            self.assertCountEqual(Statement.objects.all(), [self.statement_3, self.statement_4])
            self.assertCountEqual(Cake.objects.all(), [self.cake_2])

        with use_shard(self.target_shard):
            self.assertCountEqual(Organization.objects.all(), [self.organization_1])
            self.assertCountEqual(User.objects.all(), [self.user_1])
            self.assertCountEqual(Statement.objects.all(), [self.statement_1, self.statement_2])
            self.assertCountEqual(Cake.objects.all(), [self.cake_1])

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.copy_expert', side_effect=DatabaseError)
    def test_failure_on_move(self, mock_copy_expert):
        """
        Case: Call move_data_to_shard command, and let it fail during move_data
        Expected: Transaction to be rolled back, no data moved or lost.
        Note: System test
        """
        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        with use_shard(self.source_shard):
            # Fails at the moment: The data is not removed from the source yet.
            self.assertCountEqual(Organization.objects.all(), [self.organization_1, self.organization_2])
            self.assertCountEqual(User.objects.all(), [self.user_1, self.user_2])
            self.assertCountEqual(Statement.objects.all(), [self.statement_1, self.statement_2, self.statement_3,
                                                            self.statement_4])

        with use_shard(self.target_shard):
            self.assertFalse(Organization.objects.all().exists())
            self.assertFalse(User.objects.all().exists())
            self.assertFalse(Statement.objects.all().exists())

        self.assertTrue(mock_copy_expert.called)

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.confirm_data_integrity', return_value=False)
    def test_failure_on_integrity(self, mock_copy_expert):
        """
        Case: Call move_data_to_shard command, and let it fail during confirm_data_integrity
        Expected: Transaction to be rolled back, no data moved or lost.
        Note: System test
        """
        with self.assertRaises(IntegrityError):
            self.command.handle(**self.options)

        with use_shard(self.source_shard):
            # Fails at the moment: The data is not removed from the source yet.
            self.assertCountEqual(Organization.objects.all(), [self.organization_1, self.organization_2])
            self.assertCountEqual(User.objects.all(), [self.user_1, self.user_2])
            self.assertCountEqual(Statement.objects.all(), [self.statement_1, self.statement_2, self.statement_3,
                                                            self.statement_4])

        with use_shard(self.target_shard):
            self.assertFalse(Organization.objects.all().exists())
            self.assertFalse(User.objects.all().exists())
            self.assertFalse(Statement.objects.all().exists())

        self.assertTrue(mock_copy_expert.called)

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.delete_data', side_effect=DatabaseError)
    def test_failure_on_delete(self, mock_copy_expert):
        """
        Case: Call move_data_to_shard command, and let it fail during delete_data
        Expected: Transaction to be rolled back, no data moved or lost.
        Note: System test
        """
        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        with use_shard(self.source_shard):
            # Fails at the moment: The data is not removed from the source yet.
            self.assertCountEqual(Organization.objects.all(), [self.organization_1, self.organization_2])
            self.assertCountEqual(User.objects.all(), [self.user_1, self.user_2])
            self.assertCountEqual(Statement.objects.all(), [self.statement_1, self.statement_2, self.statement_3,
                                                            self.statement_4])

        with use_shard(self.target_shard):
            self.assertFalse(Organization.objects.all().exists())
            self.assertFalse(User.objects.all().exists())
            self.assertFalse(Statement.objects.all().exists())

        self.assertTrue(mock_copy_expert.called)

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.get_target_shard')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.pre_execution')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.get_object')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.get_data')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.move_data')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.confirm_data_integrity')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.delete_data')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.post_execution')
    def test_handle(self, mock_post_execution, mock_delete_data, mock_confirm, mock_move_data, mock_get_data,
                    mock_get_object, mock_pre_execution, mock_get_target_shard):
        """
        Case: Call the handle
        Expected: All sub-functions to be called with the correct arguments.
        """
        data = {Statement: [self.statement_1, self.statement_2]}  # dummy data

        mock_get_target_shard.return_value = self.target_shard
        mock_get_object.return_value = self.organization_1
        mock_get_data.return_value = data

        self.command.handle(**self.options)

        mock_get_target_shard.assert_called_once_with(options=self.options)
        mock_get_object.assert_called_once_with('example.organization', self.organization_1.id, self.source_shard)
        self.assertEqual(mock_pre_execution.call_count, 1)
        mock_get_data.assert_called_once_with(source_shard=self.source_shard, root_object=self.organization_1)
        mock_move_data.assert_called_once_with(data=data, source_shard=self.source_shard,
                                               target_shard=self.target_shard)
        mock_confirm.assert_called_once_with(data=data, source_shard=self.source_shard, target_shard=self.target_shard)
        mock_delete_data.assert_called_once_with(data=data, source_shard=self.source_shard)
        self.assertEqual(mock_post_execution.call_count, 1)

    @mock.patch('sharding.management.commands.move_data_to_shard.transaction_for_nodes')
    def test_handle_transaction(self, mock_transaction_for_nodes):
        """
        Case: Call the handle
        Expected: transaction_for_nodes to be used with the correct node names as argument.
        """
        self.command.handle(**self.options)

        mock_transaction_for_nodes.assert_called_once_with(nodes=['default'])

    def test_get_object(self):
        """
        Case: Call get_object
        Expected: The correct object to be returned
        """
        self.assertEqual(self.command.get_object(model_name='example.organization',
                                                 root_object_id=self.organization_1.id,
                                                 source_shard=self.source_shard),
                         self.organization_1)

    def test_get_object_that_is_not_sharded(self):
        """
        Case: Call get_object for model that is not sharded
        Expected: CommandError raised
        """
        with self.assertRaises(CommandError):
            self.command.get_object(model_name='example.type', root_object_id=self.type_1.id,
                                    source_shard=self.source_shard)

    def test_get_shard(self):
        """
        Case: Call get_shard
        Expected: The correct shard object to be returned
        """
        self.assertEqual(self.command.get_shard(alias='court'), self.source_shard)

    def test_get_shard_for_nonexistent_model(self):
        """
        Case: Call get_shard with an nonexistent alias
        Expected: CommandError to be raised
        """
        with self.assertRaises(CommandError):
            self.command.get_shard(alias='void')

    def test_get_shard_for_mirrored_model(self):
        """
        Case: Call get_shard with an alias to a mirrored model
        Expected: CommandError to be raised
        """
        with self.assertRaises(CommandError):
            self.command.get_shard(alias='type')

    def test_get_target_shard(self):
        """
        Case: Call get_target_shard with options
        Expected: The shard for the target_shard_alias option to be returned
        """
        self.assertEqual(self.command.get_target_shard(options={'target_shard_alias': 'Curious Village'}),
                         self.target_shard)

    @mock.patch('sharding.management.commands.move_data_to_shard.NestedObjects')
    def test_get_data_nestedobjects(self, mock_nested_objects):
        """
        Case: Call get_data
        Expected: Collector called
        """
        self.command.get_data(source_shard=self.source_shard, root_object=self.organization_1)
        mock_nested_objects.assert_called_once_with(using=self.source_shard.node_name)

    @mock.patch('sharding.management.commands.move_data_to_shard.NestedObjects.collect')
    def test_get_data_collect(self, mock_collect):
        """
        Case: Call get_data
        Expected: Collector called
        """
        self.command.get_data(source_shard=self.source_shard, root_object=self.organization_1)
        mock_collect.assert_called_once_with([self.organization_1])

    def test_get_data_result(self):
        """
        Case: Call get_data
        Expected: A dict with the correct data to be returned
        """
        self.assertEqual(self.command.get_data(source_shard=self.source_shard, root_object=self.organization_1),
                         self.data)

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.copy_from')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.copy_expert')
    def test_move_data(self, mock_copy_expert, mock_copy_from):
        """
        Case: Call move_data.
        Expected: copy_expert and copy_from to be called for each model
        """
        self.command.move_data(data=self.data, source_shard=self.source_shard, target_shard=self.target_shard)
        # Since a cursor object is given, we cannot assert the calls specifically.
        self.assertEqual(mock_copy_expert.call_count, 4)
        self.assertEqual(mock_copy_from.call_count, 4)

    @mock.patch('sharding.management.commands.move_data_to_shard.filecmp.cmp', return_value=True)
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.copy_expert')
    def test_data_integrity(self, mock_copy_expert, mock_filecmp):
        # from psycopg2._psycopg import cursor
        """
        Case: Call data_integrity.
        Expected: copy_expert to be called for each model for both shards, and compare_files called with file paths.
        """
        self.assertTrue(self.command.confirm_data_integrity(data=self.data, source_shard=self.source_shard,
                                                            target_shard=self.target_shard))
        # Since a cursor object is given, we cannot assert the calls specifically.
        self.assertEqual(mock_copy_expert.call_count, 8)
        # We callot specifically assert the filecmp call, since the given arguments are randomly named temp files
        self.assertEqual(mock_filecmp.call_count, 1)

    @mock.patch('django.db.models.sql.DeleteQuery.delete_batch')
    @mock.patch('sharding.management.commands.move_data_to_shard.sql.DeleteQuery')
    def test_delete_data(self, mock_delete_query, mock_delete_batch):
        """
        Case: Call delete_data.
        Expected: sql.DeleteQuery, and delete_batch to be called for each model.
        """
        self.command.delete_data(data=self.data, source_shard=self.source_shard)
        mock_delete_query.assert_any_call(Organization)
        mock_delete_query.assert_any_call(User)
        mock_delete_query.assert_any_call(Statement)
        mock_delete_query.mock_delete_batch([self.organization_1.id], using='default')
        mock_delete_query.mock_delete_batch([self.user_1.id], using='default')
        mock_delete_query.mock_delete_batch([self.statement_1.id, self.statement_2.id], using='default')

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_pre_execution_non_mapping(self):
        """
        Case: Call pre_execution without a mapping_model.
        Expected: The source shard to be put into maintenance, old state saved.
        """
        self.command.old_source_state = None
        self.source_shard.state = State.ACTIVE
        self.source_shard.save()

        self.command.pre_execution(options=self.options, source_shard=self.source_shard, target_shard=self.target_shard,
                                   data=self.data, root_object=self.organization_1)

        self.source_shard.refresh_from_db()
        self.assertEqual(self.source_shard.state, State.MAINTENANCE)
        self.assertEqual(self.command.old_source_state, State.ACTIVE)

    @override_settings(SHARDING={'MAPPING_MODEL': 'example.models.OrganizationShards',
                                 'SHARD_CLASS': 'example.models.Shard'})
    def test_pre_execution_with_mapping(self):
        """
        Case: Call pre_execution with a mapping_model.
        Expected: The organization's mapping object to be put into maintenance, old state saved.
        """
        self.command.old_source_state = None
        self.command.pre_execution(options=self.options, source_shard=self.source_shard, target_shard=self.target_shard,
                                   data=self.data, root_object=self.organization_1)

        self.organization_shard.refresh_from_db()
        self.assertEqual(self.organization_shard.state, State.MAINTENANCE)
        self.assertEqual(self.command.old_source_state, State.ACTIVE)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    def test_post_execution_non_mapping(self):
        """
        Case: Call post_execution without a mapping_model.
        Expected: The source shard's state to be restored.
        """
        self.command.old_source_state = State.ACTIVE
        self.source_shard.state = State.MAINTENANCE
        self.source_shard.save()

        self.command.post_execution(options=self.options, source_shard=self.source_shard,
                                    target_shard=self.target_shard, data=self.data, root_object=self.organization_1)

        self.source_shard.refresh_from_db()
        self.assertEqual(self.source_shard.state, State.ACTIVE)

    @override_settings(SHARDING={'MAPPING_MODEL': 'example.models.OrganizationShards',
                                 'SHARD_CLASS': 'example.models.Shard'})
    def test_post_execution_with_mapping(self):
        """
        Case: Call post_execution with a mapping_model.
        Expected: The organization's mapping object's state to be restored.
                  And The shard to be moved as well.
        """
        self.command.old_source_state = State.ACTIVE
        self.organization_shard.state = State.MAINTENANCE
        self.organization_shard.save()

        self.command.post_execution(options=self.options, source_shard=self.source_shard,
                                    target_shard=self.target_shard, data=self.data, root_object=self.organization_1)

        self.organization_shard.refresh_from_db()
        self.assertEqual(self.organization_shard.state, State.ACTIVE)
        self.assertEqual(self.organization_shard.shard, self.target_shard)

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.move_data', side_effect=DatabaseError)
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.post_execution')
    def test_post_execution_on_failure(self, mock_post_execution, mock_move_data):
        """
        Case: Call the handle while move_data will raise an exception.
        Expected: post_execution still called.
        """
        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        self.assertEqual(mock_move_data.call_count, 1)
        self.assertEqual(mock_post_execution.call_count, 1)

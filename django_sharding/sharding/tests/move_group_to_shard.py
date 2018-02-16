from unittest import mock

from django.core.management import call_command
from django.db import DatabaseError, IntegrityError

from example.models import Type, User, SuperType, Organization, Shard, Statement
from sharding.tests.utils import ShardingTestCase
from sharding.utils import use_shard, create_template_schema, State
from sharding.management.commands.move_group_to_shard import Command as MoveCommand


class MoveModelsCommandTestCase(ShardingTestCase):
    def fake_allow_migrate(self, *args, **hints):
        model = hints.pop('model', False)
        if getattr(model, 'test_model', False):
            return False

    def setUp(self):
        super().setUp()

        self.command = MoveCommand()
        self.command.silent = True

        create_template_schema()
        self.source_shard = Shard.objects.create(alias='court', node_name='default', schema_name='test_source',
                                                 state=State.ACTIVE)
        self.target_shard = Shard.objects.create(alias='Curious Village', node_name='default', schema_name='test_target',
                                                 state=State.ACTIVE)

        with use_shard(self.source_shard):
            self.super = SuperType.objects.create(name='Character')
            self.type_1 = Type.objects.create(name='Attorney', super=self.super)
            self.organization_1 = Organization.objects.create(name='Ace')
            self.user_1 = User.objects.create(name='Phoenix Wright', email='p@wright.cap',
                                              organization=self.organization_1, type=self.type_1)
            self.statement_1 = Statement.objects.create(content='Objection!', user=self.user_1)
            self.statement_2 = Statement.objects.create(content='discrepancy', user=self.user_1)

            self.type_2 = Type.objects.create(name='Professor', super=self.super)
            self.organization_2 = Organization.objects.create(name='Layton inc.',)
            self.user_2 = User.objects.create(name='Layton', email='professor@layton.l5',
                                              organization=self.organization_2, type=self.type_2)
            self.statement_3 = Statement.objects.create(content='Luke!', user=self.user_2)
            self.statement_4 = Statement.objects.create(content='Try to solve this puzzle', user=self.user_2)

        self.data = {Organization: {self.organization_1}, User: {self.user_1},
                     Statement: {self.statement_1, self.statement_2}}

        self.options = {'database': 'default',
                        'source_shard_alias': self.source_shard.alias,
                        'target_shard_alias': self.target_shard.alias,
                        'group_id': self.organization_1.pk,
                        'model_name': 'example.organization',
                        'no_input': True,
                        'silent': True}

    def test(self):
        """
        Case: Move an organization to another shard using the move_group_to_shard command.
        Expected: Only that organization and all associated data to be moved over.
        Note: System test
        """
        call_command('move_group_to_shard', **self.options)

        with use_shard(self.source_shard):
            # Fails at the moment: The data is not removed from the source yet.
            self.assertCountEqual(Organization.objects.all(), [self.organization_2])
            self.assertCountEqual(User.objects.all(), [self.user_2])
            self.assertCountEqual(Statement.objects.all(), [self.statement_3, self.statement_4])

        with use_shard(self.target_shard):
            self.assertCountEqual(Organization.objects.all(), [self.organization_1])
            self.assertCountEqual(User.objects.all(), [self.user_1])
            self.assertCountEqual(Statement.objects.all(), [self.statement_1, self.statement_2])

    @mock.patch('sharding.management.commands.move_group_to_shard.Command.copy_expert', side_effect=DatabaseError)
    def test_failure_on_move(self, mock_copy_expert):
        """
        Case: Call move_group_to_shard command, and let it fail during move_data
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

    @mock.patch('sharding.management.commands.move_group_to_shard.Command.confirm_data_integrity', return_value=False)
    def test_failure_on_integrity(self, mock_copy_expert):
        """
        Case: Call move_group_to_shard command, and let it fail during confirm_data_integrity
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

    @mock.patch('sharding.management.commands.move_group_to_shard.Command.delete_data', side_effect=DatabaseError)
    def test_failure_on_delete(self, mock_copy_expert):
        """
        Case: Call move_group_to_shard command, and let it fail during delete_data
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

    @mock.patch('sharding.management.commands.move_group_to_shard.Command.get_target_shard')
    @mock.patch('sharding.management.commands.move_group_to_shard.Command.pre_execution')
    @mock.patch('sharding.management.commands.move_group_to_shard.Command.get_object')
    @mock.patch('sharding.management.commands.move_group_to_shard.Command.get_data')
    @mock.patch('sharding.management.commands.move_group_to_shard.Command.move_data')
    @mock.patch('sharding.management.commands.move_group_to_shard.Command.confirm_data_integrity')
    @mock.patch('sharding.management.commands.move_group_to_shard.Command.delete_data')
    @mock.patch('sharding.management.commands.move_group_to_shard.Command.post_execution')
    def test_handle(self, mock_post_execution, mock_delete_data, mock_confirm, mock_move_data, mock_get_data,
                    mock_get_object, mock_pre_execution, mock_get_target_shard):
        """
        Case: Call the handle
        Expected: All sub-functions to be called with the correct arguments.
        """
        options = {'database': 'default',
                   'source_shard_alias': self.source_shard.alias,
                   'target_shard_alias': self.target_shard.alias,
                   'group_id': self.organization_1.pk,
                   'model_name': 'example.organization',
                   'no_input': True}
        data = {Statement: [self.statement_1, self.statement_2]}  # dummy data

        mock_get_target_shard.return_value = self.target_shard
        mock_get_object.return_value = self.organization_1
        mock_get_data.return_value = data

        self.command.handle(**options)

        mock_get_target_shard.assert_called_once_with(options=options)
        mock_get_object.assert_called_once_with('example.organization', self.organization_1.id, self.source_shard)
        self.assertEqual(mock_pre_execution.call_count, 1)
        mock_get_data.assert_called_once_with(source_shard=self.source_shard, group=self.organization_1)
        mock_move_data.assert_called_once_with(data=data, source_shard=self.source_shard,
                                               target_shard=self.target_shard)
        mock_confirm.assert_called_once_with(data=data, source_shard=self.source_shard, target_shard=self.target_shard)
        mock_delete_data.assert_called_once_with(data=data, source_shard=self.source_shard)
        self.assertEqual(mock_post_execution.call_count, 1)

    def test_get_object(self):
        """
        Case: Call get_object
        Expected: The correct object to be returned
        """
        self.assertEqual(self.command.get_object(model_name='example.organization', group_id=self.organization_1.id,
                                                 source_shard=self.source_shard),
                         self.organization_1)

    def test_get_shard(self):
        """
        Case: Call get_shard
        Expected: The correct shard object to be returned
        """
        self.assertEqual(self.command.get_shard(alias='court'),
                         self.source_shard)

    def test_get_target_shard(self):
        """
        Case: Call get_target_shard with options
        Expected: The shard for the target_shard_alias option to be returned
        """
        self.assertEqual(self.command.get_target_shard(options={'target_shard_alias': 'Curious Village'}),
                         self.target_shard)

    @mock.patch('sharding.management.commands.move_group_to_shard.NestedObjects')
    def test_get_data_nestedobjects(self, mock_nested_objects):
        """
        Case: Call get_data
        Expected: Collector called
        """
        self.command.get_data(source_shard=self.source_shard, group=self.organization_1)
        mock_nested_objects.assert_called_once_with(using=self.source_shard.node_name)

    @mock.patch('sharding.management.commands.move_group_to_shard.NestedObjects.collect')
    def test_get_data_collect(self, mock_collect):
        """
        Case: Call get_data
        Expected: Collector called
        """
        self.command.get_data(source_shard=self.source_shard, group=self.organization_1)
        mock_collect.assert_called_once_with([self.organization_1])

    def test_get_data_result(self):
        """
        Case: Call get_data
        Expected: A dict with the correct data to be returned
        """
        self.assertEqual(self.command.get_data(source_shard=self.source_shard, group=self.organization_1),
                         self.data)

    @mock.patch('sharding.management.commands.move_group_to_shard.Command.copy_from')
    @mock.patch('sharding.management.commands.move_group_to_shard.Command.copy_expert')
    def test_move_data(self, mock_copy_expert, mock_copy_from):
        # from psycopg2._psycopg import cursor
        """
        Case: Call move_data.
        Expected: copy_expert and copy_from to be called for each model
        """
        self.command.move_data(data=self.data, source_shard=self.source_shard, target_shard=self.target_shard)
        # Since a cursor object is given, we cannot assert the calls specifically.
        self.assertEqual(mock_copy_expert.call_count, 3)
        self.assertEqual(mock_copy_from.call_count, 3)

    @mock.patch('sharding.management.commands.move_group_to_shard.filecmp.cmp', return_value=True)
    @mock.patch('sharding.management.commands.move_group_to_shard.Command.copy_expert')
    def test_data_integrity(self, mock_copy_expert, mock_filecmp):
        # from psycopg2._psycopg import cursor
        """
        Case: Call data_integrity.
        Expected: copy_expert to be called for each model for both shards, and compare_files called with file paths.
        """
        self.assertTrue(self.command.confirm_data_integrity(data=self.data, source_shard=self.source_shard,
                                                            target_shard=self.target_shard))
        # Since a cursor object is given, we cannot assert the calls specifically.
        self.assertEqual(mock_copy_expert.call_count, 6)
        mock_filecmp.assert_called_once_with('/tmp/source_buffer.txt', '/tmp/target_buffer.txt')

    @mock.patch('django.db.models.sql.DeleteQuery.delete_batch')
    @mock.patch('sharding.management.commands.move_group_to_shard.sql.DeleteQuery')
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

    def test_pre_execution(self):
        """
        Case: Call pre_execution.
        Expected: Both shard to be put into maintenance, old states saved.
        """
        self.command.old_source_state = None
        self.command.old_target_state = None
        self.source_shard.state = State.ACTIVE
        self.source_shard.save()
        self.target_shard.state = State.ACTIVE
        self.target_shard.save()

        self.command.pre_execution(options=self.options, source_shard=self.source_shard, target_shard=self.target_shard,
                                   data=self.data)

        self.source_shard.refresh_from_db()
        self.target_shard.refresh_from_db()
        self.assertEqual(self.source_shard.state, State.MAINTENANCE)
        self.assertEqual(self.target_shard.state, State.MAINTENANCE)
        self.assertEqual(self.command.old_source_state, State.ACTIVE)
        self.assertEqual(self.command.old_target_state, State.ACTIVE)

    def test_post_execution(self):
        """
        Case: Call post_execution.
        Expected: Both shard's state to be restored.
        """
        self.command.old_source_state = State.MAINTENANCE
        self.command.old_target_state = State.ACTIVE
        self.source_shard.state = State.ACTIVE
        self.source_shard.save()
        self.target_shard.state = State.ACTIVE
        self.target_shard.save()

        self.command.post_execution(options=self.options, source_shard=self.source_shard,
                                    target_shard=self.target_shard, data=self.data)

        self.source_shard.refresh_from_db()
        self.target_shard.refresh_from_db()
        self.assertEqual(self.source_shard.state, State.MAINTENANCE)
        self.assertEqual(self.target_shard.state, State.ACTIVE)

    @mock.patch('sharding.management.commands.move_group_to_shard.Command.move_data', side_effect=DatabaseError)
    @mock.patch('sharding.management.commands.move_group_to_shard.Command.post_execution')
    def test_post_execution_on_failure(self, mock_post_execution, mock_move_data):
        """
        Case: Call the handle while move_data will raise an exception.
        Expected: post_execution still called.
        :return:
        """
        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        self.assertEqual(mock_move_data.call_count, 1)
        self.assertEqual(mock_post_execution.call_count, 1)

import copy
from unittest import mock

from django.core.management import call_command, CommandError
from django.db import DatabaseError, IntegrityError
from django.test import override_settings

from example.models import Type, User, SuperType, Organization, Shard, Statement, OrganizationShards, Suborganization, \
    Cake
from sharding.collector import SimpleCollector
from sharding.tests.utils import ShardingTestCase, ShardingTransactionTestCase
from sharding.utils import use_shard, create_template_schema, State
from sharding.management.commands.move_data_to_shard import Command as MoveCommand


class MoveDataToShardTransactionTestCase(ShardingTransactionTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        self.source_shard = Shard.objects.create(alias='court', node_name='default', schema_name='test_source',
                                                 state=State.ACTIVE)
        self.target_shard = Shard.objects.create(alias='Curious Village', node_name='default',
                                                 schema_name='test_target', state=State.ACTIVE)

        with use_shard(self.source_shard):
            self.organization = Organization.objects.create(name='Ace')
            self.organization_shard = OrganizationShards.objects.create(shard=self.source_shard,
                                                                        organization_id=self.organization.id,
                                                                        state=State.ACTIVE)
            self.type = Type.objects.create(name='student')
            self.user = User.objects.create(name='Luke', email='luke@layton.l15', type=self.type,
                                            organization=self.organization)

        self.options = [
            '--source-shard-alias', self.source_shard.alias,
            '--target-shard-alias', self.target_shard.alias,
            '--root-object-id', self.organization.pk,
            '--model-name', 'example.organization',
            '--no-input',
            '--quiet'
        ]

    def test_sequencer_on_same_node(self):
        """
        Case: Move an organization to another shard and make another organization afterwards.
        Expected: No id collision to occur.
        """
        call_command('move_data_to_shard', *map(str, self.options))

        with use_shard(self.source_shard):
            self.assertFalse(Organization.objects.all().exists())

        with use_shard(self.target_shard, override_class_method_use_shard=True):
            self.organization.refresh_from_db()
            self.user.refresh_from_db()

        with use_shard(self.target_shard):
            # make new organization and user, check if the id does not collide
            organization_new = Organization.objects.create(name='Scribblenauts')
            self.assertEqual(organization_new.id, self.organization.id+1)

            user_new = User.objects.create(name='Jean Descole', email='jean@layton.l15', type=self.type,
                                           organization=organization_new)
            self.assertEqual(user_new.id, self.user.id+1)


class MoveDataToShardTestCase(ShardingTestCase):
    maxDiff = None

    def setUp(self):
        super().setUp()

        create_template_schema()
        self.source_shard = Shard.objects.create(alias='Curious Village', node_name='default',
                                                 schema_name='test_source', state=State.ACTIVE)
        self.target_shard = Shard.objects.create(alias='Court', node_name='default',
                                                 schema_name='test_target', state=State.ACTIVE)

        with use_shard(self.source_shard):
            self.super = SuperType.objects.create(name='Character')

            self.type_1 = Type.objects.create(name='Professor', super=self.super)
            self.type_2 = Type.objects.create(name='Child', super=self.super)

            self.organization_1 = Organization.objects.create(name='Layton inc.')
            self.organization_2 = Organization.objects.create(name='Curious Village')
            self.suborganization = Suborganization.objects.create(parent=self.organization_1,
                                                                  child=self.organization_2)

            self.user_1 = User.objects.create(name='Layton', email='professor@layton.l5',
                                              organization=self.organization_1, type=self.type_1)
            self.user_2 = User.objects.create(name='Luke', email='luke@layton.l5',
                                              organization=self.organization_1, type=self.type_2)
            self.user_3 = User.objects.create(name='Flora', email='f@reinhold.cap',
                                              organization=self.organization_2, type=self.type_2)

            self.statement_1 = Statement.objects.create(content="'Luke'!", user=self.user_1, offset=1)
            self.statement_2 = Statement.objects.create(content='Try to; solve this "puzzle."', user=self.user_1,
                                                        offset=2)
            self.statement_3 = Statement.objects.create(content='Do you see the sun?', user=self.user_3, offset=3)

            self.organization_shard1 = OrganizationShards.objects.create(shard=self.source_shard,
                                                                         organization_id=self.organization_1.id,
                                                                         state=State.ACTIVE)
            self.organization_shard2 = OrganizationShards.objects.create(shard=self.source_shard,
                                                                         organization_id=self.organization_2.id,
                                                                         state=State.ACTIVE)

            self.type_3 = Type.objects.create(name='Attorney', super=self.super)
            self.organization_3 = Organization.objects.create(name='Ace',)
            self.user_4 = User.objects.create(name='Phoenix Wright', email='p@wright.cap',
                                              organization=self.organization_3, type=self.type_3)
            self.statement_4 = Statement.objects.create(content='Objection!', user=self.user_4, offset=4)
            self.statement_5 = Statement.objects.create(content='discrepancy', user=self.user_4, offset=5)

            self.organization_shard3 = OrganizationShards.objects.create(shard=self.source_shard,
                                                                         organization_id=self.organization_3.id,
                                                                         state=State.ACTIVE)

            # Some many-to-many models
            self.cake_1 = Cake.objects.create(name='Butter cake')
            self.cake_2 = Cake.objects.create(name='Chocolate cake')
            self.cake_3 = Cake.objects.create(name='Sponge cake')
            self.cake_4 = Cake.objects.create(name='Coffee cake')

            self.user_1.cake.add(self.cake_1)
            self.user_1.cake.add(self.cake_2)

            self.user_2.cake.add(self.cake_3)

            self.user_3.cake.add(self.cake_4)

            self.user_cake_model = User.cake.through  # Auto-created model
            self.user_cake_1 = self.user_cake_model.objects.get(cake=self.cake_1, user=self.user_1)
            self.user_cake_2 = self.user_cake_model.objects.get(cake=self.cake_2, user=self.user_1)
            self.user_cake_3 = self.user_cake_model.objects.get(cake=self.cake_3, user=self.user_2)

        self.data = {
            Organization: {
                self.organization_1
            },
            Suborganization: {
                self.suborganization
            },
            User: {
                self.user_1,
                self.user_2
            },
            Statement: {
                self.statement_1,
                self.statement_2
            },
            self.user_cake_model: {
                self.user_cake_1,
                self.user_cake_2,
                self.user_cake_3
            }
        }

        self.pk_set = self.get_pk_set_from_data(self.data)

        self.command = MoveCommand()
        self.command.quiet = True
        self.command.source_shard = self.source_shard
        self.command.target_shard = self.target_shard
        self.command.model_name = 'example.organization'
        self.command.root_object_id = self.organization_1.pk
        self.command.old_source_state = {}

        self.options = {
            'source_shard_alias': self.source_shard.alias,
            'target_shard_alias': self.target_shard.alias,
            'root_object_id': self.command.root_object_id,
            'model_name': self.command.model_name,
            'reuse_simple_collector_for_delete': False,
            'no_input': True,
            'quiet': True
        }

    def format_options_to_args(self, options=None):
        """
        Helper method that change the options to command line args, needed if we want to call the command with
        call_command
        """
        if not options:
            options = self.options

        arg_options = []

        for option, value in options.items():
            arg_options.append('--{}'.format(option.replace('_', '-')))

            if isinstance(value, bool):
                # Skip boolean values, because only the existence of the argument is enough to set it to True
                continue

            arg_options.append(str(value))

        return arg_options

    def get_pk_set_from_data(self, data):
        return {model: [instance.pk for instance in instances] for model, instances in data.items()}

    def test(self):
        """
        Case: Move an organization to another shard using the move_data_to_shard command.
        Expected: Only that organization and all associated data (so no suborganizations) to be moved over.
        Note: System test
        """
        call_command('move_data_to_shard', *self.format_options_to_args())

        with use_shard(self.source_shard):
            self.assertCountEqual(Organization.objects.all(), [self.organization_2, self.organization_3])
            self.assertCountEqual(User.objects.all(), [self.user_3, self.user_4])
            self.assertCountEqual(Statement.objects.all(), [self.statement_3, self.statement_4, self.statement_5])
            self.organization_2.refresh_from_db()
            self.organization_3.refresh_from_db()

        with use_shard(self.target_shard):
            self.assertCountEqual(Organization.objects.all(), [self.organization_1])
            self.assertCountEqual(User.objects.all(), [self.user_1, self.user_2])
            self.assertCountEqual(Statement.objects.all(), [self.statement_1, self.statement_2])

            # Check if the content is still in tact, due to escaping and what not.
            self.assertEqual(Statement.objects.get(id=self.statement_1.id).content, "'Luke'!")
            self.assertEqual(Statement.objects.get(id=self.statement_2.id).content, 'Try to; solve this "puzzle."')

        with use_shard(self.target_shard, override_class_method_use_shard=True):
            self.organization_1.refresh_from_db()

    def test_multiple_objects(self):
        """
        Case: Move an organization to another shard using an altered move_data_to_shard command,
              which takes suborganizations into account.
        Expected: The organization, it's suborganization and all associated data to be moved over.
        Note: System test
        """
        class AlteredCommand(MoveCommand):
            def get_objects(self, shard):
                objects = super().get_objects(shard)
                for obj in copy.copy(objects):
                    objects.extend(obj.get_all_descendants())
                return objects

        AlteredCommand().handle(**self.options)

        with use_shard(self.source_shard):
            self.assertCountEqual(Organization.objects.all(), [self.organization_3])
            self.assertFalse(Suborganization.objects.all().exists())
            self.assertCountEqual(User.objects.all(), [self.user_4])
            self.assertCountEqual(Statement.objects.all(), [self.statement_4, self.statement_5])
            self.organization_3.refresh_from_db()

        with use_shard(self.target_shard):
            self.assertCountEqual(Organization.objects.all(), [self.organization_1, self.organization_2])
            self.assertCountEqual(Suborganization.objects.all(), [self.suborganization])
            self.assertCountEqual(User.objects.all(), [self.user_1, self.user_2, self.user_3])
            self.assertCountEqual(Statement.objects.all(), [self.statement_1, self.statement_2, self.statement_3])

        with use_shard(self.target_shard, override_class_method_use_shard=True):
            self.organization_1.refresh_from_db()
            self.organization_2.refresh_from_db()

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.copy_expert', side_effect=DatabaseError)
    def test_failure_on_move(self, mock_copy_expert):
        """
        Case: Call move_data_to_shard command, and let it fail during move_data.
        Expected: Transaction to be rolled back, no data moved or lost.
        Note: System test
        """
        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        with use_shard(self.source_shard):
            # Fails at the moment: The data is not removed from the source yet.
            self.assertCountEqual(Organization.objects.all(), [self.organization_1, self.organization_2,
                                                               self.organization_3])
            self.assertCountEqual(Suborganization.objects.all(), [self.suborganization])
            self.assertCountEqual(User.objects.all(), [self.user_1, self.user_2, self.user_3, self.user_4])
            self.assertCountEqual(Statement.objects.all(), [self.statement_1, self.statement_2, self.statement_3,
                                                            self.statement_4, self.statement_5])

        with use_shard(self.target_shard):
            self.assertFalse(Organization.objects.all().exists())
            self.assertFalse(Suborganization.objects.all().exists())
            self.assertFalse(User.objects.all().exists())
            self.assertFalse(Statement.objects.all().exists())

        self.assertTrue(mock_copy_expert.called)

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.confirm_data_integrity', return_value=False)
    def test_failure_on_integrity(self, mock_copy_expert):
        """
        Case: Call move_data_to_shard command, and let it fail during confirm_data_integrity.
        Expected: Transaction to be rolled back, no data moved or lost.
        Note: System test
        """
        with self.assertRaises(IntegrityError):
            self.command.handle(**self.options)

        with use_shard(self.source_shard):
            # Fails at the moment: The data is not removed from the source yet.
            self.assertCountEqual(Organization.objects.all(), [self.organization_1, self.organization_2,
                                                               self.organization_3])
            self.assertCountEqual(Suborganization.objects.all(), [self.suborganization])
            self.assertCountEqual(User.objects.all(), [self.user_1, self.user_2, self.user_3, self.user_4])
            self.assertCountEqual(Statement.objects.all(), [self.statement_1, self.statement_2, self.statement_3,
                                                            self.statement_4, self.statement_5])

        with use_shard(self.target_shard):
            self.assertFalse(Organization.objects.all().exists())
            self.assertFalse(Suborganization.objects.all().exists())
            self.assertFalse(User.objects.all().exists())
            self.assertFalse(Statement.objects.all().exists())

        self.assertTrue(mock_copy_expert.called)

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.delete_data', side_effect=DatabaseError)
    def test_failure_on_delete(self, mock_copy_expert):
        """
        Case: Call move_data_to_shard command, and let it fail during delete_data.
        Expected: Transaction to be rolled back, no data moved or lost.
        Note: System test
        """
        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        with use_shard(self.source_shard):
            # Fails at the moment: The data is not removed from the source yet.
            self.assertCountEqual(Organization.objects.all(), [self.organization_1, self.organization_2,
                                                               self.organization_3])
            self.assertCountEqual(Suborganization.objects.all(), [self.suborganization])
            self.assertCountEqual(User.objects.all(), [self.user_1, self.user_2, self.user_3, self.user_4])
            self.assertCountEqual(Statement.objects.all(), [self.statement_1, self.statement_2, self.statement_3,
                                                            self.statement_4, self.statement_5])

        with use_shard(self.target_shard):
            self.assertFalse(Organization.objects.all().exists())
            self.assertFalse(Suborganization.objects.all().exists())
            self.assertFalse(User.objects.all().exists())
            self.assertFalse(Statement.objects.all().exists())

        self.assertTrue(mock_copy_expert.called)

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.get_target_shard')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.pre_execution')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.get_objects')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.get_data_collector')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.reset_sequencers')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.move_data')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.confirm_data_integrity')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.delete_data')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.post_execution')
    def test_handle(self, mock_post_execution, mock_delete_data, mock_confirm, mock_move_data, mock_reset_sequencers,
                    mock_get_data_collector, mock_get_objects, mock_pre_execution, mock_get_target_shard):
        """
        Case: Call the handle.
        Expected: All sub-functions to be called with the correct arguments.
        """
        data = {Statement: [self.statement_1, self.statement_2]}  # Dummy data
        pk_set = self.get_pk_set_from_data(data)

        mock_get_target_shard.return_value = self.target_shard
        mock_get_objects.return_value = self.organization_1

        mock_get_data_collector_value = mock.Mock()
        mock_get_data_collector_value.data = data
        mock_get_data_collector.return_value = mock_get_data_collector_value

        self.command.handle(**self.options)

        mock_get_target_shard.assert_called_once_with(options=self.options)
        mock_get_objects.assert_called_once_with(self.source_shard)
        self.assertEqual(mock_pre_execution.call_count, 1)
        mock_get_data_collector.assert_any_call(objects=self.organization_1)
        mock_get_data_collector.assert_any_call(objects=self.organization_1, use_original_collector=True)
        mock_move_data.assert_called_once_with(pk_set=pk_set)
        mock_reset_sequencers.assert_called_once_with(data=data)
        mock_confirm.assert_called_once_with(pk_set=pk_set, model_fields=mock_move_data.return_value)
        mock_delete_data.assert_called_once_with(collector=mock_get_data_collector_value)
        mock_post_execution.assert_called_once_with(succeeded=True)

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.get_target_shard')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.pre_execution')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.get_objects')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.get_data_collector')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.reset_sequencers')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.move_data')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.confirm_data_integrity')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.delete_data')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.post_execution')
    def test_handle_reuse_data(self, mock_post_execution, mock_delete_data, mock_confirm, mock_move_data,
                               mock_reset_sequencers, mock_get_data_collector, mock_get_objects, mock_pre_execution,
                               mock_get_target_shard):
        """
        Case: Call the handle with reuse_simple_collector_for_delete set to True.
        Expected: All sub-functions to be called with the correct arguments.
                  Which are the same as the test_handle above, except the move_data function is only called once.
        """
        data = {Statement: [self.statement_1, self.statement_2]}  # Dummy data
        pk_set = self.get_pk_set_from_data(data)

        mock_get_target_shard.return_value = self.target_shard
        mock_get_objects.return_value = self.organization_1

        mock_get_data_collector_value = mock.Mock()
        mock_get_data_collector_value.data = data
        mock_get_data_collector.return_value = mock_get_data_collector_value

        options = self.options
        options['reuse_simple_collector_for_delete'] = True
        self.command.handle(**self.options)

        mock_get_target_shard.assert_called_once_with(options=self.options)
        mock_get_objects.assert_called_once_with(self.source_shard)
        self.assertEqual(mock_pre_execution.call_count, 1)
        mock_get_data_collector.assert_called_once_with(objects=self.organization_1)
        mock_move_data.assert_called_once_with(pk_set=pk_set)
        mock_confirm.assert_called_once_with(pk_set=pk_set, model_fields=mock_move_data.return_value)
        mock_delete_data.assert_called_once_with(collector=mock_get_data_collector_value)
        self.assertEqual(mock_post_execution.call_count, 1)

    @mock.patch('sharding.management.commands.move_data_to_shard.transaction_for_nodes')
    def test_handle_transaction(self, mock_transaction_for_nodes):
        """
        Case: Call the handle.
        Expected: transaction_for_nodes to be used with the correct node names as argument.
        """
        self.command.handle(**self.options)

        mock_transaction_for_nodes.assert_called_once_with(nodes=['default'])

    def test_get_objects(self):
        """
        Case: Call get_objects.
        Expected: The correct objects to be returned.
        """
        self.assertEqual(self.command.get_objects(shard=self.source_shard), [self.organization_1])

    def test_get_objects_that_are_not_sharded(self):
        """
        Case: Call get_objects for model that is not sharded.
        Expected: CommandError raised.
        """
        self.command.model_name = 'example.type'
        self.command.root_object_id = self.type_1.id
        with self.assertRaises(CommandError):
            self.command.get_objects(shard=self.source_shard)

    def test_get_shard(self):
        """
        Case: Call get_shard.
        Expected: The correct shard object to be returned.
        """
        self.assertEqual(self.command.get_shard(alias='Curious Village'), self.source_shard)

    def test_get_shard_for_nonexistent_model(self):
        """
        Case: Call get_shard with an nonexistent alias.
        Expected: CommandError to be raised.
        """
        with self.assertRaises(CommandError):
            self.command.get_shard(alias='void')

    def test_get_shard_for_mirrored_model(self):
        """
        Case: Call get_shard with an alias to a mirrored model.
        Expected: CommandError to be raised.
        """
        with self.assertRaises(CommandError):
            self.command.get_shard(alias='type')

    def test_get_target_shard(self):
        """
        Case: Call get_target_shard with options.
        Expected: The shard for the target_shard_alias option to be returned.
        """
        self.assertEqual(self.command.get_target_shard(options={'target_shard_alias': 'Court'}), self.target_shard)

    @mock.patch('sharding.management.commands.move_data_to_shard.SimpleCollector.collect')
    def test_get_data_collector(self, mock_collect):
        """
        Case: Call get_data_collector using the simple collector.
        Expected: SimpleCollector.collect() called.
        """
        self.command.get_data_collector(objects=[self.organization_1])
        mock_collect.assert_called_once_with([self.organization_1])

    def test_get_data_collector_result(self):
        """
        Case: Call get_data_collector using the simple collector and check the data attribute
        Expected: A dict with the correct data to be returned, with only sharded models and no mirrored models
        """
        collector = self.command.get_data_collector(objects=[self.organization_1])
        self.assertEqual(collector.data, self.data)

    @mock.patch('sharding.management.commands.move_data_to_shard.NestedObjects.collect')
    def test_get_data_collector_nested_collector(self, mock_collect):
        """
        Case: Call get_data, with use_original_collector set to True.
        Expected: NestedObjects.collect() called
        """
        self.command.get_data_collector(objects=[self.organization_1], use_original_collector=True)
        mock_collect.assert_called_once_with([self.organization_1])

    def test_get_data_collector_nested_collector_result(self):
        """
        Case: Call get_data_collector using Django's NestedCollector and check the data attribute
        Expected: A dict with the correct data to be returned, with only sharded models and no mirrored models
        """
        collector = self.command.get_data_collector(objects=[self.organization_1], use_original_collector=True)
        self.assertEqual(collector.data, self.data)

    @mock.patch('sharding.management.commands.move_data_to_shard.csv.reader')
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.copy_expert')
    def test_move_data(self, mock_copy_expert, mock_csv_reader):
        """
        Case: Call move_data.
        Expected: copy_expert and copy_from to be called twice for each model (one for export, one for import).
        """
        self.command.move_data(pk_set=self.pk_set)

        # Since a cursor object is given, we cannot assert the calls specifically.
        self.assertEqual(mock_copy_expert.call_count, len(self.data) * 2)
        self.assertEqual(mock_csv_reader.call_count, len(self.data.keys()))  # Once for each model

    def test_move_data_return_value(self):
        """
        Case: Call move_data.
        Expected: A dict with <model>:'<field>,<field>,<etc>' to be returned.
        """
        self.assertEqual(
            self.command.move_data(pk_set=self.pk_set),
            {
                Organization: '"id","name","created_at"',
                Suborganization: '"id","parent_id","child_id"',
                User: '"id","password","last_login","name","email","created_at","organization_id","type_id"',
                Statement: '"id","content","user_id","offset"',
                self.user_cake_model: '"id","user_id","cake_id"',
            }
        )

    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.reset_sequence')
    def test_reset_sequencers(self, mock_reset_sequence):
        """
        Case: Call reset_sequencers
        Expected: reset_sequence on the connection to be called with the correct model list.
        """
        self.command.reset_sequencers(data=self.data)
        self.assertCountEqual(mock_reset_sequence.call_args[1]['model_list'], [User, Statement, Organization,
                                                                               Suborganization, self.user_cake_model])

    @mock.patch('sharding.management.commands.move_data_to_shard.filecmp.cmp', return_value=True)
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.copy_expert')
    def test_data_integrity(self, mock_copy_expert, mock_filecmp):
        # from psycopg2._psycopg import cursor
        """
        Case: Call data_integrity.
        Expected: copy_expert to be called for each model for both shards, and compare_files called with file paths.
        """
        self.command.source_shard = self.source_shard
        self.command.target_shard = self.target_shard
        self.assertTrue(self.command.confirm_data_integrity(pk_set=self.pk_set, model_fields=mock.Mock()))
        # Since a cursor object is given, we cannot assert the calls specifically.
        self.assertEqual(mock_copy_expert.call_count, len(self.data) * 2)
        # We callot specifically assert the filecmp call, since the given arguments are randomly named temp files
        self.assertEqual(mock_filecmp.call_count, 1)

    def test_delete_data(self):
        """
        Case: Call delete_data.
        Expected: The delete method of the collector will be called
        """
        mock_collector = mock.Mock()

        self.command.source_shard = self.source_shard
        self.command.delete_data(collector=mock_collector)
        mock_collector.delete.assert_called_once_with()

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.acquire_advisory_lock')
    def test_pre_execution_non_mapping(self, mock_acquire_lock):
        """
        Case: Call pre_execution without a mapping_model.
        Expected: The source shard to be put into maintenance, old state saved.
        """
        self.command.old_source_state = None
        self.source_shard.state = State.ACTIVE
        self.source_shard.save()

        self.command.pre_execution(root_objects=[self.organization_1])

        self.source_shard.refresh_from_db()
        self.assertEqual(self.source_shard.state, State.MAINTENANCE)
        self.assertEqual(self.command.old_shard_state, State.ACTIVE)

        mock_acquire_lock.assert_called_once_with(key='shard_{}'.format(self.source_shard.id), shared=False)

    @override_settings(SHARDING={'MAPPING_MODEL': 'example.models.OrganizationShards',
                                 'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.acquire_advisory_lock')
    def test_pre_execution_with_mapping(self, mock_acquire_lock):
        """
        Case: Call pre_execution with a mapping_model.
        Expected: The organization's mapping object to be put into maintenance, old state saved.
        """
        self.command.pre_execution(root_objects=[self.organization_1])

        self.organization_shard1.refresh_from_db()
        self.assertEqual(self.organization_shard1.state, State.MAINTENANCE)
        self.assertEqual(self.command.old_source_state, {self.organization_1.id: State.ACTIVE})
        mock_acquire_lock.assert_called_once_with(key='mapping_{}'.format(self.organization_1.id), shared=False)

    @override_settings(SHARDING={'MAPPING_MODEL': 'example.models.OrganizationShards',
                                 'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.acquire_advisory_lock')
    def test_pre_execution_with_mapping_multiple_objects(self, mock_acquire_lock):
        """
        Case: Call pre_execution with a mapping_model and multiple objects.
        Expected: The organization's mapping objects to be put into maintenance, old state saved.
        """
        self.command.pre_execution(root_objects=[self.organization_1, self.organization_2])

        self.organization_shard1.refresh_from_db()
        self.organization_shard2.refresh_from_db()

        self.assertEqual(self.organization_shard1.state, State.MAINTENANCE)
        self.assertEqual(self.organization_shard2.state, State.MAINTENANCE)
        self.assertEqual(self.command.old_source_state, {
            self.organization_1.id: State.ACTIVE,
            self.organization_2.id: State.ACTIVE
        })

        mock_acquire_lock.assert_any_call(key='mapping_{}'.format(self.organization_1.id), shared=False)
        mock_acquire_lock.assert_any_call(key='mapping_{}'.format(self.organization_2.id), shared=False)

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.release_advisory_lock')
    def test_post_execution_non_mapping(self, mock_release_lock):
        """
        Case: Call post_execution without a mapping_model.
        Expected: The source shard's state to be restored.
        """
        self.command.old_shard_state = State.ACTIVE
        self.source_shard.state = State.MAINTENANCE
        self.source_shard.save()

        self.command.post_execution(succeeded=True)

        self.source_shard.refresh_from_db()
        self.assertEqual(self.source_shard.state, State.ACTIVE)
        mock_release_lock.assert_called_once_with(key='shard_{}'.format(self.source_shard.id), shared=False)

    @override_settings(SHARDING={'MAPPING_MODEL': 'example.models.OrganizationShards',
                                 'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.release_advisory_lock')
    def test_post_execution_with_mapping(self, mock_release_lock):
        """
        Case: Call post_execution with a mapping_model.
        Expected: The organization's mapping object's state to be restored and the lock to be released.
                  And The shard to be moved as well.
        """
        self.command.old_source_state[self.command.root_object_id] = State.ACTIVE
        self.organization_shard1.state = State.MAINTENANCE
        self.organization_shard1.save(update_fields=['state'])
        self.assertEqual(self.organization_shard1.shard, self.source_shard)

        self.command.post_execution(succeeded=True)

        self.organization_shard1.refresh_from_db()
        self.assertEqual(self.organization_shard1.state, State.ACTIVE)
        self.assertEqual(self.organization_shard1.shard, self.target_shard)

        mock_release_lock.assert_called_once_with(key='mapping_{}'.format(self.organization_1.id), shared=False)

    @override_settings(SHARDING={'MAPPING_MODEL': 'example.models.OrganizationShards',
                                 'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.release_advisory_lock')
    def test_post_execution_with_mapping(self, mock_release_lock):
        """
        Case: Call post_execution with a mapping_model and multiple objects, where one of the original states was in
              maintenance.
        Expected: The organization's mapping object's state to be restored and the shards to be moved as well.
        """
        self.command.old_source_state = {
            self.organization_1.id: State.ACTIVE,
            self.organization_2.id: State.ACTIVE,
            self.organization_3.id: State.MAINTENANCE,
        }
        OrganizationShards.objects.update(state=State.MAINTENANCE)  # Put all mapping models in maintenance

        self.assertEqual(self.organization_shard1.shard, self.source_shard)
        self.assertEqual(self.organization_shard2.shard, self.source_shard)
        self.assertEqual(self.organization_shard3.shard, self.source_shard)

        self.command.post_execution(succeeded=True)

        self.organization_shard1.refresh_from_db()
        self.organization_shard2.refresh_from_db()
        self.organization_shard3.refresh_from_db()

        self.assertEqual(self.organization_shard1.state, State.ACTIVE)
        self.assertEqual(self.organization_shard2.state, State.ACTIVE)
        self.assertEqual(self.organization_shard3.state, State.MAINTENANCE)

        self.assertEqual(self.organization_shard1.shard, self.target_shard)
        self.assertEqual(self.organization_shard2.shard, self.target_shard)
        self.assertEqual(self.organization_shard3.shard, self.target_shard)

        mock_release_lock.assert_any_call(key='mapping_{}'.format(self.organization_1.id), shared=False)
        mock_release_lock.assert_any_call(key='mapping_{}'.format(self.organization_2.id), shared=False)
        mock_release_lock.assert_any_call(key='mapping_{}'.format(self.organization_3.id), shared=False)

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.move_data', side_effect=DatabaseError)
    @mock.patch('sharding.management.commands.move_data_to_shard.Command.post_execution')
    def test_post_execution_on_failure(self, mock_post_execution, mock_move_data):
        """
        Case: Call the handle while move_data will raise an exception.
        Expected: post_execution called with succeeded=False.
        """
        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        self.assertEqual(mock_move_data.call_count, 1)
        mock_post_execution.assert_called_once_with(succeeded=False)

    @mock.patch('sharding.management.commands.move_data_to_shard.Command.move_data', side_effect=DatabaseError)
    def test_no_change_on_failure(self, mock_move_data):
        """
        Case: Call the handle while move_data will raise an exception.
        Expected: Mapping object not altered.
        """
        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        self.organization_shard1.refresh_from_db()
        self.assertEqual(self.organization_shard1.shard, self.source_shard)
        self.assertEqual(mock_move_data.call_count, 1)

    @mock.patch('sharding.management.commands.move_data_to_shard.SimpleCollector')
    def test_get_data_collector_mirrored_models(self, mock_collector):
        """
        Case: Have the collector return mirrored models
        Expected: Only sharded models are in the collector's data, and mirrored models are removed from the data
        """
        class FakeCollector(SimpleCollector):
            def collect(self, objs, *args, **kwargs):
                obj = objs[0]
                self.data = {User: {obj}, Type: {obj.type}}

        mock_collector.side_effect = FakeCollector
        collector = self.command.get_data_collector(objects=[self.user_1])
        self.assertEqual(collector.data, {User: {self.user_1}})

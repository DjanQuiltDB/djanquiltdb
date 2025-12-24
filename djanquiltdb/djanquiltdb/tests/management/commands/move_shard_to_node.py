import copy
from unittest import mock
from unittest.mock import PropertyMock

from django.apps import apps
from django.conf import settings
from django.core.management import CommandError, call_command
from django.db import DatabaseError, connections, models
from django.test import override_settings
from example.models import (
    Cake,
    CakeType,
    CoatingType,
    Organization,
    OrganizationShards,
    Shard,
    Statement,
    Suborganization,
    SuperType,
    Type,
    User,
)

from djanquiltdb.management.commands.move_shard_to_node import Command as MoveCommand
from djanquiltdb.options import ShardOptions
from djanquiltdb.tests import OverrideMirroredRoutingMixin, ShardingTestCase, ShardingTransactionTestCase
from djanquiltdb.utils import (
    State,
    create_schema_on_node,
    create_template_schema,
    get_shard_for,
    use_shard,
    use_shard_for,
)


class MoveShardToNodeTransactionTestCase(OverrideMirroredRoutingMixin, ShardingTransactionTestCase):
    maxDiff = None

    def setUp(self):
        super().setUp()

        create_template_schema('default')
        create_template_schema('other')

        self.target_shard_options = ShardOptions(schema_name='test_source', node_name='other')

        with use_shard(node_name='default', schema_name='public', override_class_method_use_shard=True):
            self.source_shard = Shard.objects.create(
                alias='Curious Village', node_name='default', schema_name='test_source', state=State.ACTIVE
            )

        with use_shard(node_name='other', schema_name='public', override_class_method_use_shard=True):
            Shard.objects.create(
                alias='Curious Village',
                node_name='default',
                schema_name='test_source',
                state=State.ACTIVE,
                id=self.source_shard.id,
            )

        with use_shard(self.source_shard):
            self.super = SuperType.objects.create(name='Character')

            self.type_1 = Type.objects.create(name='Professor', super=self.super)
            self.type_2 = Type.objects.create(name='Child', super=self.super)
            self.type_3 = Type.objects.create(name='Attorney', super=self.super)

            self.cake_type_1 = CakeType.objects.create(name='Truth')
            self.cake_type_2 = CakeType.objects.create(name='Lies')

            with use_shard(node_name='other', schema_name='public'):
                self.super_other = SuperType.objects.create(name='Character', id=self.super.id + 1)

                Type.objects.create(id=self.type_1.id, name=self.type_1.name, super=self.super_other)
                Type.objects.create(id=self.type_2.id, name=self.type_2.name, super=self.super_other)
                Type.objects.create(id=self.type_3.id, name=self.type_3.name, super=self.super_other)

                self.cake_type_other = CakeType.objects.create(name='Lies', id=self.cake_type_1.id + 10)

            self.organization_1 = Organization.objects.create(name='Layton inc.')
            self.organization_2 = Organization.objects.create(name='Curious Village')
            self.suborganization = Suborganization.objects.create(parent=self.organization_1, child=self.organization_2)

            self.user_1 = User.objects.create(
                name='Layton', email='professor@layton.l5', organization=self.organization_1, type=self.type_1
            )
            self.user_2 = User.objects.create(
                name='Luke', email='luke@layton.l5', organization=self.organization_1, type=self.type_2
            )
            self.user_3 = User.objects.create(
                name='Flora', email='f@reinhold.cap', organization=self.organization_2, type=self.type_2
            )

            self.statement_1 = Statement.objects.create(content="'Luke'!", user=self.user_1, offset=1)
            self.statement_2 = Statement.objects.create(
                content='Try to; solve this "puzzle."', user=self.user_1, offset=2
            )
            self.statement_3 = Statement.objects.create(content='Do you see the sun?', user=self.user_3, offset=3)

            self.organization_shard1 = OrganizationShards.objects.create(
                shard=self.source_shard, organization_id=self.organization_1.id, state=State.ACTIVE
            )
            self.organization_shard2 = OrganizationShards.objects.create(
                shard=self.source_shard, organization_id=self.organization_2.id, state=State.ACTIVE
            )

            self.organization_3 = Organization.objects.create(
                name='Ace',
            )
            self.user_4 = User.objects.create(
                name='Phoenix Wright', email='p@wright.cap', organization=self.organization_3, type=self.type_3
            )
            self.statement_4 = Statement.objects.create(content='Objection!', user=self.user_4, offset=4)
            self.statement_5 = Statement.objects.create(content='discrepancy', user=self.user_4, offset=5)

            self.organization_shard3 = OrganizationShards.objects.create(
                shard=self.source_shard, organization_id=self.organization_3.id, state=State.ACTIVE
            )

            # Some many-to-many models
            self.coating_1 = CoatingType.objects.create(hash='a' * 32, type=self.cake_type_1)
            self.coating_2 = CoatingType.objects.create(hash='b' * 32, type=self.cake_type_1)
            self.coating_3 = CoatingType.objects.create(hash='c' * 32, type=self.cake_type_2)

            self.cake_1 = Cake.objects.create(name='Butter cake', type=self.cake_type_1, coating_type=self.coating_1)
            self.cake_2 = Cake.objects.create(name='Chocolate cake', type=self.cake_type_1, coating_type=self.coating_2)
            self.cake_3 = Cake.objects.create(name='Sponge cake', type=self.cake_type_2, coating_type=self.coating_3)
            self.cake_4 = Cake.objects.create(name='Coffee cake', type=self.cake_type_2, coating_type=self.coating_3)

            self.user_1.cake.add(self.cake_1)
            self.user_1.cake.add(self.cake_2)

            self.user_2.cake.add(self.cake_3)

            self.user_3.cake.add(self.cake_4)

            self.user_cake_model = User.cake.through  # Auto-created model
            self.user_cake_1 = self.user_cake_model.objects.get(cake=self.cake_1, user=self.user_1)
            self.user_cake_2 = self.user_cake_model.objects.get(cake=self.cake_2, user=self.user_1)
            self.user_cake_3 = self.user_cake_model.objects.get(cake=self.cake_3, user=self.user_2)

            self.user_cake_4 = self.user_cake_model.objects.get(cake=self.cake_4, user=self.user_3)

        self.data = {
            Organization: {
                self.organization_1,
                self.organization_2,
                self.organization_3,
            },
            Suborganization: {self.suborganization},
            User: {self.user_1, self.user_2, self.user_3, self.user_4},
            Statement: {self.statement_1, self.statement_2, self.statement_3, self.statement_4, self.statement_5},
            self.user_cake_model: {self.user_cake_1, self.user_cake_2, self.user_cake_3, self.user_cake_4},
        }

        self.command = MoveCommand()
        self.command.quiet = True
        self.command.source_shard = (self.source_shard.alias,)

        self.options = {
            'source_shard_alias': self.source_shard.alias,
            'target_node_alias': 'other',
            'batch_size': 1,
            'no_input': True,
            'quiet': True,
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

    def test(self):
        """
        Case: Move an entire shard to another node using the move_shard_to_node command.
        Expected: That shard to now live on a different node.
        Note: System test
        """
        call_command('move_shard_to_node', *self.format_options_to_args())

        with use_shard(
            node_name='other',
            schema_name=self.source_shard.schema_name,
            active_only_schemas=False,
            include_public_schema=True,
        ):
            # Check if all the data that we moved is on the new shard
            for model, instances in self.data.items():
                self.assertCountEqual(model.objects.all(), instances)

            # Check if the content is still intact, due to escaping and what not.
            self.assertEqual(Statement.objects.get(id=self.statement_1.id).content, "'Luke'!")
            self.assertEqual(Statement.objects.get(id=self.statement_2.id).content, 'Try to; solve this "puzzle."')

        shard = get_shard_for(self.organization_1.id)
        self.assertEqual(shard.schema_name, self.source_shard.schema_name)
        self.assertEqual(shard.node_name, 'other')

        # Refresh the organization to make sure that it really exists on this shard (refresh would error if the
        # object does not exist anymore in the shard)
        with use_shard_for(self.organization_1.id):
            # Check if all the data that we moved is on the new shard
            for model, instances in self.data.items():
                self.assertCountEqual(model.objects.all(), instances)

            # Check if the content is still intact, due to escaping and what not.
            self.assertEqual(Statement.objects.get(id=self.statement_1.id).content, "'Luke'!")
            self.assertEqual(Statement.objects.get(id=self.statement_2.id).content, 'Try to; solve this "puzzle."')

    def test_sequences_after_moving(self):
        """
        Case: Move a shard to another node and create some more objects on it afterward.
        Expected: New ids are sequences properly.
        """
        call_command('move_shard_to_node', *self.format_options_to_args())

        shard = get_shard_for(self.organization_1.id)

        with use_shard(shard):
            self.assertEqual(User.objects.count(), 4)
            max_id = User.objects.order_by('-id').first().id
            user = User.objects.create(name='test_user', organization=self.organization_1)
            self.assertEqual(user.id, max_id + 1)

    def assert_nothing_changed(self):
        # Shard object unaltered
        with use_shard(node_name='default', schema_name='public', override_class_method_use_shard=True):
            self.assertCountEqual(
                Shard.objects.all().values_list('alias', 'node_name', 'schema_name', 'state'),
                [('Curious Village', 'default', 'test_source', State.ACTIVE)],
            )
        with use_shard(node_name='other', schema_name='public', override_class_method_use_shard=True):
            self.assertCountEqual(
                Shard.objects.all().values_list('alias', 'node_name', 'schema_name', 'state'),
                [('Curious Village', 'default', 'test_source', State.ACTIVE)],
            )

        # Mapping unaltered
        shard = get_shard_for(self.organization_1.id)
        self.assertEqual(shard.node_name, 'default')
        self.assertEqual(shard.schema_name, 'test_source')

        # All data still just on the source shard
        with use_shard(self.source_shard):
            self.assertCountEqual(
                Organization.objects.all(), [self.organization_1, self.organization_2, self.organization_3]
            )
            self.assertCountEqual(Suborganization.objects.all(), [self.suborganization])
            self.assertCountEqual(User.objects.all(), [self.user_1, self.user_2, self.user_3, self.user_4])
            self.assertCountEqual(
                Statement.objects.all(),
                [self.statement_1, self.statement_2, self.statement_3, self.statement_4, self.statement_5],
            )

        # Target schema not created
        with use_shard(node_name='other', schema_name='public') as env:
            self.assertIsNone(env.connection.get_ps_schema('test_source'))

    @mock.patch(
        'djanquiltdb.management.commands.move_shard_to_node.Command.copy_data_stream', side_effect=DatabaseError
    )
    def test_failure_on_move(self, mock_copy_data_stream):
        """
        Case: Call move_shard_to_node command, and let it fail during move_data.
        Expected: Transaction to be rolled back, no shard is altered, no data moved or lost.
        Note: System test
        """

        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        self.assert_nothing_changed()

        self.assertTrue(mock_copy_data_stream.called)

    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.retarget_relations', side_effect=ValueError)
    def test_failure_on_retargeting(self, mock_copy_data_stream):
        """
        Case: Call move_shard_to_node command, and let it fail during relation retargeting.
        Expected: Transaction to be rolled back, no shard is altered, no data moved or lost.
        Note: System test
        """

        with self.assertRaises(ValueError):
            self.command.handle(**self.options)

        self.assert_nothing_changed()

        self.assertTrue(mock_copy_data_stream.called)

    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.reset_sequences', side_effect=DatabaseError)
    def test_failure_on_sequence_resetting(self, mock_copy_data_stream):
        """
        Case: Call move_shard_to_node command, and let it fail during sequence resetting.
        Expected: Transaction to be rolled back, no shard is altered, no data moved or lost.
        Note: System test
        """

        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        self.assert_nothing_changed()

        self.assertTrue(mock_copy_data_stream.called)


class MoveShardToNodeTestCase(OverrideMirroredRoutingMixin, ShardingTestCase):
    def cleanup_retargeting(self):
        """
        Some tests in this testcase make new models. We do not want them registered after that test, so not to appear
        in tests that do not expect them.
        """
        for model_name in apps.all_models['djanquiltdb'].keys() - self.old_model_state.keys():
            apps.all_models['djanquiltdb'].pop(model_name)

    def reset_debug(self):
        settings.DEBUG = False

    def setUp(self):
        self.old_model_state = copy.copy(apps.all_models['djanquiltdb'])
        self.addCleanup(self.cleanup_retargeting)

        super().setUp()

        create_template_schema('default')
        create_template_schema('other')

        self.target_shard_options = ShardOptions(schema_name='test_target', node_name='other')

        with use_shard(node_name='default', schema_name='public', override_class_method_use_shard=True):
            self.source_shard = Shard.objects.create(
                alias='Curious Village', node_name='default', schema_name='test_source', state=State.ACTIVE
            )

        self.command = MoveCommand()
        self.command.quiet = True
        self.command.batch_size = 1

        self.options = {
            'source_shard_alias': self.source_shard.alias,
            'target_node_alias': 'other',
            'batch_size': 1,
            'no_input': True,
            'quiet': True,
        }

    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.get_source_shard', mock.Mock())
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.get_target_node', mock.Mock())
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.pre_execution', mock.Mock())
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.move_shard', mock.Mock())
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.post_execution', mock.Mock())
    def test_batch_size(self):
        with self.subTest('Batch size 1'):
            self.options['batch_size'] = 1
            self.command.handle(**self.options)

        with self.subTest('Batch size 123456789'):
            self.options['batch_size'] = 123456789
            self.command.handle(**self.options)

        with self.subTest('Batch size 0'):
            self.options['batch_size'] = 0
            with self.assertRaisesMessage(CommandError, "Batch size must be an int of 1 or higher, not '0'"):
                self.command.handle(**self.options)

        with self.subTest('Batch size -1'):
            self.options['batch_size'] = -1
            with self.assertRaisesMessage(CommandError, "Batch size must be an int of 1 or higher, not '-1'"):
                self.command.handle(**self.options)

        with self.subTest('Batch size cake'):
            self.options['batch_size'] = 'cake'
            with self.assertRaisesMessage(CommandError, "Batch size must be an int of 1 or higher, not 'cake'"):
                self.command.handle(**self.options)

    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.get_source_shard')
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.get_target_node')
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.pre_execution')
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.move_shard')
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.post_execution')
    def test_handle(
        self, mock_post_execution, mock_move_shard, mock_pre_execution, mock_get_target_node, mock_get_source_shard
    ):
        """
        Case: Call the handle.
        Expected: All sub-functions to be called with the correct arguments.
        """
        mock_get_source_shard.return_value = self.source_shard
        mock_get_target_node.return_value = 'other'

        self.command.handle(**self.options)

        mock_get_source_shard.assert_called_once_with(alias='Curious Village')
        mock_get_target_node.assert_called_once_with(options=self.options)
        mock_pre_execution.assert_called_once_with()
        mock_move_shard.assert_called_once_with()
        mock_post_execution.assert_called_once_with(succeeded=True)

    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.transaction_for_nodes')
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.create_schema_on_node')
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.copy_data')
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.retarget_relations')
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.reset_sequences')
    def test_move_shard(
        self,
        mock_reset_sequences,
        mock_retarget_relations,
        mock_copy_data,
        mock_create_schema_on_node,
        mock_transaction_for_nodes,
    ):
        """
        Case: Call move_shard.
        Expected: transaction_for_nodes to be used with the correct node names as argument.
                  Other functions (that do the actual work) also called.
        """
        self.assertFalse(hasattr(self.command, 'target_shard_options'))

        self.command.source_shard = self.source_shard
        self.command.target_node = 'other'
        self.command.move_shard()

        # Annoying assertion, since the values of the argument list can be in a random order
        self.assertEqual(mock_transaction_for_nodes.call_count, 1)
        self.assertCountEqual(mock_transaction_for_nodes.call_args[1]['nodes'], ['default', 'other'])

        mock_create_schema_on_node.assert_called_once_with(schema_name='test_source', node_name='other', migrate=True)
        mock_copy_data.assert_called_once_with()
        mock_retarget_relations.assert_called_once_with()
        mock_reset_sequences.assert_called_once_with()

        self.assertEqual(self.command.target_shard_options.node_name, 'other')
        self.assertEqual(self.command.target_shard_options.schema_name, 'test_source')

    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.copy_data_stream')
    def test_copy_data(self, mock_copy_data_stream):
        """
        Case: Call copy_data.
        Expected: copy_data_stream to be called twice the number of copied models.
        """
        self.command.source_shard = self.source_shard
        self.command.target_node = 'other'
        self.command.target_shard_options = ShardOptions(node_name='other', schema_name='test_source')
        create_schema_on_node(schema_name='test_source', node_name='other', migrate=True)

        self.command.copy_data()

        self.assertEqual(mock_copy_data_stream.call_count, 16)  # 8 sharded models * 2 = 16

    def test_get_mapped_value(self):
        """
        Case: Have some public models that have relations in their natural keys, call get_mapped_value for them.
        Expected: Have their natural keys looked up recursively.
        """

        class TopManager(models.Manager):
            def get_by_natural_key(self, name):
                return self.get(name=name)

        class Top(models.Model):
            name = models.CharField('name', max_length=100)

            objects = TopManager()

            class Meta:
                app_label = 'djanquiltdb'
                unique_together = ('name',)

            def natural_key(self):
                return self.name

        class MiddleManager(models.Manager):
            def get_by_natural_key(self, name, top):
                return self.get(name=name, top=top)

        class Middle(models.Model):
            name = models.CharField('name', max_length=100)
            top = models.ForeignKey('Top', on_delete=models.DO_NOTHING, verbose_name='middle', null=True)

            objects = MiddleManager()

            class Meta:
                app_label = 'djanquiltdb'
                unique_together = ('name', 'top')

            def natural_key(self):
                return self.name, self.top

        class BottomManager(models.Manager):
            def get_by_natural_key(self, name, middle):
                return self.get(name=name, middle=middle)

        class Bottom(models.Model):
            name = models.CharField('name', max_length=100)
            middle = models.ForeignKey('Middle', on_delete=models.DO_NOTHING, verbose_name='middle', null=True)

            objects = BottomManager()

            class Meta:
                app_label = 'djanquiltdb'
                unique_together = ('name', 'middle')

            def natural_key(self):
                return self.name, self.middle

        top = Top(name='sugar')
        mid = Middle(name='berries', top=top)
        Bottom(name='dough', middle=mid)

        self.command.source_data = {Bottom: {1: ('dough', 2)}, Middle: {2: ('berries', 3)}, Top: {3: ('sugar',)}}
        self.command.target_data = {Bottom: {('dough', 12): 11}, Middle: {('berries', 13): 12}, Top: {('sugar',): 13}}

        self.assertEqual(self.command.get_mapped_value(Bottom, ('dough', 2)), 11)
        self.assertEqual(self.command.get_mapped_value(Middle, ('berries', 3)), 12)
        self.assertEqual(self.command.get_mapped_value(Top, ('sugar',)), 13)

    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command._check_relations')
    def test_get_mapped_value_missing_allow_copy(self, mock_check_relations):
        """
        Case: Call get_mapped_value for the target data that is missing, and a model that allows copying.
        Expected: The missing data to be copied from the source node to the target node.
                  _check_relations called on the newly copied object.
                  target_data to be appended with the copied datapoint.
        """
        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )

        with use_shard(node_name='default', schema_name='public'):
            CakeType.objects.create(name='lime', id=11)

        self.command.source_data = {CakeType: {11: ('lime',)}}
        self.command.target_data = {CakeType: {}}

        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options

        with self.target_shard_options.use():
            self.assertEqual(CakeType.objects.count(), 0)

        # The new CakeType will have an id of 1, since it's the first object on that node.
        self.assertEqual(self.command.get_mapped_value(CakeType, ('lime',)), 1)

        with self.target_shard_options.use():
            self.assertEqual(CakeType.objects.count(), 1)
            self.assertEqual(CakeType.objects.get(name='lime').id, 1)

        # We have to be a bit indirect at asserting call arguments if model instances are involved.
        mock_check_relations.assert_called_once()
        call_args = mock_check_relations.call_args[0]
        self.assertEqual(call_args[0], CakeType)
        self.assertEqual(call_args[1]._meta.model, CakeType)
        self.assertEqual(call_args[1].id, 1)

        # Our given target_data is a pointer. The dict is to be altered by get_mapped_value.
        self.assertCountEqual(self.command.target_data, {CakeType: {('lime',): 1}})

    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command._check_relations')
    def test_get_mapped_value_missing_disallow_copy(self, mock_check_relations):
        """
        Case: Call get_mapped_value for the target data that is missing, and a model that forbids copying.
        Expected: ValueError raised. target_data remains unaltered, _check_relations not called.
        """
        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )

        with use_shard(node_name='default', schema_name='public'):
            SuperType.objects.create(name='lime', id=11)

        self.command.source_data = {SuperType: {11: ('lime',)}}
        self.command.target_data = {SuperType: {}}

        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options

        with self.target_shard_options.use():
            self.assertEqual(SuperType.objects.count(), 0)

        # The new SuperType will have an id of 1, since it's the first object on that node.
        with self.assertRaisesMessage(
            ValueError,
            'Data "example.SuperType: (\'lime\',) - lime" not found for on '
            'target shard "other", and the model does not allow the data to be '
            'copied.',
        ):
            self.assertIsNone(self.command.get_mapped_value(SuperType, ('lime',)))

        with self.target_shard_options.use():
            self.assertEqual(SuperType.objects.count(), 0)

        self.assertFalse(mock_check_relations.called)

        # Our given target_data is a pointer. The dict is to be altered by get_mapped_value.
        self.assertCountEqual(self.command.target_data, {SuperType: {}})

    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.get_mapped_value')
    def test_check_relations(self, mock_get_mapped_value):
        """
        Case: Call _check_relations for a given object.
        Expected: All relation fields of the object to be remapped to the return value of get_mapped_value.
        """
        with use_shard(node_name='default', schema_name='public'):
            cake_type = CakeType.objects.create(name='Pantheon', id=1)
            cake_type2 = CakeType.objects.create(name='Monotheism', id=2)
        with use_shard(node_name='other', schema_name='public'):
            cake_type = CakeType.objects.create(name='Pantheon', id=1)
            cake_type2 = CakeType.objects.create(name='Monotheism', id=2)
        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )
        with self.source_shard.use():
            cake = Cake.objects.create(name='A very normal deity', type=cake_type, id=1)

        mock_get_mapped_value.return_value = cake_type2

        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options
        self.command.field_definitions = {Cake: {}}
        self.command.field_definitions[Cake]['type_id'] = {'natural_keys': ('name',), 'related_model': CakeType}
        self.command.field_definitions[Cake]['coating_type_id'] = {
            'natural_keys': ('type', 'hash'),
            'related_model': CoatingType,
        }
        self.command.source_data = {CakeType: {1: ('A very normal deity')}, CoatingType: {}}
        self.command._check_relations(Cake, cake)

        cake.refresh_from_db()
        self.assertEqual(cake.name, 'A very normal deity')
        self.assertEqual(cake.type_id, 2)
        self.assertEqual(cake.type, cake_type2)

    def test_check_relations_save(self):
        """
        Case: Call check_relations for objects that need and do not need remapping.
        Expected: Only objects that have remapped fields are saved, otherwise it is a no-op.
        """
        self.addCleanup(self.reset_debug)

        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )

        with use_shard(node_name='default', schema_name='public'):
            cake_type_1 = CakeType.objects.create(name='Vanilla', id=1)
            cake_type_2 = CakeType.objects.create(name='Chocolate', id=2)

        with use_shard(node_name='other', schema_name='public'):
            CakeType.objects.create(name='Vanilla', id=10)  # Different id
            CakeType.objects.create(name='Chocolate', id=2)  # Same id

        with self.target_shard_options.use():
            cake_1 = Cake.objects.create(name='Different Moist', type=cake_type_1)
            cake_2 = Cake.objects.create(name='Same Moist', type=cake_type_2)

        settings.DEBUG = True

        self.command.batch_size = 3
        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options
        self.command.field_definitions = {Cake: {}}
        self.command.field_definitions[Cake]['type_id'] = {'natural_keys': ('name',), 'related_model': CakeType}
        self.command.field_definitions[Cake]['coating_type_id'] = {
            'natural_keys': ('type', 'hash'),
            'related_model': CoatingType,
        }
        self.command.source_data = {CakeType: {1: ('Vanilla',), 2: ('Chocolate',)}, CoatingType: {}}
        self.command.target_data = {CakeType: {('Vanilla',): 10, ('Chocolate',): 2}, CoatingType: {}}
        self.command._check_relations(Cake, cake_1)
        self.command._check_relations(Cake, cake_2)

        # Only the cake where the relation to type_id needs to be retargeted from 1 to 10 is saved.
        queries = [i['sql'] for i in connections['other'].queries]
        self.assertEqual(len(queries), 1)
        self.assertIn(
            'UPDATE "example_cake" SET "type_id" = 10, "coating_type_id" = NULL WHERE "example_cake"."id" = 1', queries
        )
        self.assertNotIn(
            'UPDATE "example_cake" SET "type_id" = 2, "coating_type_id" = NULL WHERE "example_cake"."id" = 2', queries
        )

    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command._check_relations')
    def test_retarget_relations(self, mock_check_relations):
        """
        Case: Call retarget_relations for a set of data.
        Expected: source_data, target_data and field_definitions to be populated,
                  _check_relations called for all objects in public models.
        """
        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )

        with use_shard(node_name='default', schema_name='public'):
            super = SuperType.objects.create(name='Concepts', id=1)
            type = Type.objects.create(name='Humanity', super=super)
            cake_type = CakeType.objects.create(name='PCGMR', id=1)
        with use_shard(node_name='other', schema_name='public'):
            super = SuperType.objects.create(name='Concepts', id=1)
            type = Type.objects.create(name='Humanity', super=super)
            cake_type = CakeType.objects.create(name='PCGMR', id=1)

        with self.target_shard_options.use():
            organization = Organization.objects.create(name='Valve.')
            user = User.objects.create(name='G. Newell', email='gaben@valve.com', organization=organization, type=type)
            cake_1 = Cake.objects.create(name='Smooth Valve-t cake', type=cake_type)
            Cake.objects.create(name='Never a third slice', type=cake_type)
            user.cake.add(cake_1)
            user_cake_model = User.cake.through  # Auto-created model
            user_cake_model.objects.get(cake=cake_1, user=user)

        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options
        self.command.retarget_relations()

        # We have to be a bit indirect at asserting call arguments if model instances are involved.
        # We'll just check if it's called twice for only Cake model objects.
        self.assertEqual(mock_check_relations.call_count, 2)
        self.assertEqual(mock_check_relations.call_args_list[0][0][0], Cake)
        self.assertEqual(mock_check_relations.call_args_list[1][0][0], Cake)

    def test_retarget_relations_system(self):
        """
        Case: Call retarget_relations for a set of data.
        Expected: Only relations targeting public models to be retargeted.
        """
        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )

        with use_shard(node_name='default', schema_name='public'):
            super = SuperType.objects.create(name='Character', id=1)

            type_1 = Type.objects.create(name='Professor', super=super)
            type_2 = Type.objects.create(name='Child', super=super)

            cake_type_1 = CakeType.objects.create(name='Lies', id=1)
            cake_type_2 = CakeType.objects.create(name='Moist', id=2)

        with use_shard(node_name='other', schema_name='public'):
            # Super and CakeType are public. Their ids do not have to match across nodes.
            super_2 = SuperType.objects.create(name='Character', id=10)

            # Type is mirrored, so we need to keep the ids in sync
            Type.objects.create(name='Professor', super=super_2, id=type_1.id)
            Type.objects.create(name='Child', super=super_2, id=type_2.id)

            CakeType.objects.create(name='Lies', id=10)
            CakeType.objects.create(name='Moist', id=20)

        with self.target_shard_options.use():
            organization_1 = Organization.objects.create(name='Layton inc.')
            user_1 = User.objects.create(
                name='Layton', email='professor@layton.l5', organization=organization_1, type=type_1
            )
            user_2 = User.objects.create(name='Luke', email='luke@layton.l5', organization=organization_1, type=type_2)

            # Some many-to-many models
            cake_1 = Cake.objects.create(name='Butter cake', type=cake_type_1)
            cake_2 = Cake.objects.create(name='Chocolate cake', type=cake_type_1)
            cake_3 = Cake.objects.create(name='Sponge cake', type=cake_type_2)
            cake_4 = Cake.objects.create(name='Coffee cake', type=cake_type_2)

            user_1.cake.add(cake_1)
            user_1.cake.add(cake_2)
            user_2.cake.add(cake_3)
            user_2.cake.add(cake_4)

            user_cake_model = User.cake.through  # Auto-created model
            user_cake_model.objects.get(cake=cake_1, user=user_1)
            user_cake_model.objects.get(cake=cake_2, user=user_1)
            user_cake_model.objects.get(cake=cake_3, user=user_2)
            user_cake_model.objects.get(cake=cake_4, user=user_2)

        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options
        self.command.retarget_relations()

        with self.target_shard_options.use():
            # Cake, Type, and SuperType left unaltered
            user_1.refresh_from_db()
            user_2.refresh_from_db()

            self.assertEqual(user_1.type_id, type_1.id)
            self.assertEqual(user_2.type_id, type_2.id)

            self.assertEqual(Type.objects.get(id=user_1.type_id).super_id, 10)
            self.assertEqual(Type.objects.get(id=user_2.type_id).super_id, 10)

            self.assertCountEqual(user_1.cake.all().values_list('id', flat=True), [cake_1.id, cake_2.id])
            self.assertCountEqual(user_2.cake.all().values_list('id', flat=True), [cake_3.id, cake_4.id])

            # CakeType is retargeted
            cake_1.refresh_from_db()
            cake_2.refresh_from_db()
            cake_3.refresh_from_db()
            cake_4.refresh_from_db()

            self.assertEqual(cake_1.type_id, 10)
            self.assertEqual(cake_2.type_id, 10)
            self.assertEqual(cake_3.type_id, 20)
            self.assertEqual(cake_4.type_id, 20)

    def test_retarget_relations_batches(self):
        """
        Case: Call retarget_relations for a set of data with a batch size of 3.
        Expected: Objects to be retargeted are selected by groups of 3 (and not individual), also only their `id` and
                  relation fields are fetches.
        """
        self.addCleanup(self.reset_debug)

        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )

        with use_shard(node_name='default', schema_name='public'):
            cake_type_1 = CakeType.objects.create(name='Lies', id=1)

        with use_shard(node_name='other', schema_name='public'):
            CakeType.objects.create(name='Lies', id=10)

        with self.target_shard_options.use():
            for i in range(0, 12):
                Cake.objects.create(name=f'cake {i}', type=cake_type_1)

        settings.DEBUG = True
        self.command.batch_size = 3
        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options
        self.command.retarget_relations()

        # The objects that are getting retargets are selected by batches. In this case batches of three so:
        #   1<4
        #   3<7
        #   5<10
        #   10<13
        queries = [i['sql'] for i in connections['other'].queries]
        self.assertIn(
            'SELECT "example_cake"."id", "example_cake"."type_id", "example_cake"."coating_type_id" FROM '
            '"example_cake" WHERE ("example_cake"."id" >= 1 AND "example_cake"."id" < 4)',
            queries,
        )
        self.assertIn(
            'SELECT "example_cake"."id", "example_cake"."type_id", "example_cake"."coating_type_id" FROM '
            '"example_cake" WHERE ("example_cake"."id" >= 4 AND "example_cake"."id" < 7)',
            queries,
        )
        self.assertIn(
            'SELECT "example_cake"."id", "example_cake"."type_id", "example_cake"."coating_type_id" FROM '
            '"example_cake" WHERE ("example_cake"."id" >= 7 AND "example_cake"."id" < 10)',
            queries,
        )
        self.assertIn(
            'SELECT "example_cake"."id", "example_cake"."type_id", "example_cake"."coating_type_id" FROM '
            '"example_cake" WHERE ("example_cake"."id" >= 10 AND "example_cake"."id" < 13)',
            queries,
        )

    def test_retarget_relations_system_with_missing_data_allow_copy(self):
        """
        Case: Call retarget_relations for a set of data, which has relations to data that is missing on the target node,
              but is allowed to be copied.
        Expected: Only relations targeting public models to be retargeted, missing data to be copied.
        """
        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )

        with use_shard(node_name='default', schema_name='public'):
            super = SuperType.objects.create(name='Character', id=1)
            Type.objects.create(name='Professor', super=super)

            cake_type_1 = CakeType.objects.create(name='Lies', id=1)
            cake_type_2 = CakeType.objects.create(name='Moist', id=2)

        with use_shard(node_name='other', schema_name='public'):
            super = SuperType.objects.create(name='Character', id=1)
            type_1 = Type.objects.create(name='Professor', super=super)

            CakeType.objects.create(name='Lies', id=10)
            # Missing Moist caketype

        with self.target_shard_options.use():
            organization_1 = Organization.objects.create(name='Layton inc.')
            user_1 = User.objects.create(
                name='Layton', email='professor@layton.l5', organization=organization_1, type=type_1
            )

            # Some many-to-many models
            cake_1 = Cake.objects.create(name='Butter cake', type=cake_type_1)
            cake_2 = Cake.objects.create(name='Chocolate cake', type=cake_type_2)

            user_1.cake.add(cake_1)
            user_1.cake.add(cake_2)

            user_cake_model = User.cake.through  # Auto-created model
            user_cake_model.objects.get(cake=cake_1, user=user_1)
            user_cake_model.objects.get(cake=cake_2, user=user_1)

        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options
        self.command.retarget_relations()

        with self.target_shard_options.use():
            # Cake, Type, and SuperType left unaltered
            user_1.refresh_from_db()

            self.assertEqual(user_1.type_id, type_1.id)

            self.assertEqual(Type.objects.get(id=user_1.type_id).super_id, 1)

            self.assertCountEqual(user_1.cake.all().values_list('id', flat=True), [cake_1.id, cake_2.id])

            # CakeType is retargeted
            cake_1.refresh_from_db()
            cake_2.refresh_from_db()

            self.assertEqual(cake_1.type_id, 10)
            self.assertEqual(cake_2.type_id, CakeType.objects.get(name=cake_type_2.name).id)

            # Missing object is created, other still exists
            self.assertTrue(CakeType.objects.filter(name='Lies').exists())
            self.assertTrue(CakeType.objects.filter(name='Moist').exists())

    def test_retarget_relations_system_with_missing_data_allow_copy2(self):
        """
        Case: Call retarget_relations for a set of data, which has relations to data that is missing on the target node,
              but is allowed to be copied. That datapoint has a relation that does exist, but has a different id.
        Expected: Only relations targeting public models to be retargeted, missing data to be copied.

        Source node:
            model A:    model B:
            1: aaa      1: bbb
               -> bbb

        Target node:
            model A:    model B:
            -empty-     2: bbb
        """
        with use_shard(self.source_shard):
            cake_type_s = CakeType.objects.create(name='delicious', id=1)
            coating_type = CoatingType.objects.create(hash='a' * 32, type=cake_type_s, id=1)
            cake = Cake.objects.create(name='syrup cake', coating_type=coating_type, id=1)

        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )
        with use_shard(node_name=self.target_shard_options.node_name, schema_name='public'):
            cake_type_t = CakeType.objects.create(name='delicious', id=2)
            # Missing CoatingType aaaaaaaaaaaaa

        self.command.quiet = True
        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options
        self.command.target_node = self.target_shard_options.node_name
        self.command.copy_data()
        self.command.retarget_relations()

        with self.target_shard_options.use():
            # CakeType left unaltered
            cake_type_t.refresh_from_db()
            self.assertEqual(cake_type_t.id, 2)

            # CoatingType created and linked to CakeType id=2
            coating_type_t = CoatingType.objects.get(hash='a' * 32)
            self.assertEqual(coating_type_t.type_id, 2)

            # Cake object migrated and targeting the SugerType made
            cake = Cake.objects.get(id=cake.id)
            self.assertEqual(cake.id, 1)
            self.assertEqual(cake.coating_type, coating_type)
            self.assertEqual(cake.coating_type_id, 1)
            self.assertEqual(cake.coating_type.type_id, 2)

    def test_retarget_relations_system_with_missing_data_allow_copy3(self):
        """
        Case: Call retarget_relations for a set of data, which has relations to data that is missing on the target node,
              but is allowed to be copied. That datapoint has also a relation that does not exist, and thus will also be
              copied.
        Expected: Only relations targeting public models to be retargeted, missing data to be copied.

        Source node:
            model A:    model B:
            1: aaa      1: delicious
               -> bbb

        Target node:
            model A:    model B:
            -empty-     1: mildly moist
                        -missing delicious-
        """
        with use_shard(self.source_shard):
            cake_type_s = CakeType.objects.create(name='delicious', id=1)
            coating_type = CoatingType.objects.create(hash='a' * 32, type=cake_type_s, id=1)
            cake = Cake.objects.create(name='syrup cake', coating_type=coating_type, id=1)

        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )
        with use_shard(node_name=self.target_shard_options.node_name, schema_name='public') as env:
            # We create nothing relevant on the target node, for it is missing all the public data needed.
            # But we do make a random CakeType so that the copied over objects will get a different id.
            cake_type_t = CakeType.objects.create(name='mildly moist', id=1)
            # We can create an object with id 1 (for the table should be empty) but the sequence can be at anyhing
            # due to running other tests before this. So reset it to be at 1 as well.
            env.connection.reset_sequence([CakeType, CoatingType])
            # Missing CoatingType aaaaaaaaaaaaa

        self.command.quiet = True
        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options
        self.command.target_node = self.target_shard_options.node_name
        self.command.copy_data()
        self.command.retarget_relations()

        with self.target_shard_options.use():
            # CakeType created
            cake_type_t = CakeType.objects.get(name='delicious')
            self.assertEqual(cake_type_t.id, 2)

            # CoatingType created and linked to CakeType id=1
            coating_type_t = CoatingType.objects.get(hash='a' * 32)
            self.assertEqual(coating_type_t.id, 1)
            self.assertEqual(coating_type_t.type_id, 2)

            # Cake object migrated and targeting the SugerType made
            cake = Cake.objects.get(id=cake.id)
            self.assertEqual(cake.id, 1)
            self.assertEqual(cake.coating_type, coating_type)
            self.assertEqual(cake.coating_type_id, 1)
            self.assertEqual(cake.coating_type.type_id, 2)

    def test_retarget_relations_missing_natural_keys(self):
        """
        Case: Call retarget_relations for a data set while a model lacks natural keys.
        Expected: Value error raised.
        """

        def restore_cake_type(cake_type_unique_together, cake_type_natural_key):
            CakeType._meta.unique_together = cake_type_unique_together
            CakeType.natural_key = cake_type_natural_key

        self.addCleanup(restore_cake_type, CakeType._meta.unique_together, CakeType.natural_key)

        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )

        with use_shard(node_name='default', schema_name='public'):
            CakeType.objects.create(name='Gone', id=1)

        with use_shard(node_name='other', schema_name='public'):
            CakeType.objects.create(name='Gone', id=10)

        with self.target_shard_options.use():
            Cake.objects.create(name='Butter cake', type_id=1)

        CakeType._meta.unique_together = None
        delattr(CakeType, 'natural_key')

        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options
        with self.assertRaisesMessage(
            ValueError, "Model <class 'example.models.CakeType'> does not appear to have natural keys!"
        ):
            self.command.retarget_relations()

        # Remove cake so cleanup goes without issues
        with self.target_shard_options.use():
            Cake.objects.all().delete(force=True)

    def test_retarget_relations_missing_source(self):
        """
        Case: Call retarget_relations for a data set where some source data is missing.
        Expected: Value error raised.
        """
        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )

        with use_shard(node_name='default', schema_name='public'):
            CakeType.objects.create(name='Gone', id=1)

        with use_shard(node_name='other', schema_name='public'):
            CakeType.objects.create(name='Gone', id=10)

        with self.target_shard_options.use():
            cake = Cake.objects.create(name='Butter cake', type_id=1)

        # Rework cake_type so it's effectively gone, without postgres noticing
        with use_shard(node_name='default', schema_name='public'):
            CakeType.objects.filter(name='Gone').update(name='Really Gone', id=2)

        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options
        with self.assertRaisesMessage(
            ValueError, f'No related data found for <Cake>{cake.id}.type_id: 1 on source shard'
        ):
            self.command.retarget_relations()

        # Remove cake so cleanup goes without issues
        with self.target_shard_options.use():
            Cake.objects.all().delete(force=True)

    def test_retarget_relations_missing_target(self):
        """
        Case: Call retarget_relations for a data set where some target data is missing, and not allowed to be copied.
        Expected: Value error raised.
        """
        create_schema_on_node(
            schema_name=self.target_shard_options.schema_name,
            node_name=self.target_shard_options.node_name,
            migrate=True,
        )

        with use_shard(node_name='default', schema_name='public'):
            CakeType.objects.create(name='Gone', id=1)

        with use_shard(node_name='other', schema_name='public'):
            CakeType.objects.create(name='Gone', id=10)

        with self.target_shard_options.use():
            Cake.objects.create(name='Butter cake', type_id=1)

        # Rework cake_type on the target so it's effectively gone, without postgres noticing
        with use_shard(node_name='other', schema_name='public'):
            CakeType.objects.filter(name='Gone').update(name='Really Gone', id=3)

        self.command.source_shard = self.source_shard
        self.command.target_shard_options = self.target_shard_options
        with self.assertRaises(ValueError):
            with mock.patch('example.models.CakeType.__allow_copy', new_callable=PropertyMock, return_value=False):
                self.command.retarget_relations()

        # Remove cake so cleanup goes without issues
        with self.target_shard_options.use():
            Cake.objects.all().delete(force=True)

    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.reset_sequence')
    @mock.patch(
        'djanquiltdb.management.commands.move_shard_to_node.get_all_sharded_models',
        return_value=['app', 'noot', 'mies'],
    )
    def test_reset_sequences(self, mock_get_all_models, mock_reset_sequence):
        """
        Case: Call reset_sequences.
        Expected: reset_sequence called for all sharded models.
        """
        self.command.target_shard_options = self.target_shard_options
        self.command.reset_sequences()

        mock_get_all_models.assert_called_once_with()
        mock_reset_sequence.assert_called_once_with(model_list=['app', 'noot', 'mies'])

    @override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'})
    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.release_advisory_lock')
    def test_post_execution_non_mapping(self, mock_release_lock):
        """
        Case: Call post_execution without a mapping_model.
        Expected: The source shard's state to be restored, node is switched.
        """
        self.command.old_shard_state = State.ACTIVE
        self.source_shard.state = State.MAINTENANCE
        self.source_shard.save()
        self.assertEqual(self.source_shard.node_name, 'default')

        self.command.source_shard = self.source_shard
        self.command.target_node = 'other'
        self.command.post_execution(succeeded=True)

        self.source_shard.refresh_from_db()
        self.assertEqual(self.source_shard.state, State.ACTIVE)
        self.assertEqual(self.source_shard.node_name, 'other')
        mock_release_lock.assert_called_once_with(key='shard_{}'.format(self.source_shard.id), shared=False)

    @override_settings(
        SHARDING={'MAPPING_MODEL': 'example.models.OrganizationShards', 'SHARD_CLASS': 'example.models.Shard'}
    )
    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.release_advisory_lock')
    def test_post_execution_with_mapping(self, mock_release_lock):
        """
        Case: Call post_execution with a mapping_model.
        Expected: The organization's mapping object's state to be restored and the lock to be released.
                  And the shard's node has switched as well.
        """
        with use_shard(self.source_shard):
            organization = Organization.objects.create(name='Layton inc.')
        self.organization_shard1 = OrganizationShards.objects.create(
            shard=self.source_shard, organization_id=organization.id, state=State.ACTIVE
        )

        self.command.old_shard_state = State.ACTIVE
        self.command.old_source_states = {self.organization_shard1.id: State.ACTIVE}
        self.organization_shard1.state = State.MAINTENANCE
        self.organization_shard1.save(update_fields=['state'])

        self.assertEqual(self.source_shard.node_name, 'default')

        mock_release_lock.reset_mock()

        self.command.source_shard = self.source_shard
        self.command.target_node = 'other'
        self.command.post_execution(succeeded=True)

        self.organization_shard1.refresh_from_db()
        self.assertEqual(self.organization_shard1.state, State.ACTIVE)
        self.assertEqual(self.source_shard.node_name, 'other')

        mock_release_lock.assert_has_calls(
            [
                mock.call(key='mapping_{}'.format(self.organization_shard1.id), shared=False),
                mock.call(key='shard_{}'.format(self.source_shard.id), shared=False),
            ]
        )

    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.copy_data', side_effect=DatabaseError)
    @mock.patch('djanquiltdb.management.commands.move_shard_to_node.Command.post_execution')
    def test_post_execution_on_failure(self, mock_post_execution, mock_copy_data):
        """
        Case: Call the handle while move_data will raise an exception.
        Expected: post_execution called with succeeded=False.
        """
        self.command.source_shard = self.source_shard
        self.command.target_node = 'other'

        with self.assertRaises(DatabaseError):
            self.command.handle(**self.options)

        self.assertEqual(mock_copy_data.call_count, 1)
        mock_post_execution.assert_called_once_with(succeeded=False)

from unittest import mock

from django.core.management import CommandError, call_command
from django.db.utils import IntegrityError

from example.models import Shard, SuperType, Type, Organization, User, Cake, CakeType
from djanquiltdb.options import ShardOptions
from djanquiltdb.tests import ShardingTestCase, ShardingTransactionTestCase, OverrideMirroredRoutingMixin
from djanquiltdb.utils import use_shard, create_template_schema, create_schema_on_node
from djanquiltdb.management.commands.purge_schema import Command


class PurgeShardDataTransactionTestCase(OverrideMirroredRoutingMixin, ShardingTransactionTestCase):
    maxDiff = None

    def setUp(self):
        super().setUp()

        create_template_schema()
        create_template_schema('other')

        self.node_options = ShardOptions(node_name='other', schema_name='desolate_lands')

        create_schema_on_node(node_name='other', schema_name='desolate_lands')

        with self.node_options.use():
            self.super = SuperType.objects.create(name='Character')

            self.type_1 = Type.objects.create(name='Professor', super=self.super)
            self.type_2 = Type.objects.create(name='Child', super=self.super)

            self.organization_1 = Organization.objects.create(name='Layton inc.')

            self.user_1 = User.objects.create(name='Layton', email='professor@layton.l5',
                                              organization=self.organization_1, type=self.type_1)

            self.cake_type = CakeType.objects.create(name='The Best')  # public model
            self.cake_1 = Cake.objects.create(name='Butter cake', type=self.cake_type)

            self.user_1.cake.add(self.cake_1)

        # Used for unit tests
        self.options = {
            'schema_name': 'desolate_lands',
            'node_alias': 'other',
            'quiet': True,
            'interactive': False,
        }

        self.command = Command()
        self.command.node = 'other'
        self.command.options = self.options

    def test(self):
        """
        Case: Call `purge_schema` for a inactive schema.
        Expected: The schema is deleted from the database. It's data cannot be access anymore.
        Note: System test
        """
        # Check if we have data on the schema
        with self.node_options.use():
            self.assertTrue(Organization.objects.exists())

        call_command('purge_schema', **self.options)

        # No more data to be found
        with self.assertRaisesMessage(IntegrityError, "Schema 'desolate_lands' does not exist."):
            with self.node_options.use():
                self.assertTrue(Organization.objects.exists())

        with use_shard(node_name='other', schema_name='public') as env:
            self.assertFalse(env.connection.get_ps_schema('desolate_lands'))

        # Should still not exist on default (since it never did) :p
        with use_shard(node_name='default', schema_name='public') as env:
            self.assertFalse(env.connection.get_ps_schema('desolate_lands'))


class PurgeSchemaTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema('default')
        create_template_schema('other')
        create_schema_on_node(node_name='other', schema_name='desolate_lands')

        self.node_options = ShardOptions(node_name='other', schema_name='desolate_lands')

        self.options = {
            'schema_name': 'desolate_lands',
            'node_alias': 'other',
            'quiet': True,
            'interactive': False,
        }

        self.command = Command()
        self.command.node = 'other'
        self.command.options = self.options

    @mock.patch('djanquiltdb.management.commands.purge_schema.Command.get_node', return_value='some node')
    @mock.patch('djanquiltdb.management.commands.purge_schema.ShardOptions')
    @mock.patch('djanquiltdb.management.commands.purge_schema.Command.get_schema_name', return_value='some schema')
    @mock.patch('djanquiltdb.management.commands.purge_schema.Command.confirm')
    @mock.patch('djanquiltdb.management.commands.purge_schema.Command.delete_schema')
    @mock.patch('djanquiltdb.management.commands.purge_schema.Command.print')
    def test_handle(self, mock_print, mock_delete_schema, mock_confirm, mock_get_schema_name, mock_shard_options,
                    mock_get_node):
        """
        Case: Call handle on the command.
        Expected: Bunch of functions called with correct arguments. Completion of the operation reported.
        """
        self.command.handle({}, self.options)

        mock_get_node.assert_called_once_with(self.options)
        mock_get_schema_name.assert_called_once_with(self.options, mock_shard_options.return_value)
        mock_confirm.assert_called_once_with()
        mock_delete_schema.assert_called_once_with()
        mock_print.assert_called_once_with("\nDone. schema 'some schema' has been removed from node 'some node'")

    def test_get_node(self):
        """
        Case: Call get_node on the command with an existing node.
        Expected: The node name returned.
        """
        self.assertEqual(self.command.get_node(self.options), 'other')

    def test_get_node_alias_not_provided(self):
        """
        Case: Call get_node on the command with no value given.
        Expected: CommandError raised
        """
        self.options['node_alias'] = None
        with self.assertRaisesMessage(CommandError, 'A node alias must be provided.'):
            self.command.get_node(self.options)

    def test_get_node_for_non_existing_alias(self):
        """
        Case: Call get_node on the command for a name of a node that does not exist.
        Expected: CommandError raised
        """
        self.options['node_alias'] = 'whoeps'
        with self.assertRaisesMessage(CommandError, "Could not find node 'whoeps' in known set of databases"):
            self.command.get_node(self.options)

    def test_get_schema_name(self):
        """
        Case: Call get_schema_name on the command with an existing, unused schema.
        Expected: The schema name returned.
        """
        self.assertEqual(self.command.get_schema_name(self.options, self.node_options), 'desolate_lands')

    def test_get_schema_name_not_provided(self):
        """
        Case: Call get_schema_name on the command with an existing, unused schema.
        Expected: The schema name returned.
        """
        self.options['schema_name'] = None
        with self.assertRaisesMessage(CommandError, 'A schema name must be provided.'):
            self.command.get_schema_name(self.options, self.node_options)

    def test_get_schema_name_for_non_existing_schema(self):
        """
        Case: Call get_schema_name on the command for a name of a schema that does not exist.
        Expected: CommandError raised
        """
        self.options['schema_name'] = 'whoeps'
        with self.assertRaisesMessage(CommandError, "Could not find schema 'whoeps' on node 'other'"):
            self.command.get_schema_name(self.options, self.node_options)

    def test_get_schema_name_for_wrong_node(self):
        """
        Case: Call get_schema_name on the command for a name of a schema that lives on a different node.
        Expected: CommandError raised
        """
        self.options['node_alias'] = 'default'
        self.node_options.node_name = 'default'
        with self.assertRaisesMessage(CommandError, "Could not find schema 'desolate_lands' on node 'default'"):
            self.command.get_schema_name(self.options, self.node_options)

    def test_get_schema_name_for_active_schema(self):
        """
        Case: Call get_schema_name on the command for a name of a schema that is still in use.
        Expected: CommandError raised
        """
        Shard.objects.create(alias='desolace', node_name='other', schema_name='desolate_lands')
        with self.assertRaisesMessage(CommandError, "schema 'desolate_lands' on node 'other' is in use! "
                                                    "Delete the Shard object using it if you want to remove it."):
            self.command.get_schema_name(self.options, self.node_options)

    def test_get_schema_name_for_schema_active_on_other_node(self):
        """
        Case: Call get_schema_name on the command for a name of a schema that exists on two nodes, and is active on the
              one we are not deleting.
        Expected: The schema name returned
        """
        Shard.objects.create(alias='desolace', node_name='default', schema_name='desolate_lands')
        self.assertEqual(self.command.get_schema_name(self.options, self.node_options), 'desolate_lands')

    @mock.patch('djanquiltdb.management.commands.purge_schema.input')
    def test_confirm_interactive(self, mock_input):
        """
        Case: Call confirm on the command with interactive = True
        Expected: Confirmation input asked from the user. Return value is dependent on the user's answer.
        """
        self.command.schema_name = 'desolate_lands'
        self.command.options['interactive'] = True

        with self.subTest('yes'):
            mock_input.return_value = 'yes'
            self.assertTrue(self.command.confirm())

            mock_input.assert_called_once_with(
                "\nYou have requested to delete schema '\x1b[1mdesolate_lands\x1b[0m' from node \x1b[1mother\x1b[0m.\n"
                "This will IRREVERSIBLY DESTROY all data that lives on this schema.\n"
                "Are you sure you want to do this?\n\n"
                "\tType 'yes' to continue, or 'no' to cancel: ")

            mock_input.reset_mock()

        with self.subTest('no'):
            mock_input.return_value = 'no'
            self.assertFalse(self.command.confirm())

            mock_input.assert_called_once_with(
                "\nYou have requested to delete schema '\x1b[1mdesolate_lands\x1b[0m' from node \x1b[1mother\x1b[0m.\n"
                "This will IRREVERSIBLY DESTROY all data that lives on this schema.\n"
                "Are you sure you want to do this?\n\n"
                "\tType 'yes' to continue, or 'no' to cancel: ")

            mock_input.reset_mock()

    @mock.patch('djanquiltdb.management.commands.purge_schema.input')
    def test_confirm_not_interactive(self, mock_input):
        """
        Case: Call confirm on the command with interactive = False.
        Expected: No confirmation input asked from the user, True returned anyway.
        """
        self.command.schema_name = 'desolate_lands'

        self.command.options['interactive'] = False
        self.assertTrue(self.command.confirm())

        self.assertFalse(mock_input.called)

    @mock.patch('djanquiltdb.postgresql_backend.base.DatabaseWrapper.delete_schema')
    def test_delete_schema(self, mock_delete_schema):
        """
        Case: Call delete_schema on the command.
        Expected: delete_schema called on the connectionwrapper with the schema name as argument.
        """
        self.command.schema_name = 'desolate_lands'
        self.command.node_options = self.node_options
        self.command.delete_schema()

        mock_delete_schema.assert_called_once_with('desolate_lands')

    @mock.patch('djanquiltdb.management.commands.purge_schema.Command.confirm')
    @mock.patch('djanquiltdb.management.commands.purge_schema.Command.print')
    @mock.patch('djanquiltdb.management.commands.purge_schema.Command.delete_schema')
    def test_confirm_not_yes_cancels_operation(self, mock_delete_schema, mock_print, mock_confirm):
        """
        Case: Call command and do not confirm.
        Expected: A message is shown and `delete_data` is not called.
        """
        mock_confirm.return_value = False

        call_command('purge_schema', **self.options)

        mock_print.assert_any_call('\nOperation cancelled.')

        self.assertFalse(mock_delete_schema.called)

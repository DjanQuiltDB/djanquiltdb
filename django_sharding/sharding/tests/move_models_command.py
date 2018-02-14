from unittest import mock

from django.core.management import call_command, CommandError
from django.db import connection

from example.models import Type, User, SuperType, Organization, Shard
from sharding.management.commands.move_sharded_models import Command as MoveCommand
from sharding.tests.utils import ShardingTransactionTestCase
from sharding.utils import migrate_schema, use_shard, create_template_schema, State, get_all_sharded_models, \
    get_template_name


class MoveModelsCommandTestCase(ShardingTransactionTestCase):
    def fake_allow_migrate(self, *args, **hints):
        model = hints.pop('model', False)
        if getattr(model, 'test_model', False):
            return False

    def test(self):
        """
        Case: While all models live on the public schema, call the move_sharded_models command.
        Expected: Only the sharded models (not mirrored) to be moved to a newly created schema.
                  And have a proper template schema.
        Note: System test.
        """
        # Create a situation where the sharded models are on the public schema
        # We do this by flushing the public schema, and migrating it with the router disabled
        with use_shard(node_name='default', schema_name='public') as env:
            env.connection.flush_schema(schema_name='public')
        with mock.patch('sharding.utils.DynamicDbRouter.allow_migrate', side_effect=self.fake_allow_migrate):
            migrate_schema(node_name='default', schema_name='public')

        # Make sure all models now live on the public schema
        self.assertCountEqual(connection.get_schema_for_model(SuperType), [('public',)])
        self.assertCountEqual(connection.get_schema_for_model(Type), [('public',)])
        self.assertCountEqual(connection.get_schema_for_model(User), [('public',)])
        self.assertCountEqual(connection.get_schema_for_model(Organization), [('public',)])

        call_command('move_sharded_models', database='default', target_schema_name='test_target', no_input=True)

        # Sharded models are now moved to the newly created default_shard and the template.
        # Mirrored models are unaffected.
        self.assertTrue(Shard.objects.filter(alias='test_target', node_name='default',
                                             schema_name='test_target').exists())
        self.assertCountEqual(connection.get_schema_for_model(SuperType), [('public',)])
        self.assertCountEqual(connection.get_schema_for_model(Type), [('public',)])
        self.assertCountEqual(connection.get_schema_for_model(User), [('test_target',), ('template', )])
        self.assertCountEqual(connection.get_schema_for_model(Organization), [('test_target',), ('template', )])

    @mock.patch('sharding.management.commands.move_sharded_models.get_all_databases',
                return_value=['default', 'other', 'another'])
    @mock.patch('sharding.management.commands.move_sharded_models.Command.move_models')
    def test_call_on_node_function(self, mock_move_models, mock_get_all_databases):
        """
        Case: Call move_sharded_models command.
        Expected: move_models to be called with the correct argumetns.
        """
        call_command('move_sharded_models', target_schema_name='test_target', no_input=True)

        mock_move_models.assert_called_once_with(node_name='default',
                                                 target_schema_name='test_target',
                                                 sharded_models=get_all_sharded_models())

        self.assertTrue(mock_get_all_databases.called)

    def test_on_existing_shard(self):
        """
        Case: Call move_sharded_models while the target schema already exists.
        Expected: This fact to be called out and the move not to be performed.
        """
        create_template_schema('default')
        Shard.objects.create(alias='another', node_name='default', schema_name='test_target', state=State.ACTIVE)

        # The User table is already on the sharded schema.
        with self.assertRaises(ValueError):
            call_command('move_sharded_models', database='default', target_schema_name='test_target', no_input=True)

    @mock.patch('sharding.management.commands.move_sharded_models.Command.move_models')
    def test_on_not_existing_shard(self, mock_move_models):
        """
        Case: Call move_sharded_models while the target schema already exists on a different node than our target.
        Expected: No error is raised.
        """
        # Make a shard on the 'other' database.
        create_template_schema('other')
        Shard.objects.create(alias='another', node_name='other', schema_name='test_target', state=State.ACTIVE)

        call_command('move_sharded_models', database='default', target_schema_name='test_target', no_input=True)
        self.assertTrue(mock_move_models.called)

    def test_with_public_argument(self):
        """
        Case: Call move_sharded_models command with 'public' as target schema name.
        Expected: CommandError to be raised.
        """
        with self.assertRaises(CommandError):
            call_command('move_sharded_models', database='default', target_schema_name='public', no_input=True)

    def test_with_template_argument(self):
        """
        Case: Call move_sharded_models command with 'public' as target schema name.
        Expected: CommandError to be raised.
        """
        with self.assertRaises(CommandError):
            call_command('move_sharded_models', database='default', target_schema_name=get_template_name(),
                         no_input=True)

    @mock.patch('sharding.management.commands.move_sharded_models.move_model_to_schema')
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.flush_schema')
    @mock.patch('sharding.models.BaseShard.save')
    @mock.patch('sharding.management.commands.move_sharded_models.create_template_schema')
    def test_move_models(self, mock_create_template_schema, mock_model_save, mock_flush_schema,
                         mock_move_model_to_schema):
        """
        Case: Call move_models.
        Expected: Several functions to be called with the correct arguments:
                  - create_template_schema
                  - flush_schema
                  - move_model_to_schema for each model.
                  And a Shard to be created
        """
        MoveCommand().move_models(node_name='default',
                                  target_schema_name='test_target',
                                  sharded_models=get_all_sharded_models())

        mock_create_template_schema.assert_called_once_with('default')
        self.assertTrue(mock_model_save.called)
        mock_flush_schema.assert_called_once_with('test_target')

        for model in get_all_sharded_models():
            mock_move_model_to_schema.assert_any_call(model=model, node_name='default', from_schema_name='public',
                                                      to_schema_name='test_target')

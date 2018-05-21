from unittest import mock

from django.apps import apps
from django.core.exceptions import ValidationError
from django.core.management import CommandError
from django.db import ProgrammingError

from example.models import Organization, Shard, Statement
from sharding.db import connection
from sharding.management.commands.move_sharded_models import Command as MoveCommand
from sharding.tests.utils import ShardingTransactionTestCase
from sharding.utils import migrate_schema, use_shard, create_template_schema, State, get_all_sharded_models, \
    get_template_name


class MoveModelsCommandTestCase(ShardingTransactionTestCase):
    available_apps = ['example']

    def fake_allow_migrate(self, *args, **hints):
        model = hints.pop('model', False)
        if getattr(model, 'test_model', False):
            return False

    def test(self):
        """
        Case: While all models live on the public schema, call the move_sharded_models command.
        Expected: Only the sharded models (not mirrored) to be moved to a newly created schema.
                  This should also include all automatically created fields and sequences.
                  And have a proper template schema.
        Note: System test.
        """
        # Create a situation where the sharded models are on the public schema
        # We do this by flushing the public schema, and migrating it with the router disabled
        with use_shard(node_name='default', schema_name='public') as env:
            env.connection.flush_schema(schema_name='public')
        with mock.patch('sharding.utils.DynamicDbRouter.allow_migrate', side_effect=self.fake_allow_migrate):
            migrate_schema(node_name='default', schema_name='public')

        all_models = [m for m in apps.get_models(include_auto_created=True) if not m._meta.proxy]
        sharded_models = get_all_sharded_models(include_auto_created=True)
        non_sharded_models = set(all_models) - set(sharded_models)

        # Make sure all tables and sequences now live on the public schema
        for model in all_models:
            self.assertCountEqual(connection.get_schema_for_model(model), [('public',)])
            self.assertCountEqual(connection.get_schema_for_sequence('{}_id_seq'.format(model._meta.db_table)),
                                  [('public',)])

        MoveCommand().handle(database='default', target_schema_name='test_target_schema', no_input=True)

        # A new shard is created.
        self.assertTrue(Shard.objects.filter(alias='test_target_schema', node_name='default',
                                             schema_name='test_target_schema').exists())

        # Sharded models are now moved to the newly created default_shard and the template.
        for model in sharded_models:
            self.assertCountEqual(connection.get_schema_for_model(model), [('test_target_schema',), ('template',)])
            self.assertCountEqual(connection.get_schema_for_sequence('{}_id_seq'.format(model._meta.db_table)),
                                  [('test_target_schema',), ('template',)])

        # Mirrored models are unaffected.
        for model in non_sharded_models:
            self.assertCountEqual(connection.get_schema_for_model(model), [('public',)])
            self.assertCountEqual(connection.get_schema_for_sequence('{}_id_seq'.format(model._meta.db_table)),
                                  [('public',)])

    @mock.patch('sharding.management.commands.move_sharded_models.Command.validate')
    def test_rollback_on_validation(self, mock_validate):
        """
        Case: Run move_sharded_models command, and let the validator raise an error.
        Expected: The transaction to be rolled back, no data is modified.
        """
        def fake_validate(target_shard):
            raise ValidationError('test')

        mock_validate.side_effect = fake_validate

        # Create a situation where the sharded models are on the public schema
        # We do this by flushing the public schema, and migrating it with the router disabled
        with use_shard(node_name='default', schema_name='public') as env:
            env.connection.flush_schema(schema_name='public')
        with mock.patch('sharding.utils.DynamicDbRouter.allow_migrate', side_effect=self.fake_allow_migrate):
            migrate_schema(node_name='default', schema_name='public')

        all_models = apps.get_models(include_auto_created=True)

        # Make sure all tables and sequences now live on the public schema
        for model in all_models:
            self.assertCountEqual(connection.get_schema_for_model(model), [('public',)])
            self.assertCountEqual(connection.get_schema_for_sequence('{}_id_seq'.format(model._meta.db_table)),
                                  [('public',)])

        with self.assertRaises(ValidationError):
            MoveCommand().handle(database='default', target_schema_name='test_target_schema', no_input=True)

        self.assertEqual(mock_validate.call_count, 1)

        # Flush the template schema, so it does not turn up in the check below
        with use_shard(node_name='default', schema_name=get_template_name(), include_public=False) as env:
            env.connection.flush_schema(get_template_name())

        # All tables and sequences should still live on the public schema
        for model in all_models:
            self.assertCountEqual(connection.get_schema_for_model(model), [('public',)])
            self.assertCountEqual(connection.get_schema_for_sequence('{}_id_seq'.format(model._meta.db_table)),
                                  [('public',)])

    @mock.patch('sharding.management.commands.move_sharded_models.move_model_to_schema')
    def test_rollback_on_error_during_move(self, mock_move_model_to_schema):
        """
        Case: Run move_sharded_models command, and let the move_model_to_schema util raise an error.
        Expected: The transaction to be rolled back, no data is modified.
        """
        def fake_move_model(*args, **kwargs):
            raise ProgrammingError('test')

        mock_move_model_to_schema.side_effect = fake_move_model

        # Create a situation where the sharded models are on the public schema
        # We do this by flushing the public schema, and migrating it with the router disabled
        with use_shard(node_name='default', schema_name='public') as env:
            env.connection.flush_schema(schema_name='public')
        with mock.patch('sharding.utils.DynamicDbRouter.allow_migrate', side_effect=self.fake_allow_migrate):
            migrate_schema(node_name='default', schema_name='public')

        all_models = apps.get_models(include_auto_created=True)

        # Make sure all tables and sequences now live on the public schema
        for model in all_models:
            self.assertCountEqual(connection.get_schema_for_model(model), [('public',)])
            self.assertCountEqual(connection.get_schema_for_sequence('{}_id_seq'.format(model._meta.db_table)),
                                  [('public',)])

        with self.assertRaises(ProgrammingError):
            MoveCommand().handle(database='default', target_schema_name='test_target_schema', no_input=True)

        self.assertEqual(mock_move_model_to_schema.call_count, 1)  # We error out on the first call

        # Flush the template schema, so it does not turn up in the check below
        with use_shard(node_name='default', schema_name=get_template_name(), include_public=False) as env:
            env.connection.flush_schema(get_template_name())

        # All tables and sequences should still live on the public schema
        for model in all_models:
            self.assertCountEqual(connection.get_schema_for_model(model), [('public',)])
            self.assertCountEqual(connection.get_schema_for_sequence('{}_id_seq'.format(model._meta.db_table)),
                                  [('public',)])

    @mock.patch('sharding.management.commands.move_sharded_models.Command.move_models')
    @mock.patch('sharding.management.commands.move_sharded_models.Command.copy_migration_table')
    @mock.patch('sharding.management.commands.move_sharded_models.Command.validate')
    def test_handle(self, mock_validate, mock_copy_migration_table, mock_move_models):
        """
        Case: Call the handle function of the command.
        Expected: Various functions to be called with the correct arguments.
        """
        create_template_schema('default')

        with mock.patch('sharding.management.commands.move_sharded_models.create_template_schema') \
                as mock_create_template:
            MoveCommand().handle(database='default', target_schema_name='test_target_schema', no_input=True)

        shard = Shard.objects.get(alias='test_target_schema')

        mock_create_template.assert_called_once_with('default')
        mock_move_models.assert_called_once_with(target_shard=shard,
                                                 sharded_models=get_all_sharded_models(include_auto_created=True))
        mock_copy_migration_table.assert_called_once_with(target_shard=shard)
        mock_validate.assert_called_once_with(target_shard=shard)

    @mock.patch('sharding.management.commands.move_sharded_models.Command.copy_migration_table', mock.Mock())
    @mock.patch('sharding.management.commands.move_sharded_models.Command.validate', mock.Mock())
    @mock.patch('sharding.management.commands.move_sharded_models.move_model_to_schema', mock.Mock())
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.flush_schema')
    def test_on_existing_shard(self, mock_flush_schema):
        """
        Case: Call move_sharded_models while the target schema already exists.
        Expected: flush_shard to be called.
        """
        create_template_schema('default')
        shard = Shard.objects.create(alias='test_target_schema', node_name='default', schema_name='test_target_schema',
                                     state=State.ACTIVE)
        with use_shard(shard):
            Organization.objects.create(name="Lilly inc.")

        # The User table is already on the sharded schema.
        MoveCommand().handle(database='default', target_schema_name='test_target_schema', no_input=True)

        mock_flush_schema.assert_called_once_with(shard.schema_name)

    @mock.patch('sharding.management.commands.move_sharded_models.Command.copy_migration_table', mock.Mock())
    @mock.patch('sharding.management.commands.move_sharded_models.Command.validate', mock.Mock())
    @mock.patch('sharding.management.commands.move_sharded_models.Command.move_models')
    def test_on_not_existing_shard(self, mock_move_models):
        """
        Case: Call move_sharded_models while the target schema already exists on a different node than our target.
        Expected: No error is raised.
        """
        # Make a shard on the 'other' database.
        create_template_schema('other')
        Shard.objects.create(alias='test_target', node_name='other', schema_name='test_target', state=State.ACTIVE)

        MoveCommand().handle(database='default', target_schema_name='test_target_schema', no_input=True)
        self.assertTrue(mock_move_models.called)

    def test_with_public_argument(self):
        """
        Case: Call move_sharded_models command with 'public' as target schema name.
        Expected: CommandError to be raised.
        """
        with self.assertRaises(CommandError):
            MoveCommand().handle(database='default', target_schema_name='public', no_input=True)

    def test_with_template_argument(self):
        """
        Case: Call move_sharded_models command with 'public' as target schema name.
        Expected: CommandError to be raised.
        """
        with self.assertRaises(CommandError):
            MoveCommand().handle(database='default', target_schema_name=get_template_name(), no_input=True)

    @mock.patch('sharding.management.commands.move_sharded_models.move_model_to_schema')
    @mock.patch('sharding.postgresql_backend.base.DatabaseWrapper.flush_schema')
    @mock.patch('sharding.models.BaseShard.save')
    def test_move_models(self, mock_model_save, mock_flush_schema, mock_move_model_to_schema):
        """
        Case: Call move_models.
        Expected: Several functions to be called with the correct arguments:
                  - flush_schema
                  - move_model_to_schema for each model.
                  And a Shard to be created
        """
        create_template_schema('default')
        shard = Shard.objects.create(alias='test_target', node_name='default', schema_name='test_target',
                                     state=State.ACTIVE)

        MoveCommand().move_models(target_shard=shard, sharded_models=get_all_sharded_models())

        self.assertTrue(mock_model_save.called)
        mock_flush_schema.assert_called_once_with('test_target')

        for model in get_all_sharded_models():
            mock_move_model_to_schema.assert_any_call(model=model, node_name='default', from_schema_name='public',
                                                      to_schema_name='test_target')

    def test_copy_migration_table(self):
        """
        Case: Make a shard and purge it of all tables, then call copy_migration_table.
        Expected: An exact copy of the django_migration table is created on the target shard.
        """
        # Save contents of public.django_migrations
        with use_shard(node_name='default', schema_name='public') as env:
            cursor = env.connection.cursor()
            cursor.execute('SELECT * FROM "django_migrations";')
            public_migration_table_contents = cursor.fetchall()

        create_template_schema('default')
        shard = Shard.objects.create(alias='test_target', node_name='default', schema_name='test_target',
                                     state=State.ACTIVE)
        with use_shard(shard) as env:
            env.connection.flush_schema(shard.schema_name)

        MoveCommand().copy_migration_table(target_shard=shard)

        with use_shard(shard, include_public=False) as env:
            self.assertCountEqual(env.connection.get_all_table_headers(schema_name=shard.schema_name),
                                  ['django_migrations'])
            self.assertCountEqual(env.connection.get_all_table_sequences(schema_name=shard.schema_name),
                                  ['django_migrations_id_seq'])

            # Check contents vs public.django_migrations
            cursor = env.connection.cursor()
            cursor.execute('SELECT * FROM "django_migrations";')
            self.assertCountEqual(cursor.fetchall(), public_migration_table_contents)

        # Check if the default value for django_mirgations.id is set to the correct sequence.
        # We do this from the template schema so the schema name is added to the column's default value.
        with use_shard(node_name='default', schema_name='template') as env:
            cursor = env.connection.cursor()
            cursor.execute("SELECT column_default FROM information_schema.columns "
                           "WHERE table_schema='{}' AND table_name='django_migrations' AND column_name='id';"
                           .format(shard.schema_name))
            self.assertCountEqual(cursor.fetchall(),
                                  [("nextval('test_target.django_migrations_id_seq'::regclass)", )])
            # This would be "nextval('django_migrations_id_seq'::regclass)" if we're on the test_target schema.
            # And "nextval('public.django_migrations_id_seq'::regclass)" if the command failed to set the default.

    def test_validate(self):
        """
        Case: Use validate() to compare two schemas who are not equal
        Expected: A ValidationError to be raised.
        """
        create_template_schema('default')
        shard = Shard.objects.create(alias='test_target', node_name='default', schema_name='test_target',
                                     state=State.ACTIVE)

        with use_shard(shard) as env:
            cursor = env.connection.cursor()
            cursor.execute('DROP TABLE "{}";'.format(Statement._meta.db_table))

        with self.assertRaises(ValidationError) as error:
            MoveCommand().validate(target_shard=shard)
            self.assertEqual(error.message, "The following tables are not moved: {'example_statement'}")

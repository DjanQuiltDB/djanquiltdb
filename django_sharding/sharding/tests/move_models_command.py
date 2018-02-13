from unittest import mock

from django.core.management import call_command, CommandError
from django.db import connection
from django.test import TransactionTestCase

from example.models import Type, User, SuperType, Organization, Shard
from sharding.utils import migrate_schema, use_shard, create_template_schema, State


class MoveModelsCommandTestCase(TransactionTestCase):
    def cleanup(self):
        if Shard.objects.filter(schema_name='other_schema').exists():
            connection.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format('template'))
            connection.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format('target'))

    def setUp(self):
        self.addCleanup(self.cleanup)

    def fake_allow_migrate(self, *args, **hints):
        model = hints.pop('model', False)
        if getattr(model, 'test_model', False):
            return False

    def test(self):
        """
        Case: While all models live on the public schema, call the move_sharded_models command.
        Expected: Only the sharded models (not mirrored) to be moved to a newly created schema.
                  And have a proper template schema.
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

        call_command('move_sharded_models', database='default', target_schema_name='target', no_input=True)

        # Sharded models are now moved to the newly created default_shard and the template.
        # Mirrored models are unaffected.
        self.assertTrue(Shard.objects.filter(alias='target', node_name='default', schema_name='target').exists())
        self.assertCountEqual(connection.get_schema_for_model(SuperType), [('public',)])
        self.assertCountEqual(connection.get_schema_for_model(Type), [('public',)])
        self.assertCountEqual(connection.get_schema_for_model(User), [('target',), ('template', )])
        self.assertCountEqual(connection.get_schema_for_model(Organization), [('target',), ('template', )])

        # Cleanup
        connection.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format('template'))
        connection.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format('target'))

    def test_on_existing_shard(self):
        """
        Case: Call move_sharded_models while the target schema already exists
        Expected: This fact to be called out and the move not to be performed.
        """
        create_template_schema('default')
        Shard.objects.create(alias='another', node_name='default', schema_name='target', state=State.ACTIVE)

        # The User table is already on the sharded schema.
        with self.assertRaises(ValueError):
            call_command('move_sharded_models', database='default', target_schema_name='target', no_input=True)

        # Cleanup
        connection.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format('template'))
        connection.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format('target'))

    def test_with_invalid_argument(self):
        """
        Case: Call move_sharded_models command with 'public' as target schema name.
        Expected: CommandError to be raised.
        """
        with self.assertRaises(CommandError):
            call_command('move_sharded_models', database='default', target_schema_name='public', no_input=True)

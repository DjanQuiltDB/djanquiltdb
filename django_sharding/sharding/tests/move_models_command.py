from unittest import mock

from django.core.management import call_command
from django.db import connection
from django.test import TestCase, TransactionTestCase

from example.models import Type, User, SuperType, Organization
from sharding.utils import migrate_schema, use_shard


class MoveModelsCommandTestCase(TransactionTestCase):
    def fake_allow_migrate(self, connection_name, app_label, model_name=None, **hints):
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

        call_command('move_sharded_models', database='default', target_schema_name='target')

        # Sharded models are now moved to the newly created default_shard and the template.
        # Mirrored models are unaffected.
        self.assertCountEqual(connection.get_schema_for_model(SuperType), [('public',)])
        self.assertCountEqual(connection.get_schema_for_model(Type), [('public',)])
        self.assertCountEqual(connection.get_schema_for_model(User), [('target',), ('template', )])
        self.assertCountEqual(connection.get_schema_for_model(Organization), [('target',), ('template', )])

        # Cleanup
        connection.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format('template'))
        connection.cursor().execute('DROP SCHEMA "{}" CASCADE;'.format('target'))

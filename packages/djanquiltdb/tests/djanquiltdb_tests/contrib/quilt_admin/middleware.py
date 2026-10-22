from unittest import mock

from django.db import DatabaseError
from django.http import HttpResponse
from django.test.client import RequestFactory

from djanquiltdb.contrib.quilt_admin.middleware import ShardIdAdminOverrideMiddleware, _check_maintenance_status
from djanquiltdb.db import connection
from djanquiltdb.decorators import override_sharding_setting
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import State, create_template_schema, use_shard
from djanquiltdb_tests import ShardingTestCase
from example.models import OrganizationShard, Shard


class CheckMaintenanceStatusTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema()

    def test_a_maintenance_shard_blocks(self):
        """
        Case: The session override points at a shard that is in maintenance.
        Expected: The check reports maintenance and stores the message on the request.
        """
        shard = Shard.objects.create(
            alias='sleepy', schema_name='sleepy_schema', node_name='default', state=State.MAINTENANCE
        )
        request = mock.Mock()

        self.assertTrue(_check_maintenance_status(request, shard_id=shard.id))
        self.assertTrue(request._shard_maintenance_mode)

    def test_a_stale_shard_id_is_not_maintenance(self):
        """
        Case: The session still holds an override for a shard that no longer exists.
        Expected: The admin keeps working so the stale override can be cleared.
        """
        request = mock.Mock()

        self.assertFalse(_check_maintenance_status(request, shard_id=999999))
        self.assertFalse(request._shard_maintenance_mode)

    def test_a_mapping_lookup_error_surfaces(self):
        """
        Case: The mapping lookup during the maintenance check fails with a database error.
        Expected: The error surfaces.
        """
        mapping_class = mock.Mock()
        mapping_class.mapping_field = 'organization_id'
        mapping_class.objects.using.side_effect = DatabaseError('boom')

        request = mock.Mock()

        with mock.patch('djanquiltdb.contrib.quilt_admin.middleware.get_mapping_class', return_value=mapping_class):
            with self.assertRaises(DatabaseError):
                _check_maintenance_status(request, mapping_value=1)

    def test_a_shard_in_maintenance_is_read_where_the_router_reads_the_shard_model(self):
        """
        Case: The session override points at a shard that is in maintenance in the shard registry of the other node,
              which is active, while the registry of the default node, which is the primary node, does not have it.
        Expected: The check reports maintenance, because it reads the shard registry where the router reads the Shard
                  model, which is the active node.
        """
        request = mock.Mock()

        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME) as env:
            env.connection.cursor().execute(
                'INSERT INTO "{}" (id, alias, schema_name, node_name, state) VALUES (%s, %s, %s, %s, %s)'.format(
                    Shard._meta.db_table
                ),
                [9001, 'sleepy', 'sleepy_schema', 'other', State.MAINTENANCE],
            )

            self.assertTrue(_check_maintenance_status(request, shard_id=9001))

    def test_a_mapping_in_maintenance_is_read_where_the_router_reads_the_mapping_model(self):
        """
        Case: The session override points at a mapping value that is in maintenance in the mapping table of the default
              node, which is active, while PRIMARY_DB_ALIAS is the other node and the mapping model sets
              route_to_primary_db to False.
        Expected: The check reports maintenance, because it reads the mapping table where the router reads the mapping
                  model, which is the active node.
        """
        shard = Shard.objects.create(alias='awake', schema_name='awake_schema', node_name='default', state=State.ACTIVE)
        OrganizationShard.objects.create(organization_id=1, shard=shard, state=State.MAINTENANCE)
        request = mock.Mock()

        with (
            override_sharding_setting('PRIMARY_DB_ALIAS', 'other'),
            mock.patch.object(OrganizationShard, 'route_to_primary_db', False),
            use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME),
        ):
            self.assertTrue(_check_maintenance_status(request, mapping_value=1))


class AdminOverrideUseShardMiddlewareTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema('default')
        self.entered_shard = Shard.objects.create(
            alias='entered_shard', schema_name='test_entered_schema', node_name='default', state=State.ACTIVE
        )
        self.override_shard = Shard.objects.create(
            alias='override_shard', schema_name='test_override_schema', node_name='default', state=State.ACTIVE
        )

    def test_enable_shard_inside_a_shard_without_the_public_schema(self):
        """
        Case: Enter the shard that the session override points at, while another shard is entered without the public
              schema.
        Expected: The middleware reads the shard from the shard registry, and enters it.
        """
        request = RequestFactory().get('/admin/')
        middleware = ShardIdAdminOverrideMiddleware(lambda request: None)

        with use_shard(self.entered_shard, include_public=False):
            middleware._enable_shard(request, self.override_shard.id)
            self.assertEqual(connection.schema_name, 'test_override_schema')
            middleware.process_response(request, HttpResponse())

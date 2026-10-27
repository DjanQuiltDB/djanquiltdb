from unittest import mock

from django.test.client import RequestFactory

from djanquiltdb.contrib.quilt_admin import apps as quilt_admin_apps
from djanquiltdb.contrib.quilt_admin.views import SwitchShardView
from djanquiltdb.decorators import override_sharding_setting
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import State, create_template_schema, use_shard
from djanquiltdb_tests import ShardingTestCase
from example.models import OrganizationShard, Shard


class SwitchShardViewTestCase(ShardingTestCase):
    def test_switch_reads_the_mapping_table_where_the_router_reads_the_mapping_model(self):
        """
        Case: Switch to a mapping value that the mapping table of the default node, which is active, has, while
              PRIMARY_DB_ALIAS is the other node and the mapping model sets route_to_primary_db to False.
        Expected: The view stores the mapping value as the override, because it reads the mapping table where the router
                  reads the mapping model.
        """
        create_template_schema('default')
        shard = Shard.objects.create(alias='awake', schema_name='awake_schema', node_name='default', state=State.ACTIVE)
        OrganizationShard.objects.create(organization_id=1, shard=shard)

        request = RequestFactory().post('/admin/switch-shard/', {'mapping_value': '1', 'next': '/admin/'})
        request.session = {}
        request._messages = mock.Mock()

        # The test project has no admin URLs, and the view reverses the admin index for its fallback redirect.
        with (
            mock.patch('djanquiltdb.contrib.quilt_admin.views.reverse', return_value='/admin/'),
            override_sharding_setting('PRIMARY_DB_ALIAS', 'other'),
            mock.patch.object(OrganizationShard, 'route_to_primary_db', False),
            use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME),
        ):
            SwitchShardView().post(request)

        self.assertEqual(quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS.retrieve_override_value(request), 1)

    def test_switch_reads_the_shard_registry_where_the_router_reads_the_shard_model(self):
        """
        Case: Without a mapping model, switch to a shard that the shard registry of the other node, which is active,
              has, while the registry of the default node, which is the primary node, does not have it.
        Expected: The view stores the shard id as the override, because it reads the shard registry where the router
                  reads the Shard model, which is the active node.
        """
        request = RequestFactory().post('/admin/switch-shard/', {'shard_id': '9001'})
        request.session = {}
        request._messages = mock.Mock()

        # The test project has no admin URLs, and the view reverses the admin index for its fallback redirect.
        with (
            mock.patch('djanquiltdb.contrib.quilt_admin.views.reverse', return_value='/admin/'),
            override_sharding_setting('MAPPING_MODEL'),
            use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME) as env,
        ):
            env.connection.cursor().execute(
                'INSERT INTO "{}" (id, alias, schema_name, node_name, state) VALUES (%s, %s, %s, %s, %s)'.format(
                    Shard._meta.db_table
                ),
                [9001, 'on_the_other_node', 'on_the_other_node_schema', 'other', State.ACTIVE],
            )

            SwitchShardView().post(request)

        self.assertEqual(quilt_admin_apps.ADMIN_SHARD_SELECTOR_CLASS.retrieve_override_value(request), '9001')

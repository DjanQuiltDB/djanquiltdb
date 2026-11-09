from unittest import mock

from django.core.exceptions import PermissionDenied
from django.test.client import RequestFactory

from djanquiltdb.contrib.quilt_admin import apps as quilt_admin_apps
from djanquiltdb.contrib.quilt_admin.utils import CrossShardMappingUserProxy, CrossShardUserProxy
from djanquiltdb.contrib.quilt_admin.views import SwitchShardView
from djanquiltdb.decorators import override_sharding_setting
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import State, create_template_schema, use_shard
from djanquiltdb_tests import ShardingTestCase
from djanquiltdb_tests.contrib.quilt_admin.shard_selectors import (
    EvenAliasShardSelector,
    EvenOrganizationsShardSelector,
    QuerysetIgnoringShardSelector,
    RecordingShardSelector,
)
from example.models import OrganizationShard, Shard


def _build_switch_request(post_data):
    request = RequestFactory().post('/admin/switch-shard/', {**post_data, 'next': '/admin/'})
    request.session = {}
    request._messages = mock.Mock()
    return request


def _post_with_shard_selector_class(request, shard_selector_class):
    # The test project has no admin URLs, and the view reverses the admin index for its fallback redirect.
    with (
        mock.patch('djanquiltdb.contrib.quilt_admin.views.reverse', return_value='/admin/'),
        mock.patch('djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', shard_selector_class),
    ):
        return SwitchShardView().post(request)


class SwitchShardViewSwitchableMappingsTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema('default')
        for organization_id in (1, 2):
            shard = Shard.objects.create(
                alias=f'shard_{organization_id}',
                schema_name=f'schema_{organization_id}',
                node_name='default',
                state=State.ACTIVE,
            )
            OrganizationShard.objects.create(organization_id=organization_id, shard=shard)

    def test_switch_to_a_switchable_mapping_sets_the_override(self):
        """
        Case: Switch to organization 2, which switchable_mappings() of the shard selector class returns.
        Expected: The view stores the mapping value as the override and redirects.
        """
        request = _build_switch_request({'mapping_value': '2'})

        response = _post_with_shard_selector_class(request, EvenOrganizationsShardSelector)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(request.session[EvenOrganizationsShardSelector.override_shard_selector_key], 2)

    def test_switch_to_a_mapping_that_is_not_switchable_is_forbidden(self):
        """
        Case: Switch to organization 1, which has a mapping row that switchable_mappings() of the shard selector class
              does not return.
        Expected: The view raises PermissionDenied, which Django turns into a 403 response, and stores no override.
        """
        request = _build_switch_request({'mapping_value': '1'})

        with self.assertRaises(PermissionDenied):
            _post_with_shard_selector_class(request, EvenOrganizationsShardSelector)

        self.assertEqual(request.session, {})

    def test_switch_with_a_hook_that_ignores_its_queryset_is_forbidden(self):
        """
        Case: Switch to organization 1, while switchable_mappings() of the shard selector class returns a queryset of
              organization 2 that it builds itself instead of filtering the queryset that it receives.
        Expected: The view raises PermissionDenied and stores no override, because the queryset that the hook returns
                  does not have organization 1.
        """
        request = _build_switch_request({'mapping_value': '1'})

        with self.assertRaises(PermissionDenied):
            _post_with_shard_selector_class(request, QuerysetIgnoringShardSelector)

        self.assertEqual(request.session, {})

    def test_switchable_mappings_receives_the_user_in_the_home_shard(self):
        """
        Case: The admin of organization 2 has switched to organization 1, so request.user is the cross-shard proxy and
              the shard of organization 1 is entered. The admin switches to organization 2.
        Expected: switchable_mappings() of the shard selector class receives the user itself while the shard of
                  organization 2, which is the home shard of the user, is entered.
        """
        request = _build_switch_request({'mapping_value': '2'})
        request.user = CrossShardMappingUserProxy(mock.sentinel.user, 2)

        with (
            mock.patch.object(RecordingShardSelector, 'calls', []),
            use_shard(Shard.objects.get(alias='shard_1')),
        ):
            _post_with_shard_selector_class(request, RecordingShardSelector)
            calls = RecordingShardSelector.calls

        self.assertEqual(calls, [(mock.sentinel.user, 'schema_2')])
        self.assertIsInstance(request.user, CrossShardMappingUserProxy)

    def test_switch_to_a_nonexistent_mapping_reports_it_does_not_exist(self):
        """
        Case: Switch to organization 3, which has no mapping row.
        Expected: The view reports that the mapping value does not exist, and stores no override.
        """
        request = _build_switch_request({'mapping_value': '3'})

        response = _post_with_shard_selector_class(request, EvenOrganizationsShardSelector)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(request.session, {})
        request._messages.add.assert_called_once()
        self.assertIn('does not exist', request._messages.add.call_args.args[1])


@override_sharding_setting('MAPPING_MODEL')
class SwitchShardViewSwitchableShardsTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema('default')
        self.shard_1 = Shard.objects.create(
            alias='shard_1', schema_name='schema_1', node_name='default', state=State.ACTIVE
        )
        self.shard_2 = Shard.objects.create(
            alias='shard_2', schema_name='schema_2', node_name='default', state=State.ACTIVE
        )

    def test_switch_to_a_switchable_shard_sets_the_override(self):
        """
        Case: Without a mapping model, switch to shard_2, which switchable_shards() of the shard selector class
              returns.
        Expected: The view stores the shard id as the override and redirects.
        """
        request = _build_switch_request({'shard_id': str(self.shard_2.id)})

        response = _post_with_shard_selector_class(request, EvenAliasShardSelector)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(request.session[EvenAliasShardSelector.override_shard_selector_key], str(self.shard_2.id))

    def test_switch_to_a_shard_that_is_not_switchable_is_forbidden(self):
        """
        Case: Without a mapping model, switch to shard_1, which the shard registry has but switchable_shards() of the
              shard selector class does not return.
        Expected: The view raises PermissionDenied, which Django turns into a 403 response, and stores no override.
        """
        request = _build_switch_request({'shard_id': str(self.shard_1.id)})

        with self.assertRaises(PermissionDenied):
            _post_with_shard_selector_class(request, EvenAliasShardSelector)

        self.assertEqual(request.session, {})

    def test_switch_with_a_hook_that_ignores_its_queryset_is_forbidden(self):
        """
        Case: Without a mapping model, switch to shard_1, while switchable_shards() of the shard selector class returns
              a queryset of shard_2 that it builds itself instead of filtering the queryset that it receives.
        Expected: The view raises PermissionDenied and stores no override, because the queryset that the hook returns
                  does not have shard_1.
        """
        request = _build_switch_request({'shard_id': str(self.shard_1.id)})

        with self.assertRaises(PermissionDenied):
            _post_with_shard_selector_class(request, QuerysetIgnoringShardSelector)

        self.assertEqual(request.session, {})

    def test_switchable_shards_receives_the_user_in_the_home_shard(self):
        """
        Case: Without a mapping model, the admin of shard_2 has switched to shard_1, so request.user is the cross-shard
              proxy and shard_1 is entered. The admin switches to shard_2.
        Expected: switchable_shards() of the shard selector class receives the user itself while shard_2, which is the
                  home shard of the user, is entered.
        """
        request = _build_switch_request({'shard_id': str(self.shard_2.id)})
        request.user = CrossShardUserProxy(mock.sentinel.user, self.shard_2.id)

        with mock.patch.object(RecordingShardSelector, 'calls', []), use_shard(self.shard_1):
            _post_with_shard_selector_class(request, RecordingShardSelector)
            calls = RecordingShardSelector.calls

        self.assertEqual(calls, [(mock.sentinel.user, 'schema_2')])
        self.assertIsInstance(request.user, CrossShardUserProxy)

    def test_switch_to_a_nonexistent_shard_reports_it_does_not_exist(self):
        """
        Case: Without a mapping model, switch to a shard id that the shard registry does not have.
        Expected: The view reports that the shard does not exist, and stores no override.
        """
        request = _build_switch_request({'shard_id': '999999'})

        response = _post_with_shard_selector_class(request, EvenAliasShardSelector)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(request.session, {})
        request._messages.add.assert_called_once()
        self.assertIn('does not exist', request._messages.add.call_args.args[1])

    def test_switch_to_a_shard_id_that_is_not_a_number_reports_it_is_invalid(self):
        """
        Case: Without a mapping model, switch to the shard id "abc".
        Expected: The view reports that the shard id is invalid, and stores no override.
        """
        request = _build_switch_request({'shard_id': 'abc'})

        response = _post_with_shard_selector_class(request, EvenAliasShardSelector)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(request.session, {})
        request._messages.add.assert_called_once()
        self.assertIn('invalid', request._messages.add.call_args.args[1])


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

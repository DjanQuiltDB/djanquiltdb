from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError
from django.template import Context, Template
from django.test import RequestFactory, SimpleTestCase, override_settings

import djanquiltdb.contrib.quilt_admin as quilt_admin_pkg
from djanquiltdb.contrib.quilt_admin.apps import ShardedAdminAppConfig
from djanquiltdb.contrib.quilt_admin.context_processors import admin_shard_context, get_shard_switcher_sort_function
from djanquiltdb.contrib.quilt_admin.utils import CrossShardMappingUserProxy, CrossShardUserProxy
from djanquiltdb.decorators import override_sharding_setting
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import State, create_template_schema, use_shard
from djanquiltdb_tests import ShardingTestCase
from djanquiltdb_tests.contrib.quilt_admin.shard_selectors import (
    EvenAliasShardSelector,
    EvenOrganizationsShardSelector,
    RecordingShardSelector,
    SlicingShardSelector,
)
from example.models import OrganizationShard, Shard


def _make_anonymous_request():
    request = mock.Mock()
    request.user.is_authenticated = False
    request.path = '/not-admin/'
    return request


def _make_admin_request():
    request = mock.Mock()
    request.user.is_authenticated = True
    request.path = '/admin/'
    return request


class ShardSwitcherSelectorBindingTestCase(ShardingTestCase):
    def test_selector_class_is_read_dynamically(self):
        """
        Case: Change ADMIN_SHARD_SELECTOR_CLASS of the quilt_admin apps module after the context processor module is
              imported, as happens when a module that imports the context processor is imported before ready() of the
              app runs.
        Expected: The context processor uses the selector class that the apps module has at the time of the call.
        """
        fake_selector = mock.Mock()
        fake_selector.retrieve_override_value.return_value = None
        fake_selector.switchable_mappings.side_effect = lambda request, queryset: queryset

        with mock.patch('djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', fake_selector):
            admin_shard_context(_make_admin_request())

        self.assertTrue(fake_selector.retrieve_override_value.called)


class ShardSwitcherRegistryTestCase(ShardingTestCase):
    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    def test_switcher_reads_the_shard_registry_where_the_router_reads_the_shard_model(self):
        """
        Case: The shard registry of the other node, which is active, has a shard that the registry of the default node
              does not have.
        Expected: The switcher lists the shard of the other node, because it reads the shard registry where the router
                  reads the Shard model, which is the active node.
        """
        with use_shard(node_name='other', schema_name=PUBLIC_SCHEMA_NAME) as env:
            env.connection.cursor().execute(
                'INSERT INTO "{}" (id, alias, schema_name, node_name, state) VALUES (%s, %s, %s, %s, %s)'.format(
                    Shard._meta.db_table
                ),
                [1, 'on_the_other_node', 'on_the_other_node_schema', 'other', State.ACTIVE],
            )

            context = admin_shard_context(_make_admin_request())

        self.assertEqual([shard.alias for shard in context['available_shards']], ['on_the_other_node'])

    def test_switcher_reads_the_mapping_table_where_the_router_reads_the_mapping_model(self):
        """
        Case: PRIMARY_DB_ALIAS is the other node, the mapping model sets route_to_primary_db to False, and the mapping
              table of the default node, which is active, has a row.
        Expected: The switcher lists the row of the default node, because it reads the mapping table where the router
                  reads the mapping model.
        """
        create_template_schema('default')
        shard = Shard.objects.create(alias='awake', schema_name='awake_schema', node_name='default', state=State.ACTIVE)
        OrganizationShard.objects.create(organization_id=1, shard=shard)

        with (
            override_sharding_setting('PRIMARY_DB_ALIAS', 'other'),
            mock.patch.object(OrganizationShard, 'route_to_primary_db', False),
            use_shard(node_name='default', schema_name=PUBLIC_SCHEMA_NAME),
        ):
            context = admin_shard_context(_make_admin_request())

        self.assertEqual([mapping.organization_id for mapping in context['available_shards']], [1])


class ShardSwitcherSwitchableMappingsTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema('default')
        for organization_id in (1, 2, 3, 4):
            shard = Shard.objects.create(
                alias=f'shard_{organization_id}',
                schema_name=f'schema_{organization_id}',
                node_name='default',
                state=State.ACTIVE,
            )
            OrganizationShard.objects.create(organization_id=organization_id, shard=shard)

    def test_default_selector_lists_every_mapping(self):
        """
        Case: The mapping table has four rows, and the shard selector class does not override switchable_mappings().
        Expected: The switcher lists all four mappings.
        """
        context = admin_shard_context(_make_admin_request())

        self.assertEqual([option['value'] for option in context['shard_switcher_options']], ['1', '2', '3', '4'])
        self.assertEqual([mapping.organization_id for mapping in context['available_shards']], [1, 2, 3, 4])

    def test_switcher_lists_only_the_switchable_mappings(self):
        """
        Case: The mapping table has four rows, and switchable_mappings() of the shard selector class returns the
              mappings of even organization ids only.
        Expected: The switcher lists the mappings of organizations 2 and 4, and not the mappings of organizations 1 and
                  3.
        """
        with mock.patch(
            'djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', EvenOrganizationsShardSelector
        ):
            context = admin_shard_context(_make_admin_request())

        self.assertEqual([option['value'] for option in context['shard_switcher_options']], ['2', '4'])
        self.assertEqual([mapping.organization_id for mapping in context['available_shards']], [2, 4])

    def test_switchable_mappings_receives_the_request(self):
        """
        Case: Build the switcher context for a request, with a mock in place of switchable_mappings() of the shard
              selector class.
        Expected: switchable_mappings() receives that request as its first argument.
        """
        request = _make_admin_request()

        with (
            mock.patch(
                'djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', EvenOrganizationsShardSelector
            ),
            mock.patch.object(
                EvenOrganizationsShardSelector,
                'switchable_mappings',
                side_effect=lambda request, queryset: queryset,
            ) as switchable_mappings,
        ):
            admin_shard_context(request)

        self.assertIs(switchable_mappings.call_args.args[0], request)

    def test_switchable_mappings_receives_the_user_in_the_home_shard(self):
        """
        Case: The admin of organization 2 has switched to organization 1, so request.user is the cross-shard proxy and
              the shard of organization 1 is entered. The switcher context is built.
        Expected: switchable_mappings() of the shard selector class receives the user itself while the shard of
                  organization 2, which is the home shard of the user, is entered. Afterwards, request.user is the
                  cross-shard proxy.
        """
        request = _make_admin_request()
        user = mock.Mock(is_authenticated=True)
        request.user = CrossShardMappingUserProxy(user, 2)

        with (
            mock.patch('djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', RecordingShardSelector),
            mock.patch.object(RecordingShardSelector, 'calls', []),
            use_shard(Shard.objects.get(alias='shard_1')),
        ):
            admin_shard_context(request)
            calls = RecordingShardSelector.calls

        self.assertEqual(calls, [(user, 'schema_2')])
        self.assertIsInstance(request.user, CrossShardMappingUserProxy)

    def test_a_hook_that_returns_a_slice_is_improperly_configured(self):
        """
        Case: switchable_mappings() of the shard selector class returns a queryset with a slice.
        Expected: Building the switcher context raises ImproperlyConfigured, which names switchable_mappings().
        """
        with mock.patch('djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', SlicingShardSelector):
            with self.assertRaisesMessage(ImproperlyConfigured, 'switchable_mappings() of SlicingShardSelector'):
                admin_shard_context(_make_admin_request())


@override_sharding_setting('MAPPING_MODEL')
class ShardSwitcherSwitchableShardsTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema('default')
        for number in (1, 2, 3, 4):
            Shard.objects.create(
                alias=f'shard_{number}', schema_name=f'schema_{number}', node_name='default', state=State.ACTIVE
            )

    def test_default_selector_lists_every_shard(self):
        """
        Case: The project has no mapping model, the shard registry has four shards, and the shard selector class does
              not override switchable_shards().
        Expected: The switcher lists all four shards.
        """
        context = admin_shard_context(_make_admin_request())

        self.assertEqual(
            [shard.alias for shard in context['available_shards']], ['shard_1', 'shard_2', 'shard_3', 'shard_4']
        )

    def test_switcher_lists_only_the_switchable_shards(self):
        """
        Case: The project has no mapping model, the shard registry has four shards, and switchable_shards() of the
              shard selector class returns shard_2 and shard_4 only.
        Expected: The switcher lists shard_2 and shard_4, both in its options and in available_shards, and not shard_1
                  and shard_3.
        """
        with mock.patch('djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', EvenAliasShardSelector):
            context = admin_shard_context(_make_admin_request())

        self.assertEqual([shard.alias for shard in context['available_shards']], ['shard_2', 'shard_4'])
        self.assertEqual(
            [option['value'] for option in context['shard_switcher_options']],
            [str(shard.id) for shard in Shard.objects.filter(alias__in=['shard_2', 'shard_4']).order_by('alias')],
        )

    def test_switchable_shards_receives_the_request(self):
        """
        Case: The project has no mapping model. Build the switcher context for a request, with a mock in place of
              switchable_shards() of the shard selector class.
        Expected: switchable_shards() receives that request as its first argument.
        """
        request = _make_admin_request()

        with (
            mock.patch('djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', EvenAliasShardSelector),
            mock.patch.object(
                EvenAliasShardSelector,
                'switchable_shards',
                side_effect=lambda request, queryset: queryset,
            ) as switchable_shards,
        ):
            admin_shard_context(request)

        self.assertIs(switchable_shards.call_args.args[0], request)

    def test_switchable_shards_receives_the_user_in_the_home_shard(self):
        """
        Case: The project has no mapping model. The admin of shard_2 has switched to shard_1, so request.user is the
              cross-shard proxy and shard_1 is entered. The switcher context is built.
        Expected: switchable_shards() of the shard selector class receives the user itself while shard_2, which is the
                  home shard of the user, is entered. Afterwards, request.user is the cross-shard proxy.
        """
        request = _make_admin_request()
        user = mock.Mock(is_authenticated=True)
        request.user = CrossShardUserProxy(user, Shard.objects.get(alias='shard_2').id)

        with (
            mock.patch('djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', RecordingShardSelector),
            mock.patch.object(RecordingShardSelector, 'calls', []),
            use_shard(Shard.objects.get(alias='shard_1')),
        ):
            admin_shard_context(request)
            calls = RecordingShardSelector.calls

        self.assertEqual(calls, [(user, 'schema_2')])
        self.assertIsInstance(request.user, CrossShardUserProxy)

    def test_a_hook_that_returns_a_slice_is_improperly_configured(self):
        """
        Case: The project has no mapping model, and switchable_shards() of the shard selector class returns a queryset
              with a slice.
        Expected: Building the switcher context raises ImproperlyConfigured, which names switchable_shards().
        """
        with mock.patch('djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', SlicingShardSelector):
            with self.assertRaisesMessage(ImproperlyConfigured, 'switchable_shards() of SlicingShardSelector'):
                admin_shard_context(_make_admin_request())


def descending_organization_id_sort_key(value, shard, mapping):
    return -mapping.organization_id


class ShardSwitcherSortTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()
        create_template_schema('default')

    def _create_shard_with_mapping_row(self, alias, schema_name, organization_id):
        shard = Shard.objects.create(alias=alias, schema_name=schema_name, node_name='default', state=State.ACTIVE)
        OrganizationShard.objects.create(organization_id=organization_id, shard=shard)

    def _create_three_shards_with_mapping_rows(self):
        self._create_shard_with_mapping_row(alias='Zulu_shard', schema_name='mike_schema', organization_id=2)
        self._create_shard_with_mapping_row(alias='alpha_shard', schema_name='yankee_schema', organization_id=10)
        self._create_shard_with_mapping_row(alias='Bravo_shard', schema_name='delta_schema', organization_id=30)

    def _shard_switcher_option_values(self, context):
        return [option['value'] for option in context['shard_switcher_options']]

    def test_default_sorts_string_mapping_values_ignoring_case(self):
        """
        Case: The mapping table has the string mapping values beta_tenant, Alpha_tenant and gamma_tenant, and
              QUILT_ADMIN has no SHARD_SWITCHER_SORT_KEY.
        Expected: The switcher lists Alpha_tenant, beta_tenant and gamma_tenant, because the default sorts by mapping
                  value and ignores case.
        """
        mappings = [
            SimpleNamespace(
                tenant_slug=slug,
                state=State.ACTIVE,
                shard=SimpleNamespace(alias=slug, node_name='default', schema_name=slug, state=State.ACTIVE),
            )
            for slug in ('beta_tenant', 'Alpha_tenant', 'gamma_tenant')
        ]
        mapping_class = mock.Mock()
        mapping_class.mapping_field = 'tenant_slug'
        all_mappings = mapping_class.objects.using.return_value.select_related.return_value.all.return_value
        all_mappings.query.is_sliced = False
        all_mappings.order_by.return_value = mappings

        with mock.patch(
            'djanquiltdb.contrib.quilt_admin.context_processors.get_mapping_class', return_value=mapping_class
        ):
            context = admin_shard_context(_make_admin_request())

        self.assertEqual(self._shard_switcher_option_values(context), ['Alpha_tenant', 'beta_tenant', 'gamma_tenant'])

    def test_default_sorts_integer_mapping_values_as_numbers(self):
        """
        Case: The mapping table has the organization ids 10 and 2, and QUILT_ADMIN has no SHARD_SWITCHER_SORT_KEY.
        Expected: The switcher lists 2 before 10, because integer mapping values are compared as numbers.
        """
        self._create_shard_with_mapping_row(alias='ten_shard', schema_name='ten_schema', organization_id=10)
        self._create_shard_with_mapping_row(alias='two_shard', schema_name='two_schema', organization_id=2)

        context = admin_shard_context(_make_admin_request())

        self.assertEqual(self._shard_switcher_option_values(context), ['2', '10'])

    @override_settings(QUILT_ADMIN={'SHARD_SWITCHER_SORT_KEY': 'shard_alias'})
    def test_shard_alias_sorts_by_the_alias_of_the_shard_ignoring_case(self):
        """
        Case: SHARD_SWITCHER_SORT_KEY is shard_alias, and the mapping table has rows whose shards have the aliases
              Zulu_shard, alpha_shard and Bravo_shard.
        Expected: The switcher lists the rows of alpha_shard, Bravo_shard and Zulu_shard, in that order, both in its
                  options and in available_shards.
        """
        self._create_three_shards_with_mapping_rows()

        context = admin_shard_context(_make_admin_request())

        self.assertEqual(self._shard_switcher_option_values(context), ['10', '30', '2'])
        self.assertEqual([mapping.organization_id for mapping in context['available_shards']], [10, 30, 2])

    @override_settings(QUILT_ADMIN={'SHARD_SWITCHER_SORT_KEY': 'schema_name'})
    def test_schema_name_sorts_by_the_schema_name_of_the_shard(self):
        """
        Case: SHARD_SWITCHER_SORT_KEY is schema_name, and the mapping table has rows whose shards have the schema names
              mike_schema, yankee_schema and delta_schema.
        Expected: The switcher lists the rows of delta_schema, mike_schema and yankee_schema, in that order.
        """
        self._create_three_shards_with_mapping_rows()

        context = admin_shard_context(_make_admin_request())

        self.assertEqual(self._shard_switcher_option_values(context), ['30', '2', '10'])

    @override_settings(
        QUILT_ADMIN={
            'SHARD_SWITCHER_SORT_KEY': (
                'djanquiltdb_tests.contrib.quilt_admin.context_processors.descending_organization_id_sort_key'
            )
        }
    )
    def test_dotted_path_names_a_custom_sort_function(self):
        """
        Case: SHARD_SWITCHER_SORT_KEY is the dotted path of a function that sorts by descending organization id.
        Expected: The switcher lists the organization ids 30, 10 and 2, in that order.
        """
        self._create_three_shards_with_mapping_rows()

        context = admin_shard_context(_make_admin_request())

        self.assertEqual(self._shard_switcher_option_values(context), ['30', '10', '2'])

    def test_callable_is_used_as_the_sort_function(self):
        """
        Case: SHARD_SWITCHER_SORT_KEY is a function that sorts by descending organization id.
        Expected: The switcher lists the organization ids 30, 10 and 2, in that order, and the function receives the
                  mapping value, the shard and the mapping row of each option.
        """
        self._create_three_shards_with_mapping_rows()
        sort_function = mock.Mock(side_effect=descending_organization_id_sort_key)

        with override_settings(QUILT_ADMIN={'SHARD_SWITCHER_SORT_KEY': sort_function}):
            context = admin_shard_context(_make_admin_request())

        self.assertEqual(self._shard_switcher_option_values(context), ['30', '10', '2'])
        mapping = OrganizationShard.objects.get(organization_id=2)
        sort_function.assert_any_call(2, mapping.shard, mapping)

    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    def test_default_sorts_by_alias_ignoring_case_without_a_mapping_model(self):
        """
        Case: The project has no mapping model, QUILT_ADMIN has no SHARD_SWITCHER_SORT_KEY, and the shard registry has
              the shards beta_shard, Charlie_shard and alpha_shard.
        Expected: The switcher lists alpha_shard, beta_shard and Charlie_shard, in that order, because the mapping
                  value falls back to the alias of the shard and case is ignored.
        """
        for alias in ('beta_shard', 'Charlie_shard', 'alpha_shard'):
            Shard.objects.create(alias=alias, schema_name=f'{alias.lower()}_schema', node_name='default')

        context = admin_shard_context(_make_admin_request())

        self.assertEqual(
            [shard.alias for shard in context['available_shards']], ['alpha_shard', 'beta_shard', 'Charlie_shard']
        )
        self.assertEqual(
            self._shard_switcher_option_values(context), [str(shard.id) for shard in context['available_shards']]
        )

    @override_settings(QUILT_ADMIN={'SHARD_SWITCHER_SORT_KEY': 'tenant_name'})
    def test_unknown_name_is_improperly_configured(self):
        """
        Case: SHARD_SWITCHER_SORT_KEY is tenant_name, which is not a built-in choice and not a dotted path.
        Expected: get_shard_switcher_sort_function() and ready() of the quilt_admin app raise ImproperlyConfigured.
        """
        with self.assertRaises(ImproperlyConfigured):
            get_shard_switcher_sort_function()

        with self.assertRaises(ImproperlyConfigured):
            ShardedAdminAppConfig('djanquiltdb.contrib.quilt_admin', quilt_admin_pkg).ready()

    @override_settings(QUILT_ADMIN={'SHARD_SWITCHER_SORT_KEY': ['shard_alias', 'schema_name']})
    def test_list_is_improperly_configured(self):
        """
        Case: SHARD_SWITCHER_SORT_KEY is the list ['shard_alias', 'schema_name'].
        Expected: get_shard_switcher_sort_function() raises ImproperlyConfigured, because the setting is not a
                  string or a function.
        """
        with self.assertRaises(ImproperlyConfigured):
            get_shard_switcher_sort_function()


class ShardSwitcherErrorHandlingTestCase(ShardingTestCase):
    def test_a_database_error_degrades_the_switcher_but_is_logged(self):
        """
        Case: Listing the entries of the switcher fails with a database error.
        Expected: The context has no switcher options, and the error is logged as a warning.
        """
        mapping_class = mock.Mock()
        mapping_class.mapping_field = 'organization_id'
        mapping_class.objects.using.side_effect = DatabaseError('boom')

        with mock.patch(
            'djanquiltdb.contrib.quilt_admin.context_processors.get_mapping_class', return_value=mapping_class
        ):
            with self.assertLogs('djanquiltdb.contrib.quilt_admin.context_processors', 'WARNING'):
                context = admin_shard_context(_make_admin_request())

        self.assertEqual(context['shard_switcher_options'], [])


class ShardSwitcherMaintenanceContextTestCase(SimpleTestCase):
    def test_request_that_the_override_middleware_did_not_process_is_not_in_maintenance(self):
        """
        Case: Build the context of a request that no admin override middleware processed, so the request has no
              maintenance status.
        Expected: shard_maintenance_mode is False and shard_maintenance_message is None.
        """
        request = RequestFactory().get('/not-admin/')
        request.user = AnonymousUser()

        context = admin_shard_context(request)

        self.assertIs(context['shard_maintenance_mode'], False)
        self.assertIsNone(context['shard_maintenance_message'])


class UseCspNonceContextTests(SimpleTestCase):
    @override_settings()
    def test_defaults_to_false_when_quilt_admin_unset(self):
        """
        Case: Build the context of a request while the project has no QUILT_ADMIN setting.
        Expected: use_csp_nonce is False, so the shard switcher's script is rendered without a nonce attribute.
        """
        from django.conf import settings as django_settings

        if hasattr(django_settings, 'QUILT_ADMIN'):
            del django_settings.QUILT_ADMIN

        context = admin_shard_context(_make_anonymous_request())

        self.assertIs(context['use_csp_nonce'], False)

    @override_settings(QUILT_ADMIN={})
    def test_defaults_to_false_when_setting_missing(self):
        """
        Case: Build the context of a request while QUILT_ADMIN is a dict without USE_CSP_NONCE.
        Expected: use_csp_nonce is False, because USE_CSP_NONCE defaults to False.
        """
        context = admin_shard_context(_make_anonymous_request())

        self.assertIs(context['use_csp_nonce'], False)

    @override_settings(QUILT_ADMIN={'USE_CSP_NONCE': True})
    def test_true_when_setting_enabled(self):
        """
        Case: Build the context of a request while QUILT_ADMIN['USE_CSP_NONCE'] is True.
        Expected: use_csp_nonce is True, so the shard switcher's script is rendered with the nonce of the request.
        """
        context = admin_shard_context(_make_anonymous_request())

        self.assertIs(context['use_csp_nonce'], True)


class ShardSwitcherScriptNonceTemplateTests(SimpleTestCase):
    """
    Exercises the conditional `nonce=` attribute used by the shard-switcher inline script in `admin/base_site.html`.
    Rendering the full template is avoided because the test settings don't install `django.contrib.admin`.
    """

    fragment = (
        '<script{% if use_csp_nonce %} nonce="{{ csp_nonce }}"{% endif %}>'
        "document.getElementById('shard-select').addEventListener('change', function () {"
        'this.form.submit();'
        '});'
        '</script>'
    )

    def test_no_nonce_attribute_when_disabled(self):
        """
        Case: Render the script fragment with use_csp_nonce False and a csp_nonce in the context.
        Expected: A plain <script> tag without a nonce attribute, although the context has a nonce.
        """
        rendered = Template(self.fragment).render(
            Context(
                {
                    'use_csp_nonce': False,
                    'csp_nonce': 'abc123',
                }
            )
        )

        self.assertIn('<script>', rendered)
        self.assertNotIn('nonce=', rendered)

    def test_nonce_attribute_when_enabled(self):
        """
        Case: Render the script fragment with use_csp_nonce True and the csp_nonce abc123 in the context.
        Expected: The <script> tag has the attribute nonce="abc123".
        """
        rendered = Template(self.fragment).render(
            Context(
                {
                    'use_csp_nonce': True,
                    'csp_nonce': 'abc123',
                }
            )
        )

        self.assertIn('nonce="abc123"', rendered)


class BaseSiteTemplateFileTests(SimpleTestCase):
    """Verifies the real template file reflects the CSP-nonce transplant."""

    def _template_contents(self):
        path = Path(quilt_admin_pkg.__file__).parent / 'templates' / 'admin' / 'base_site.html'
        return path.read_text()

    def test_inline_onchange_attribute_removed(self):
        """
        Case: Read admin/base_site.html of the quilt_admin app.
        Expected: No inline onchange= attribute, because a Content-Security-Policy without 'unsafe-inline' blocks inline
                  event handlers.
        """
        self.assertNotIn('onchange=', self._template_contents())

    def test_script_block_and_conditional_nonce_present(self):
        """
        Case: Read admin/base_site.html of the quilt_admin app.
        Expected: The template adds the change listener of the shard switcher in a script block, whose nonce attribute
                  depends on use_csp_nonce.
        """
        contents = self._template_contents()
        self.assertIn("addEventListener('change'", contents)
        self.assertIn('{% if use_csp_nonce %} nonce="{{ csp_nonce }}"{% endif %}', contents)

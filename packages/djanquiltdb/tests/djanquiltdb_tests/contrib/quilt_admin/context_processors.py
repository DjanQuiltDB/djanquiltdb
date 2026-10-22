from pathlib import Path
from unittest import mock

from django.db import DatabaseError
from django.template import Context, Template
from django.test import SimpleTestCase, override_settings

import djanquiltdb.contrib.quilt_admin as quilt_admin_pkg
from djanquiltdb.contrib.quilt_admin.context_processors import admin_shard_context
from djanquiltdb.decorators import override_sharding_setting
from djanquiltdb.postgresql_backend.base import PUBLIC_SCHEMA_NAME
from djanquiltdb.utils import State, create_template_schema, use_shard
from djanquiltdb_tests import ShardingTestCase
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

from pathlib import Path
from unittest import mock

from django.template import Context, Template
from django.test import SimpleTestCase, override_settings

import djanquiltdb.contrib.quilt_admin as quilt_admin_pkg
from djanquiltdb.contrib.quilt_admin.context_processors import admin_shard_context
from djanquiltdb.utils import State, use_shard
from djanquiltdb_tests import ShardingTestCase
from example.models import Shard


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
        Case: The selector class binding on the quilt_admin apps module changes after this module was mported, as
              happens when a consumer module is imported before the app's ready() ran.
        Expected: The context processor uses the current binding rather than a stale import-time copy.
        """
        fake_selector = mock.Mock()
        fake_selector.retrieve_override_value.return_value = None

        with mock.patch('djanquiltdb.contrib.quilt_admin.apps.ADMIN_SHARD_SELECTOR_CLASS', fake_selector):
            admin_shard_context(_make_admin_request())

        self.assertTrue(fake_selector.retrieve_override_value.called)


class ShardSwitcherPrimaryAliasTestCase(ShardingTestCase):
    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard', 'PRIMARY_DB_ALIAS': 'other'})
    def test_switcher_reads_from_the_primary_alias(self):
        """
        Case: PRIMARY_DB_ALIAS points at a non-default node.
        Expected: The switcher lists the shards from that node instead of the literal default alias.
        """
        with use_shard(node_name='other', schema_name='public') as env:
            env.connection.cursor().execute(
                'INSERT INTO "{}" (id, alias, schema_name, node_name, state) '
                'VALUES (%s, %s, %s, %s, %s)'.format(Shard._meta.db_table),
                [1, 'failover', 'failover_schema', 'other', State.ACTIVE],
            )

        context = admin_shard_context(_make_admin_request())

        self.assertEqual([shard.alias for shard in context['available_shards']], ['failover'])


class UseCspNonceContextTests(SimpleTestCase):
    @override_settings()
    def test_defaults_to_false_when_quilt_admin_unset(self):
        from django.conf import settings as django_settings

        if hasattr(django_settings, 'QUILT_ADMIN'):
            del django_settings.QUILT_ADMIN

        context = admin_shard_context(_make_anonymous_request())

        self.assertIs(context['use_csp_nonce'], False)

    @override_settings(QUILT_ADMIN={})
    def test_defaults_to_false_when_setting_missing(self):
        context = admin_shard_context(_make_anonymous_request())

        self.assertIs(context['use_csp_nonce'], False)

    @override_settings(QUILT_ADMIN={'USE_CSP_NONCE': True})
    def test_true_when_setting_enabled(self):
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
        self.assertNotIn('onchange=', self._template_contents())

    def test_script_block_and_conditional_nonce_present(self):
        contents = self._template_contents()
        self.assertIn("addEventListener('change'", contents)
        self.assertIn('{% if use_csp_nonce %} nonce="{{ csp_nonce }}"{% endif %}', contents)

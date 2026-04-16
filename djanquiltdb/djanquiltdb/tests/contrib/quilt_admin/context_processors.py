from pathlib import Path
from unittest import mock

from django.template import Context, Template
from django.test import SimpleTestCase, override_settings

import djanquiltdb.contrib.quilt_admin as quilt_admin_pkg
from djanquiltdb.contrib.quilt_admin.context_processors import admin_shard_context


def _make_anonymous_request():
    request = mock.Mock()
    request.user.is_authenticated = False
    request.path = '/not-admin/'
    return request


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

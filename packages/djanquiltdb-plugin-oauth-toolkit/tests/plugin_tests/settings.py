from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase, override_settings

from djanquiltdb_plugin_oauth_toolkit.settings import QUILT_OAUTH_TOOLKIT_DEFAULTS, quilt_oauth_toolkit_setting


class QuiltOAuthToolkitSettingTestCase(SimpleTestCase):
    def test_every_key_has_its_default_without_the_setting(self):
        """
        Case: quilt_oauth_toolkit_setting() is called for each key while the project has no QUILT_OAUTH_TOOLKIT.
        Expected: The default of each key.
        """
        for name, default in QUILT_OAUTH_TOOLKIT_DEFAULTS.items():
            with self.subTest(name=name):
                self.assertEqual(quilt_oauth_toolkit_setting(name), default)

    @override_settings(QUILT_OAUTH_TOOLKIT={'TOKEN_DELIMITER': 'X'})
    def test_a_key_of_the_setting_overrides_its_default(self):
        """
        Case: QUILT_OAUTH_TOOLKIT sets TOKEN_DELIMITER to 'X'.
        Expected: TOKEN_DELIMITER is 'X', and CLIENT_ID_DELIMITER keeps its default.
        """
        self.assertEqual(quilt_oauth_toolkit_setting('TOKEN_DELIMITER'), 'X')
        self.assertEqual(
            quilt_oauth_toolkit_setting('CLIENT_ID_DELIMITER'), QUILT_OAUTH_TOOLKIT_DEFAULTS['CLIENT_ID_DELIMITER']
        )

    @override_settings(QUILT_OAUTH_TOOLKIT=['TOKEN_DELIMITER'])
    def test_a_setting_that_is_not_a_dict_raises(self):
        """
        Case: QUILT_OAUTH_TOOLKIT is a list, and quilt_oauth_toolkit_setting() is called.
        Expected: ImproperlyConfigured that says QUILT_OAUTH_TOOLKIT is not a dict.
        """
        with self.assertRaisesMessage(ImproperlyConfigured, 'QUILT_OAUTH_TOOLKIT is not a dict.'):
            quilt_oauth_toolkit_setting('TOKEN_DELIMITER')

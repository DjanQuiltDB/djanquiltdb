from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase, override_settings

from djanquiltdb_plugin_celery.settings import QUILT_CELERY_DEFAULTS, quilt_celery_setting


class QuiltCelerySettingTestCase(SimpleTestCase):
    def test_every_key_has_its_default_without_the_setting(self):
        """
        Case: quilt_celery_setting() is called for each key while the project has no QUILT_CELERY.
        Expected: The default of each key.
        """
        for name, default in QUILT_CELERY_DEFAULTS.items():
            with self.subTest(name=name):
                self.assertEqual(quilt_celery_setting(name), default)

    @override_settings(QUILT_CELERY={'MAX_RETRIES': 2})
    def test_a_key_of_the_setting_overrides_its_default(self):
        """
        Case: QUILT_CELERY sets MAX_RETRIES to 2.
        Expected: MAX_RETRIES is 2, and RETRY_BACKOFF keeps its default.
        """
        self.assertEqual(quilt_celery_setting('MAX_RETRIES'), 2)
        self.assertEqual(quilt_celery_setting('RETRY_BACKOFF'), QUILT_CELERY_DEFAULTS['RETRY_BACKOFF'])

    def test_a_setting_that_is_not_a_dict_raises(self):
        """
        Case: quilt_celery_setting() is called for each key while QUILT_CELERY is None, and then a list.
        Expected: ImproperlyConfigured, and not the defaults, so that the plugin never runs on settings that the
                  project did not give it.
        """
        for setting in [None, ['MAX_RETRIES']]:
            with self.settings(QUILT_CELERY=setting):
                for name in QUILT_CELERY_DEFAULTS:
                    with (
                        self.subTest(setting=setting, name=name),
                        self.assertRaisesMessage(ImproperlyConfigured, 'QUILT_CELERY is not a dict'),
                    ):
                        quilt_celery_setting(name)

from unittest import skipIf, skipUnless

from django.apps import apps
from django.core.checks.registry import registry
from django.test import SimpleTestCase, override_settings

from djanquiltdb.checks import MUTE_SETTING, PGTRIGGER_APP_NAME, check_pgtrigger_compatibility
from djanquiltdb.decorators import override_sharding_setting

# Only the dedicated tox environment installs django-pgtrigger and puts it in INSTALLED_APPS. Everywhere else the
# check must stay silent, which is a case worth asserting in its own right.
PGTRIGGER_INSTALLED = apps.is_installed(PGTRIGGER_APP_NAME)


def warning_ids():
    return [warning.id for warning in check_pgtrigger_compatibility(None)]


class PgtriggerCompatibilityCheckTestCase(SimpleTestCase):
    def test_check_is_registered(self):
        """
        Case: The app config has been readied, as it is for every test run
        Expected: The check is part of Django's check registry
        """
        self.assertIn(check_pgtrigger_compatibility, registry.get_checks())

    @skipIf(PGTRIGGER_INSTALLED, 'django-pgtrigger is installed in this environment')
    def test_incompatible_settings_without_pgtrigger(self):
        """
        Case: Both pgtrigger settings hold an incompatible value, but pgtrigger is not installed
        Expected: No warnings, since the settings mean nothing without the app
        """
        with override_settings(PGTRIGGER_MIGRATIONS=False, PGTRIGGER_INSTALL_ON_MIGRATE=True):
            self.assertEqual(warning_ids(), [])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_default_settings(self):
        """
        Case: pgtrigger is installed and both of its settings are left at their defaults
        Expected: No warnings
        """
        self.assertEqual(warning_ids(), [])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_migrations_disabled(self):
        """
        Case: pgtrigger is installed with PGTRIGGER_MIGRATIONS turned off
        Expected: The triggers-outside-migrations warning
        """
        with override_settings(PGTRIGGER_MIGRATIONS=False):
            self.assertEqual(warning_ids(), ['djanquiltdb.W001'])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_install_on_migrate_enabled(self):
        """
        Case: pgtrigger is installed with PGTRIGGER_INSTALL_ON_MIGRATE turned on
        Expected: The install-at-migrate-time warning
        """
        with override_settings(PGTRIGGER_INSTALL_ON_MIGRATE=True):
            self.assertEqual(warning_ids(), ['djanquiltdb.W002'])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_both_settings_incompatible(self):
        """
        Case: pgtrigger is installed and both of its settings hold an incompatible value
        Expected: A warning for each of them
        """
        with override_settings(PGTRIGGER_MIGRATIONS=False, PGTRIGGER_INSTALL_ON_MIGRATE=True):
            self.assertEqual(warning_ids(), ['djanquiltdb.W001', 'djanquiltdb.W002'])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_muted(self):
        """
        Case: Both pgtrigger settings hold an incompatible value, but the check is muted
        Expected: No warnings
        """
        with override_settings(PGTRIGGER_MIGRATIONS=False, PGTRIGGER_INSTALL_ON_MIGRATE=True):
            with override_sharding_setting(MUTE_SETTING, True):
                self.assertEqual(warning_ids(), [])

    @skipUnless(PGTRIGGER_INSTALLED, 'django-pgtrigger is not installed in this environment')
    def test_muted_with_a_false_value(self):
        """
        Case: Both pgtrigger settings hold an incompatible value and the mute setting is present but False
        Expected: A warning for each of them, since only a true value mutes the check
        """
        with override_settings(PGTRIGGER_MIGRATIONS=False, PGTRIGGER_INSTALL_ON_MIGRATE=True):
            with override_sharding_setting(MUTE_SETTING, False):
                self.assertEqual(warning_ids(), ['djanquiltdb.W001', 'djanquiltdb.W002'])

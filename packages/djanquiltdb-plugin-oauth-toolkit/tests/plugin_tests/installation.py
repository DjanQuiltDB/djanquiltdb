import importlib
import subprocess  # nosec
import sys

import djanquiltdb.base.oauth_toolkit as base_namespace
import djanquiltdb.utils.oauth_toolkit as utils_namespace
from django.test import SimpleTestCase
from django.utils.module_loading import import_string
from djanquiltdb import base, middleware, utils
from djanquiltdb.base.oauth_toolkit import ShardSelectorClientIdGenerator
from djanquiltdb.plugins import load_plugins
from djanquiltdb.utils.oauth_toolkit import generate_access_token

import djanquiltdb_plugin_oauth_toolkit


class PluginImportTestCase(SimpleTestCase):
    def test_importing_the_distribution_package_needs_no_configured_django(self):
        """
        Case: The distribution package is imported on its own, in a process where Django is not configured. Loading the
              djanquiltdb.plugins entry points imports every installed plugin in this way.
        Expected: The import succeeds.
        """
        result = subprocess.run(  # nosec
            [sys.executable, '-c', 'import djanquiltdb_plugin_oauth_toolkit'], capture_output=True, text=True
        )

        self.assertEqual(result.returncode, 0, result.stderr)


class PluginDiscoveryTestCase(SimpleTestCase):
    def test_core_discovers_the_plugin_and_its_install_callable(self):
        """
        Case: load_plugins() is called. Core calls install() on every discovered plugin from AppConfig.ready().
        Expected: The oauth-toolkit plugin object is among the loaded plugins, and has a callable install attribute.
        """
        self.assertIn(djanquiltdb_plugin_oauth_toolkit.plugin, load_plugins())
        self.assertTrue(callable(djanquiltdb_plugin_oauth_toolkit.plugin.install))

    def test_the_helpers_are_served_from_the_core_utils_module(self):
        """
        Case: djanquiltdb.utils.oauth_toolkit is imported, read as an attribute of djanquiltdb.utils, and
              generate_access_token is imported from inside it.
        Expected: Each is the plugin's utils module or its function, and each name that the module lists is the same
                  object that the distribution package exports.
        """
        self.assertIs(utils_namespace, djanquiltdb_plugin_oauth_toolkit.utils)
        self.assertIs(utils.oauth_toolkit, djanquiltdb_plugin_oauth_toolkit.utils)
        self.assertIs(generate_access_token, djanquiltdb_plugin_oauth_toolkit.generate_access_token)

        for name in djanquiltdb_plugin_oauth_toolkit.utils.__all__:
            with self.subTest(name=name):
                self.assertIs(getattr(utils_namespace, name), getattr(djanquiltdb_plugin_oauth_toolkit, name))

    def test_the_client_id_generator_class_is_served_from_the_core_base_module(self):
        """
        Case: djanquiltdb.base.oauth_toolkit is imported, read as an attribute of djanquiltdb.base, and
              ShardSelectorClientIdGenerator is imported from inside it.
        Expected: Each is the plugin's base module or its class, and each name that the module lists is the same object
                  that the distribution package exports.
        """
        self.assertIs(base_namespace, djanquiltdb_plugin_oauth_toolkit.base)
        self.assertIs(base.oauth_toolkit, djanquiltdb_plugin_oauth_toolkit.base)
        self.assertIs(ShardSelectorClientIdGenerator, djanquiltdb_plugin_oauth_toolkit.ShardSelectorClientIdGenerator)

        for name in djanquiltdb_plugin_oauth_toolkit.base.__all__:
            with self.subTest(name=name):
                self.assertIs(getattr(base_namespace, name), getattr(djanquiltdb_plugin_oauth_toolkit, name))

    def test_the_middleware_is_served_from_the_core_middleware_module(self):
        """
        Case: djanquiltdb.middleware.oauth_toolkit is read as an attribute of djanquiltdb.middleware, and the middleware
              class is loaded by the path that a MIDDLEWARE entry names.
        Expected: The plugin's middleware module, and its middleware class.
        """
        self.assertIs(middleware.oauth_toolkit, djanquiltdb_plugin_oauth_toolkit.middleware)
        self.assertIs(
            import_string('djanquiltdb.middleware.oauth_toolkit.OAuthToolkitUseShardForMiddleware'),
            djanquiltdb_plugin_oauth_toolkit.OAuthToolkitUseShardForMiddleware,
        )

    def test_the_oauth2_provider_settings_load_the_generators_of_the_plugin(self):
        """
        Case: The three paths that the OAUTH2_PROVIDER setting names are loaded with import_string(), as the toolkit
              loads them.
        Expected: The plugin's generator class and functions.
        """
        self.assertIs(
            import_string('djanquiltdb.base.oauth_toolkit.ShardSelectorClientIdGenerator'),
            djanquiltdb_plugin_oauth_toolkit.ShardSelectorClientIdGenerator,
        )
        self.assertIs(
            import_string('djanquiltdb.utils.oauth_toolkit.generate_access_token'),
            djanquiltdb_plugin_oauth_toolkit.generate_access_token,
        )
        self.assertIs(
            import_string('djanquiltdb.utils.oauth_toolkit.generate_refresh_token'),
            djanquiltdb_plugin_oauth_toolkit.generate_refresh_token,
        )

    def test_the_plugin_serves_no_decorators(self):
        """
        Case: djanquiltdb.decorators.oauth_toolkit is imported.
        Expected: ModuleNotFoundError, because the plugin has no decorators.
        """
        with self.assertRaises(ModuleNotFoundError):
            importlib.import_module('djanquiltdb.decorators.oauth_toolkit')

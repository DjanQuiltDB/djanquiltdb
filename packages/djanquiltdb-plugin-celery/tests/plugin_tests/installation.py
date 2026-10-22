import subprocess  # nosec
import sys

import djanquiltdb.utils.celery as utils_namespace
from django.test import SimpleTestCase
from djanquiltdb import decorators, utils
from djanquiltdb.decorators import celery
from djanquiltdb.decorators.celery import task_for_shard
from djanquiltdb.plugins import load_plugins
from djanquiltdb.utils.celery import fan_out

import djanquiltdb_plugin_celery


class PluginImportTestCase(SimpleTestCase):
    def test_importing_the_distribution_package_needs_no_configured_django(self):
        """
        Case: The distribution package is imported on its own, in a process where Django is not configured. Loading the
              djanquiltdb.plugins entry points imports every installed plugin in this way.
        Expected: The import succeeds.
        """
        result = subprocess.run(  # nosec
            [sys.executable, '-c', 'import djanquiltdb_plugin_celery'], capture_output=True, text=True
        )

        self.assertEqual(result.returncode, 0, result.stderr)


class PluginDiscoveryTestCase(SimpleTestCase):
    def test_core_discovers_the_plugin_and_its_install_callable(self):
        """
        Case: load_plugins() is called. Core calls install() on every discovered plugin from AppConfig.ready().
        Expected: The celery plugin object is among the loaded plugins, and has a callable install attribute.
        """
        self.assertIn(djanquiltdb_plugin_celery.plugin, load_plugins())
        self.assertTrue(callable(djanquiltdb_plugin_celery.plugin.install))

    def test_the_decorators_are_served_from_the_core_decorators_module(self):
        """
        Case: celery is imported from djanquiltdb.decorators, and read as an attribute of it.
        Expected: Both are the plugin's decorators module, and each name in the __all__ of that module is the same
                  object that the distribution package exports.
        """
        self.assertIs(celery, djanquiltdb_plugin_celery.decorators)
        self.assertIs(decorators.celery, djanquiltdb_plugin_celery.decorators)

        for name in djanquiltdb_plugin_celery.decorators.__all__:
            with self.subTest(name=name):
                self.assertIs(getattr(celery, name), getattr(djanquiltdb_plugin_celery, name))

    def test_a_decorator_can_be_imported_from_inside_the_namespace(self):
        """
        Case: task_for_shard is imported from djanquiltdb.decorators.celery.
        Expected: It is the plugin's own decorator.
        """
        self.assertIs(task_for_shard, djanquiltdb_plugin_celery.task_for_shard)

    def test_the_helpers_are_served_from_the_core_utils_module(self):
        """
        Case: djanquiltdb.utils.celery is imported, read as an attribute of djanquiltdb.utils, and fan_out is imported
              from inside it.
        Expected: Each is the plugin's utils module or the plugin's fan_out function, and each name in the __all__ of
                  the utils module is the same object that the distribution package exports.
        """
        self.assertIs(utils_namespace, djanquiltdb_plugin_celery.utils)
        self.assertIs(utils.celery, djanquiltdb_plugin_celery.utils)
        self.assertIs(fan_out, djanquiltdb_plugin_celery.fan_out)

        for name in djanquiltdb_plugin_celery.utils.__all__:
            with self.subTest(name=name):
                self.assertIs(getattr(utils_namespace, name), getattr(djanquiltdb_plugin_celery, name))

    def test_the_decorators_are_not_attributes_of_the_core_decorators_module(self):
        """
        Case: task_for_shard is read directly from djanquiltdb.decorators, without the plugin's namespace.
        Expected: AttributeError, because the plugin sets SERVE_UNQUALIFIED = False.
        """
        with self.assertRaises(AttributeError):
            decorators.task_for_shard

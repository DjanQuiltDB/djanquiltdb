import subprocess  # nosec
import sys

import djanquiltdb.base.pghistory as base_namespace
import djanquiltdb.models.pghistory as models_namespace
from django.db.migrations.serializer import serializer_factory
from django.test import SimpleTestCase
from djanquiltdb import base, decorators, models
from djanquiltdb.decorators import pghistory
from djanquiltdb.decorators.pghistory import track, track_sharded
from djanquiltdb.models.pghistory import EventQuerySet, ShardedEvent, create_event_model
from djanquiltdb.plugins import load_plugins

import djanquiltdb_plugin_pghistory


class PluginImportDoesNotImportModelsTestCase(SimpleTestCase):
    def test_importing_the_distribution_package_imports_no_models(self):
        """
        Case: The distribution package is imported on its own, in a process where Django is not configured. Loading the
              djanquiltdb.plugins entry points imports every installed plugin in this way.
        Expected: The import succeeds, and imports neither django-pghistory nor djanquiltdb_plugin_pghistory.core, which
                  imports the event bases.
        """
        subprocess_source = (
            'import sys\n'
            'import djanquiltdb_plugin_pghistory\n'
            "assert 'pghistory' not in sys.modules, 'pghistory imported eagerly'\n"
            "assert 'djanquiltdb_plugin_pghistory.core' not in sys.modules, 'core imported eagerly'\n"
        )

        result = subprocess.run([sys.executable, '-c', subprocess_source], capture_output=True, text=True)  # nosec

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_setup_without_the_pghistory_app_does_not_import_its_models(self):
        """
        Case: django.setup() runs with the plugin distribution installed but without the pghistory app, as in any
              project that installs djanquiltdb[pghistory] before it starts tracking.
        Expected: Setup succeeds. Discovery loads the plugin and runs install(), but nothing imports pghistory.models,
                  whose concrete Context model cannot exist without the app.
        """
        subprocess_source = (
            'import sys\n'
            'import django\n'
            'from django.conf import settings\n'
            'settings.configure(\n'
            "    INSTALLED_APPS=['django.contrib.contenttypes', 'djanquiltdb'],\n"
            '    DATABASES={},\n'
            "    DATABASE_ROUTERS=['djanquiltdb.router.DynamicDbRouter'],\n"
            "    QUILT_DB={'SHARD_CLASS': 'djanquiltdb.models.BaseShard'},\n"
            ')\n'
            'django.setup()\n'
            "assert 'pghistory.models' not in sys.modules, 'pghistory.models imported during setup'\n"
            "assert 'djanquiltdb_plugin_pghistory.core' not in sys.modules, 'event bases imported during setup'\n"
        )

        result = subprocess.run([sys.executable, '-c', subprocess_source], capture_output=True, text=True)  # nosec

        self.assertEqual(result.returncode, 0, result.stderr)


class PluginDiscoveryTestCase(SimpleTestCase):
    def test_core_discovers_the_plugin_and_its_install_callable(self):
        """
        Case: load_plugins() is called. Core calls install() on every discovered plugin from AppConfig.ready().
        Expected: The pghistory plugin object is among the loaded plugins, and has a callable install attribute.
        """
        self.assertIn(djanquiltdb_plugin_pghistory.plugin, load_plugins())
        self.assertTrue(callable(djanquiltdb_plugin_pghistory.plugin.install))

    def test_the_tracking_namespace_is_served_from_the_core_decorators_module(self):
        """
        Case: pghistory is imported from djanquiltdb.decorators.
        Expected: It has the plugin's own tracking functions, and each name that it lists is the same object that the
                  distribution package exports.
        """
        self.assertIs(pghistory.track, djanquiltdb_plugin_pghistory.track)
        self.assertIs(pghistory.track_sharded, djanquiltdb_plugin_pghistory.track_sharded)
        self.assertIs(pghistory.track_mirrored, djanquiltdb_plugin_pghistory.track_mirrored)
        self.assertIs(pghistory.track_public, djanquiltdb_plugin_pghistory.track_public)

        for name in djanquiltdb_plugin_pghistory.decorators.__all__:
            with self.subTest(name=name):
                self.assertIs(getattr(pghistory, name), getattr(djanquiltdb_plugin_pghistory, name))

    def test_the_tracking_functions_can_be_imported_from_inside_the_tracking_namespace(self):
        """
        Case: A tracking function is imported from inside the namespace, and not read as an attribute of it.
        Expected: It is the plugin's own function.
        """
        self.assertIs(track, djanquiltdb_plugin_pghistory.track)
        self.assertIs(track_sharded, djanquiltdb_plugin_pghistory.track_sharded)

    def test_the_models_namespace_serves_the_event_bases_and_queryset(self):
        """
        Case: djanquiltdb.models.pghistory is imported.
        Expected: It is the plugin's models module, and each of its names is the same object that the distribution
                  package exports.
        """
        self.assertIs(models_namespace, djanquiltdb_plugin_pghistory.models)

        for name in djanquiltdb_plugin_pghistory.models.__all__:
            with self.subTest(name=name):
                self.assertIs(getattr(models_namespace, name), getattr(djanquiltdb_plugin_pghistory, name))

    def test_every_namespace_supports_plain_attribute_access(self):
        """
        Case: The plugin is read as an attribute of each core module, without importing the submodule first.
        Expected: Each attribute is the plugin module of the same name.
        """
        self.assertIs(base.pghistory, djanquiltdb_plugin_pghistory.base)
        self.assertIs(decorators.pghistory, djanquiltdb_plugin_pghistory.decorators)
        self.assertIs(models.pghistory, djanquiltdb_plugin_pghistory.models)

    def test_the_base_namespace_serves_the_revert_mixin(self):
        """
        Case: djanquiltdb.base.pghistory is imported.
        Expected: It is the plugin's base module, and ShardRoutedRevert, which every event base inherits from, is read
                  from it as the class that the plugin defines.
        """
        self.assertIs(base_namespace, djanquiltdb_plugin_pghistory.base)
        self.assertIs(base_namespace.ShardRoutedRevert, djanquiltdb_plugin_pghistory.base.ShardRoutedRevert)

    def test_a_migration_names_the_revert_mixin_by_its_served_path(self):
        """
        Case: ShardRoutedRevert is serialized as makemigrations serializes the bases of a new event model.
        Expected: The migration names it as djanquiltdb.base.pghistory.ShardRoutedRevert and imports that module, so
                  that a generated migration does not import from the distribution package.
        """
        serialized, imports = serializer_factory(djanquiltdb_plugin_pghistory.base.ShardRoutedRevert).serialize()

        self.assertEqual(serialized, 'djanquiltdb.base.pghistory.ShardRoutedRevert')
        self.assertEqual(imports, {'import djanquiltdb.base.pghistory'})

    def test_the_event_bases_the_queryset_and_create_event_model_can_be_imported_from_inside_the_models_namespace(
        self,
    ):
        """
        Case: An event base, the queryset and create_event_model are imported from inside the models namespace.
        Expected: Each one is the plugin's own class or function.
        """
        self.assertIs(ShardedEvent, djanquiltdb_plugin_pghistory.ShardedEvent)
        self.assertIs(EventQuerySet, djanquiltdb_plugin_pghistory.EventQuerySet)
        self.assertIs(create_event_model, djanquiltdb_plugin_pghistory.create_event_model)

    def test_the_tracking_functions_are_not_attributes_of_the_core_decorators_module(self):
        """
        Case: A tracking function is read directly from djanquiltdb.decorators, without the plugin's namespace.
        Expected: AttributeError, because the plugin sets SERVE_UNQUALIFIED = False.
        """
        with self.assertRaises(AttributeError):
            decorators.track

    def test_unknown_names_are_not_served_by_the_lazy_exports(self):
        """
        Case: A name that nothing exports is read from the distribution package and from each served module.
        Expected: AttributeError from each.
        """
        for module in (
            djanquiltdb_plugin_pghistory,
            djanquiltdb_plugin_pghistory.base,
            djanquiltdb_plugin_pghistory.decorators,
            djanquiltdb_plugin_pghistory.models,
        ):
            with self.subTest(module=module.__name__):
                with self.assertRaises(AttributeError):
                    module.does_not_exist

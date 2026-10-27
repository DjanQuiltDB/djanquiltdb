import contextlib
import importlib
import importlib.util
import subprocess
import sys
import types
from unittest import mock

from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase

from djanquiltdb import base as djanquiltdb_base
from djanquiltdb import decorators as djanquiltdb_decorators
from djanquiltdb import models as djanquiltdb_models
from djanquiltdb import plugins
from djanquiltdb import utils as djanquiltdb_utils
from djanquiltdb_tests import plugin_fixture

FIXTURE_NAME = 'fixture'

#: Each namespace, with the fixture module that it is expected to serve. The tests that import a plugin in each
#: supported way loop over these pairs, so that one namespace cannot support something that another does not.
NAMESPACE_AND_SERVED_FIXTURE_MODULE = (
    ('djanquiltdb.base', plugin_fixture.base),
    ('djanquiltdb.decorators', plugin_fixture.decorators),
    ('djanquiltdb.models', plugin_fixture.models),
    ('djanquiltdb.utils', plugin_fixture.utils),
)


def fake_entry_point(name=FIXTURE_NAME, plugin=None):
    """
    Returns a stand-in for an entry point of the plugin group. The resolver only uses ``name`` and ``load()``.
    """
    return types.SimpleNamespace(
        name=name,
        load=lambda: plugin_fixture.plugin if plugin is None else plugin,
    )


class PluginNamespaceTestCase(SimpleTestCase):
    def setUp(self):
        super().setUp()
        self._remove_cached_fixture_aliases()
        self.addCleanup(self._remove_cached_fixture_aliases)

    @staticmethod
    def _remove_cached_fixture_aliases():
        # An alias is cached in sys.modules, and as an attribute of the parent module. Both are removed, so that each
        # test resolves the alias through the registry and __getattr__, and not from a leftover.
        for namespace, _ in NAMESPACE_AND_SERVED_FIXTURE_MODULE:
            for name in [name for name in sys.modules if name.startswith(f'{namespace}.')]:
                del sys.modules[name]

            # hasattr() would call the __getattr__ of the module and resolve the plugin, so vars() is used.
            vars(sys.modules[namespace]).pop(FIXTURE_NAME, None)

    def patch_installed_entry_points(self, *entry_points):
        return mock.patch.object(
            plugins,
            '_entry_points_by_name',
            return_value={entry_point.name: entry_point for entry_point in entry_points},
        )

    def test_every_way_of_importing_returns_the_module_of_the_plugin(self):
        """
        Case: The plugin module is reached in each of the three ways, in every namespace: a from-import of the plugin
              name, an import of the submodule, and plain attribute access on the core module.
        Expected: Each way returns the module of the plugin, and not a copy.
        """
        for namespace, expected in NAMESPACE_AND_SERVED_FIXTURE_MODULE:
            with self.subTest(namespace=namespace):
                with self.patch_installed_entry_points(fake_entry_point()):
                    # from djanquiltdb.<namespace> import fixture, run as a real import statement.
                    self._remove_cached_fixture_aliases()
                    imported_names = {}
                    exec(f'from {namespace} import {FIXTURE_NAME}', imported_names)
                    self.assertIs(imported_names[FIXTURE_NAME], expected)

                    # import djanquiltdb.<namespace>.fixture
                    self._remove_cached_fixture_aliases()
                    self.assertIs(importlib.import_module(f'{namespace}.{FIXTURE_NAME}'), expected)

                    # Plain attribute access, which only __getattr__ handles.
                    self._remove_cached_fixture_aliases()
                    self.assertIs(getattr(sys.modules[namespace], FIXTURE_NAME), expected)

    def test_an_individual_name_can_be_imported_from_every_namespace(self):
        """
        Case: A single name is imported from the plugin module in each namespace, as in
              ``from djanquiltdb.decorators.fixture import shiny_decorator``.
        Expected: The imported name is the object that the plugin defines.
        """
        with self.patch_installed_entry_points(fake_entry_point()):
            from djanquiltdb.base.fixture import FixtureBase
            from djanquiltdb.decorators.fixture import shiny_decorator
            from djanquiltdb.models.fixture import FixtureEvent
            from djanquiltdb.utils.fixture import fixture_helper

            self.assertIs(FixtureBase, plugin_fixture.base.FixtureBase)
            self.assertIs(shiny_decorator, plugin_fixture.decorators.shiny_decorator)
            self.assertIs(FixtureEvent, plugin_fixture.models.FixtureEvent)
            self.assertIs(fixture_helper, plugin_fixture.utils.fixture_helper)

    def test_the_namespaces_serve_different_modules(self):
        """
        Case: The same plugin is reached in every namespace.
        Expected: djanquiltdb.base serves the base module of the plugin, djanquiltdb.decorators serves its decorators
                  module, djanquiltdb.models serves its models module, and djanquiltdb.utils serves its utils module.
        """
        with self.patch_installed_entry_points(fake_entry_point()):
            from djanquiltdb.base import fixture as served_base_module
            from djanquiltdb.decorators import fixture as served_decorators_module
            from djanquiltdb.models import fixture as served_models_module
            from djanquiltdb.utils import fixture as served_utils_module

            self.assertIs(served_base_module, plugin_fixture.base)
            self.assertIs(served_decorators_module, plugin_fixture.decorators)
            self.assertIs(served_models_module, plugin_fixture.models)
            self.assertIs(served_utils_module, plugin_fixture.utils)

    def test_a_plugin_cannot_replace_a_name_that_utils_defines(self):
        """
        Case: A plugin registers the name use_shard, which djanquiltdb.utils defines itself, and use_shard is read from
              djanquiltdb.utils.
        Expected: The use_shard of djanquiltdb.utils, and not the utils module of the plugin.
        """
        with self.patch_installed_entry_points(fake_entry_point(name='use_shard')):
            from djanquiltdb.utils import use_shard

            self.assertIsNot(use_shard, plugin_fixture.utils)
            self.assertIs(use_shard, vars(djanquiltdb_utils)['use_shard'])

    def test_a_name_that_no_plugin_registers_raises_the_usual_missing_name_errors(self):
        """
        Case: A name that no installed plugin registers is used in each namespace.
        Expected: ModuleNotFoundError for an import, and AttributeError for an attribute.
        """
        with self.patch_installed_entry_points(fake_entry_point()):
            for namespace, _ in NAMESPACE_AND_SERVED_FIXTURE_MODULE:
                with self.subTest(namespace=namespace):
                    with self.assertRaises(ModuleNotFoundError):
                        importlib.import_module(f'{namespace}.not_installed')

                    with self.assertRaises(AttributeError):
                        getattr(sys.modules[namespace], 'not_installed')

    def test_a_plugin_without_a_models_module_is_not_served_under_models(self):
        """
        Case: A plugin object has decorators but no base, no models and no utils, as the postgres-objects plugin does.
        Expected: The plugin is served under djanquiltdb.decorators. Under djanquiltdb.base, djanquiltdb.models and
                  djanquiltdb.utils, an import raises ModuleNotFoundError and an attribute raises AttributeError.
        """
        plugin_with_only_decorators = types.SimpleNamespace(decorators=plugin_fixture.decorators, install=lambda: None)

        with self.patch_installed_entry_points(fake_entry_point(plugin=plugin_with_only_decorators)):
            import djanquiltdb.decorators.fixture  # noqa: F401

            with self.assertRaises(ModuleNotFoundError):
                import djanquiltdb.base.fixture  # noqa: F401

            with self.assertRaises(ModuleNotFoundError):
                import djanquiltdb.models.fixture  # noqa: F401

            with self.assertRaises(ModuleNotFoundError):
                import djanquiltdb.utils.fixture  # noqa: F401

            with self.assertRaises(AttributeError):
                djanquiltdb_base.fixture

            with self.assertRaises(AttributeError):
                djanquiltdb_models.fixture

            with self.assertRaises(AttributeError):
                djanquiltdb_utils.fixture

    def test_private_names_are_never_served(self):
        """
        Case: A name that starts with an underscore is read from a namespace.
        Expected: AttributeError, so that no plugin name can hide a dunder attribute that the import system looks for.
        """
        with self.patch_installed_entry_points(fake_entry_point()):
            for namespace, _ in NAMESPACE_AND_SERVED_FIXTURE_MODULE:
                with self.subTest(namespace=namespace):
                    with self.assertRaises(AttributeError):
                        getattr(sys.modules[namespace], '_private_helper')

    def test_serving_an_alias_keeps_the_name_and_spec_of_the_plugin_module(self):
        """
        Case: A plugin module is imported under its name in a djanquiltdb namespace.
        Expected: The plugin's own module keeps its real name and spec, so that reloading it and reading its spec work.
        """
        with self.patch_installed_entry_points(fake_entry_point()):
            import djanquiltdb.models.fixture  # noqa: F401

            self.assertEqual(plugin_fixture.models.__name__, 'djanquiltdb_tests.plugin_fixture.models')
            self.assertEqual(plugin_fixture.models.__spec__.name, 'djanquiltdb_tests.plugin_fixture.models')

    def test_a_submodule_of_a_plugin_is_the_submodule_of_the_plugin_package(self):
        """
        Case: A module inside the plugin's package is imported through a namespace.
        Expected: It is the plugin's own submodule, and not a second copy that is imported through the alias.
        """
        plugin_serving_its_package_as_models = types.SimpleNamespace(models=plugin_fixture, install=lambda: None)

        with self.patch_installed_entry_points(fake_entry_point(plugin=plugin_serving_its_package_as_models)):
            import djanquiltdb.models.fixture.decorators  # noqa: F401

            self.assertIs(sys.modules['djanquiltdb.models.fixture.decorators'], plugin_fixture.decorators)

    def test_a_missing_submodule_of_a_plugin_is_not_found(self):
        """
        Case: A module that the plugin's package does not have is looked up through a namespace, and then imported.
        Expected: find_spec() returns None, as for any module that does not exist, and the import raises
                  ModuleNotFoundError.
        """
        plugin_serving_its_package_as_models = types.SimpleNamespace(models=plugin_fixture, install=lambda: None)

        with self.patch_installed_entry_points(fake_entry_point(plugin=plugin_serving_its_package_as_models)):
            self.assertIsNone(importlib.util.find_spec('djanquiltdb.models.fixture.missing'))

            with self.assertRaises(ModuleNotFoundError):
                importlib.import_module('djanquiltdb.models.fixture.missing')

    def test_the_importer_is_installed_once(self):
        """
        Case: install_plugin_importer() runs again.
        Expected: sys.meta_path has exactly one PluginImporter.
        """
        meta_path_before_install = list(sys.meta_path)

        plugins.install_plugin_importer()

        self.assertEqual(len(sys.meta_path), len(meta_path_before_install))
        self.assertEqual(sum(isinstance(finder, plugins.PluginImporter) for finder in sys.meta_path), 1)


class PluginDecoratorResolutionTestCase(SimpleTestCase):
    """
    Covers how :mod:`djanquiltdb.decorators` chooses between the registered name of a plugin and its unqualified
    decorators.
    """

    def test_decorators_are_served_unqualified_by_default(self):
        """
        Case: A plugin does not set SERVE_UNQUALIFIED, as the postgres-objects plugin does not.
        Expected: Its public decorators can be read directly from djanquiltdb.decorators, and its private helpers
                  cannot.
        """
        plugin_with_fixture_decorators = types.SimpleNamespace(
            decorators=plugin_fixture.decorators, install=lambda: None
        )

        with mock.patch.object(plugins, 'load_plugins', return_value=(plugin_with_fixture_decorators,)):
            self.assertIs(djanquiltdb_decorators.shiny_decorator, plugin_fixture.decorators.shiny_decorator)

            with self.assertRaises(AttributeError):
                djanquiltdb_decorators._private_helper

    def test_a_decorators_module_with_all_serves_only_those_names_unqualified(self):
        """
        Case: A plugin's decorators module defines __all__ with one decorator, and also has a helper it imported for its
              own use, which __all__ leaves out.
        Expected: The decorator can be read directly from djanquiltdb.decorators, and the helper cannot, so that a
                  name a plugin imports does not resolve across plugins.
        """
        decorators_with_all = types.SimpleNamespace(
            shiny_decorator=object(), imported_helper=object(), __all__=['shiny_decorator']
        )
        plugin_with_all = types.SimpleNamespace(decorators=decorators_with_all, install=lambda: None)

        with mock.patch.object(plugins, 'load_plugins', return_value=(plugin_with_all,)):
            self.assertIs(djanquiltdb_decorators.shiny_decorator, decorators_with_all.shiny_decorator)

            with self.assertRaises(AttributeError):
                djanquiltdb_decorators.imported_helper

    def test_serve_unqualified_false_keeps_decorators_out_of_the_core_decorators_module(self):
        """
        Case: A plugin sets SERVE_UNQUALIFIED = False, as the pghistory plugin does, so that its call sites read
              ``@pghistory.track()``.
        Expected: Its decorators are not served directly from djanquiltdb.decorators.
        """
        decorators_that_keep_their_namespace = types.SimpleNamespace(shiny_decorator=object(), SERVE_UNQUALIFIED=False)
        plugin_that_keeps_its_namespace = types.SimpleNamespace(
            decorators=decorators_that_keep_their_namespace, install=lambda: None
        )

        with mock.patch.object(plugins, 'load_plugins', return_value=(plugin_that_keeps_its_namespace,)):
            with self.assertRaises(AttributeError):
                djanquiltdb_decorators.shiny_decorator

    def test_models_are_never_served_unqualified(self):
        """
        Case: A name from the models module of a plugin is read directly from djanquiltdb.models.
        Expected: AttributeError. The models module of a plugin is served only under the registered name of the plugin.
        """
        with mock.patch.object(plugins, 'load_plugins', return_value=(plugin_fixture.plugin,)):
            with self.assertRaises(AttributeError):
                djanquiltdb_models.FixtureEvent

    def test_a_registered_plugin_name_takes_precedence_over_an_unqualified_decorator(self):
        """
        Case: A plugin registers a name that the decorators module of another plugin also has as an unqualified
              decorator.
        Expected: The decorators module of the registered plugin is served, and not the decorator.
        """
        entry_point = fake_entry_point(
            name='shiny_decorator',
            plugin=types.SimpleNamespace(decorators=plugin_fixture.models, install=lambda: None),
        )

        with (
            mock.patch.object(plugins, '_entry_points_by_name', return_value={entry_point.name: entry_point}),
            mock.patch.object(plugins, 'load_plugins', return_value=(plugin_fixture.plugin,)),
        ):
            self.assertIs(djanquiltdb_decorators.shiny_decorator, plugin_fixture.models)


class DuplicatePluginNameTestCase(SimpleTestCase):
    def setUp(self):
        super().setUp()
        # The caches of the resolver and the loader are cleared before each test, so that they read the patched entry
        # points, and after each test, so that they do not keep them.
        for cached_function in (plugins._entry_points_by_name, plugins.load_plugins):
            cached_function.cache_clear()
            self.addCleanup(cached_function.cache_clear)

    def patch_entry_points_with_a_duplicate(self, first_dist=None, second_dist=None):
        """
        Patches the installed entry points with two that register FIXTURE_NAME, and one that registers 'other'.
        """
        return mock.patch.object(
            plugins,
            'entry_points',
            return_value=(
                types.SimpleNamespace(name=FIXTURE_NAME, value='one:plugin', dist=first_dist),
                types.SimpleNamespace(name=FIXTURE_NAME, value='two:plugin', dist=second_dist),
                types.SimpleNamespace(
                    name='other', value='other:plugin', dist=None, load=lambda: types.SimpleNamespace(install=None)
                ),
            ),
        )

    def test_two_distributions_registering_one_name_raise_improperly_configured(self):
        """
        Case: Two installed distributions both register a plugin under the same entry-point name, and the duplicate
              names are checked, as the app registry does when it is ready.
        Expected: ImproperlyConfigured, naming the duplicated name and both distributions.
        """
        with self.patch_entry_points_with_a_duplicate(
            first_dist=types.SimpleNamespace(name='one-dist'), second_dist=types.SimpleNamespace(name='two-dist')
        ):
            with self.assertRaisesMessage(ImproperlyConfigured, FIXTURE_NAME) as raised:
                plugins.raise_for_duplicate_plugin_names()

        self.assertIn('one-dist', str(raised.exception))
        self.assertIn('two-dist', str(raised.exception))

    def test_a_duplicate_without_distribution_metadata_is_named_by_its_value(self):
        """
        Case: The duplicate entry points have no distribution metadata, as is the case for entry points that are built
              by hand.
        Expected: The error names the value of each entry point, so the message still says what to uninstall.
        """
        with self.patch_entry_points_with_a_duplicate():
            with self.assertRaisesMessage(ImproperlyConfigured, 'one:plugin') as raised:
                plugins.raise_for_duplicate_plugin_names()

        self.assertIn('two:plugin', str(raised.exception))

    def test_the_app_registry_does_not_start_with_a_duplicate_name(self):
        """
        Case: The plugins are installed, as AppConfig.ready() does, while two distributions register the same name.
        Expected: ImproperlyConfigured, so the project does not start.
        """
        from djanquiltdb import apps as djanquiltdb_apps

        with self.patch_entry_points_with_a_duplicate():
            with self.assertRaisesMessage(ImproperlyConfigured, FIXTURE_NAME):
                djanquiltdb_apps._install_plugins()

    def test_only_the_duplicated_name_raises_when_it_is_looked_up(self):
        """
        Case: Two distributions register the same name. The duplicated name, another registered name and a name that
              no plugin registers are looked up, and a name that no plugin serves is probed with hasattr() on
              djanquiltdb.decorators.
        Expected: The duplicated name raises ImproperlyConfigured every time. The other names are looked up as usual,
                  so hasattr() returns False.
        """
        with self.patch_entry_points_with_a_duplicate():
            for _ in range(2):
                with self.assertRaisesMessage(ImproperlyConfigured, FIXTURE_NAME):
                    plugins.load_plugin_by_name(FIXTURE_NAME)

            self.assertIsNotNone(plugins.load_plugin_by_name('other'))
            self.assertIsNone(plugins.load_plugin_by_name('not_installed'))
            self.assertFalse(hasattr(djanquiltdb_decorators, 'not_installed'))


class PluginLoadingIsLazyTestCase(SimpleTestCase):
    def test_importing_the_namespace_modules_does_not_read_the_entry_points(self):
        """
        Case: The four core modules that serve a namespace are imported on their own, as djanquiltdb.apps imports
              djanquiltdb.decorators while the app registry is still being populated.
        Expected: The entry points have not been read, because the models module of a plugin defines abstract models,
                  which cannot be imported that early.
        """
        # djanquiltdb is not in INSTALLED_APPS, because its AppConfig.ready() reads the entry points. The settings are
        # the minimum that djanquiltdb.models needs to import.
        subprocess_source = (
            'import django\n'
            'from django.conf import settings\n'
            "settings.configure(INSTALLED_APPS=['django.contrib.contenttypes'], DATABASES={}, "
            "QUILT_DB={'SHARD_CLASS': 'djanquiltdb.models.BaseShard'})\n"
            'django.setup()\n'
            'import djanquiltdb.base, djanquiltdb.decorators, djanquiltdb.models, djanquiltdb.utils\n'
            'from djanquiltdb.plugins import _entry_points_by_name\n'
            'info = _entry_points_by_name.cache_info()\n'
            'assert info.misses == 0 and info.hits == 0, info\n'
        )

        result = subprocess.run([sys.executable, '-c', subprocess_source], capture_output=True, text=True)

        self.assertEqual(result.returncode, 0, result.stderr)


class PluginHooksTestCase(SimpleTestCase):
    def test_a_hook_is_found_by_its_command_section_and_its_name(self):
        """
        Case: Three plugins are loaded: one has the hook move_data_to_shard.pre_copy, one has the move_data_to_shard
              section without that hook, and one has no such section. The hook is run with arguments.
        Expected: Only the hook of the first plugin is called, with those arguments.
        """
        calls = []
        loaded_plugins = (
            types.SimpleNamespace(move_data_to_shard=types.SimpleNamespace(pre_copy=lambda *args: calls.append(args))),
            types.SimpleNamespace(move_data_to_shard=types.SimpleNamespace()),
            types.SimpleNamespace(),
        )

        with mock.patch.object(plugins, 'load_plugins', return_value=loaded_plugins):
            plugins.run_plugin_hooks('move_data_to_shard.pre_copy', 'command', 'pk_set')

        self.assertEqual(calls, [('command', 'pk_set')])

    def test_the_contexts_of_every_plugin_are_entered_in_order_and_exited_in_reverse(self):
        """
        Case: Two plugins have the context hook move_data_to_shard.move_context, and the hook contexts are entered.
        Expected: Both contexts are entered with the arguments, in the order of the plugins, before the block runs, and
                  exited in reverse order after it.
        """
        events = []

        def recording_context(name):
            @contextlib.contextmanager
            def move_context(command):
                events.append(('enter', name, command))
                yield
                events.append(('exit', name))

            return types.SimpleNamespace(move_data_to_shard=types.SimpleNamespace(move_context=move_context))

        with mock.patch.object(
            plugins, 'load_plugins', return_value=(recording_context('one'), recording_context('two'))
        ):
            with plugins.entered_plugin_hook_contexts('move_data_to_shard.move_context', 'command'):
                events.append('block')

        self.assertEqual(
            events,
            [('enter', 'one', 'command'), ('enter', 'two', 'command'), 'block', ('exit', 'two'), ('exit', 'one')],
        )

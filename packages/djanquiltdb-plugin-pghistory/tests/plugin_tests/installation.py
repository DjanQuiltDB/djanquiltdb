from django.test import SimpleTestCase


class PluginDiscoveryTestCase(SimpleTestCase):
    def test_core_discovers_the_plugin(self):
        """
        Case: The core plugin registry loads every distribution registered under the djanquiltdb.plugins
              entry-point group.
        Expected: The pghistory plugin object is among the loaded plugins.
        """
        from djanquiltdb.plugins import load_plugins

        import djanquiltdb_plugin_pghistory

        self.assertIn(djanquiltdb_plugin_pghistory.plugin, load_plugins())

    def test_plugin_exposes_the_install_hook(self):
        """
        Case: Core calls install() on every discovered plugin from AppConfig.ready().
        Expected: The plugin object has a callable install attribute.
        """
        from djanquiltdb_plugin_pghistory import plugin

        self.assertTrue(callable(plugin.install))

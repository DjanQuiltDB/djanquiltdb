from django.apps import apps
from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase, override_settings
from django.db import models


# Dummy Models used for the test_incompatible_models_settings test.
class DummyNode(models.Model):
    class Meta:
        app_label = 'sharding'


class DummyShard(models.Model):
    node = models.ForeignKey(DummyNode, on_delete=models.PROTECT)

    class Meta:
        app_label = 'sharding'


class ShardingSettingsTestCase(SimpleTestCase):
    def test_no_settings(self):
        """
        Case: None or empty SHARDING setting
        Expected: ImproperlyConfigured raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING=None):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

        with override_settings(SHARDING=""):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

    def test_incomplete_settings(self):
        """
        Case: Incomplete SHARDING setting
        Expected: ImproperlyConfigured raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        # incomplete setting
        with override_settings(SHARDING={'SHARD_CLASS': 'shardingtest.models.Shard'}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

        with override_settings(SHARDING={'NODE_CLASS': 'shardingtest.models.Node'}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

    def test_incomplete_models_settings(self):
        """
        Case: given model in SHARDING setting is incomplete (missing a foreignkey field)
        Expected: ImproperlyConfigured raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={'SHARD_CLASS': 'sharding.models.BaseShard',
                                         'NODE_CLASS': 'sharding.models.BaseNode'}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

    def test_incompatible_models_settings(self):
        """
        Case: given model in SHARDING setting is incompatable (not extending BaseShard/BaseNode)
        Expected: ImproperlyConfigured raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={'SHARD_CLASS': 'sharding.tests.app_config.DummyShard',
                                         'NODE_CLASS': 'sharding.tests.app_config.DummyNode'}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

    def test_correct_settings(self):
        """
        Case: correct SHARDING setting
        Expected: ImproperlyConfigured NOT raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={'SHARD_CLASS': 'shardingtest.models.Shard',
                                         'NODE_CLASS': 'shardingtest.models.Node'}):
            sharding_app.ready()  # no error

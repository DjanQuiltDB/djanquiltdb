from django.apps import apps
from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase, override_settings
from django.db import models

from sharding.tests.utils import test_model


@test_model()
class DummyShard(models.Model):

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

    def test_incomplete_models_settings(self):
        """
        Case: given incomplete SHARDING setting
        Expected: ImproperlyConfigured raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

    def test_incompatible_models_settings(self):
        """
        Case: given model in SHARDING setting is incompatible (not extending BaseShard/BaseNode)
        Expected: ImproperlyConfigured raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={'SHARD_CLASS': 'sharding.tests.app_config.DummyShard'}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

    def test_correct_settings(self):
        """
        Case: correct SHARDING setting
        Expected: ImproperlyConfigured NOT raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={'SHARD_CLASS': 'shardingtest.models.Shard'}):
            sharding_app.ready()  # no error

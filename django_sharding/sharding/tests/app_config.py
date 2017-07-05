from django.apps import apps
from django.contrib.auth.models import AbstractBaseUser
from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase, override_settings
from django.db import models

from sharding.tests.utils import test_model
from sharding.decorators import sharded_model


@test_model()
class DummyShard(models.Model):

    class Meta:
        app_label = 'sharding'


@test_model()
@sharded_model()
class DummyUser(AbstractBaseUser):

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


class SessionsBackendTestCase(SimpleTestCase):
    def test_no_sessions_setting(self):
        """
        Case: None or empty SESSION_ENGINE setting.
        Expected: no error raised.
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SESSION_ENGINE=None):
            sharding_app.ready()

        with override_settings(SESSION_ENGINE=""):
            sharding_app.ready()

    def test_sessions_setting_db_backend_no_sharded_users_model(self):
        """
        Case: SESSION_ENGINE set to cached_db, no altered user model (no sharded either).
        Expected: no error raised.
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SESSION_ENGINE='django.contrib.sessions.backends.cached_db',
                               AUTH_USER_MODEL='auth.User'):
            sharding_app.ready()

    def test_sessions_setting_db_backend_with_sharded_user_model(self):
        """
        Case: SESSION_ENGINE set to cached_db, custom user model and sharded it.
        Expected: ImproperlyConfigured raised.
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SESSION_ENGINE='django.contrib.sessions.backends.cached_db',
                               AUTH_USER_MODEL='sharding.tests.app_config.DummyUser'):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()


class DataBaseRouterTestCase(SimpleTestCase):
    def test_invalid_dbrouting_settings(self):
        """
        Case: DATABASE_ROUTERS does not contain sharding.utils.DynamicDbRouter.
        Expected: ImproperlyConfigured raised.
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(DATABASE_ROUTERS=[]):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

    def test_valid_dbrouting_settings(self):
        """
        Case: DATABASE_ROUTERS contains sharding.utils.DynamicDbRouter.
        Expected: No ImproperlyConfigured raised.
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(DATABASE_ROUTERS=['sharding.utils.DynamicDbRouter']):
            sharding_app.ready()

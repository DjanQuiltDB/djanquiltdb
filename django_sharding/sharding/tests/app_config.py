from unittest import mock

from django.apps import apps
from django.contrib.auth.models import AbstractBaseUser
from django.core.exceptions import ImproperlyConfigured
from django.db import models
from django.test import SimpleTestCase, override_settings

from sharding.decorators import sharded_model
from sharding.models import BaseShard
from sharding.tests.utils import test_model
from sharding.utils import ShardingMode


@test_model()
class DummyShard(models.Model):

    class Meta:
        app_label = 'sharding'


@test_model()
@sharded_model()
class DummyShardedShard(BaseShard):

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
        Case: Given incomplete SHARDING setting
        Expected: ImproperlyConfigured raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

    def test_incompatible_models_settings(self):
        """
        Case: Given model in SHARDING setting is incompatible (not extending BaseShard/BaseNode)
        Expected: ImproperlyConfigured raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={'SHARD_CLASS': 'sharding.tests.app_config.DummyShard'}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

    def test_sharded_models_settings(self):
        """
        Case: Given sharding model is sharded itself.
        Expected: ImproperlyConfigured raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={'SHARD_CLASS': 'sharding.tests.app_config.DummyShardedShard'}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

    def test_correct_settings(self):
        """
        Case: Correct SHARDING setting
        Expected: ImproperlyConfigured NOT raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'}):
            try:
                sharding_app.ready()
            except ImproperlyConfigured:
                self.fail('A valid configuration raises an exception')

    def test_invalid_override_model_shard_mode_setting(self):
        """
        Case: A few invalid configuration values for SHARDING["OVERRIDE_SHARDING_MODE"].
        Expected: ImproperlyConfigured raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'OVERRIDE_SHARDING_MODE': []}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             ('app', 'model'): 'asd',
                                         }}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             1: ShardingMode.MIRRORED,
                                         }}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             ('nonexistent', ): ShardingMode.MIRRORED,
                                         }}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             ('example', 'nonexistent'): ShardingMode.MIRRORED,
                                         }}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

        # Uppercase
        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             ('Example',): ShardingMode.MIRRORED,
                                         }}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             ('example', 'User'): ShardingMode.MIRRORED,
                                         }}):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

    def test_override_model_shard_mode_setting(self):
        """
        Case: A valid configuration value for SHARDING["OVERRIDE_SHARDING_MODE"] is provided.
        Expected: No ImproperlyConfigured is raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             ('example', 'user'): ShardingMode.MIRRORED,
                                         }}):
            try:
                sharding_app.ready()
            except ImproperlyConfigured:
                self.fail('A valid configuration raises an exception')

    def test_override_app_shard_mode_setting(self):
        """
        Case: A valid configuration value for SHARDING["OVERRIDE_SHARDING_MODE"] is provided.
        Expected: No ImproperlyConfigured is raised
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             ('example', ): ShardingMode.MIRRORED,
                                         }}):
            try:
                sharding_app.ready()
            except ImproperlyConfigured:
                self.fail('A valid configuration raises an exception')


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


class SessionEngineTestCase(SimpleTestCase):
    def test_sharded_user_model_and_cached_db_backend(self):
        """
        Case: User model is sharded and use of cached_db session backend
        Expected: ImproperlyConfigured raised.
        """
        @sharded_model()
        @test_model()
        class User1(AbstractBaseUser):
            class Meta:
                app_label = 'sharding'

        sharding_app = apps.get_app_config(app_label='sharding')
        with mock.patch('sharding.apps.get_user_model', return_value=User1):
            with override_settings(SESSION_ENGINE='django.contrib.sessions.backends.cached_db'):
                with self.assertRaises(ImproperlyConfigured):
                    sharding_app.ready()

    def test_sharded_user_model_no_cached_db_backend(self):
        """
        Case: User model is sharded and no use of cached_db session backend
        Expected: No ImproperlyConfigured raised.
        """
        @sharded_model()
        @test_model()
        class User2(AbstractBaseUser):
            class Meta:
                app_label = 'sharding'

        sharding_app = apps.get_app_config(app_label='sharding')
        with mock.patch('sharding.apps.get_user_model', return_value=User2):
            with override_settings(SESSION_ENGINE='django.contrib.sessions.backends.signed_cookies'):
                sharding_app.ready()

    def test_user_model__not_sharded_and_cached_db_backend(self):
        """
        Case: User model is not sharded and use of cached_db session backend
        Expected: No ImproperlyConfigured raised.
        """
        @test_model()
        class User3(AbstractBaseUser):
            class Meta:
                app_label = 'sharding'

        sharding_app = apps.get_app_config(app_label='sharding')
        with mock.patch('sharding.apps.get_user_model', return_value=User3):
            with override_settings(SESSION_ENGINE='django.contrib.sessions.backends.cached_db'):
                sharding_app.ready()

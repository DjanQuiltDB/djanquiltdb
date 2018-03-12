from unittest import mock

from django.apps import apps
from django.contrib.auth.models import AbstractBaseUser
from django.core.exceptions import ImproperlyConfigured
from django.db import models
from django.test import SimpleTestCase, override_settings

from sharding import ShardingMode
from sharding.decorators import sharded_model
from sharding.models import BaseShard


class DummyShard(models.Model):
    test_model = True

    class Meta:
        app_label = 'sharding'


@sharded_model()
class DummyShardedShard(BaseShard):
    test_model = True

    class Meta:
        app_label = 'sharding'


@sharded_model()
class DummyUser(AbstractBaseUser):
    test_model = True

    class Meta:
        app_label = 'sharding'


class ShardingSettingsTestCase(SimpleTestCase):
    def setUp(self):
        self.sharding_app = apps.get_app_config(app_label='sharding')

    def test_no_settings(self):
        """
        Case: None or empty SHARDING setting
        Expected: ImproperlyConfigured raised
        """
        with override_settings(SHARDING=None):
            with self.assertRaises(ImproperlyConfigured):
                self.sharding_app.ready()

        with override_settings(SHARDING=""):
            with self.assertRaises(ImproperlyConfigured):
                self.sharding_app.ready()

    def test_incomplete_models_settings(self):
        """
        Case: Given incomplete SHARDING setting
        Expected: ImproperlyConfigured raised
        """
        with override_settings(SHARDING={}):
            with self.assertRaises(ImproperlyConfigured):
                self.sharding_app.ready()

    def test_incompatible_models_settings(self):
        """
        Case: Given model in SHARDING setting is incompatible (not extending BaseShard/BaseNode)
        Expected: ImproperlyConfigured raised
        """
        with override_settings(SHARDING={'SHARD_CLASS': 'sharding.tests.app_config.DummyShard'}):
            with self.assertRaises(ImproperlyConfigured):
                self.sharding_app.ready()

    def test_sharded_models_settings(self):
        """
        Case: Given sharding model is sharded itself.
        Expected: ImproperlyConfigured raised
        """
        with override_settings(SHARDING={'SHARD_CLASS': 'sharding.tests.app_config.DummyShardedShard'}):
            with self.assertRaises(ImproperlyConfigured):
                self.sharding_app.ready()

    def test_correct_settings(self):
        """
        Case: Correct SHARDING setting
        Expected: ImproperlyConfigured NOT raised
        """
        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard'}):
            try:
                self.sharding_app.ready()
            except ImproperlyConfigured:
                self.fail('A valid configuration raises an exception')

    def test_invalid_override_shard_mode_list(self):
        """
        Case: Pass a list instead of a dict to SHARDING["OVERRIDE_SHARDING_MODE"].
        Expected: ImproperlyConfigured raised
        """
        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard', 'OVERRIDE_SHARDING_MODE': []}):
            with self.assertRaises(ImproperlyConfigured):
                self.sharding_app.ready()

    def test_invalid_override_shard_mode_invalid_value_type(self):
        """
        Case: Pass an object that is not ShardingMode enum as a value to the dict SHARDING["OVERRIDE_SHARDING_MODE"].
        Expected: ImproperlyConfigured raised
        """
        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             ('app', 'model'): 'asd',
                                         }}):
            with self.assertRaises(ImproperlyConfigured):
                self.sharding_app.ready()

    def test_invalid_override_shard_mode_invalid_key_type(self):
        """
        Case: Pass an object that is not a tuple or list as a key to the dict SHARDING["OVERRIDE_SHARDING_MODE"].
        Expected: ImproperlyConfigured raised
        """
        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             1: ShardingMode.MIRRORED,
                                         }}):
            with self.assertRaises(ImproperlyConfigured):
                self.sharding_app.ready()

    def test_invalid_override_shard_mode_nonexistent_app(self):
        """
        Case: Pass a non-existent app to SHARDING["OVERRIDE_SHARDING_MODE"].
        Expected: ImproperlyConfigured raised
        """
        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             ('nonexistent', ): ShardingMode.MIRRORED,
                                         }}):
            with self.assertRaises(ImproperlyConfigured):
                self.sharding_app.ready()

    def test_invalid_override_shard_mode_nonexistent_model(self):
        """
        Case: Pass a non-existent model to SHARDING["OVERRIDE_SHARDING_MODE"].
        Expected: ImproperlyConfigured raised
        """
        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             ('example', 'nonexistent'): ShardingMode.MIRRORED,
                                         }}):
            with self.assertRaises(ImproperlyConfigured):
                self.sharding_app.ready()

    def test_override_shard_mode_case_insensitive(self):
        """
        Case: Pass an upcase app and model name to SHARDING["OVERRIDE_SHARDING_MODE"].
        Expected:
        """
        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             ('Example',): ShardingMode.MIRRORED,
                                         }}):
            try:
                self.sharding_app.ready()
            except ImproperlyConfigured:
                self.fail('It should not trigger an exception')

        with override_settings(SHARDING={'SHARD_CLASS': 'example.models.Shard',
                                         'OVERRIDE_SHARDING_MODE': {
                                             ('example', 'User'): ShardingMode.MIRRORED,
                                         }}):
            try:
                self.sharding_app.ready()
            except ImproperlyConfigured:
                self.fail('It should not trigger an exception')

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
        class User1(AbstractBaseUser):
            test_model = True

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
        class User2(AbstractBaseUser):
            test_model = True

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
        class User3(AbstractBaseUser):
            test_model = True

            class Meta:
                app_label = 'sharding'

        sharding_app = apps.get_app_config(app_label='sharding')
        with mock.patch('sharding.apps.get_user_model', return_value=User3):
            with override_settings(SESSION_ENGINE='django.contrib.sessions.backends.cached_db'):
                sharding_app.ready()

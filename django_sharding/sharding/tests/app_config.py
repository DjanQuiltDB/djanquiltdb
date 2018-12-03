from unittest import mock

from django.apps import apps
from django.contrib.auth.models import AbstractBaseUser
from django.core.exceptions import ImproperlyConfigured
from django.db import models, connections
from django.test import SimpleTestCase, override_settings

from example.models import Shard
from sharding import ShardingMode, State
from sharding.db import connection
from sharding.decorators import sharded_model
from sharding.models import BaseShard
from sharding.options import ShardOptions
from sharding.postgresql_backend.base import DatabaseWrapper, PUBLIC_SCHEMA_NAME, ShardDatabaseWrapper
from sharding.tests import ShardingTestCase
from sharding.utils import create_template_schema


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


class DatabaseRouterTestCase(SimpleTestCase):
    def test_invalid_dbrouting_settings(self):
        """
        Case: DATABASE_ROUTERS does not contain sharding.router.DynamicDbRouter.
        Expected: ImproperlyConfigured raised.
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(DATABASE_ROUTERS=[]):
            with self.assertRaises(ImproperlyConfigured):
                sharding_app.ready()

    def test_valid_dbrouting_settings(self):
        """
        Case: DATABASE_ROUTERS contains sharding.router.DynamicDbRouter.
        Expected: No ImproperlyConfigured raised.
        """
        sharding_app = apps.get_app_config(app_label='sharding')

        with override_settings(DATABASE_ROUTERS=['sharding.router.DynamicDbRouter']):
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


class ConnectionsTestCase(ShardingTestCase):
    def setUp(self):
        super().setUp()

        create_template_schema()
        create_template_schema('other')

        self.shard = Shard.objects.create(node_name='default', schema_name='test_schema', alias='test',
                                          state=State.ACTIVE)
        self.other_shard = Shard.objects.create(node_name='other', schema_name='test_schema', alias='other_test',
                                                state=State.ACTIVE)

        self.default_connection = connections['default']
        self.other_connection = connections['other']

    def assertShardConnection(self, connection_, shard):
        """
        Checks if the provided connection is a ShardDatabaseWrapper and checks whether the attributes are those of the
        shard we provided.
        """
        self.assertIsInstance(connection_, ShardDatabaseWrapper)
        self.assertEqual(connection_._main_connection, connections[shard.node_name])
        self.assertEqual(connection_._main_connection.alias, shard.node_name)
        self.assertEqual(connection_.alias, '{}|{}'.format(shard.node_name, shard.schema_name))
        self.assertEqual(connection_.schema_name, shard.schema_name)

    def test_no_shard(self):
        """
        Case: Call connections['other']
        Expected: Return a DatabaseWrapper with the alias being 'other'
        """
        connection_ = connections['other']
        self.assertIsInstance(connection_, DatabaseWrapper)
        self.assertEqual(connection_.alias, 'other')

    def test_shard_options_public_schema(self):
        """
        Case: Call connections with a ShardOptions instance that point to a public schema
        Expected: DatabaseWrapper returned, being equal to connections[<node_name>]
        """
        shard_options = ShardOptions(node_name='default', schema_name=PUBLIC_SCHEMA_NAME)
        connection_ = connections[shard_options]
        self.assertEqual(connection_, self.default_connection)
        self.assertIsInstance(connection_, DatabaseWrapper)

    def test_pipe_node_name_schema_name(self):
        """
        Case: Call connections with a string containing the node name and the schema name, separated by a pipe character
        Expected: ShardDatabaseWrapper returned, with the correct schema
        """
        connection_ = connections['default|test_schema']
        self.assertShardConnection(connection_, self.shard)

        self.assertEqual(connection_.shard_options.options, frozenset({
            ('node_name', 'default'),
            ('schema_name', 'test_schema'),
        }))

    def test_tuple_node_name_schema_name(self):
        """
        Case: Call connections with a tuple containing the node name and the schema name
        Expected: ShardDatabaseWrapper returned, with the correct schema
        """
        connection_ = connections[('default', 'test_schema')]
        self.assertShardConnection(connection_, self.shard)

        self.assertEqual(connection_.shard_options.options, frozenset({
            ('node_name', 'default'),
            ('schema_name', 'test_schema'),
        }))

    def test_shard_instance(self):
        """
        Case: Call connections with a shard instance
        Expected: ShardDatabaseWrapper returned, with the correct schema
        """
        connection_ = connections[self.shard]
        self.assertShardConnection(connection_, self.shard)

        self.assertEqual(connection_.shard_options.options, frozenset({
            ('node_name', 'default'),
            ('schema_name', 'test_schema'),
            ('shard_id', self.shard.id),
        }))

    def test_shard_options_instance(self):
        """
        Case: Call connections with a ShardOptions instance
        Expected: ShardDatabaseWrapper returned, with the correct schema
        """
        shard_options = ShardOptions.from_shard(self.shard)
        connection_ = connections[shard_options]
        self.assertShardConnection(connection_, self.shard)

        self.assertEqual(connection_.shard_options, shard_options)


class ConnectionTestCase(SimpleTestCase):
    def test(self):
        """
        Case: Import django.db.connection
        Expected: django.db.connection is equal to sharding.db.connection
        """
        from django.db import connection as django_connection
        self.assertIs(django_connection, connection)

from types import SimpleNamespace
from unittest import mock

from django.apps import apps
from django.conf import settings
from django.core.checks.registry import registry
from django.core.management import call_command
from django.core.management.base import SystemCheckError
from django.test import SimpleTestCase, override_settings
from djanquiltdb import ShardingMode
from djanquiltdb.decorators import override_sharding_setting

from djanquiltdb_plugin_oauth_toolkit.checks import check_oauth_toolkit_setup

PLUGIN_MIDDLEWARE = 'djanquiltdb.middleware.oauth_toolkit.OAuthToolkitUseShardForMiddleware'
SESSION_MIDDLEWARE = 'django.contrib.sessions.middleware.SessionMiddleware'
TOKEN_MIDDLEWARE = 'oauth2_provider.middleware.OAuth2TokenMiddleware'


def message_ids():
    return [message.id for message in check_oauth_toolkit_setup(None)]


def app_configs_named(*names):
    """
    Returns app configs with ``names``, in that order, as apps.get_app_configs() returns the installed apps.
    """
    return [SimpleNamespace(name=name) for name in names]


def function_middleware(get_response):
    """
    A middleware that is a function, as Django accepts in MIDDLEWARE, so that it is not a class.
    """
    return get_response


def oauth2_provider_settings(**overrides):
    return {**settings.OAUTH2_PROVIDER, **overrides}


class OAuthToolkitSetupCheckTestCase(SimpleTestCase):
    def test_check_is_registered(self):
        """
        Case: The plugin is installed, as it is for every test run.
        Expected: The check is part of Django's check registry.
        """
        self.assertIn(check_oauth_toolkit_setup, registry.get_checks())

    def test_the_test_project_has_no_messages(self):
        """
        Case: The settings of the test project, which set up the plugin as its documentation describes.
        Expected: No messages.
        """
        self.assertEqual(message_ids(), [])

    @override_settings(QUILT_OAUTH_TOOLKIT={'TOKEN_DELIMITER': 'T', 'TOKEN_SEPARATOR': 'X'})
    def test_an_unknown_key_is_an_error(self):
        """
        Case: QUILT_OAUTH_TOOLKIT has the key TOKEN_SEPARATOR, which the plugin does not read.
        Expected: E001, naming the key.
        """
        messages = check_oauth_toolkit_setup(None)

        self.assertEqual([message.id for message in messages], ['djanquiltdb_plugin_oauth_toolkit.E001'])
        self.assertIn('TOKEN_SEPARATOR', messages[0].msg)

    @override_settings(QUILT_OAUTH_TOOLKIT=['TOKEN_DELIMITER'])
    def test_a_setting_that_is_not_a_dict_is_an_error(self):
        """
        Case: QUILT_OAUTH_TOOLKIT is a list.
        Expected: E002 only.
        """
        self.assertEqual(message_ids(), ['djanquiltdb_plugin_oauth_toolkit.E002'])

    @override_sharding_setting('SHARD_SELECTOR_REGEX', '[0-9A-Z]+')
    def test_a_problem_of_the_identifier_format_is_an_error(self):
        """
        Case: QUILT_DB['SHARD_SELECTOR_REGEX'] matches both delimiters.
        Expected: E003 for each delimiter.
        """
        self.assertEqual(message_ids(), ['djanquiltdb_plugin_oauth_toolkit.E003'] * 2)

    def test_a_toolkit_that_is_not_installed_is_an_error(self):
        """
        Case: oauth2_provider is not in INSTALLED_APPS.
        Expected: E004 only, since the other checks need the toolkit.
        """
        with mock.patch.object(apps, 'is_installed', return_value=False):
            self.assertEqual(message_ids(), ['djanquiltdb_plugin_oauth_toolkit.E004'])

    def test_a_project_without_a_mapping_model_is_an_error(self):
        """
        Case: QUILT_DB has no MAPPING_MODEL.
        Expected: E004, because a shard selector is a mapping value.
        """
        with override_sharding_setting('MAPPING_MODEL'):
            self.assertEqual(message_ids(), ['djanquiltdb_plugin_oauth_toolkit.E004'])

    def test_a_toolkit_model_that_is_not_sharded_is_an_error(self):
        """
        Case: OVERRIDE_SHARDING_MODE makes the toolkit's AccessToken mirrored, and the other models of the toolkit
              sharded.
        Expected: E004, naming the AccessToken model.
        """
        override = {
            **settings.QUILT_DB['OVERRIDE_SHARDING_MODE'],
            ('oauth2_provider', 'accesstoken'): ShardingMode.MIRRORED,
        }

        with override_sharding_setting('OVERRIDE_SHARDING_MODE', override):
            messages = check_oauth_toolkit_setup(None)

        self.assertEqual([message.id for message in messages], ['djanquiltdb_plugin_oauth_toolkit.E004'])
        self.assertIn('oauth2_provider.AccessToken', messages[0].msg)

    @override_settings(
        OAUTH2_PROVIDER=oauth2_provider_settings(
            CLIENT_ID_GENERATOR_CLASS='oauth2_provider.generators.ClientIdGenerator', ACCESS_TOKEN_GENERATOR=None
        )
    )
    def test_generators_of_the_toolkit_are_an_error(self):
        """
        Case: OAUTH2_PROVIDER names the toolkit's own client id generator, and no access token generator.
        Expected: E005 for each of the two settings.
        """
        self.assertEqual(message_ids(), ['djanquiltdb_plugin_oauth_toolkit.E005'] * 2)

    @override_settings(
        OAUTH2_PROVIDER=oauth2_provider_settings(
            REFRESH_TOKEN_GENERATOR='oauthlib.oauth2.rfc6749.tokens.random_token_generator'
        )
    )
    def test_a_refresh_token_generator_of_oauthlib_is_an_error(self):
        """
        Case: OAUTH2_PROVIDER names oauthlib's random token generator as the refresh token generator.
        Expected: E005, because a refresh token then has no shard selector.
        """
        self.assertEqual(message_ids(), ['djanquiltdb_plugin_oauth_toolkit.E005'])

    @override_settings(OAUTH2_PROVIDER=oauth2_provider_settings(REFRESH_TOKEN_GENERATOR=None))
    def test_no_refresh_token_generator_is_allowed(self):
        """
        Case: OAUTH2_PROVIDER has no refresh token generator, so oauthlib generates refresh tokens with the access token
              generator.
        Expected: No messages.
        """
        self.assertEqual(message_ids(), [])

    @override_settings(MIDDLEWARE=[SESSION_MIDDLEWARE])
    def test_a_missing_middleware_is_an_error(self):
        """
        Case: MIDDLEWARE does not have the middleware of the plugin.
        Expected: E006.
        """
        self.assertEqual(message_ids(), ['djanquiltdb_plugin_oauth_toolkit.E006'])

    @override_settings(MIDDLEWARE=[TOKEN_MIDDLEWARE, PLUGIN_MIDDLEWARE, SESSION_MIDDLEWARE])
    def test_a_middleware_in_the_wrong_place_is_an_error(self):
        """
        Case: MIDDLEWARE has the toolkit's OAuth2TokenMiddleware before the middleware of the plugin, and the
              SessionMiddleware after it.
        Expected: E007 for each of the two.
        """
        self.assertEqual(message_ids(), ['djanquiltdb_plugin_oauth_toolkit.E007'] * 2)

    @override_settings(MIDDLEWARE=[SESSION_MIDDLEWARE, 'missing_package.middleware.Middleware', PLUGIN_MIDDLEWARE])
    def test_a_middleware_that_does_not_import_is_skipped(self):
        """
        Case: MIDDLEWARE has an entry that does not import, between SessionMiddleware and the middleware of the plugin.
        Expected: No messages, and no ImportError. Django reports such an entry when it loads the request handler.
        """
        self.assertEqual(message_ids(), [])

    @override_settings(
        MIDDLEWARE=[SESSION_MIDDLEWARE, 'plugin_tests.checks.function_middleware', PLUGIN_MIDDLEWARE, TOKEN_MIDDLEWARE]
    )
    def test_a_middleware_that_is_a_function_is_not_compared_with_classes(self):
        """
        Case: MIDDLEWARE has a middleware that is a function, before the middleware of the plugin.
        Expected: No messages, and no TypeError.
        """
        self.assertEqual(message_ids(), [])

    @override_settings(OAUTH2_PROVIDER=oauth2_provider_settings(CIMD_ENABLED=True, DCR_ENABLED=True))
    def test_client_id_metadata_documents_and_dynamic_registration_are_reported(self):
        """
        Case: OAUTH2_PROVIDER turns Client ID Metadata Documents and dynamic client registration on.
        Expected: E008 for the documents, and W001 for the registration.
        """
        self.assertEqual(
            message_ids(), ['djanquiltdb_plugin_oauth_toolkit.E008', 'djanquiltdb_plugin_oauth_toolkit.W001']
        )

    def test_the_plugin_app_after_the_toolkit_is_a_warning(self):
        """
        Case: INSTALLED_APPS lists djanquiltdb_plugin_oauth_toolkit after oauth2_provider, so the cleartokens command of
              the toolkit replaces the plugin's.
        Expected: W002, whose hint says to list the plugin's app first.
        """
        app_configs = app_configs_named('djanquiltdb', 'oauth2_provider', 'djanquiltdb_plugin_oauth_toolkit')

        with mock.patch.object(apps, 'get_app_configs', return_value=app_configs):
            messages = check_oauth_toolkit_setup(None)

        self.assertEqual([message.id for message in messages], ['djanquiltdb_plugin_oauth_toolkit.W002'])
        self.assertIn("before 'oauth2_provider'", messages[0].hint)

    def test_a_missing_plugin_app_is_a_warning(self):
        """
        Case: INSTALLED_APPS has oauth2_provider, and not djanquiltdb_plugin_oauth_toolkit.
        Expected: W002.
        """
        with mock.patch.object(
            apps, 'get_app_configs', return_value=app_configs_named('djanquiltdb', 'oauth2_provider')
        ):
            self.assertEqual(message_ids(), ['djanquiltdb_plugin_oauth_toolkit.W002'])

    @override_settings(MIDDLEWARE=[SESSION_MIDDLEWARE])
    def test_the_check_command_fails_on_an_error(self):
        """
        Case: manage.py check runs while MIDDLEWARE does not have the middleware of the plugin.
        Expected: SystemCheckError that names E006.
        """
        with self.assertRaisesMessage(SystemCheckError, 'djanquiltdb_plugin_oauth_toolkit.E006'):
            call_command('check')

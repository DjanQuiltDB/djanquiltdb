from unittest import mock

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase, override_settings

from djanquiltdb_plugin_oauth_toolkit import identifiers
from djanquiltdb_plugin_oauth_toolkit.identifiers import (
    generate_client_id_for_selector,
    generate_token_for_selector,
    identifier_setting_problems,
    parse_client_id,
    parse_token,
)

QUILT_DB = {
    'SHARD_CLASS': 'example.models.Shard',
    'MAPPING_MODEL': 'example.models.OrganizationShard',
}

UUID_REGEX = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'


class IdentifierFormatTestCase(SimpleTestCase):
    def test_a_client_id_has_the_prefix_the_selector_and_forty_random_characters(self):
        """
        Case: A client id is generated for the shard selector 1.
        Expected: 'O1I' followed by 40 letters and digits, which parses back to '1'.
        """
        client_id = generate_client_id_for_selector(1)

        self.assertRegex(client_id, r'^O1I[A-Za-z0-9]{40}$')
        self.assertEqual(parse_client_id(client_id), '1')

    def test_a_token_has_the_prefix_the_selector_and_thirty_random_characters(self):
        """
        Case: A token is generated for the shard selector 1.
        Expected: 'O1T' followed by 30 letters and digits, as many as oauthlib generates. The token parses back to '1'.
        """
        token = generate_token_for_selector(1)

        self.assertRegex(token, r'^O1T[A-Za-z0-9]{30}$')
        self.assertEqual(parse_token(token), '1')

    def test_two_identifiers_for_one_selector_differ(self):
        """
        Case: Two tokens and two client ids are generated for the same shard selector.
        Expected: The two tokens differ, and so do the two client ids.
        """
        self.assertNotEqual(generate_token_for_selector(1), generate_token_for_selector(1))
        self.assertNotEqual(generate_client_id_for_selector(1), generate_client_id_for_selector(1))

    def test_a_selector_of_several_digits_is_parsed_whole(self):
        """
        Case: Identifiers for the shard selector 12, and identifiers whose random part starts with the delimiter.
        Expected: Each parses to '12', and not to a shorter or longer selector.
        """
        self.assertEqual(parse_client_id('O12Iabc'), '12')
        self.assertEqual(parse_client_id('O12IIabc'), '12')
        self.assertEqual(parse_token('O12TTabc'), '12')

    def test_a_value_without_the_format_parses_to_none(self):
        """
        Case: Garbage, an empty string, None, a prefix with a selector only, a client id, a value with the delimiter of
              a session key, a token with nothing after the delimiter, a token whose selector does not match the regex,
              and a number are each parsed as a token. None of them is a token of the format. A token is parsed as a
              client id.
        Expected: None for each, and no exception.
        """
        for value in ('garbage', '', None, 'O1', 'O1Iabc', 'O1Kabc', 'O1T', 'Oa1Tabc', 12):
            with self.subTest(value=value):
                self.assertIsNone(parse_token(value))

        self.assertIsNone(parse_client_id('O1Tabc'))

    @override_settings(
        QUILT_DB={**QUILT_DB, 'SHARD_SELECTOR_REGEX': UUID_REGEX},
        QUILT_OAUTH_TOOLKIT={'IDENTIFIER_PREFIX': 'tenant_', 'CLIENT_ID_DELIMITER': '_C_', 'TOKEN_DELIMITER': '_T_'},
    )
    def test_a_custom_regex_prefix_and_delimiters_round_trip(self):
        """
        Case: QUILT_DB['SHARD_SELECTOR_REGEX'] matches a UUID, and QUILT_OAUTH_TOOLKIT sets another prefix and other
              delimiters. A client id and a token are generated for a UUID.
        Expected: Each starts with the prefix, the UUID and its delimiter, and parses back to the UUID.
        """
        selector = '0f8fad5b-d9cb-469f-a165-70867728950e'

        client_id = generate_client_id_for_selector(selector)
        token = generate_token_for_selector(selector)

        self.assertTrue(client_id.startswith('tenant_{}_C_'.format(selector)))
        self.assertTrue(token.startswith('tenant_{}_T_'.format(selector)))
        self.assertEqual(parse_client_id(client_id), selector)
        self.assertEqual(parse_token(token), selector)

    @override_settings(QUILT_DB={**QUILT_DB, 'SHARD_SELECTOR_REGEX': '[-a-z0-9]+'})
    def test_a_slug_selector_round_trips(self):
        """
        Case: QUILT_DB['SHARD_SELECTOR_REGEX'] matches slugs, and a client id and a token are generated for the shard
              selector 'acme'.
        Expected: The client id starts with 'OacmeI' and the token with 'OacmeT', and each parses back to 'acme'.
        """
        client_id = generate_client_id_for_selector('acme')
        token = generate_token_for_selector('acme')

        self.assertTrue(client_id.startswith('OacmeI'))
        self.assertTrue(token.startswith('OacmeT'))
        self.assertEqual(parse_client_id(client_id), 'acme')
        self.assertEqual(parse_token(token), 'acme')

    def test_a_selector_that_the_regex_does_not_match_is_refused(self):
        """
        Case: A token is generated for the shard selector 'abc', which the default regex '[0-9]+' does not match.
        Expected: ImproperlyConfigured that names the selector and the regex.
        """
        with self.assertRaisesMessage(ImproperlyConfigured, "'abc'") as raised:
            generate_token_for_selector('abc')

        self.assertIn('[0-9]+', str(raised.exception))

    @override_settings(QUILT_DB={**QUILT_DB, 'SHARD_SELECTOR_REGEX': '[0-9]+T?'})
    def test_a_delimiter_that_reads_as_part_of_the_selector_is_refused(self):
        """
        Case: QUILT_DB['SHARD_SELECTOR_REGEX'] does not match the token delimiter 'T' on its own, but matches the
              selector '1' followed by it. A token is generated for the selector '1'.
        Expected: ImproperlyConfigured that names TOKEN_DELIMITER, because '1T' would parse as the selector.
        """
        with self.assertRaisesMessage(ImproperlyConfigured, 'TOKEN_DELIMITER'):
            generate_token_for_selector('1')

    @override_settings(QUILT_DB={**QUILT_DB, 'SHARD_SELECTOR_REGEX': '[0-9A-Z]+'})
    def test_a_regex_that_matches_a_delimiter_is_refused(self):
        """
        Case: QUILT_DB['SHARD_SELECTOR_REGEX'] matches the delimiters 'I' and 'T', and an identifier is generated or
              parsed.
        Expected: ImproperlyConfigured, because the end of a shard selector is unclear.
        """
        with self.assertRaisesMessage(ImproperlyConfigured, 'CLIENT_ID_DELIMITER'):
            generate_client_id_for_selector('1')

        with self.assertRaisesMessage(ImproperlyConfigured, 'TOKEN_DELIMITER'):
            parse_token('O1Tabc')

    def test_the_settings_are_checked_once_until_a_setting_changes(self):
        """
        Case: Two client ids are parsed and a token is generated, then QUILT_DB changes and a token is parsed.
        Expected: The settings of the format are checked once before the change, and once more after it.
        """
        with (
            override_settings(QUILT_OAUTH_TOOLKIT={}),
            mock.patch.object(identifiers, 'identifier_setting_problems', wraps=identifier_setting_problems) as check,
        ):
            parse_client_id('O1Iabc')
            parse_client_id('O2Iabc')
            generate_token_for_selector(1)

            self.assertEqual(check.call_count, 1)

            with override_settings(QUILT_DB={**QUILT_DB, 'SHARD_SELECTOR_REGEX': '[-a-z0-9]+'}):
                self.assertEqual(parse_token('OacmeTabc'), 'acme')

            self.assertEqual(check.call_count, 2)

    @override_settings(QUILT_DB={**QUILT_DB, 'SHARD_SELECTOR_REGEX': '[-a-z0-9.]+'})
    def test_a_client_id_for_a_selector_with_a_dot_is_refused_while_the_device_flow_is_configured(self):
        """
        Case: QUILT_DB['SHARD_SELECTOR_REGEX'] matches 'acme.example', the test project sets
              OAUTH2_PROVIDER['OAUTH_DEVICE_VERIFICATION_URI'], and a client id and a token are generated for
              'acme.example'. The device flow pages read a client id from a URL path with the slug converter, which
              does not match a dot.
        Expected: ImproperlyConfigured for the client id, which names the characters and the setting. The token is
                  generated, and parses back to 'acme.example'.
        """
        with self.assertRaisesMessage(ImproperlyConfigured, '[-A-Za-z0-9_]') as raised:
            generate_client_id_for_selector('acme.example')

        self.assertIn('OAUTH_DEVICE_VERIFICATION_URI', str(raised.exception))
        self.assertEqual(parse_token(generate_token_for_selector('acme.example')), 'acme.example')

    @override_settings(QUILT_DB={**QUILT_DB, 'SHARD_SELECTOR_REGEX': '[-a-z0-9.]+'})
    def test_a_client_id_for_a_selector_with_a_dot_is_generated_without_the_device_flow(self):
        """
        Case: QUILT_DB['SHARD_SELECTOR_REGEX'] matches 'acme.example', OAUTH2_PROVIDER does not set
              OAUTH_DEVICE_VERIFICATION_URI, and a client id is generated for 'acme.example'.
        Expected: The client id starts with 'Oacme.exampleI', and parses back to 'acme.example'.
        """
        oauth2_provider = {
            name: value for name, value in settings.OAUTH2_PROVIDER.items() if name != 'OAUTH_DEVICE_VERIFICATION_URI'
        }

        with override_settings(OAUTH2_PROVIDER=oauth2_provider):
            client_id = generate_client_id_for_selector('acme.example')

        self.assertTrue(client_id.startswith('Oacme.exampleI'))
        self.assertEqual(parse_client_id(client_id), 'acme.example')


class IdentifierSettingProblemsTestCase(SimpleTestCase):
    def test_the_defaults_have_no_problems(self):
        """
        Case: Neither QUILT_DB['SHARD_SELECTOR_REGEX'] nor QUILT_OAUTH_TOOLKIT is set.
        Expected: No problems.
        """
        self.assertEqual(identifier_setting_problems(), [])

    @override_settings(QUILT_DB={**QUILT_DB, 'SHARD_SELECTOR_REGEX': '[0-9'})
    def test_a_regex_that_does_not_compile_is_a_problem(self):
        """
        Case: QUILT_DB['SHARD_SELECTOR_REGEX'] is not a valid regular expression.
        Expected: One problem, which names the regex.
        """
        problems = identifier_setting_problems()

        self.assertEqual(len(problems), 1)
        self.assertIn("'[0-9'", problems[0])

    @override_settings(QUILT_DB={**QUILT_DB, 'SHARD_SELECTOR_REGEX': '[0-9I]+'})
    def test_a_regex_that_matches_a_delimiter_is_a_problem(self):
        """
        Case: QUILT_DB['SHARD_SELECTOR_REGEX'] matches the client id delimiter 'I', and not the token delimiter 'T'.
        Expected: One problem, which names CLIENT_ID_DELIMITER.
        """
        problems = identifier_setting_problems()

        self.assertEqual(len(problems), 1)
        self.assertIn('CLIENT_ID_DELIMITER', problems[0])

    @override_settings(QUILT_OAUTH_TOOLKIT={'IDENTIFIER_PREFIX': '', 'CLIENT_ID_DELIMITER': ':', 'TOKEN_DELIMITER': 5})
    def test_an_empty_an_unsafe_and_a_non_string_value_are_problems(self):
        """
        Case: QUILT_OAUTH_TOOLKIT sets an empty prefix, a token delimiter that is not a string, and a client id
              delimiter that is a colon. A client id in Basic authentication or in a URL path cannot have a colon.
        Expected: One problem for each, naming its key.
        """
        problems = identifier_setting_problems()

        self.assertEqual(len(problems), 3)
        self.assertIn('IDENTIFIER_PREFIX', problems[0])
        self.assertIn('CLIENT_ID_DELIMITER', problems[1])
        self.assertIn('TOKEN_DELIMITER', problems[2])

    @override_settings(QUILT_OAUTH_TOOLKIT={'IDENTIFIER_PREFIX': 'a.b'})
    def test_a_prefix_with_a_dot_is_a_problem(self):
        """
        Case: QUILT_OAUTH_TOOLKIT sets a prefix with a dot. django-oauth-toolkit reads the client id of the device flow
              from a URL path with Django's slug converter, which does not match a dot.
        Expected: One problem, which names the characters that are accepted.
        """
        problems = identifier_setting_problems()

        self.assertEqual(len(problems), 1)
        self.assertIn('[-A-Za-z0-9_]', problems[0])

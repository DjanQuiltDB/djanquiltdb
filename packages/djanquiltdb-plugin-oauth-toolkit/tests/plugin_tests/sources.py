import base64
from urllib.parse import urlencode

from django.test import RequestFactory, SimpleTestCase, override_settings
from django.urls import resolve
from jwcrypto import jwk, jwt

from djanquiltdb_plugin_oauth_toolkit.sources import (
    SOURCE_ACCESS_TOKEN_PARAMETER,
    SOURCE_BASIC_HEADER,
    SOURCE_BEARER_HEADER,
    SOURCE_CLIENT_ID_PARAMETER,
    SOURCE_ID_TOKEN_HINT,
    SOURCE_PATH_CLIENT_ID,
    FoundShardSelector,
    ShardSelectorRequestError,
    form_body,
    resolve_request_shard_selector,
    selector_from_access_token_parameter,
    selector_from_basic_auth_header,
    selector_from_bearer_header,
    selector_from_client_id_parameter,
    selector_from_id_token_hint,
    selector_from_path_client_id,
)
from plugin_tests.base import FORM_CONTENT_TYPE, basic_authorization

# Identifiers of the format, written out so that the tests read them as a client sends them.
CLIENT_ID_OF_1 = 'O1I' + 'a' * 40
CLIENT_ID_OF_2 = 'O2I' + 'b' * 40
TOKEN_OF_1 = 'O1T' + 'c' * 30
TOKEN_OF_2 = 'O2T' + 'd' * 30

SIGNING_KEY = jwk.JWK.generate(kty='RSA', size=2048)


def id_token(audience):
    """
    Returns a signed ID token for ``audience``. The middleware reads the token without checking its signature.
    """
    token = jwt.JWT(header={'alg': 'RS256'}, claims={'aud': audience, 'sub': '1'})
    token.make_signed_token(SIGNING_KEY)

    return token.serialize()


class SourceTestCase(SimpleTestCase):
    factory = RequestFactory()

    def get_request(self, path='/resource/', data=None, **headers):
        return self.factory.get(path, data or {}, headers=headers)

    def form_post_request(self, path='/resource/', data=None, **headers):
        """
        Returns a POST request with a form-encoded body, as OAuth clients send it.
        """
        return self.factory.generic(
            'POST', path, urlencode(data or {}), content_type=FORM_CONTENT_TYPE, headers=headers
        )


class BearerHeaderTestCase(SourceTestCase):
    def test_a_bearer_token_names_its_selector(self):
        """
        Case: The Authorization header is a Bearer token of organization 1, with the scheme in any case.
        Expected: The selector '1', from the Bearer header, with invalid_token as its error.
        """
        for scheme in ('Bearer', 'bearer', 'BEARER'):
            with self.subTest(scheme=scheme):
                request = self.get_request(Authorization='{} {}'.format(scheme, TOKEN_OF_1))

                self.assertEqual(
                    selector_from_bearer_header(request, None),
                    FoundShardSelector('1', SOURCE_BEARER_HEADER, 'invalid_token'),
                )

    def test_a_bearer_value_of_another_format_is_an_invalid_token(self):
        """
        Case: The Authorization header is a Bearer value that is not a token of the format.
        Expected: ShardSelectorRequestError invalid_token with status 401, from the Bearer header.
        """
        with self.assertRaises(ShardSelectorRequestError) as raised:
            selector_from_bearer_header(self.get_request(Authorization='Bearer garbage'), None)

        self.assertEqual(raised.exception.error, 'invalid_token')
        self.assertEqual(raised.exception.status_code, 401)
        self.assertEqual(raised.exception.source, SOURCE_BEARER_HEADER)

    def test_another_header_is_not_a_bearer_token(self):
        """
        Case: No Authorization header, a Basic header, and a Bearer header with three words, which oauthlib does not
              read as a token either.
        Expected: None for each.
        """
        self.assertIsNone(selector_from_bearer_header(self.get_request(), None))
        self.assertIsNone(
            selector_from_bearer_header(self.get_request(Authorization=basic_authorization(CLIENT_ID_OF_1)), None)
        )
        self.assertIsNone(
            selector_from_bearer_header(self.get_request(Authorization='Bearer {} extra'.format(TOKEN_OF_1)), None)
        )


class AccessTokenParameterTestCase(SourceTestCase):
    def test_a_token_in_the_form_body_and_in_the_query_names_its_selector(self):
        """
        Case: access_token is a token of organization 1 in a form body, and in a query string.
        Expected: The selector '1' for each, from the access_token parameter, with invalid_token as its error.
        """
        expected = FoundShardSelector('1', SOURCE_ACCESS_TOKEN_PARAMETER, 'invalid_token')

        self.assertEqual(
            selector_from_access_token_parameter(self.form_post_request(data={'access_token': TOKEN_OF_1}), None),
            expected,
        )
        self.assertEqual(
            selector_from_access_token_parameter(self.get_request(data={'access_token': TOKEN_OF_1}), None), expected
        )

    def test_the_parameter_is_not_read_beside_an_authorization_header(self):
        """
        Case: access_token is in the query string while the request also has an Authorization header, which oauthlib
              then reads instead.
        Expected: None.
        """
        request = self.get_request(data={'access_token': TOKEN_OF_1}, Authorization=basic_authorization(CLIENT_ID_OF_2))

        self.assertIsNone(selector_from_access_token_parameter(request, None))

    @override_settings(OAUTH2_PROVIDER={'COMPLIANT_BCP_RFC9700_ACCESS_TOKEN_TRANSPORT': True})
    def test_the_query_is_not_read_when_the_toolkit_refuses_it(self):
        """
        Case: access_token is in the query string while COMPLIANT_BCP_RFC9700_ACCESS_TOKEN_TRANSPORT is on, so the
              toolkit refuses the token before it looks it up.
        Expected: None.
        """
        self.assertIsNone(
            selector_from_access_token_parameter(self.get_request(data={'access_token': TOKEN_OF_1}), None)
        )

    def test_another_format_is_an_invalid_token(self):
        """
        Case: access_token is a value that is not a token of the format.
        Expected: ShardSelectorRequestError invalid_token, from the access_token parameter.
        """
        with self.assertRaises(ShardSelectorRequestError) as raised:
            selector_from_access_token_parameter(self.get_request(data={'access_token': 'garbage'}), None)

        self.assertEqual(raised.exception.error, 'invalid_token')
        self.assertEqual(raised.exception.source, SOURCE_ACCESS_TOKEN_PARAMETER)

    def test_a_body_that_is_not_form_encoded_is_not_read(self):
        """
        Case: access_token is in a multipart body, and in a JSON body.
        Expected: None for each, because only a form-encoded body is read, as the RFCs ask.
        """
        multipart_request = self.factory.post('/resource/', {'access_token': TOKEN_OF_1})
        json_request = self.factory.post(
            '/resource/', '{"access_token": "%s"}' % TOKEN_OF_1, content_type='application/json'
        )

        self.assertIsNone(selector_from_access_token_parameter(multipart_request, None))
        self.assertIsNone(selector_from_access_token_parameter(json_request, None))
        self.assertEqual(form_body(multipart_request), {})


class BasicHeaderTestCase(SourceTestCase):
    def test_a_basic_client_id_names_its_selector(self):
        """
        Case: The Basic authorization header has a client id of organization 1.
        Expected: The selector '1', from the Basic header, with invalid_client as its error.
        """
        request = self.form_post_request('/o/token/', Authorization=basic_authorization(CLIENT_ID_OF_1))

        self.assertEqual(
            selector_from_basic_auth_header(request, None),
            FoundShardSelector('1', SOURCE_BASIC_HEADER, 'invalid_client'),
        )

    def test_a_form_encoded_client_id_is_decoded_as_the_toolkit_decodes_it(self):
        """
        Case: The client id in the Basic header is percent-encoded character by character.
        Expected: The selector '1', because the client id is unquoted as the toolkit unquotes it.
        """
        encoded_client_id = ''.join('%{:02X}'.format(ord(character)) for character in CLIENT_ID_OF_1)
        credentials = base64.b64encode('{}:secret'.format(encoded_client_id).encode()).decode()

        request = self.form_post_request('/o/token/', Authorization='Basic ' + credentials)

        self.assertEqual(selector_from_basic_auth_header(request, None).selector, '1')

    def test_a_basic_client_id_of_another_format_is_an_invalid_client(self):
        """
        Case: The Basic authorization header has a client id that is not of the format.
        Expected: ShardSelectorRequestError invalid_client with status 401, from the Basic header.
        """
        with self.assertRaises(ShardSelectorRequestError) as raised:
            selector_from_basic_auth_header(self.form_post_request(Authorization=basic_authorization('garbage')), None)

        self.assertEqual(raised.exception.error, 'invalid_client')
        self.assertEqual(raised.exception.status_code, 401)
        self.assertEqual(raised.exception.source, SOURCE_BASIC_HEADER)

    def test_a_header_that_the_toolkit_does_not_read_as_basic_is_not_read(self):
        """
        Case: No Authorization header, a Bearer header, the Basic scheme in lower case, credentials that are not
              base64, base64 that is not text, credentials without a colon, and the Basic scheme without credentials.
              The toolkit ignores each of these headers and reads the client id from the body.
        Expected: None for each.
        """
        for authorization in (
            None,
            'Bearer ' + TOKEN_OF_1,
            basic_authorization(CLIENT_ID_OF_1).replace('Basic', 'basic'),
            'Basic !!!',
            'Basic ' + base64.b64encode(b'\xff\xfe').decode(),
            'Basic ' + base64.b64encode(CLIENT_ID_OF_1.encode()).decode(),
            'Basic',
        ):
            with self.subTest(authorization=authorization):
                headers = {} if authorization is None else {'Authorization': authorization}

                self.assertIsNone(selector_from_basic_auth_header(self.form_post_request(**headers), None))

    def test_credentials_with_a_character_outside_ascii_are_not_read(self):
        """
        Case: The credentials of the Basic authorization header have a character outside ASCII, which a WSGI server
              passes on from the bytes of the header, so they are not base64.
        Expected: None, and no exception.
        """
        request = self.form_post_request('/o/token/', Authorization='Basic \xe9' + 'a' * 11)

        self.assertIsNone(selector_from_basic_auth_header(request, None))


class ClientIdParameterTestCase(SourceTestCase):
    def test_a_client_id_in_the_query_of_a_get_names_its_selector(self):
        """
        Case: GET /o/authorize/ with the client id of organization 1 in the query string.
        Expected: The selector '1', from the client_id parameter, with invalid_request as its error.
        """
        request = self.get_request('/o/authorize/', data={'client_id': CLIENT_ID_OF_1})

        self.assertEqual(
            selector_from_client_id_parameter(request, resolve('/o/authorize/')),
            FoundShardSelector('1', SOURCE_CLIENT_ID_PARAMETER, 'invalid_request'),
        )

    def test_a_client_id_in_the_body_of_a_post_names_its_selector(self):
        """
        Case: POST /o/token/ with the client id of organization 1 in the form body, and the client id of organization 2
              in the query string. oauthlib refuses a query string on a POST to the token endpoint.
        Expected: The selector '1', with invalid_client as its error, since the token endpoint authenticates the client.
        """
        request = self.form_post_request('/o/token/?client_id={}'.format(CLIENT_ID_OF_2), {'client_id': CLIENT_ID_OF_1})

        self.assertEqual(
            selector_from_client_id_parameter(request, resolve('/o/token/')),
            FoundShardSelector('1', SOURCE_CLIENT_ID_PARAMETER, 'invalid_client'),
        )

    def test_a_client_id_of_another_format_gets_the_error_of_its_endpoint(self):
        """
        Case: A client id that is not of the format, on the token endpoint, which authenticates the client, and on the
              authorization endpoint, which does not.
        Expected: ShardSelectorRequestError invalid_client with status 401 for the token endpoint, and invalid_request
                  with status 400 for the authorization endpoint.
        """
        with self.assertRaises(ShardSelectorRequestError) as raised:
            selector_from_client_id_parameter(
                self.form_post_request('/o/token/', {'client_id': 'x'}), resolve('/o/token/')
            )

        self.assertEqual((raised.exception.error, raised.exception.status_code), ('invalid_client', 401))

        with self.assertRaises(ShardSelectorRequestError) as raised:
            selector_from_client_id_parameter(
                self.get_request('/o/authorize/', {'client_id': 'x'}), resolve('/o/authorize/')
            )

        self.assertEqual((raised.exception.error, raised.exception.status_code), ('invalid_request', 400))

    def test_a_request_without_a_client_id_parameter_gives_no_selector(self):
        """
        Case: A request without a client_id parameter, and one with an empty client_id parameter, which oauthlib
              treats as a missing one. Neither path resolves.
        Expected: None for each.
        """
        self.assertIsNone(selector_from_client_id_parameter(self.get_request('/nowhere/'), None))
        self.assertIsNone(selector_from_client_id_parameter(self.get_request('/nowhere/', {'client_id': ''}), None))


class PathClientIdTestCase(SourceTestCase):
    def test_the_client_id_of_a_device_confirmation_path_names_its_selector(self):
        """
        Case: The path of the device confirmation page, with the client id of organization 1.
        Expected: The selector '1', from the client id in the URL path, with invalid_request as its error.
        """
        path = '/o/device-confirm/{}/ABCD1234'.format(CLIENT_ID_OF_1)

        self.assertEqual(
            selector_from_path_client_id(self.get_request(path), resolve(path)),
            FoundShardSelector('1', SOURCE_PATH_CLIENT_ID, 'invalid_request'),
        )

    def test_a_path_client_id_of_another_format_is_an_invalid_request(self):
        """
        Case: The path of the device confirmation page, with a client id that is not of the format.
        Expected: ShardSelectorRequestError invalid_request, from the client id in the URL path.
        """
        path = '/o/device-confirm/garbage/ABCD1234'

        with self.assertRaises(ShardSelectorRequestError) as raised:
            selector_from_path_client_id(self.get_request(path), resolve(path))

        self.assertEqual(raised.exception.error, 'invalid_request')
        self.assertEqual(raised.exception.source, SOURCE_PATH_CLIENT_ID)

    def test_a_path_without_a_client_id_gives_no_selector(self):
        """
        Case: A path that does not resolve, and a path that resolves without a client id.
        Expected: None for each.
        """
        self.assertIsNone(selector_from_path_client_id(self.get_request('/nowhere/'), None))
        self.assertIsNone(selector_from_path_client_id(self.get_request('/o/token/'), resolve('/o/token/')))


class IdTokenHintTestCase(SourceTestCase):
    def test_the_audience_of_an_id_token_hint_names_its_selector(self):
        """
        Case: The logout request has an id_token_hint whose aud is the client id of organization 1, as a string in the
              query string, and as a list in a form body.
        Expected: The selector '1' for each, from the id_token_hint, with invalid_request as its error.
        """
        expected = FoundShardSelector('1', SOURCE_ID_TOKEN_HINT, 'invalid_request')

        self.assertEqual(
            selector_from_id_token_hint(
                self.get_request('/o/logout/', {'id_token_hint': id_token(CLIENT_ID_OF_1)}), None
            ),
            expected,
        )
        self.assertEqual(
            selector_from_id_token_hint(
                self.form_post_request('/o/logout/', {'id_token_hint': id_token([CLIENT_ID_OF_1, CLIENT_ID_OF_1])}),
                None,
            ),
            expected,
        )

    def test_a_hint_that_names_no_single_selector_is_an_invalid_request(self):
        """
        Case: An id_token_hint that is not a token, one whose aud is null, one whose aud is not a client id of the
              format, one whose aud is a number, and one whose aud names two organizations.
        Expected: ShardSelectorRequestError invalid_request for each, from the id_token_hint.
        """
        for hint in (
            'garbage',
            id_token(None),
            id_token('garbage'),
            id_token(12),
            id_token([CLIENT_ID_OF_1, CLIENT_ID_OF_2]),
        ):
            with self.subTest(hint=hint[:20]):
                with self.assertRaises(ShardSelectorRequestError) as raised:
                    selector_from_id_token_hint(self.get_request('/o/logout/', {'id_token_hint': hint}), None)

                self.assertEqual(raised.exception.error, 'invalid_request')
                self.assertEqual(raised.exception.source, SOURCE_ID_TOKEN_HINT)

    def test_a_request_without_a_hint_gives_no_selector(self):
        """
        Case: A logout request without an id_token_hint.
        Expected: None.
        """
        self.assertIsNone(selector_from_id_token_hint(self.get_request('/o/logout/'), None))

    def test_a_hint_in_the_query_string_of_a_post_is_not_read(self):
        """
        Case: POST /o/logout/ with an id_token_hint of organization 1 in the query string only. The toolkit reads the
              parameters of a POST from its form body.
        Expected: None.
        """
        request = self.form_post_request('/o/logout/?{}'.format(urlencode({'id_token_hint': id_token(CLIENT_ID_OF_1)})))

        self.assertIsNone(selector_from_id_token_hint(request, None))


class ResolveRequestShardSelectorTestCase(SourceTestCase):
    sources = (selector_from_basic_auth_header, selector_from_client_id_parameter)

    def test_the_first_found_selector_is_returned(self):
        """
        Case: A token request with the client id of organization 1 in the Basic header and in the body.
        Expected: The selector '1', from the Basic header, which comes first.
        """
        request = self.form_post_request(
            '/o/token/', {'client_id': CLIENT_ID_OF_1}, Authorization=basic_authorization(CLIENT_ID_OF_1)
        )

        found = resolve_request_shard_selector(request, resolve('/o/token/'), self.sources)

        self.assertEqual(found, FoundShardSelector('1', SOURCE_BASIC_HEADER, 'invalid_client'))

    def test_two_sources_that_name_different_selectors_are_an_invalid_request(self):
        """
        Case: A token request with the client id of organization 1 in the Basic header and the client id of
              organization 2 in the body.
        Expected: ShardSelectorRequestError invalid_request with status 400, naming both sources.
        """
        request = self.form_post_request(
            '/o/token/', {'client_id': CLIENT_ID_OF_2}, Authorization=basic_authorization(CLIENT_ID_OF_1)
        )

        with self.assertRaises(ShardSelectorRequestError) as raised:
            resolve_request_shard_selector(request, resolve('/o/token/'), self.sources)

        self.assertEqual((raised.exception.error, raised.exception.status_code), ('invalid_request', 400))
        self.assertIn(SOURCE_BASIC_HEADER, raised.exception.description)
        self.assertIn(SOURCE_CLIENT_ID_PARAMETER, raised.exception.description)

    def test_a_request_without_a_client_id_or_a_token_gives_no_selector(self):
        """
        Case: A token request without a client id.
        Expected: None.
        """
        self.assertIsNone(resolve_request_shard_selector(self.form_post_request('/o/token/'), None, self.sources))

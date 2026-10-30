import json
from types import SimpleNamespace
from urllib.parse import urlencode

from django.core.exceptions import ImproperlyConfigured
from django.http import JsonResponse
from django.test import RequestFactory, SimpleTestCase, override_settings
from djanquiltdb.decorators import override_sharding_setting
from djanquiltdb.router import get_active_connection

from djanquiltdb_plugin_oauth_toolkit.middleware import OAuthToolkitUseShardForMiddleware
from djanquiltdb_plugin_oauth_toolkit.sources import selector_from_access_token_parameter
from example.views import active_connection_description
from plugin_tests.base import FORM_CONTENT_TYPE, PluginTestCase, basic_authorization

TOKEN_OF_1 = 'O1T' + 'c' * 30
TOKEN_OF_2 = 'O2T' + 'd' * 30
TOKEN_OF_3 = 'O3T' + 'e' * 30
TOKEN_OF_999 = 'O999T' + 'f' * 30
CLIENT_ID_OF_999 = 'O999I' + 'a' * 40


def view_that_echoes_the_active_connection(request):
    """
    Stands in for the view, and returns the node and the schema that the middleware has entered.
    """
    return JsonResponse(active_connection_description())


class MiddlewareTestCase(PluginTestCase):
    factory = RequestFactory()
    middleware_class = OAuthToolkitUseShardForMiddleware

    def run_middleware(self, request, session_selector=None):
        """
        Runs the middleware around a view that echoes the active connection, with a session whose shard selector is
        ``session_selector``, and returns the response.
        """
        if session_selector is not None:
            request.session = SimpleNamespace(shard_selector=session_selector)

        return self.middleware_class(view_that_echoes_the_active_connection)(request)

    def assertRanIn(self, response, schema_name):
        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(json.loads(response.content)['schema'], schema_name)

    def assertError(self, response, status_code, error):
        self.assertEqual(response.status_code, status_code, response.content)
        self.assertEqual(json.loads(response.content)['error'], error)
        self.assertEqual(response['Cache-Control'], 'no-store')


class SelectionTestCase(MiddlewareTestCase):
    def test_the_session_selector_is_used_without_a_client_id_or_a_token(self):
        """
        Case: A request without a client id or a token, with a session of organization 1.
        Expected: The view runs in the shard of organization 1, and the mapping value of the request is '1'.
        """
        request = self.factory.get('/resource/')

        self.assertRanIn(self.run_middleware(request, session_selector='1'), 'first_schema')
        self.assertEqual(request._mapping_value, '1')

    def test_a_bearer_token_names_the_shard_without_a_session(self):
        """
        Case: A request with a Bearer token of organization 2, whose shard is on the other node, and no session.
        Expected: The view runs in the shard of organization 2.
        """
        response = self.run_middleware(
            self.factory.get('/resource/', headers={'Authorization': 'Bearer ' + TOKEN_OF_2})
        )

        self.assertRanIn(response, 'remote_schema')

    def test_a_bearer_token_that_agrees_with_the_session_is_used(self):
        """
        Case: A request with a Bearer token of organization 1 and a session of organization 1.
        Expected: The view runs in the shard of organization 1.
        """
        request = self.factory.get('/resource/', headers={'Authorization': 'Bearer ' + TOKEN_OF_1})

        self.assertRanIn(self.run_middleware(request, session_selector='1'), 'first_schema')

    def test_a_bearer_token_that_disagrees_with_the_session_is_refused(self):
        """
        Case: A request with a Bearer token of organization 1 and a session of organization 2. The user of the session
              is stored in the shard of organization 2.
        Expected: 400 invalid_request that names both sources, and the view does not run.
        """
        request = self.factory.get('/resource/', headers={'Authorization': 'Bearer ' + TOKEN_OF_1})

        response = self.run_middleware(request, session_selector='2')

        self.assertError(response, 400, 'invalid_request')
        self.assertIn('session', json.loads(response.content)['error_description'])

    def test_no_shard_is_entered_without_any_selector(self):
        """
        Case: A request without a client id, a token or a session.
        Expected: The view runs on the default connection.
        """
        response = self.run_middleware(self.factory.get('/resource/'))

        self.assertEqual(json.loads(response.content), {'node': 'default', 'schema': None})

    def test_a_path_that_does_not_resolve_reads_the_sources_of_the_project(self):
        """
        Case: A request for a path that no URL pattern matches has a Bearer token of organization 2.
        Expected: The view runs in the shard of organization 2, because the middleware reads the sources of the URLs of
                  the project for such a path.
        """
        response = self.run_middleware(self.factory.get('/nowhere/', headers={'Authorization': 'Bearer ' + TOKEN_OF_2}))

        self.assertRanIn(response, 'remote_schema')

    def test_the_response_leaves_the_shard(self):
        """
        Case: A request with a Bearer token of organization 2 runs through the middleware.
        Expected: After the response, the active connection is the default connection again.
        """
        self.run_middleware(self.factory.get('/resource/', headers={'Authorization': 'Bearer ' + TOKEN_OF_2}))

        self.assertEqual(get_active_connection(), 'default')

    def test_a_url_without_sources_reads_no_header(self):
        """
        Case: The JWKS document of the toolkit is requested with an Authorization header that is not a token of the
              format.
        Expected: The view runs on the default connection, because the toolkit's discovery and key documents have no
                  sources.
        """
        response = self.run_middleware(
            self.factory.get('/o/.well-known/jwks.json', headers={'Authorization': 'Bearer x'})
        )

        self.assertEqual(json.loads(response.content), {'node': 'default', 'schema': None})

    def test_basic_authentication_is_only_read_on_the_endpoints_that_authenticate_a_client(self):
        """
        Case: A request for a URL of the project has a Basic authorization header with a client id of organization 2.
        Expected: The view runs on the default connection, because the middleware only reads tokens on a URL of the
                  project.
        """
        request = self.factory.get('/resource/', headers={'Authorization': basic_authorization('O2I' + 'b' * 40)})

        self.assertEqual(json.loads(self.run_middleware(request).content), {'node': 'default', 'schema': None})


class ErrorTestCase(MiddlewareTestCase):
    def test_a_token_of_an_unknown_organization_is_an_invalid_token(self):
        """
        Case: A request with a Bearer token of organization 999, which has no mapping row.
        Expected: 401 invalid_token, with the WWW-Authenticate header that oauthlib gives an invalid token.
        """
        response = self.run_middleware(
            self.factory.get('/resource/', headers={'Authorization': 'Bearer ' + TOKEN_OF_999})
        )

        self.assertError(response, 401, 'invalid_token')
        self.assertTrue(response['WWW-Authenticate'].startswith('Bearer error="invalid_token"'))

    def test_a_token_of_another_format_is_an_invalid_token(self):
        """
        Case: A request with a Bearer value that is not a token of the format.
        Expected: 401 invalid_token.
        """
        response = self.run_middleware(self.factory.get('/resource/', headers={'Authorization': 'Bearer garbage'}))

        self.assertError(response, 401, 'invalid_token')

    def test_a_basic_client_id_of_an_unknown_organization_is_an_invalid_client(self):
        """
        Case: A token request with a Basic authorization header whose client id belongs to organization 999.
        Expected: 401 invalid_client, with a WWW-Authenticate header.
        """
        request = self.factory.post(
            '/o/token/',
            'grant_type=client_credentials',
            content_type=FORM_CONTENT_TYPE,
            headers={'Authorization': basic_authorization(CLIENT_ID_OF_999)},
        )

        response = self.run_middleware(request)

        self.assertError(response, 401, 'invalid_client')
        self.assertTrue(response['WWW-Authenticate'].startswith('Bearer error="invalid_client"'))

    def test_a_client_id_of_an_unknown_organization_on_the_authorize_page_is_an_invalid_request(self):
        """
        Case: GET /o/authorize/ with the client id of organization 999 in the query string.
        Expected: 400 invalid_request.
        """
        request = self.factory.get('/o/authorize/', {'client_id': CLIENT_ID_OF_999})

        self.assertError(self.run_middleware(request), 400, 'invalid_request')

    def test_a_session_of_an_unknown_organization_leaves_the_request_without_a_shard(self):
        """
        Case: A request without a client id or a token, with a session of organization 999, which has no mapping row, as
              after the mapping row of an organization is deleted.
        Expected: The view runs on the default connection, and the mapping value of the request is None, as with
                  UseShardForMiddleware. djanquiltdb.sessions reads such a session as empty.
        """
        request = self.factory.get('/resource/')

        response = self.run_middleware(request, session_selector='999')

        self.assertEqual(json.loads(response.content), {'node': 'default', 'schema': None})
        self.assertIsNone(request._mapping_value)

    def test_a_token_request_with_only_a_session_of_an_unknown_organization_is_an_invalid_client(self):
        """
        Case: POST /o/token/ without a client id, with a session of organization 999, which has no mapping row.
        Expected: 401 invalid_client, as without a session, because the toolkit would look up the client on the default
                  connection.
        """
        request = self.factory.post('/o/token/', 'grant_type=client_credentials', content_type=FORM_CONTENT_TYPE)

        self.assertError(self.run_middleware(request, session_selector='999'), 401, 'invalid_client')

    def test_a_token_request_without_a_client_id_is_an_invalid_client(self):
        """
        Case: POST /o/token/ without a client id and without a session. The toolkit would look up the client on the
              default connection.
        Expected: 401 invalid_client.
        """
        request = self.factory.post('/o/token/', 'grant_type=client_credentials', content_type=FORM_CONTENT_TYPE)

        self.assertError(self.run_middleware(request), 401, 'invalid_client')

    def test_an_introspection_without_credentials_is_access_denied(self):
        """
        Case: POST /o/introspect/ without client credentials, without a Bearer token and without a session.
        Expected: 403 access_denied, the status that the toolkit gives an introspection without credentials.
        """
        request = self.factory.post('/o/introspect/', urlencode({'token': TOKEN_OF_1}), content_type=FORM_CONTENT_TYPE)

        self.assertError(self.run_middleware(request), 403, 'access_denied')

    def test_a_token_of_an_organization_in_maintenance_is_unavailable(self):
        """
        Case: The shard of organization 3 is in maintenance, and a request has a Bearer token of organization 3.
        Expected: 503, as the core middleware gives a shard in maintenance.
        """
        self.put_shard_in_maintenance(self.maintained)

        response = self.run_middleware(
            self.factory.get('/resource/', headers={'Authorization': 'Bearer ' + TOKEN_OF_3})
        )

        self.assertEqual(response.status_code, 503)


class HookTestCase(MiddlewareTestCase):
    def test_a_subclass_renders_its_own_error_response(self):
        """
        Case: A subclass overrides error_response(), and a request has a token of organization 999.
        Expected: The response of the subclass, which is given the error.
        """

        class TeapotMiddleware(OAuthToolkitUseShardForMiddleware):
            def error_response(self, request, error):
                return JsonResponse({'teapot': error.error}, status=418)

        self.middleware_class = TeapotMiddleware

        response = self.run_middleware(
            self.factory.get('/resource/', headers={'Authorization': 'Bearer ' + TOKEN_OF_999})
        )

        self.assertEqual(response.status_code, 418)
        self.assertEqual(json.loads(response.content), {'teapot': 'invalid_token'})

    def test_a_subclass_reads_the_session_selector_its_own_way(self):
        """
        Case: A subclass overrides get_session_shard_selector() to return '2', and a request has no session.
        Expected: The view runs in the shard of organization 2.
        """

        class SessionOfTwoMiddleware(OAuthToolkitUseShardForMiddleware):
            def get_session_shard_selector(self, request):
                return '2'

        self.middleware_class = SessionOfTwoMiddleware

        self.assertRanIn(self.run_middleware(self.factory.get('/resource/')), 'remote_schema')

    def test_a_subclass_reads_other_sources_on_the_urls_of_the_project(self):
        """
        Case: A subclass only reads the access_token parameter on the URLs of the project, and a request has a Bearer
              token of organization 2.
        Expected: The view runs on the default connection, because the subclass does not read the Bearer header.
        """

        class ParameterOnlyMiddleware(OAuthToolkitUseShardForMiddleware):
            default_shard_selector_sources = (selector_from_access_token_parameter,)

        self.middleware_class = ParameterOnlyMiddleware

        request = self.factory.get('/resource/', headers={'Authorization': 'Bearer ' + TOKEN_OF_2})

        self.assertEqual(json.loads(self.run_middleware(request).content), {'node': 'default', 'schema': None})

    @override_sharding_setting('SESSION_SHARD_SELECTOR_KEY', 'organization')
    def test_the_session_attribute_is_named_by_session_shard_selector_key(self):
        """
        Case: QUILT_DB['SESSION_SHARD_SELECTOR_KEY'] is 'organization', and the session has 2 in its attribute
              organization.
        Expected: The view runs in the shard of organization 2.
        """
        request = self.factory.get('/resource/')
        request.session = SimpleNamespace(organization=2)

        self.assertRanIn(self.run_middleware(request), 'remote_schema')


class MissingMappingModelTestCase(SimpleTestCase):
    @override_settings(QUILT_DB={'SHARD_CLASS': 'example.models.Shard'})
    def test_a_project_without_a_mapping_model_gets_the_error_of_the_setting(self):
        """
        Case: QUILT_DB has no MAPPING_MODEL, and a request has a Bearer token of organization 1.
        Expected: ImproperlyConfigured that names MAPPING_MODEL, the setting that the system check reports.
        """
        request = RequestFactory().get('/resource/', headers={'Authorization': 'Bearer ' + TOKEN_OF_1})

        with self.assertRaisesMessage(ImproperlyConfigured, 'MAPPING_MODEL'):
            OAuthToolkitUseShardForMiddleware(view_that_echoes_the_active_connection)(request)

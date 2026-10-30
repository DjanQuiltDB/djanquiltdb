from djanquiltdb.utils import use_shard
from oauth2_provider.models import get_access_token_model

from plugin_tests.base import REDIRECT_URI, PluginTestCase, basic_authorization

TOKEN_OF_2 = 'O2T' + 'd' * 30
TOKEN_OF_999 = 'O999T' + 'f' * 30
CLIENT_ID_OF_999 = 'O999I' + 'a' * 40


class OtherShardTestCase(PluginTestCase):
    def setUp(self):
        super().setUp()
        self.first_token = self.request_client_credentials_token(self.create_application(1))['access_token']
        self.remote_token = self.request_client_credentials_token(self.create_application(2))['access_token']

    def test_a_token_of_the_other_node_runs_in_its_own_shard(self):
        """
        Case: The protected resource is requested with a token of organization 2, whose shard is on the other node.
        Expected: 200 from the shard of organization 2.
        """
        response = self.request_protected_resource(self.remote_token)

        self.assertEqual(response.json(), {'node': 'other', 'schema': 'remote_schema', 'resource_owner': None})

    def test_a_token_with_a_rewritten_selector_is_an_unknown_token(self):
        """
        Case: The shard selector of a token of organization 1 is rewritten to 2, and the protected resource is
              requested with it, and with a random token of organization 2.
        Expected: 403 for each, the toolkit's answer to a token that it does not know, and the shard of organization 2
                  still has only its own token.
        """
        rewritten_token = 'O2' + self.first_token[2:]

        self.assertEqual(self.request_protected_resource(rewritten_token).status_code, 403)
        self.assertEqual(self.request_protected_resource(TOKEN_OF_2).status_code, 403)

        with use_shard(self.remote):
            self.assertEqual(get_access_token_model().objects.count(), 1)

    def test_a_token_from_the_bearer_header_sets_the_user_of_the_request(self):
        """
        Case: A user of organization 2 gets a token, and a URL of the project that reads request.user is requested with
              it. The toolkit's OAuth2TokenMiddleware runs after the middleware of the plugin.
        Expected: request.user is the user of organization 2, read from the shard of organization 2.
        """
        application = self.create_authorization_code_application(2)
        user = self.create_user(2, email='remote@example.com')
        access_token = self.request_authorization_code_tokens(2, application, user)['access_token']
        self.forget_session()

        response = self.client.get('/whoami/', headers={'Authorization': 'Bearer ' + access_token})

        self.assertEqual(response.json(), {'node': 'other', 'schema': 'remote_schema', 'user': 'remote@example.com'})


class UnknownOrganizationTestCase(PluginTestCase):
    def test_a_token_of_an_unknown_organization_is_an_invalid_token(self):
        """
        Case: The protected resource is requested with a token of organization 999, and with a Bearer value of another
              format.
        Expected: 401 invalid_token for each.
        """
        for token in (TOKEN_OF_999, 'garbage'):
            with self.subTest(token=token):
                response = self.request_protected_resource(token)

                self.assertEqual(response.status_code, 401, response.content)
                self.assertEqual(response.json()['error'], 'invalid_token')

    def test_a_client_of_an_unknown_organization_is_an_invalid_client(self):
        """
        Case: A client with a client id of organization 999 requests a token with Basic authentication.
        Expected: 401 invalid_client.
        """
        response = self.post_form(
            '/o/token/', {'grant_type': 'client_credentials'}, Authorization=basic_authorization(CLIENT_ID_OF_999)
        )

        self.assertEqual(response.status_code, 401, response.content)
        self.assertEqual(response.json()['error'], 'invalid_client')

    def test_an_authorization_for_an_unknown_organization_is_an_invalid_request(self):
        """
        Case: The authorization page is requested for a client id of organization 999.
        Expected: 400 invalid_request.
        """
        response = self.client.get('/o/authorize/', {'client_id': CLIENT_ID_OF_999, 'response_type': 'code'})

        self.assertEqual(response.status_code, 400, response.content)
        self.assertEqual(response.json()['error'], 'invalid_request')

    def test_an_authorization_for_a_client_of_another_organization_than_the_session_is_refused(self):
        """
        Case: A user of organization 1 is logged in, and opens the authorization page of an application of
              organization 2.
        Expected: 400 invalid_request, because the toolkit would write the grant of the user of organization 1 in the
                  shard of organization 2.
        """
        application = self.create_authorization_code_application(2)
        self.log_in(1, self.create_user(1))

        response = self.client.get(
            '/o/authorize/',
            {'client_id': application.client_id, 'response_type': 'code', 'redirect_uri': REDIRECT_URI},
        )

        self.assertEqual(response.status_code, 400, response.content)
        self.assertEqual(response.json()['error'], 'invalid_request')


class MaintenanceTestCase(PluginTestCase):
    def test_a_token_and_a_client_of_an_organization_in_maintenance_are_unavailable(self):
        """
        Case: The shard of organization 3 is in maintenance, and the protected resource is requested with a token of
              organization 3, and a client of organization 3 requests a token.
        Expected: 503 for each.
        """
        application = self.create_application(3)
        access_token = self.request_client_credentials_token(application)['access_token']
        self.put_shard_in_maintenance(self.maintained)

        self.assertEqual(self.request_protected_resource(access_token).status_code, 503)

        response = self.post_form(
            '/o/token/', {'grant_type': 'client_credentials'}, Authorization=basic_authorization(application.client_id)
        )

        self.assertEqual(response.status_code, 503)

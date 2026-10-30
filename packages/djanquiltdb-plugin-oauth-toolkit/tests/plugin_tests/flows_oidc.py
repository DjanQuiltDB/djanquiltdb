from urllib.parse import urlencode

from plugin_tests.base import PluginTestCase

CLIENT_ID_OF_999 = 'O999I' + 'a' * 40


class OpenIdConnectTestCase(PluginTestCase):
    def setUp(self):
        super().setUp()
        self.application = self.create_authorization_code_application(1)
        self.user = self.create_user(1)
        self.tokens = self.request_authorization_code_tokens(1, self.application, self.user)
        self.forget_session()

    def test_userinfo_reads_the_token_from_the_header_the_body_and_the_query(self):
        """
        Case: The userinfo endpoint is requested with the access token of a user of organization 1, in the Bearer
              header, in a form body, and in the query string.
        Expected: 200 with the subject of the user, for each.
        """
        access_token = self.tokens['access_token']

        responses = [
            self.client.get('/o/userinfo/', headers={'Authorization': 'Bearer ' + access_token}),
            self.post_form('/o/userinfo/', {'access_token': access_token}),
            self.client.get('/o/userinfo/', {'access_token': access_token}),
        ]

        for response in responses:
            with self.subTest(response=response):
                self.assertEqual(response.status_code, 200, response.content)
                self.assertEqual(response.json()['sub'], str(self.user.pk))

    def test_a_logout_with_an_id_token_hint_finds_the_application_of_the_shard(self):
        """
        Case: The user of organization 1 is logged in, and requests the logout endpoint with its ID token as
              id_token_hint.
        Expected: 200 with the confirmation page for the application, which the toolkit finds in the shard that the
                  audience of the hint names.
        """
        self.log_in(1, self.user)

        response = self.client.get('/o/logout/?' + urlencode({'id_token_hint': self.tokens['id_token']}))

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.context['application'], self.application)

    def test_a_logout_without_a_session_reads_the_shard_from_the_id_token_hint(self):
        """
        Case: Without a session, the logout endpoint is requested with the ID token of the user of organization 1 as
              id_token_hint. The toolkit logs a request without a user out at once.
        Expected: A redirect, because the toolkit finds the ID token in the shard that the audience of the hint names.
                  It answers an ID token that it does not find with 400.
        """
        response = self.client.get('/o/logout/?' + urlencode({'id_token_hint': self.tokens['id_token']}))

        self.assertEqual(response.status_code, 302, response.content)

    def test_a_logout_with_a_client_id_finds_the_application_of_the_shard(self):
        """
        Case: The user of organization 1 is logged in, and requests the logout endpoint with the client id of the
              application of organization 1.
        Expected: 200 with the confirmation page for the application.
        """
        self.log_in(1, self.user)

        response = self.client.get('/o/logout/', {'client_id': self.application.client_id})

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.context['application'], self.application)

    def test_a_logout_with_a_client_id_of_an_unknown_organization_is_an_invalid_request(self):
        """
        Case: The logout endpoint is requested with a client id of organization 999, which has no mapping row.
        Expected: 400 invalid_request.
        """
        response = self.client.get('/o/logout/', {'client_id': CLIENT_ID_OF_999})

        self.assertEqual(response.status_code, 400, response.content)
        self.assertEqual(response.json()['error'], 'invalid_request')

    def test_a_logout_with_the_hint_of_another_organization_than_the_session_is_refused(self):
        """
        Case: A user of organization 2 is logged in, and requests the logout endpoint with the ID token of organization
              1 as id_token_hint.
        Expected: 400 invalid_request, because the hint names another shard than the session.
        """
        self.log_in(2, self.create_user(2))

        response = self.client.get('/o/logout/?' + urlencode({'id_token_hint': self.tokens['id_token']}))

        self.assertEqual(response.status_code, 400, response.content)
        self.assertEqual(response.json()['error'], 'invalid_request')

    def test_the_discovery_documents_need_no_shard(self):
        """
        Case: The OpenID Connect discovery document and the JWKS document are requested without a session.
        Expected: 200 for each.
        """
        self.assertEqual(self.client.get('/o/.well-known/openid-configuration').status_code, 200)
        self.assertEqual(self.client.get('/o/.well-known/jwks.json').status_code, 200)

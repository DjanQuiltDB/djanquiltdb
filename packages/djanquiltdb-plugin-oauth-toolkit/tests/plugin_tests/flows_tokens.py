from django.conf import settings
from django.test import override_settings
from djanquiltdb.utils import use_shard, use_shard_for
from oauth2_provider.models import get_access_token_model, get_application_model

from djanquiltdb_plugin_oauth_toolkit.identifiers import parse_client_id, parse_token
from plugin_tests.base import CLIENT_SECRET, REDIRECT_URI, PluginTestCase, basic_authorization, unverified_claims


class ClientCredentialsTestCase(PluginTestCase):
    def test_a_client_with_basic_authentication_gets_a_token_of_its_shard(self):
        """
        Case: An application of organization 1 requests a token with the client credentials grant and Basic
              authentication, and then requests the protected resource with it.
        Expected: The token parses to '1', it is stored in the shard of organization 1 only, and the resource runs in
                  that shard.
        """
        application = self.create_application(1)

        access_token = self.request_client_credentials_token(application)['access_token']

        self.assertEqual(parse_token(access_token), '1')

        with use_shard(self.first):
            self.assertEqual(get_access_token_model().objects.count(), 1)

        with use_shard(self.remote):
            self.assertEqual(get_access_token_model().objects.count(), 0)

        response = self.request_protected_resource(access_token)

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()['schema'], 'first_schema')

    def test_a_client_with_credentials_in_the_body_gets_a_token_of_its_shard(self):
        """
        Case: An application of organization 2, on the other node, requests a token with its client id and secret in
              the form body.
        Expected: The token parses to '2'.
        """
        application = self.create_application(2)

        response = self.post_form(
            '/o/token/',
            {'grant_type': 'client_credentials', 'client_id': application.client_id, 'client_secret': CLIENT_SECRET},
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(parse_token(response.json()['access_token']), '2')

    @override_settings(OAUTH2_PROVIDER={**settings.OAUTH2_PROVIDER, 'COMPLIANT_BCP_RFC9700_TOKEN_STORAGE': True})
    def test_a_token_that_is_stored_as_a_checksum_only_is_found(self):
        """
        Case: The toolkit stores only the checksum of a token, as COMPLIANT_BCP_RFC9700_TOKEN_STORAGE asks, and the
              protected resource is requested with a token of organization 1.
        Expected: The resource runs in the shard of organization 1, because the shard selector is read from the token
                  in the request.
        """
        application = self.create_application(1)
        access_token = self.request_client_credentials_token(application)['access_token']

        with use_shard(self.first):
            self.assertEqual(get_access_token_model().objects.get().token, '')

        self.assertEqual(self.request_protected_resource(access_token).json()['schema'], 'first_schema')


class AuthorizationCodeTestCase(PluginTestCase):
    def test_a_user_authorizes_a_client_of_its_organization(self):
        """
        Case: A user of organization 1 logs in, allows an application of organization 1 on the authorization page, and
              the application exchanges the code for tokens with PKCE.
        Expected: The access and refresh tokens parse to '1', the audience of the ID token is the client id, and the
                  resource runs in the shard of organization 1 for the user.
        """
        application = self.create_authorization_code_application(1)
        user = self.create_user(1)

        tokens = self.request_authorization_code_tokens(1, application, user)

        self.assertEqual(parse_token(tokens['access_token']), '1')
        self.assertEqual(parse_token(tokens['refresh_token']), '1')
        self.assertEqual(unverified_claims(tokens['id_token'])['aud'], application.client_id)

        self.forget_session()
        response = self.request_protected_resource(tokens['access_token'])

        self.assertEqual(response.json(), {'node': 'default', 'schema': 'first_schema', 'resource_owner': user.email})

    def test_an_application_that_skips_authorization_redirects_at_once(self):
        """
        Case: A user of organization 2 opens the authorization page of an application of organization 2 that skips
              the authorization.
        Expected: A redirect to the client with a code.
        """
        application = self.create_application(
            2, authorization_grant_type='authorization-code', redirect_uris=REDIRECT_URI, skip_authorization=True
        )
        self.log_in(2, self.create_user(2))

        response = self.client.get(
            '/o/authorize/',
            {
                'client_id': application.client_id,
                'response_type': 'code',
                'redirect_uri': REDIRECT_URI,
                'code_challenge': 'a' * 43,
                'code_challenge_method': 'S256',
            },
        )

        self.assertEqual(response.status_code, 302, response.content)
        self.assertIn('code=', response['Location'])


class RefreshRevokeAndIntrospectTestCase(PluginTestCase):
    def setUp(self):
        super().setUp()
        self.application = self.create_authorization_code_application(1)
        self.tokens = self.request_authorization_code_tokens(1, self.application, self.create_user(1))
        self.forget_session()

    def test_a_refresh_gives_new_tokens_of_the_shard(self):
        """
        Case: The application of organization 1 refreshes its tokens.
        Expected: A new access token and a new refresh token, which parse to '1'.
        """
        response = self.post_form(
            '/o/token/',
            {'grant_type': 'refresh_token', 'refresh_token': self.tokens['refresh_token']},
            Authorization=basic_authorization(self.application.client_id),
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertNotEqual(response.json()['access_token'], self.tokens['access_token'])
        self.assertEqual(parse_token(response.json()['refresh_token']), '1')

    def test_a_revoked_token_is_refused(self):
        """
        Case: The application of organization 1 revokes its access token, and then requests the protected resource
              with it.
        Expected: 200 for the revocation, and 403 for the resource, which is the toolkit's answer to an unknown token.
        """
        response = self.post_form(
            '/o/revoke_token/',
            {'token': self.tokens['access_token']},
            Authorization=basic_authorization(self.application.client_id),
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(self.request_protected_resource(self.tokens['access_token']).status_code, 403)

    def test_a_client_introspects_its_token(self):
        """
        Case: The application of organization 1 introspects its access token with Basic authentication.
        Expected: The token is active, and belongs to the application.
        """
        response = self.post_form(
            '/o/introspect/',
            {'token': self.tokens['access_token']},
            Authorization=basic_authorization(self.application.client_id),
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()['active'], True)
        self.assertEqual(response.json()['client_id'], self.application.client_id)

    def test_a_token_with_the_introspection_scope_introspects_a_token(self):
        """
        Case: A client of organization 1 has a token with the introspection scope, and introspects the access token of
              the application with that token as a Bearer.
        Expected: The token is active.
        """
        introspecting_client = self.create_application(1)
        bearer = self.request_client_credentials_token(introspecting_client, scope='introspection')['access_token']

        response = self.post_form(
            '/o/introspect/', {'token': self.tokens['access_token']}, Authorization='Bearer ' + bearer
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()['active'], True)


class OtherOrganizationTokenTestCase(PluginTestCase):
    """
    A client of organization 1 sends a token of organization 2 to an endpoint that acts on the token. The middleware
    does not read the shard selector from that token, so the endpoint looks it up in the shard of the client, where it
    does not exist.
    """

    def setUp(self):
        super().setUp()
        self.application = self.create_authorization_code_application(1)
        other_application = self.create_authorization_code_application(2)
        self.other_tokens = self.request_authorization_code_tokens(2, other_application, self.create_user(2))
        self.forget_session()

    def test_the_token_of_another_organization_is_not_active(self):
        """
        Case: The application of organization 1 introspects an access token of organization 2.
        Expected: 200 with active false, as RFC 7662 asks for a token that the caller does not know.
        """
        response = self.post_form(
            '/o/introspect/',
            {'token': self.other_tokens['access_token']},
            Authorization=basic_authorization(self.application.client_id),
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json(), {'active': False})

    def test_the_refresh_token_of_another_organization_is_an_invalid_grant(self):
        """
        Case: The application of organization 1 refreshes with a refresh token of organization 2.
        Expected: 400 invalid_grant.
        """
        response = self.post_form(
            '/o/token/',
            {'grant_type': 'refresh_token', 'refresh_token': self.other_tokens['refresh_token']},
            Authorization=basic_authorization(self.application.client_id),
        )

        self.assertEqual(response.status_code, 400, response.content)
        self.assertEqual(response.json()['error'], 'invalid_grant')

    def test_revoking_the_token_of_another_organization_leaves_it_valid(self):
        """
        Case: The application of organization 1 revokes an access token of organization 2.
        Expected: 200, as RFC 7009 asks for a token that the caller does not know, and the token still gives access to
                  the resource in the shard of organization 2.
        """
        response = self.post_form(
            '/o/revoke_token/',
            {'token': self.other_tokens['access_token']},
            Authorization=basic_authorization(self.application.client_id),
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(
            self.request_protected_resource(self.other_tokens['access_token']).json()['schema'], 'remote_schema'
        )


class ApplicationRegistrationTestCase(PluginTestCase):
    def test_a_user_registers_an_application_in_the_shard_of_the_session(self):
        """
        Case: A user of organization 1 opens the registration page of the toolkit, and registers an application with
              the client id that the page generates.
        Expected: The page generates a client id of organization 1, and the application is stored in the shard of
                  organization 1.
        """
        self.log_in(1, self.create_user(1))

        response = self.client.get('/o/applications/register/')
        generated_client_id = response.context['form']['client_id'].value()

        self.assertEqual(parse_client_id(generated_client_id), '1')

        response = self.post_form(
            '/o/applications/register/',
            {
                'name': 'Registered',
                'client_id': generated_client_id,
                'client_secret': CLIENT_SECRET,
                'hash_client_secret': 'on',
                'client_type': 'confidential',
                'authorization_grant_type': 'client-credentials',
                'algorithm': '',
            },
        )

        self.assertEqual(response.status_code, 302, response.content)

        with use_shard_for(1):
            self.assertTrue(get_application_model().objects.filter(client_id=generated_client_id).exists())
